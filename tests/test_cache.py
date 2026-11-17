from __future__ import annotations

import asyncio

import fakeredis
import httpx
import pytest
import pytest_asyncio
from redis.exceptions import ConnectionError as RedisConnectionError

from app.cache import ServiceError, TagService
from app.config import Settings


class FakeOrigin:
    """In-process stand-in mirroring origin/main.py's contract."""

    def __init__(self) -> None:
        self.tags: dict[str, dict] = {}
        self.revision = 0
        self.delay = 0.0
        self.calls = 0
        self.fail_next = 0
        self.publish_delay = 0.0
        self.publish_fail_next = 0
        self.concurrent = 0
        self.max_concurrent = 0
        self._lock = asyncio.Lock()

    def seed(self, tag: str, digest: str) -> dict:
        self.revision += 1
        self.tags[tag] = {"digest": digest, "revision": self.revision}
        return self.tags[tag]

    async def get(self, url: str, timeout=None):
        return await self.request("GET", url, timeout=timeout)

    async def post(self, url: str, json=None, timeout=None):
        return await self.request("POST", url, payload=json or {}, timeout=timeout)

    async def request(self, method: str, url: str, payload=None, timeout=None):
        if method == "GET":
            self.calls += 1
            self.concurrent += 1
            self.max_concurrent = max(self.max_concurrent, self.concurrent)
            try:
                if timeout is not None and self.delay > timeout:
                    await asyncio.sleep(timeout)
                    raise httpx.ReadTimeout("simulated origin timeout")
                await asyncio.sleep(self.delay)
            finally:
                self.concurrent -= 1
            tag = url.rsplit("/", 1)[-1]
            if self.fail_next > 0:
                self.fail_next -= 1
                return httpx.Response(503, json={"code": "origin_unavailable"})
            entry = self.tags.get(tag)
            if entry is None:
                return httpx.Response(404, json={"code": "tag_not_found"})
            return httpx.Response(200, json={"tag": tag, **entry})
        if self.publish_fail_next > 0:
            self.publish_fail_next -= 1
            return httpx.Response(503, json={"code": "origin_publish_unavailable"})
        await asyncio.sleep(self.publish_delay)
        entry = self.seed(payload["tag"], payload["digest"])
        return httpx.Response(200, json={"tag": payload["tag"], **entry})

    async def get_control(self, url: str, timeout=None):
        return httpx.Response(200, json={"request_count": self.calls})

    async def post_control(self, url: str, json=None, timeout=None):
        return httpx.Response(200, json={"request_count": self.calls})

    async def aclose(self):
        return None


def make_settings(**overrides) -> Settings:
    base = {
        "redis_url": "redis://localhost:6379/0",
        "origin_url": "http://origin",
        "origin_timeout_ms": 1000,
        "cache_wait_timeout_ms": 3000,
        "lock_ttl_ms": 5000,
        "publish_timeout_ms": 2000,
        "redis_socket_timeout_ms": 1000,
        "pending_ttl_ms": 5000,
        "unconfirmed_ttl_s": 600,
        "revalidate_ttl_s": 300,
        "fetch_state_ttl_s": 30,
        "publish_record_ttl_s": 1000,
        "instance_name": "test",
        "public_instances": ("http://a", "http://b"),
    }
    base.update(overrides)
    return Settings(**base)


@pytest_asyncio.fixture
async def env():
    server = fakeredis.FakeServer()
    redis = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    origin = FakeOrigin()
    settings = make_settings()
    services = [TagService(redis, origin, settings) for _ in range(2)]
    services[1].s = make_settings(instance_name="test-2")
    yield redis, origin, services
    await redis.aclose()


@pytest.mark.asyncio
async def test_concurrent_misses_hit_origin_once(env):
    _redis, origin, (a, b) = env
    origin.seed("v1", "sha256:1111")
    results = await asyncio.gather(
        *[a.get_tag("v1") for _ in range(5)],
        *[b.get_tag("v1") for _ in range(5)],
    )
    assert {r["digest"] for r in results} == {"sha256:1111"}
    assert origin.calls == 1
    leaders = [r for r in results if r["single_flight_role"] == "leader"]
    followers = [r for r in results if r["single_flight_role"] == "follower"]
    assert len(leaders) == 1
    assert len(followers) == 9
    assert all(f["digest"] == "sha256:1111" for f in followers)


@pytest.mark.asyncio
async def test_second_round_is_cache_hit(env):
    _redis, origin, (a, b) = env
    origin.seed("v1", "sha256:1111")
    await asyncio.gather(a.get_tag("v1"), b.get_tag("v1"))
    calls = origin.calls
    results = await asyncio.gather(*[a.get_tag("v1"), b.get_tag("v1")])
    assert origin.calls == calls
    assert all(r["cache_hit"] for r in results)


@pytest.mark.asyncio
async def test_old_origin_response_cannot_overwrite_new_publish(env):
    redis, origin, (a, b) = env
    origin.seed("v1", "sha256:old")
    origin.delay = 0.25
    # Instance A begins a fetch of the old digest.
    fetch = asyncio.create_task(a.get_tag("v1"))
    await asyncio.sleep(0.05)
    origin.delay = 0
    # A publish confirms while that fetch is still in flight.
    published = await b.publish("v1", "sha256:new", "req-1")
    assert published["status"] == "confirmed"
    result = await fetch
    assert result["digest"] == "sha256:new"
    assert result.get("fenced_by_publish") is True
    # Queries after confirmation see the new digest.
    for service in (a, b):
        got = await service.get_tag("v1")
        assert got["digest"] == "sha256:new"
        assert got["cache_hit"] is True
    state = await redis.hgetall("artifact:v1:state")
    assert state["digest"] == "sha256:new"
    assert int(state["gen"]) == published["generation"]


@pytest.mark.asyncio
async def test_publish_retry_cannot_roll_back_newer(env):
    redis, origin, (a, b) = env
    origin.seed("v1", "sha256:base")
    first = await a.publish("v1", "sha256:one", "req-one")
    second = await b.publish("v1", "sha256:two", "req-two")
    assert second["generation"] > first["generation"]
    retry = await a.publish("v1", "sha256:one", "req-one")
    assert retry["status"] == "superseded"
    assert retry["applied"] is False
    state = await redis.hgetall("artifact:v1:state")
    assert state["digest"] == "sha256:two"
    got = await a.get_tag("v1")
    assert got["digest"] == "sha256:two"


@pytest.mark.asyncio
async def test_publish_unconfirmed_marker_does_not_pass_old_off_as_new(env):
    _redis, origin, (a, b) = env
    origin.seed("v1", "sha256:old")
    await a.get_tag("v1")
    origin.publish_fail_next = 1
    with pytest.raises(ServiceError) as excinfo:
        await a.publish("v1", "sha256:new", "req-fail")
    assert excinfo.value.code == "origin_publish_failed"
    got = await b.get_tag("v1")
    assert got["digest"] == "sha256:old"
    assert got["publication"]["status"] in {"confirmed", "cached"}
    assert got["stale_due_to_pending"] is False
    status = await b.status("v1")
    assert status["publication"]["status"] in {"confirmed", "cached"}


@pytest.mark.asyncio
async def test_publish_survives_cache_commit_failure(env):
    redis, origin, (a, b) = env
    origin.seed("v1", "sha256:old")
    await a.get_tag("v1")
    # Simulate the commit failure precisely: the first eval whose script is
    # the publication commit fails, everything else runs normally.
    real_eval = redis.eval
    broke = {"done": False}

    async def eval_hook(script, *args, **kwargs):
        if not broke["done"] and "if attempt <= cur_gen" in script:
            broke["done"] = True
            raise RedisConnectionError("simulated commit failure")
        return await real_eval(script, *args, **kwargs)

    redis.eval = eval_hook
    try:
        with pytest.raises(ServiceError) as excinfo:
            await a.publish("v1", "sha256:new", "req-crash")
        assert excinfo.value.code == "cache_unavailable"
        assert broke["done"] is True
    finally:
        redis.eval = real_eval

    marker = await redis.hgetall("artifact:v1:pub_unconfirmed")
    assert marker.get("digest") == "sha256:new"
    # The old digest is still served, but explicitly flagged as not the new
    # publication result.
    got = await b.get_tag("v1")
    assert got["digest"] == "sha256:old"
    assert got["publication"]["status"] == "publishing"
    assert got["stale_due_to_pending"] is True
    assert got["digest_is_pending_target"] is False
    assert got["result_kind"] == "last_confirmed_while_publishing"

    # Retrying with the same request_id resumes and confirms the new digest.
    retry = await b.publish("v1", "sha256:new", "req-crash")
    assert retry["status"] == "confirmed"
    got = await b.get_tag("v1")
    assert got["digest"] == "sha256:new"
    assert got["publication"]["status"] == "confirmed"


@pytest.mark.asyncio
async def test_origin_timeout_is_explicit_and_waiters_can_retry(env):
    _redis, origin, (a, b) = env
    origin.seed("v1", "sha256:1111")
    origin.delay = 0.5
    a.s = make_settings(origin_timeout_ms=100, cache_wait_timeout_ms=3000)

    async def slow_one():
        return await a.get_tag("v1")

    async def waiter():
        await asyncio.sleep(0.02)
        return await b.get_tag("v1")

    results = await asyncio.gather(slow_one(), waiter(), return_exceptions=True)
    errors = [r for r in results if isinstance(r, ServiceError)]
    assert errors, results
    assert all(e.code == "origin_timeout" for e in errors)
    assert all(e.retryable for e in errors)
    assert origin.calls == 1

    origin.delay = 0
    a.s = make_settings(origin_timeout_ms=2000, cache_wait_timeout_ms=3000)
    got = await a.get_tag("v1")
    assert got["digest"] == "sha256:1111"
    assert origin.calls == 2


@pytest.mark.asyncio
async def test_origin_error_propagates_to_waiters(env):
    _redis, origin, (a, b) = env
    origin.seed("v1", "sha256:1111")
    origin.delay = 0.1
    origin.fail_next = 1

    async def waiter():
        await asyncio.sleep(0.02)
        return await b.get_tag("v1")

    results = await asyncio.gather(a.get_tag("v1"), waiter(), return_exceptions=True)
    errors = [r for r in results if isinstance(r, ServiceError)]
    assert len(errors) == 2
    assert {e.code for e in errors} == {"origin_error"}
    assert origin.calls == 1
    got = await b.get_tag("v1")
    assert got["digest"] == "sha256:1111"
    assert origin.calls == 2


@pytest.mark.asyncio
async def test_redis_failure_is_explicit(env):
    redis, origin, (a, _b) = env

    class BrokenPipeline:
        def hgetall(self, *args, **kwargs):
            return self

        def get(self, *args, **kwargs):
            return self

        def exists(self, *args, **kwargs):
            return self

        async def execute(self):
            raise RedisConnectionError("simulated redis outage")

    original_pipeline = redis.pipeline

    def broken_pipeline(*args, **kwargs):
        return BrokenPipeline()

    redis.pipeline = broken_pipeline
    try:
        with pytest.raises(ServiceError) as excinfo:
            await a.get_tag("v1")
    finally:
        redis.pipeline = original_pipeline
    assert excinfo.value.code == "cache_unavailable"
    assert excinfo.value.retryable is True
    assert excinfo.value.status_code == 503
    assert origin.calls == 0


@pytest.mark.asyncio
async def test_publish_does_not_block_query_results(env):
    _redis, origin, (a, b) = env
    origin.seed("v1", "sha256:old")
    await a.get_tag("v1")
    origin.publish_delay = 0.3
    publishing = asyncio.create_task(a.publish("v1", "sha256:new", "req-slow"))
    await asyncio.sleep(0.05)
    during = await b.get_tag("v1")
    assert during["digest"] == "sha256:old"
    assert during["publication"]["status"] == "publishing"
    assert during["stale_due_to_pending"] is True
    done = await publishing
    assert done["status"] == "confirmed"
    after = await b.get_tag("v1")
    assert after["digest"] == "sha256:new"
    assert after["stale_due_to_pending"] is False


@pytest.mark.asyncio
async def test_concurrent_publishes_serialised_and_monotonic(env):
    redis, origin, (a, b) = env
    origin.seed("v1", "sha256:base")
    await a.get_tag("v1")
    first, second = await asyncio.gather(
        a.publish("v1", "sha256:one", "r1"),
        b.publish("v1", "sha256:two", "r2"),
    )
    winner = max((first, second), key=lambda r: r["generation"])
    state = await redis.hgetall("artifact:v1:state")
    assert state["digest"] == winner["digest"]
    assert int(state["gen"]) == winner["generation"]
    got = await a.get_tag("v1")
    assert got["digest"] == winner["digest"]


@pytest.mark.asyncio
async def test_waiter_after_leader_lock_expiry_still_single_origin(env):
    redis, origin, (a, b) = env
    origin.seed("v1", "sha256:1111")
    # Simulate a leader that vanished: lock held by a dead token with a short
    # TTL, and no result.
    await redis.set("artifact:v1:lock", "dead", px=250)
    a.s = make_settings(cache_wait_timeout_ms=3000, lock_ttl_ms=5000)
    b.s = make_settings(cache_wait_timeout_ms=3000, lock_ttl_ms=5000, instance_name="b")
    results = await asyncio.gather(a.get_tag("v1"), b.get_tag("v1"))
    assert {r["digest"] for r in results} == {"sha256:1111"}
    assert origin.calls == 1


@pytest.mark.asyncio
async def test_waiter_prefers_fresh_publish_over_older_round_error(env):
    """A publish confirmed after the waiter's miss must win over a stale error.

    Regression guard: a fetch that failed in an earlier round leaves a
    ``fetch_error`` with a result sequence.  If a publication is confirmed
    afterwards, waiters must return the confirmed digest rather than replay
    that error.
    """
    redis, origin, (a, b) = env
    origin.seed("v1", "sha256:old")
    # An older round's error, later in sequence than the cached state it left.
    await redis.hset(
        "artifact:v1:state",
        mapping={
            "digest": "sha256:old",
            "gen": "0",
            "origin_revision": "1",
            "cached_at": str(0),
            "fresh_until": str(0),
            "source": "origin",
            "fetch_seq": "1",
            "result_seq": "1",
        },
    )
    await redis.set("artifact:v1:result_seq", 2)
    await redis.hset(
        "artifact:v1:fetch_error",
        mapping={
            "seq": "2",
            "result_seq": "2",
            "code": "origin_timeout",
            "message": "old round failed",
            "status_code": "504",
            "retryable": "1",
            "at": "0",
        },
    )
    # Publication confirms at sequence 3.
    published = await a.publish("v1", "sha256:new", "req-waiter")

    result = await b._wait_for_result(
        "v1", baseline_result_seq=1, deadline=asyncio.get_running_loop().time() + 1
    )
    assert result is not None
    assert result["digest"] == "sha256:new"
    assert result["generation"] == published["generation"]


@pytest.mark.asyncio
async def test_tag_and_digest_validation():
    from app.cache import TagService

    for good in ["v1.0.0", "ns/app/v1", "app:tag", "a_b-c.d/e", "x" * 200]:
        TagService.validate_tag(good)
    for bad in ["", " v1", "v1 ", "ns//app", "/abs", "a$b", "a?b", "x" * 201, "a b"]:
        with pytest.raises(ServiceError):
            TagService.validate_tag(bad)
    for good in ["sha256:abcd", "sha256:new2222", "a.b_c-d+e/f=@g"]:
        TagService.validate_digest(good)
    for bad in ["", "sha 256:abcd", "x" * 201, "sha256:ab cd"]:
        with pytest.raises(ServiceError):
            TagService.validate_digest(bad)


@pytest.mark.asyncio
async def test_origin_restart_lower_revision_does_not_wedge(env):
    """A restarted origin restarts its revision counter at 1.

    Revisions are informational; fencing uses publication generations and the
    fetch token, so a lower revision must still be cacheable and publishable.
    """
    redis, origin, (a, b) = env
    origin.seed("v1", "sha256:one")
    origin.seed("v1", "sha256:two")  # revision 2, cached
    await a.get_tag("v1")
    # Origin restarts: revision counter goes back to 1.
    origin.revision = 0
    origin.tags["v1"] = {"digest": "sha256:three", "revision": 1}
    await redis.hset("artifact:v1:state", "fresh_until", "0")
    got = await a.get_tag("v1")
    assert got["digest"] == "sha256:three"
    pub = await b.publish("v1", "sha256:four", "req-restart")
    assert pub["status"] == "confirmed"
    final = await a.get_tag("v1")
    assert final["digest"] == "sha256:four"


@pytest.mark.asyncio
async def test_publish_timeout_keeps_unconfirmed_marker(env):
    """A publish that times out at the origin has an unknown outcome.

    The old digest must not be presented as the result of that publication, so
    the unconfirmed marker has to survive the timeout path (unlike a definite
    5xx refusal, which clears it).
    """
    redis, origin, (a, b) = env
    origin.seed("v1", "sha256:old")
    await a.get_tag("v1")
    origin.publish_delay = 0.5
    a.s = make_settings(publish_timeout_ms=200)

    async def slow_post(url, json=None, timeout=None):
        raise httpx.ReadTimeout("simulated publish timeout")

    original = origin.post
    origin.post = slow_post
    try:
        with pytest.raises(ServiceError) as excinfo:
            await a.publish("v1", "sha256:new", "req-timeout")
    finally:
        origin.post = original
    assert excinfo.value.code == "origin_publish_timeout"
    marker = await redis.hgetall("artifact:v1:pub_unconfirmed")
    assert marker.get("digest") == "sha256:new"
    got = await b.get_tag("v1")
    assert got["digest"] == "sha256:old"
    assert got["stale_due_to_pending"] is True
    assert got["publication"]["status"] == "publishing"

    # Retrying the same request_id completes the publication.
    origin.publish_delay = 0
    retry = await b.publish("v1", "sha256:new", "req-timeout")
    assert retry["status"] == "confirmed"
    got = await b.get_tag("v1")
    assert got["digest"] == "sha256:new"
    assert got["stale_due_to_pending"] is False


@pytest.mark.asyncio
async def test_request_id_reuse_with_different_digest_rejected(env):
    redis, origin, (a, _b) = env
    origin.seed("v1", "sha256:base")
    await a.publish("v1", "sha256:one", "same-id")
    # Drop the idempotency record to exercise the started-record path.
    await redis.delete("artifact:publish_request:v1:same-id")
    with pytest.raises(ServiceError) as excinfo:
        await a.publish("v1", "sha256:other", "same-id")
    assert excinfo.value.code == "request_id_reused"
    got = await a.get_tag("v1")
    assert got["digest"] == "sha256:one"
