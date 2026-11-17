"""Cross-instance single-flight cache for artifact tag digests.

Redis keys (all prefixed with ``artifact:``)::

    {tag}:state         hash   cached digest + fencing metadata
    {tag}:fetch_seq     counter monotonic sequence for cache writes
    {tag}:lock          single-flight leader lock (UUID token, TTL)
    {tag}:fetch_error   hash   last failed origin fetch (for waiters)
    {tag}:pending       hash   publication currently unconfirmed
    {tag}:last_publish  hash   latest confirmed publication
    {tag}:pub_seq       counter monotonic publication attempt ids
    {tag}:pub_lock      serialises publications for one tag

Correctness rules:

* A leader captures the confirmed publication generation *before* calling the
  origin.  Its cache write is rejected atomically if that generation moved, so
  an old origin response landing after a publish confirmation can never
  overwrite the new digest.
* Cache writes are also fenced by a monotonically increasing ``fetch_seq`` and
  by the origin revision, so overlapping fetches cannot regress the cache.
* Publication commits are monotonic and guarded by a per-request record.  A
  retried publication (same ``request_id``) never re-sends an older digest
  after a newer publication has been confirmed, and never rolls the cache back.
* A concurrent round of cache misses elects exactly one leader via the Redis
  lock; followers wait for the leader's state or error instead of calling the
  origin themselves.
* ``result_seq`` is a single Redis counter shared by state writes and error
  records.  A waiter only accepts a result (or replays an error) whose
  ``result_seq`` is newer than the value it captured with its own miss, so a
  previous round's error is never replayed into a later round, and a fresh
  published state always beats a stale error.
* ``pub_unconfirmed`` marks a publication whose outcome is unknown (origin
  accepted it, gateway died before the commit, or the commit failed).  While
  it exists, readers still get the last confirmed digest but the response is
  explicitly labelled ``stale_due_to_pending`` rather than presented as the
  publication's result.  It expires after ``UNCONFIRMED_TTL_S``.
"""
from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .config import Settings

TAG_RE = re.compile(r"^(?!.*//)[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$")
DIGEST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:+/=@-]{0,199}$")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]+$")

STATE_TTL_MS = 7 * 24 * 3600 * 1000
POLL_INTERVAL_S = 0.04


def _now_ms() -> int:
    return int(time.time() * 1000)


def _iso(ms: int | str | None) -> str | None:
    if ms in (None, ""):
        return None
    try:
        value = int(ms)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat(
        timespec="milliseconds"
    )


def _key(tag: str, suffix: str) -> str:
    return f"artifact:{tag}:{suffix}"


class ServiceError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        status_code: int = 400,
        retryable: bool = False,
        extra: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.retryable = retryable
        self.extra = extra or {}


# KEYS[1]=state, 2=result_seq, 3=unconfirmed publication marker
# ARGV: 1 token, 2 expected generation, 3 digest, 4 origin revision,
#       5 now_ms, 6 fresh_until_ms, 7 source, 8 state ttl ms
_WRITE_STATE_LUA = """
local cur = redis.call('HMGET', KEYS[1], 'gen', 'fetch_seq')
local cur_gen = tonumber(cur[1] or '0')
local cur_seq = tonumber(cur[2] or '0')
local expected_gen = tonumber(ARGV[2])
local token = tonumber(ARGV[1])
-- Fence on the publication generation captured *before* the origin call, and
-- on the monotonic fetch token.  The origin revision is stored for display
-- only: it is not a fence, because the demo origin's revision counter restarts
-- at 1 and a restart must not wedge cache writes forever.
if expected_gen ~= cur_gen then return {-1, cur_gen} end
if token < cur_seq then return {-4, cur_gen} end
local result_seq = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1],
  'digest', ARGV[3],
  'origin_revision', ARGV[4],
  'gen', ARGV[2],
  'cached_at', ARGV[5],
  'fresh_until', ARGV[6],
  'source', ARGV[7],
  'fetch_seq', ARGV[1],
  'result_seq', tostring(result_seq),
  'fetch_state', 'ok')
redis.call('PEXPIRE', KEYS[1], ARGV[8])
if redis.call('HGET', KEYS[3], 'digest') == ARGV[3] then
  redis.call('DEL', KEYS[3])
end
return {'1', result_seq}
"""

# KEYS[1]=state, 2=pending, 3=fetch_seq, 4=last_publish, 5=result_seq,
#      6=unconfirmed publication marker
# ARGV: 1 attempt, 2 attempt(for pending match), 3 digest, 4 origin revision,
#       5 now_ms, 6 fresh_until_ms, 7 request_id, 8 state ttl ms,
#       9 last_publish ttl seconds
_COMMIT_PUBLISH_LUA = """
local cur = redis.call('HMGET', KEYS[1], 'gen')
local cur_gen = tonumber(cur[1] or '0')
local attempt = tonumber(ARGV[1])
-- Publication generations are monotonic, so a delayed commit for an older
-- publication can never overwrite a newer one.  Origin revisions are not used
-- as a fence: the origin's counter may restart after a restart.
if attempt <= cur_gen then return {-1, cur_gen} end
local pending_attempt = redis.call('HGET', KEYS[2], 'attempt_id')
if pending_attempt and pending_attempt ~= ARGV[2] then return {-2, cur_gen} end
local seq = redis.call('INCR', KEYS[3])
local result_seq = redis.call('INCR', KEYS[5])
redis.call('HSET', KEYS[1],
  'digest', ARGV[3],
  'origin_revision', ARGV[4],
  'gen', ARGV[1],
  'cached_at', ARGV[5],
  'fresh_until', ARGV[6],
  'source', 'publish',
  'fetch_seq', tostring(seq),
  'result_seq', tostring(result_seq),
  'fetch_state', 'ok')
redis.call('PEXPIRE', KEYS[1], ARGV[8])
redis.call('HSET', KEYS[4],
  'digest', ARGV[3],
  'gen', ARGV[1],
  'origin_revision', ARGV[4],
  'published_at', ARGV[5],
  'request_id', ARGV[7],
  'attempt_id', ARGV[2])
redis.call('EXPIRE', KEYS[4], ARGV[9])
if pending_attempt == ARGV[2] then redis.call('DEL', KEYS[2]) end
redis.call('DEL', KEYS[6])
return {1, attempt, seq, result_seq}
"""

# KEYS[1]=fetch_error, 2=result_seq
# ARGV: 1 fetch token, 2 code, 3 message, 4 status, 5 retryable, 6 now_ms,
#       7 ttl ms, 8 origin status ('' when absent)
_RECORD_ERROR_LUA = """
local result_seq = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1],
  'seq', ARGV[1],
  'result_seq', tostring(result_seq),
  'code', ARGV[2],
  'message', ARGV[3],
  'status_code', ARGV[4],
  'retryable', ARGV[5],
  'at', ARGV[6],
  'origin_status', ARGV[8])
redis.call('PEXPIRE', KEYS[1], ARGV[7])
return result_seq
"""

_RELEASE_LOCK_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

_DELETE_PENDING_IF_MATCH_LUA = """
if redis.call('HGET', KEYS[1], 'attempt_id') == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class TagService:
    def __init__(self, redis: Redis, origin: httpx.AsyncClient, settings: Settings) -> None:
        self.redis = redis
        self.origin = origin
        self.s = settings

    # ---------------------------------------------------------------- basics
    async def _stat(self, name: str) -> None:
        try:
            await self.redis.incr(f"stats:gateway:{self.s.instance_name}:{name}")
        except RedisError:
            pass

    @staticmethod
    def validate_tag(tag: str) -> None:
        if not TAG_RE.match(tag or ""):
            raise ServiceError(
                "invalid_tag",
                "标签只能包含字母、数字和 . _ : / -，长度 1-200，且以字母或数字开头",
                422,
            )

    @staticmethod
    def validate_digest(digest: str) -> None:
        if not DIGEST_RE.match(digest or ""):
            raise ServiceError(
                "invalid_digest",
                "摘要格式无效（允许字母、数字和 . _ : + / = @ -，长度 1-200）",
                422,
            )

    async def _snapshot(self, tag: str) -> dict[str, Any]:
        pipe = self.redis.pipeline()
        pipe.hgetall(_key(tag, "state"))
        pipe.hgetall(_key(tag, "pending"))
        pipe.hgetall(_key(tag, "last_publish"))
        state, pending, last = await pipe.execute()
        return {"state": state or None, "pending": pending or None, "last_publish": last or None}

    async def _miss_snapshot(self, tag: str) -> dict[str, Any]:
        """Atomically read everything needed to decide a miss.

        The fetch sequence counter and the last-seen error are captured in the
        same transaction as the state, so a waiter can tell stale errors from
        ones produced by the round it is about to wait on.
        """
        pipe = self.redis.pipeline()
        pipe.hgetall(_key(tag, "state"))
        pipe.get(_key(tag, "result_seq"))
        pipe.hgetall(_key(tag, "pending"))
        pipe.hgetall(_key(tag, "last_publish"))
        pipe.hgetall(_key(tag, "pub_unconfirmed"))
        state, result_seq, pending, last, unconfirmed = await pipe.execute()
        return {
            "state": state or None,
            "result_seq": int(result_seq or 0),
            "pending": pending or None,
            "last_publish": last or None,
            "unconfirmed": unconfirmed or None,
        }

    async def _snapshot_checked(
        self, tag: str, context: str = "读取状态"
    ) -> dict[str, Any]:
        """Snapshot for response construction; always includes result_seq.

        ``fetch_seq`` may not exist in the state hash until the first write, so
        a miss on a cold tag still gets a stable numeric baseline (0).
        """
        try:
            snap = await self._miss_snapshot(tag)
        except RedisError as exc:
            raise ServiceError(
                "cache_unavailable",
                f"缓存协调失败，无法{context}（{exc.__class__.__name__}），请重试",
                503,
                retryable=True,
            ) from exc
        snap.pop("result_seq", None)
        return snap

    async def _release_lock(self, key: str, token: str) -> None:
        try:
            await self.redis.eval(_RELEASE_LOCK_LUA, 1, key, token)
        except RedisError:
            pass

    # -------------------------------------------------------------- responses
    def _publication(
        self,
        state: dict[str, str] | None,
        pending: dict[str, str] | None,
        last: dict[str, str] | None,
        unconfirmed: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        # ``unconfirmed`` outlives the short-lived pending marker when a publish
        # succeeded at the origin but the gateway crashed before committing it.
        if pending or unconfirmed:
            status = "publishing"
        elif last:
            status = "confirmed"
        elif state:
            status = "cached"
        else:
            status = "unknown"
        pub: dict[str, Any] = {
            "status": status,
            "confirmed_digest": (state or {}).get("digest"),
            "confirmed_generation": int((state or {}).get("gen") or 0),
            "pending": None,
            "last_publish": None,
        }
        in_flight = pending or unconfirmed
        if in_flight:
            pub["pending"] = {
                "digest": in_flight.get("digest"),
                "target_generation": int(in_flight.get("attempt_id") or 0),
                "started_at": _iso(in_flight.get("started_at")),
                "request_id": in_flight.get("request_id"),
                "confirmed": False,
                "unconfirmed_marker": bool(unconfirmed and not pending),
            }
        if last:
            pub["last_publish"] = {
                "digest": last.get("digest"),
                "generation": int(last.get("gen") or 0),
                "origin_revision": int(last.get("origin_revision") or 0),
                "published_at": _iso(last.get("published_at")),
                "request_id": last.get("request_id"),
            }
        return pub

    def _response(
        self,
        tag: str,
        snap: dict[str, Any],
        *,
        cache_hit: bool,
        role: str,
        coalesced: bool = False,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        state = snap["state"]
        assert state is not None
        pending = snap["pending"] or snap.get("unconfirmed")
        publication = self._publication(
            state, snap["pending"], snap["last_publish"], snap.get("unconfirmed")
        )
        body: dict[str, Any] = {
            "tag": tag,
            "digest": state["digest"],
            "confirmed": True,
            "generation": int(state.get("gen") or 0),
            "origin_revision": int(state.get("origin_revision") or 0),
            "source": state.get("source", "origin"),
            "cached_at": _iso(state.get("cached_at")),
            "fresh_until": _iso(state.get("fresh_until")),
            "cache_hit": cache_hit,
            "single_flight_role": role,
            # True only if this specific request actually called the origin.
            # A coalesced leader that found another instance's fresh result
            # reports False even though its role is "leader".
            "origin_called": role == "leader" and not coalesced,
            "instance": self.s.instance_name,
            "publication": publication,
            # "last_confirmed_while_publishing" means a newer publication is
            # in flight and this digest is the last confirmed one, not its
            # result.
            "result_kind": (
                "last_confirmed_while_publishing"
                if pending and pending.get("digest") != state.get("digest")
                else "confirmed_current"
            ),
            # True means the returned digest is NOT the target of a pending
            # publication; it is the last confirmed value.
            "digest_is_pending_target": bool(
                pending and pending.get("digest") == state.get("digest")
            ),
            "stale_due_to_pending": bool(
                pending and pending.get("digest") != state.get("digest")
            ),
        }
        if extra:
            body.update(extra)
        return body

    # ------------------------------------------------------------------ query
    async def get_tag(self, tag: str, refresh: bool = False) -> dict[str, Any]:
        self.validate_tag(tag)
        try:
            snap = await self._miss_snapshot(tag)
        except RedisError as exc:
            raise ServiceError(
                "cache_unavailable",
                f"缓存协调失败，无法读取标签 {tag}（{exc.__class__.__name__}），请重试",
                503,
                retryable=True,
            ) from exc

        state = snap["state"]
        if not refresh and state and int(state.get("fresh_until") or 0) > _now_ms():
            await self._stat("cache_hits")
            return self._response(tag, snap, cache_hit=True, role="cache")

        await self._stat("cache_misses")
        return await self._fill(
            tag,
            baseline_result_seq=snap["result_seq"],
            force=refresh,
        )

    async def _fill(
        self,
        tag: str,
        baseline_result_seq: int,
        force: bool = False,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + self.s.cache_wait_timeout_ms / 1000
        lock_key = _key(tag, "lock")
        while True:
            lock_token = uuid.uuid4().hex
            try:
                acquired = bool(
                    await self.redis.set(
                        lock_key,
                        lock_token,
                        nx=True,
                        px=self.s.lock_ttl_ms,
                    )
                )
            except RedisError as exc:
                raise ServiceError(
                    "cache_unavailable",
                    f"缓存协调失败，无法获得回源锁（{exc.__class__.__name__}），请重试",
                    503,
                    retryable=True,
                ) from exc

            if acquired:
                return await self._lead_fetch(tag, lock_token, force=force)

            await self._stat("waits")
            result = await self._wait_for_result(
                tag,
                baseline_result_seq=baseline_result_seq,
                deadline=deadline,
            )
            if result is not None:
                return result

            if time.monotonic() >= deadline:
                raise ServiceError(
                    "cache_wait_timeout",
                    "等待其他实例回源超时，未取得结果，请重试",
                    504,
                    retryable=True,
                )
            # Leader disappeared without a usable result: try to become the
            # leader ourselves.  Refresh the baseline so a result left by the
            # vanished round is not replayed to this caller.
            try:
                current_seq = await self.redis.get(_key(tag, "result_seq"))
                baseline_result_seq = max(baseline_result_seq, int(current_seq or 0))
            except (RedisError, ValueError):
                pass
            await asyncio.sleep(0.02)

    async def _lead_fetch(
        self,
        tag: str,
        lock_token: str,
        force: bool = False,
    ) -> dict[str, Any]:
        write_token = 0
        try:
            # Under the lock, re-check whether another instance already filled
            # the cache between our miss and lock acquisition.  Without this a
            # waiter whose leader just released the lock could trigger a second
            # origin call for the same round.  A forced refresh deliberately
            # skips this check.
            try:
                state_before = await self.redis.hgetall(_key(tag, "state"))
                if (
                    not force
                    and state_before
                    and int(state_before.get("fresh_until") or 0) > _now_ms()
                ):
                    snap = await self._snapshot_checked(tag, "读取并发回源结果")
                    return self._response(
                        tag,
                        snap,
                        cache_hit=False,
                        role="leader",
                        coalesced=True,
                        extra={"coalesced": True},
                    )
                # Fence captured before the origin call.
                expected_gen = int((state_before or {}).get("gen") or 0)
                write_token = int(await self.redis.incr(_key(tag, "fetch_seq")))
            except RedisError as exc:
                raise ServiceError(
                    "cache_unavailable",
                    f"缓存协调失败，无法准备回源（{exc.__class__.__name__}），请重试",
                    503,
                    retryable=True,
                ) from exc

            await self._stat("origin_fetches")
            try:
                response = await self.origin.get(
                    f"/tags/{tag}", timeout=self.s.origin_timeout_ms / 1000
                )
            except (TimeoutError, httpx.TimeoutException) as exc:
                await self._stat("origin_errors")
                message = f"源站查询 {tag} 超时（>{self.s.origin_timeout_ms}ms），请重试"
                await self._record_fetch_error(tag, write_token, "origin_timeout", message, 504, True)
                raise ServiceError("origin_timeout", message, 504, retryable=True) from exc
            except httpx.HTTPError as exc:
                await self._stat("origin_errors")
                message = f"无法连接源站（{exc.__class__.__name__}），请重试"
                await self._record_fetch_error(tag, write_token, "origin_unreachable", message, 502, True)
                raise ServiceError("origin_unreachable", message, 502, retryable=True) from exc

            if response.status_code == 404:
                await self._stat("origin_errors")
                message = f"源站中不存在标签 {tag}"
                await self._record_fetch_error(
                    tag, write_token, "tag_not_found", message, 404, False, 404
                )
                raise ServiceError("tag_not_found", message, 404, retryable=False)
            if response.status_code >= 400:
                await self._stat("origin_errors")
                message = f"源站返回 HTTP {response.status_code}，请重试"
                await self._record_fetch_error(
                    tag, write_token, "origin_error", message, 502, True, response.status_code
                )
                raise ServiceError(
                    "origin_error",
                    message,
                    502,
                    retryable=True,
                    extra={"origin_status": response.status_code},
                )

            try:
                payload = response.json()
                digest = str(payload["digest"])
                revision = int(payload.get("revision") or 0)
                if not digest:
                    raise ValueError("empty digest")
            except (ValueError, KeyError, TypeError) as exc:
                await self._stat("origin_errors")
                message = "源站响应格式无效，请重试"
                await self._record_fetch_error(
                    tag, write_token, "origin_bad_response", message, 502, True
                )
                raise ServiceError(
                    "origin_bad_response", message, 502, retryable=True
                ) from exc

            now = _now_ms()
            try:
                written = await self.redis.eval(
                    _WRITE_STATE_LUA,
                    3,
                    _key(tag, "state"),
                    _key(tag, "result_seq"),
                    _key(tag, "pub_unconfirmed"),
                    str(write_token),
                    str(expected_gen),
                    digest,
                    str(revision),
                    str(now),
                    str(now + self.s.revalidate_ttl_s * 1000),
                    "origin",
                    str(STATE_TTL_MS),
                )
            except RedisError as exc:
                raise ServiceError(
                    "cache_unavailable",
                    f"缓存协调失败，无法写入回源结果（{exc.__class__.__name__}），请重试",
                    503,
                    retryable=True,
                ) from exc

            written_status = int(written[0]) if isinstance(written, (list, tuple)) else int(written)
            if written_status == -1:
                # A publication was confirmed while the origin response was in
                # flight.  Return its value, never our older fetch.
                snap = await self._snapshot_checked(tag, "读取发布后的状态")
                if snap["state"] and int(snap["state"].get("fresh_until") or 0) > _now_ms():
                    return self._response(
                        tag,
                        snap,
                        cache_hit=False,
                        role="leader",
                        extra={"fenced_by_publish": True},
                    )
                raise ServiceError(
                    "cache_write_rejected",
                    "回源期间发生发布，旧摘要被拒绝写入，请重试",
                    503,
                    retryable=True,
                )
            if written_status == -4:
                snap = await self._snapshot_checked(tag, "读取并发回源结果")
                if snap["state"] and int(snap["state"].get("fresh_until") or 0) > _now_ms():
                    return self._response(
                        tag,
                        snap,
                        cache_hit=False,
                        role="leader",
                        coalesced=True,
                        extra={"coalesced": True},
                    )
                raise ServiceError(
                    "cache_coordination_retry",
                    "回源结果被并发写入取代且无可用缓存，请重试",
                    503,
                    retryable=True,
                )

            snap = await self._snapshot_checked(tag, "读取回源结果")
            if not snap["state"]:
                raise ServiceError(
                    "cache_coordination_retry",
                    "回源结果未能写入缓存，请重试",
                    503,
                    retryable=True,
                )
            return self._response(tag, snap, cache_hit=False, role="leader")
        finally:
            await self._release_lock(_key(tag, "lock"), lock_token)

    async def _record_fetch_error(
        self,
        tag: str,
        token: int,
        code: str,
        message: str,
        status_code: int,
        retryable: bool,
        origin_status: int | None = None,
    ) -> None:
        try:
            await self.redis.eval(
                _RECORD_ERROR_LUA,
                2,
                _key(tag, "fetch_error"),
                _key(tag, "result_seq"),
                str(token),
                code,
                message,
                str(status_code),
                "1" if retryable else "0",
                str(_now_ms()),
                str(self.s.fetch_state_ttl_s * 1000),
                str(origin_status) if origin_status is not None else "",
            )
        except RedisError:
            # Waiters will time out and retry; the leader still returns the
            # original, explicit error.
            pass

    async def _wait_for_result(
        self, tag: str, baseline_result_seq: int, deadline: float
    ) -> dict[str, Any] | None:
        while time.monotonic() < deadline:
            try:
                pipe = self.redis.pipeline()
                pipe.hgetall(_key(tag, "state"))
                pipe.get(_key(tag, "result_seq"))
                pipe.hgetall(_key(tag, "fetch_error"))
                pipe.exists(_key(tag, "lock"))
                state, _result_seq, error, lock_exists = await pipe.execute()
            except RedisError as exc:
                raise ServiceError(
                    "cache_unavailable",
                    f"缓存协调失败，等待回源时无法读取状态（{exc.__class__.__name__}），请重试",
                    503,
                    retryable=True,
                ) from exc

            state_result_seq = int((state or {}).get("result_seq") or 0)
            error_result_seq = int((error or {}).get("result_seq") or 0)
            fresh = bool(state) and int((state or {}).get("fresh_until") or 0) > _now_ms()

            # A successful write (origin or publish) is identified by a new
            # state result sequence.  A fresh confirmed state is always a valid
            # answer for this waiter -- in particular a publication confirmed
            # after our miss must win over an older round's fetch error.
            if fresh and state_result_seq > baseline_result_seq:
                snap = await self._snapshot_checked(tag, "读取回源结果")
                return self._response(tag, snap, cache_hit=False, role="follower")
            if error and error_result_seq > baseline_result_seq and error_result_seq > state_result_seq:
                raise ServiceError(
                    error.get("code", "origin_error"),
                    error.get("message", "源站查询失败，请重试"),
                    int(error.get("status_code") or 502),
                    retryable=error.get("retryable") == "1",
                    extra=(
                        {"origin_status": int(error["origin_status"])}
                        if error.get("origin_status")
                        else None
                    ),
                )
            if not lock_exists:
                # The leader released the lock.  One more read distinguishes
                # "wrote a result we can use" from "failed with an error we
                # already returned" from "vanished without any result".
                pipe = self.redis.pipeline()
                pipe.hgetall(_key(tag, "state"))
                pipe.get(_key(tag, "result_seq"))
                pipe.hgetall(_key(tag, "fetch_error"))
                state, _result_seq, error = await pipe.execute()
                state_result_seq = int((state or {}).get("result_seq") or 0)
                error_result_seq = int((error or {}).get("result_seq") or 0)
                fresh = bool(state) and int((state or {}).get("fresh_until") or 0) > _now_ms()
                if fresh and state_result_seq > baseline_result_seq:
                    snap = await self._snapshot_checked(tag, "读取回源结果")
                    return self._response(tag, snap, cache_hit=False, role="follower")
                if error and error_result_seq > baseline_result_seq and error_result_seq > state_result_seq:
                    raise ServiceError(
                        error.get("code", "origin_error"),
                        error.get("message", "源站查询失败，请重试"),
                        int(error.get("status_code") or 502),
                        retryable=error.get("retryable") == "1",
                        extra=(
                            {"origin_status": int(error["origin_status"])}
                            if error.get("origin_status")
                            else None
                        ),
                    )
                return None
            await asyncio.sleep(POLL_INTERVAL_S)
        return None

    # ---------------------------------------------------------------- publish
    async def publish(
        self, tag: str, digest: str, request_id: str | None
    ) -> dict[str, Any]:
        self.validate_tag(tag)
        self.validate_digest(digest)
        request_id = (request_id or "").strip() or uuid.uuid4().hex
        if len(request_id) > 120 or not REQUEST_ID_RE.match(request_id):
            raise ServiceError(
                "invalid_request_id",
                "request_id 只能包含字母、数字和 . _ : -，长度不超过 120",
                422,
            )

        idem_key = f"artifact:publish_request:{tag}:{request_id}"
        started_key = f"artifact:publish_started:{tag}:{request_id}"

        try:
            cached = await self.redis.get(idem_key)
        except RedisError as exc:
            raise ServiceError(
                "cache_unavailable",
                f"缓存协调失败，无法读取发布记录（{exc.__class__.__name__}），请重试",
                503,
                retryable=True,
            ) from exc
        if cached:
            return await self._deduplicated_result(tag, cached)

        lock_token = uuid.uuid4().hex
        lock_key = _key(tag, "pub_lock")
        lock_ttl = self.s.publish_timeout_ms + 10_000
        deadline = time.monotonic() + self.s.publish_timeout_ms / 1000
        acquired = False
        while time.monotonic() < deadline:
            try:
                acquired = bool(await self.redis.set(lock_key, lock_token, nx=True, px=lock_ttl))
                if acquired:
                    break
                cached = await self.redis.get(idem_key)
            except RedisError as exc:
                raise ServiceError(
                    "cache_unavailable",
                    f"缓存协调失败，无法获得发布锁（{exc.__class__.__name__}），请重试",
                    503,
                    retryable=True,
                ) from exc
            if cached:
                return await self._deduplicated_result(tag, cached)
            await asyncio.sleep(POLL_INTERVAL_S)

        if not acquired:
            raise ServiceError(
                "publish_busy",
                "同一标签已有发布正在进行，请稍后重试",
                409,
                retryable=True,
            )

        attempt = 0
        try:
            snap = await self._snapshot_checked(tag, "检查发布前状态")
            state = snap["state"]
            current_digest = (state or {}).get("digest")
            current_gen = int((state or {}).get("gen") or 0)

            try:
                started = await self.redis.hgetall(started_key)
            except RedisError as exc:
                raise ServiceError(
                    "cache_unavailable",
                    f"缓存协调失败，无法读取发布尝试记录（{exc.__class__.__name__}），请重试",
                    503,
                    retryable=True,
                ) from exc

            if started:
                if started.get("digest") != digest:
                    raise ServiceError(
                        "request_id_reused",
                        "该 request_id 曾用于发布其他摘要，请使用新的 request_id",
                        409,
                        retryable=False,
                    )
                started_attempt = int(started.get("attempt_id") or 0)
                resumed = True
                if current_digest == digest and current_gen >= started_attempt:
                    return await self._already_applied(
                        tag, digest, request_id, started_attempt, current_gen
                    )
                if current_gen > started_attempt and current_digest != digest:
                    return await self._superseded(
                        tag, digest, request_id, started_attempt, current_gen, current_digest
                    )
            else:
                resumed = False

            try:
                attempt = int(await self.redis.incr(_key(tag, "pub_seq")))
                await self.redis.hset(
                    _key(tag, "pending"),
                    mapping={
                        "attempt_id": str(attempt),
                        "digest": digest,
                        "request_id": request_id,
                        "started_at": str(_now_ms()),
                    },
                )
                await self.redis.pexpire(_key(tag, "pending"), self.s.pending_ttl_ms)
                await self.redis.hset(
                    started_key,
                    mapping={
                        "attempt_id": str(attempt),
                        "digest": digest,
                        "request_id": request_id,
                        "started_at": str(_now_ms()),
                    },
                )
                await self.redis.expire(started_key, self.s.publish_record_ttl_s)
                # Long-lived "publication not confirmed" marker.  If this
                # instance dies after the origin accepted the publish but
                # before the cache commit, readers must not present the old
                # digest as the result of that publication.
                await self.redis.hset(
                    _key(tag, "pub_unconfirmed"),
                    mapping={
                        "attempt_id": str(attempt),
                        "digest": digest,
                        "request_id": request_id,
                        "started_at": str(_now_ms()),
                    },
                )
                await self.redis.expire(
                    _key(tag, "pub_unconfirmed"), self.s.unconfirmed_ttl_s
                )
            except RedisError as exc:
                raise ServiceError(
                    "cache_unavailable",
                    f"缓存协调失败，无法登记发布状态（{exc.__class__.__name__}），请重试",
                    503,
                    retryable=True,
                ) from exc

            try:
                response = await self.origin.post(
                    "/publish",
                    json={"tag": tag, "digest": digest},
                    timeout=self.s.publish_timeout_ms / 1000,
                )
            except (TimeoutError, httpx.TimeoutException) as exc:
                # Unknown outcome at the origin: keep the unconfirmed marker
                # so the old digest is not presented as this publication.
                await self._clear_pending(tag, attempt)
                raise ServiceError(
                    "origin_publish_timeout",
                    f"源站发布确认超时（>{self.s.publish_timeout_ms}ms），发布未确认，请用同一 request_id 重试",
                    504,
                    retryable=True,
                ) from exc
            except httpx.HTTPError as exc:
                await self._clear_publish_state(tag, attempt, clear_unconfirmed=True)
                raise ServiceError(
                    "origin_publish_failed",
                    f"无法连接源站（{exc.__class__.__name__}），发布未确认，请重试",
                    502,
                    retryable=True,
                ) from exc

            if response.status_code >= 400:
                await self._clear_publish_state(tag, attempt, clear_unconfirmed=True)
                raise ServiceError(
                    "origin_publish_failed",
                    f"源站发布失败（HTTP {response.status_code}），发布未确认，请重试",
                    502,
                    retryable=True,
                    extra={"origin_status": response.status_code},
                )
            try:
                revision = int((response.json() or {}).get("revision") or 0)
            except (ValueError, TypeError):
                await self._clear_publish_state(tag, attempt, clear_unconfirmed=True)
                raise ServiceError(
                    "origin_bad_response",
                    "源站发布响应格式无效，发布未确认，请重试",
                    502,
                    retryable=True,
                )

            now = _now_ms()
            try:
                result = await self.redis.eval(
                    _COMMIT_PUBLISH_LUA,
                    6,
                    _key(tag, "state"),
                    _key(tag, "pending"),
                    _key(tag, "fetch_seq"),
                    _key(tag, "last_publish"),
                    _key(tag, "result_seq"),
                    _key(tag, "pub_unconfirmed"),
                    str(attempt),
                    str(attempt),
                    digest,
                    str(revision),
                    str(now),
                    str(now + self.s.revalidate_ttl_s * 1000),
                    request_id,
                    str(STATE_TTL_MS),
                    str(self.s.publish_record_ttl_s),
                )
            except RedisError as exc:
                # The origin accepted the publish; the cache commit is what is
                # unconfirmed.  Keep the marker so readers know the old cached
                # digest is not this publication's result.
                await self._clear_pending(tag, attempt)
                raise ServiceError(
                    "cache_unavailable",
                    f"源站已更新但缓存确认失败（{exc.__class__.__name__}），发布未确认，"
                    "请用同一 request_id 重试",
                    503,
                    retryable=True,
                ) from exc

            status = int(result[0])
            if status == -1:
                await self._clear_publish_state(tag, attempt, clear_unconfirmed=True)
                after = await self._snapshot_checked(tag, "读取当前摘要")
                raise ServiceError(
                    "publish_stale",
                    "较新的发布已确认，本次发布未回退它，请刷新状态",
                    409,
                    retryable=False,
                    extra={
                        "current_digest": (after["state"] or {}).get("digest"),
                        "current_generation": int((after["state"] or {}).get("gen") or 0),
                    },
                )
            if status == -2:
                await self._clear_publish_state(tag, attempt, clear_unconfirmed=True)
                raise ServiceError(
                    "publish_conflict",
                    "检测到更新的发布在途，本次发布未写入，请重试",
                    409,
                    retryable=True,
                )
            record = {
                "tag": tag,
                "digest": digest,
                "generation": int(result[1]),
                "origin_revision": revision,
                "request_id": request_id,
                "status": "confirmed",
                "applied": True,
                "deduplicated": False,
                "resumed": resumed,
                "instance": self.s.instance_name,
                "published_at": _iso(now),
            }
            try:
                await self.redis.set(
                    idem_key, json.dumps(record), ex=self.s.publish_record_ttl_s
                )
            except RedisError:
                record["warning"] = "发布已确认，但幂等重试记录写入失败；重试时请先查询状态"
            return record
        finally:
            await self._release_lock(lock_key, lock_token)

    async def _already_applied(
        self,
        tag: str,
        digest: str,
        request_id: str,
        attempt: int,
        current_gen: int,
    ) -> dict[str, Any]:
        snap = await self._snapshot_checked(tag, "读取发布结果")
        record = {
            "tag": tag,
            "digest": digest,
            "generation": int((snap["state"] or {}).get("gen") or 0),
            "origin_revision": int((snap["state"] or {}).get("origin_revision") or 0),
            "request_id": request_id,
            "status": "already_applied",
            "applied": True,
            "deduplicated": True,
            "resumed": True,
            "attempt_id": attempt,
            "previous_generation": current_gen,
            "instance": self.s.instance_name,
            "published_at": _iso((snap["state"] or {}).get("cached_at")),
        }
        try:
            await self.redis.set(
                f"artifact:publish_request:{tag}:{request_id}",
                json.dumps(record),
                ex=self.s.publish_record_ttl_s,
            )
        except RedisError:
            pass
        return record

    async def _superseded(
        self,
        tag: str,
        digest: str,
        request_id: str,
        attempt: int,
        current_gen: int,
        current_digest: str | None,
    ) -> dict[str, Any]:
        snap = await self._snapshot_checked(tag, "读取当前摘要")
        record = {
            "tag": tag,
            "digest": digest,
            "generation": int((snap["state"] or {}).get("gen") or 0),
            "request_id": request_id,
            "status": "superseded",
            "applied": False,
            "superseded": True,
            "deduplicated": True,
            "resumed": True,
            "attempt_id": attempt,
            "current_digest": current_digest,
            "current_generation": current_gen,
            "message": "较新的发布已确认；本次重试未回退较新摘要",
            "instance": self.s.instance_name,
        }
        try:
            await self.redis.set(
                f"artifact:publish_request:{tag}:{request_id}",
                json.dumps(record),
                ex=self.s.publish_record_ttl_s,
            )
        except RedisError:
            pass
        return record

    async def _deduplicated_result(self, tag: str, raw: str) -> dict[str, Any]:
        try:
            record = json.loads(raw)
        except ValueError:
            record = {"tag": tag, "digest": None}
        snap = await self._snapshot_checked(tag, "读取当前摘要")
        current_digest = (snap["state"] or {}).get("digest")
        applied = current_digest == record.get("digest")
        return {
            **record,
            "tag": tag,
            "status": "already_applied" if applied else "superseded",
            "applied": applied,
            "superseded": not applied,
            "deduplicated": True,
            "current_digest": current_digest,
            "current_generation": int((snap["state"] or {}).get("gen") or 0),
            "instance": self.s.instance_name,
        }

    async def _clear_pending(self, tag: str, attempt: int) -> None:
        try:
            await self.redis.eval(
                _DELETE_PENDING_IF_MATCH_LUA, 1, _key(tag, "pending"), str(attempt)
            )
        except RedisError:
            pass

    async def _clear_publish_state(
        self, tag: str, attempt: int, clear_unconfirmed: bool = False
    ) -> None:
        await self._clear_pending(tag, attempt)
        if not clear_unconfirmed:
            return
        try:
            await self.redis.eval(
                _DELETE_PENDING_IF_MATCH_LUA,
                1,
                _key(tag, "pub_unconfirmed"),
                str(attempt),
            )
        except RedisError:
            pass

    async def invalidate(self, tag: str) -> dict[str, Any]:
        """Expire freshness without touching the fence or the origin.

        The published generation and origin revision are preserved, so this
        cannot let an older publication or an older origin revision win.  The
        next query becomes a cache miss and revalidates through the single
        flight path.
        """
        self.validate_tag(tag)
        try:
            state = await self.redis.hgetall(_key(tag, "state"))
            if state:
                await self.redis.hset(_key(tag, "state"), "fresh_until", "0")
        except RedisError as exc:
            raise ServiceError(
                "cache_unavailable",
                f"缓存协调失败，无法清除标签 {tag} 的新鲜度（{exc.__class__.__name__}），请重试",
                503,
                retryable=True,
            ) from exc
        return {
            "tag": tag,
            "invalidated": bool(state),
            "previous_digest": (state or {}).get("digest"),
            "generation": int((state or {}).get("gen") or 0),
            "instance": self.s.instance_name,
        }

    # ----------------------------------------------------------------- status
    async def status(self, tag: str) -> dict[str, Any]:
        self.validate_tag(tag)
        snap = await self._snapshot_checked(tag, "读取发布状态")
        state = snap["state"]
        return {
            "tag": tag,
            "instance": self.s.instance_name,
            "publication": self._publication(state, snap["pending"], snap["last_publish"]),
            "cached_digest": (state or {}).get("digest"),
            "generation": int((state or {}).get("gen") or 0),
            "origin_revision": int((state or {}).get("origin_revision") or 0),
            "cached_at": _iso((state or {}).get("cached_at")),
            "fresh_until": _iso((state or {}).get("fresh_until")),
        }

    # ------------------------------------------------------------------ stats
    async def gateway_stats(self) -> dict[str, Any]:
        names = ["origin_fetches", "origin_errors", "cache_hits", "cache_misses", "waits"]
        try:
            values = await self.redis.mget(
                [f"stats:gateway:{self.s.instance_name}:{n}" for n in names]
            )
        except RedisError as exc:
            raise ServiceError(
                "cache_unavailable",
                f"缓存协调失败，无法读取统计（{exc.__class__.__name__}）",
                503,
                retryable=True,
            ) from exc
        counts = {name: int(value or 0) for name, value in zip(names, values)}
        counts["instance"] = self.s.instance_name
        return counts

    async def origin_stats(self) -> dict[str, Any]:
        try:
            response = await self.origin.get(
                "/control", timeout=min(2.0, self.s.origin_timeout_ms / 1000)
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            return {
                "reachable": False,
                "error": exc.__class__.__name__,
                "instance": self.s.instance_name,
            }
        payload["reachable"] = True
        payload["instance"] = self.s.instance_name
        return payload

    async def origin_control(self, body: dict[str, Any]) -> dict[str, Any]:
        try:
            response = await self.origin.post(
                "/control", json=body, timeout=min(2.0, self.s.origin_timeout_ms / 1000)
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ServiceError(
                "origin_control_failed",
                f"源站控制请求失败（{exc.__class__.__name__}）",
                502,
                retryable=True,
            ) from exc
        payload["reachable"] = True
        payload["instance"] = self.s.instance_name
        return payload
