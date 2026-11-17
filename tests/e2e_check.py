"""End-to-end interleaving checks against real Redis + two uvicorn instances.

Run with the stack up:
    python3 tests/e2e_check.py --q1 http://127.0.0.1:8101 --q2 http://127.0.0.1:8102
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import uuid

import httpx

FAILS: list[str] = []


def check(cond: bool, message: str) -> None:
    print(("PASS  " if cond else "FAIL  ") + message)
    if not cond:
        FAILS.append(message)


async def reset(origin: httpx.AsyncClient, tag: str, digest: str) -> None:
    await origin.post(
        "/control",
        json={
            "delay_ms": 0,
            "fail_next": 0,
            # null clears the one-shot override; 0 would mean "next request
            # with zero delay", which is not a reset.
            "slow_next_ms": None,
            "publish_delay_ms": 0,
            "fail_publish_next": 0,
        },
    )
    await origin.post("/publish", json={"tag": tag, "digest": digest})
    await origin.post("/control", json={"reset_counters": True})


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--q1", default="http://127.0.0.1:8101")
    parser.add_argument("--q2", default="http://127.0.0.1:8102")
    parser.add_argument("--origin", default="http://127.0.0.1:9100")
    # Kept for parity with the Docker instructions; unused by the checks.
    parser.add_argument("--redis-cli", default="/tmp/e2e/redis-cli")
    parser.add_argument("--redis-port", default="6390")
    args = parser.parse_args()

    # Unique tags per run: Redis state from a previous run must not be able to
    # masquerade as a fresh cache entry for this run.
    run = uuid.uuid4().hex[:8]

    def tag(name: str) -> str:
        return f"{name}-{run}"

    def request_id(name: str) -> str:
        return f"{name}-{run}"

    q1 = httpx.AsyncClient(base_url=args.q1, timeout=30)
    q2 = httpx.AsyncClient(base_url=args.q2, timeout=30)
    origin = httpx.AsyncClient(base_url=args.origin, timeout=30)

    # ---------------------------------------------------------- 1. single flight
    await reset(origin, tag("e2e-single"), "sha256:a1")
    # Invalidate so the round starts from a guaranteed miss without calling the
    # origin ourselves (a forced refresh would legitimately hit the origin).
    invalidated = await q1.post("/api/cache/invalidate", json={"tag": tag("e2e-single")})
    check(invalidated.status_code == 200 and invalidated.json()["invalidated"] is False,
          "失效接口对冷标签不报错")
    await origin.post("/control", json={"delay_ms": 700, "reset_counters": True})
    responses = await asyncio.gather(
        *[q1.get(f"/api/tag/{tag('e2e-single')}") for _ in range(8)],
        *[q2.get(f"/api/tag/{tag('e2e-single')}") for _ in range(8)],
    )
    bodies = [r.json() for r in responses]
    digits = {b.get("digest") for b in bodies}
    stats = (await origin.get("/control")).json()
    leaders = sum(1 for b in bodies if b.get("single_flight_role") == "leader")
    followers = sum(1 for b in bodies if b.get("single_flight_role") == "follower")
    check(all(r.status_code == 200 for r in responses), "16 个并发查询全部成功")
    check(digits == {"sha256:a1"}, f"并发结果一致: {digits}")
    check(stats["request_count"] == 1, f"源站本轮只被调用 1 次（实际 {stats['request_count']}）")
    check(leaders == 1, f"仅 1 个单飞领导者（实际 {leaders}）")
    check(followers == 15, f"其余 15 个为等待者（实际 {followers}）")

    # ------------------------------------------------------------ 2. cache hit
    before = stats["request_count"]
    hits = await asyncio.gather(q1.get(f"/api/tag/{tag('e2e-single')}"), q2.get(f"/api/tag/{tag('e2e-single')}"))
    check(all(r.json().get("cache_hit") for r in hits), "后续查询命中缓存")
    after = (await origin.get("/control")).json()["request_count"]
    check(after == before, "缓存命中不增加源站调用")

    # ------------------------------------- 3. old origin reply cannot overwrite publish
    await reset(origin, tag("e2e-fence"), "sha256:old")
    await q1.get(f"/api/tag/{tag('e2e-fence')}")
    invalidated = await q2.post("/api/cache/invalidate", json={"tag": tag("e2e-fence")})
    check(invalidated.json().get("invalidated") is True, "失效接口确认已有缓存被标记失效")
    await origin.post("/control", json={"delay_ms": 900})
    stale_fetch = asyncio.create_task(q1.get(f"/api/tag/{tag('e2e-fence')}"))
    await asyncio.sleep(0.25)
    published = await q2.post("/api/publish", json={"tag": tag("e2e-fence"), "digest": "sha256:new", "request_id": request_id("e2e-pub-1")})
    pub_body = published.json()
    check(published.status_code == 200 and pub_body.get("status") == "confirmed", f"发布已确认: {pub_body.get('status')}")
    # The stale fetch must still be in flight when confirmation lands, and the
    # confirmation must be visible before we assert on its result.
    check(not stale_fetch.done(), "断言时旧回源仍在途中")
    confirmed = (await q2.get(f"/api/status/{tag('e2e-fence')}")).json()
    check(confirmed["publication"]["status"] == "confirmed", "发布确认已写入共享状态")
    stale_result = (await stale_fetch).json()
    check(stale_result.get("digest") == "sha256:new", f"晚到的旧回源未覆盖新发布，返回 {stale_result}")
    check(stale_result.get("fenced_by_publish") is True, "旧回源被发布代号栅栏拦截")
    post_pub = await asyncio.gather(q1.get(f"/api/tag/{tag('e2e-fence')}"), q2.get(f"/api/tag/{tag('e2e-fence')}"))
    check(all(r.json().get("digest") == "sha256:new" for r in post_pub), "确认后两实例均返回最新摘要")
    check(all(r.json().get("cache_hit") for r in post_pub), "确认后的查询直接命中缓存")

    # ----------------------------------------------- 4. retry publish no rollback
    await reset(origin, tag("e2e-retry"), "sha256:base")
    one = (await q1.post("/api/publish", json={"tag": tag("e2e-retry"), "digest": "sha256:one", "request_id": request_id("e2e-r1")})).json()
    two = (await q2.post("/api/publish", json={"tag": tag("e2e-retry"), "digest": "sha256:two", "request_id": request_id("e2e-r2")})).json()
    retry = (await q1.post("/api/publish", json={"tag": tag("e2e-retry"), "digest": "sha256:one", "request_id": request_id("e2e-r1")})).json()
    current = (await q2.get(f"/api/tag/{tag('e2e-retry')}")).json()
    check(two["generation"] > one["generation"], "两次发布会话代号递增")
    check(retry.get("status") == "superseded" and retry.get("applied") is False, f"重试旧发布返回 superseded（实际 {retry.get('status')}）")
    check(current.get("digest") == "sha256:two", f"重试未回退较新摘要（实际 {current.get('digest')}）")

    # ------------------------------------------ 5. publish unconfirmed is explicit
    await reset(origin, tag("e2e-unconf"), "sha256:keep")
    await q1.get(f"/api/tag/{tag('e2e-unconf')}")
    await origin.post("/control", json={"fail_publish_next": 1})
    failed = await q1.post("/api/publish", json={"tag": tag("e2e-unconf"), "digest": "sha256:lost", "request_id": request_id("e2e-u1")})
    check(failed.status_code >= 400, f"发布失败返回错误（HTTP {failed.status_code}）")
    body = failed.json()
    check(body.get("error", {}).get("retryable") is True, "发布失败标记为可重试")
    during = (await q2.get(f"/api/tag/{tag('e2e-unconf')}")).json()
    check(during.get("digest") == "sha256:keep", "未确认发布不改变已确认摘要")
    status = (await q2.get(f"/api/status/{tag('e2e-unconf')}")).json()
    check(status["publication"]["status"] in {"confirmed", "cached"}, f"发布状态未被标成已确认: {status['publication']['status']}")

    # --------------------------------------------------- 6. origin timeout + retry
    await reset(origin, tag("e2e-timeout"), "sha256:slow")
    await origin.post("/control", json={"delay_ms": 2500})
    timed_out = await q1.get(f"/api/tag/{tag('e2e-timeout')}")
    check(timed_out.status_code == 504, f"源站超时返回 504（实际 {timed_out.status_code}）")
    if timed_out.status_code == 504:
        err = timed_out.json()["error"]
        check(err["code"] == "origin_timeout" and err["retryable"] is True, "超时错误码明确且可重试")
    await origin.post("/control", json={"delay_ms": 0})
    recovered = await q1.get(f"/api/tag/{tag('e2e-timeout')}")
    check(recovered.status_code == 200 and recovered.json()["digest"] == "sha256:slow", "超时后可再次查询成功")

    # ------------------------------------ 7. publish while queries in flight
    await reset(origin, tag("e2e-pending"), "sha256:v1")
    await q1.get(f"/api/tag/{tag('e2e-pending')}")
    await origin.post("/control", json={"publish_delay_ms": 700})
    slow_pub = asyncio.create_task(
        q1.post("/api/publish", json={"tag": tag("e2e-pending"), "digest": "sha256:v2", "request_id": request_id("e2e-p2")})
    )
    await asyncio.sleep(0.1)
    during_pub = (await q2.get(f"/api/tag/{tag('e2e-pending')}")).json()
    check(during_pub.get("digest") == "sha256:v1", "发布未确认时返回最后已确认摘要")
    check(during_pub.get("stale_due_to_pending") is True, "明确标记该摘要不是发布目标")
    pub_done = (await slow_pub).json()
    check(pub_done.get("status") == "confirmed", "发布随后确认")
    final = (await q2.get(f"/api/tag/{tag('e2e-pending')}")).json()
    check(final.get("digest") == "sha256:v2", "确认后返回新摘要")

    # ------------------------------------------------ 8. slash-containing tags
    slash_tag = f"ns/{tag('e2e-slash')}/sub"
    await origin.post("/publish", json={"tag": slash_tag, "digest": "sha256:slash"})
    slash = await q1.get(f"/api/tag/{slash_tag}")
    check(slash.status_code == 200 and slash.json().get("digest") == "sha256:slash",
          f"含 / 的标签可查询（HTTP {slash.status_code}）")
    slash_status = await q2.get(f"/api/status/{slash_tag}")
    check(slash_status.status_code == 200, "含 / 的标签可查询状态")

    # ------------------------------------------------------- 9. invalid inputs
    bad_tag = await q1.get("/api/tag/bad%20tag")
    check(bad_tag.status_code == 422, f"非法标签返回 422（实际 {bad_tag.status_code}）")
    bad_digest = await q1.post("/api/publish", json={"tag": "ok-tag", "digest": "bad digest"})
    check(bad_digest.status_code == 422, f"非法摘要返回 422（实际 {bad_digest.status_code}）")

    await q1.aclose()
    await q2.aclose()
    await origin.aclose()

    print()
    if FAILS:
        print(f"{len(FAILS)} check(s) failed")
        return 1
    print("all end-to-end checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
