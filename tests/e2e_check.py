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

    # ------------------------------------------------- 10. event subscription
    ev_tag = tag("e2e-events")
    await reset(origin, ev_tag, "sha256:seed")
    first_pub = (
        await q1.post(
            "/api/publish",
            json={"tag": ev_tag, "digest": "sha256:ev1", "request_id": request_id("ev-1")},
        )
    ).json()
    newest = (await q2.get(f"/api/events/{ev_tag}")).json()
    check(newest["started_from"] == "latest", "首次订阅从最新游标开始")
    check(newest["events"] == [], "首次订阅不重放历史事件")
    check(newest["confirmation"]["digest"] == "sha256:ev1", "首次订阅显示当前已确认摘要")
    check(newest["confirmation"]["generation"] == first_pub["generation"], "首次订阅显示发布代号")
    check(newest["last_cursor"] == first_pub["event_cursor"], "首次订阅给出续接游标")

    # A subscriber on query-1 waiting with cursor 0 must receive the event
    # published through query-2, promptly and with the right digest/cursor.
    # The long poll returns as soon as at least one event exists (low latency);
    # the client re-polls with the new cursor, which is what the page does.
    async def publish_from_q2():
        await asyncio.sleep(0.3)
        return await q2.post(
            "/api/publish",
            json={"tag": ev_tag, "digest": "sha256:ev2", "request_id": request_id("ev-2")},
        )

    async def drain_from(q, tag_name, cursor, target_digest, timeout_s=8.0):
        seen = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while loop.time() < deadline:
            body = (
                await q.get(
                    f"/api/events/{tag_name}?cursor={cursor}&wait_ms=4000"
                )
            ).json()
            seen.extend(body["events"])
            cursor = max([cursor, *[e["cursor"] for e in body["events"]]])
            if any(e["digest"] == target_digest for e in seen):
                return body, seen, cursor
            if body.get("cursor_ahead") or body.get("gap"):
                return body, seen, cursor
        return body, seen, cursor

    waiter, second_pub = await asyncio.gather(
        drain_from(q1, ev_tag, 0, "sha256:ev2"),
        publish_from_q2(),
    )
    latest_poll, received, reconnect_cursor = waiter
    second_body = second_pub.json()
    cursors = [e["cursor"] for e in received]
    check(second_pub.status_code == 200, "跨实例发布成功")
    check(cursors == [first_pub["event_cursor"], second_body.get("event_cursor")],
          f"订阅按游标收到两条事件（实际 {cursors}）")
    check([e["digest"] for e in received] == ["sha256:ev1", "sha256:ev2"],
          "事件摘要按发布顺序送达")
    check(latest_poll["confirmation"]["digest"] == "sha256:ev2",
          "事件送达时确认状态已更新为新摘要")
    check(received[-1]["generation"] == second_body["generation"], "事件携带发布代号")
    check(received[-1]["request_id"] == request_id("ev-2"), "事件携带 request_id")
    check(received[-1]["cursor"] == reconnect_cursor, "续接游标指向最后收到的事件")

    # Reconnect with the newest cursor: nothing replayed, no gap.
    caught_up = (await q1.get(f"/api/events/{ev_tag}?cursor={reconnect_cursor}")).json()
    check(caught_up["events"] == [] and caught_up["gap"] is None, "按最新游标重连不重复、无缺口")

    # Idempotent retry must not append a second event.
    retry = await q1.post(
        "/api/publish",
        json={"tag": ev_tag, "digest": "sha256:ev2", "request_id": request_id("ev-2")},
    )
    retry_body = retry.json()
    check(retry_body["status"] == "already_applied", "重复 request_id 返回 already_applied")
    after_retry = (await q2.get(f"/api/events/{ev_tag}?cursor=0")).json()
    check(len(after_retry["events"]) == 2, f"幂等重试不重复通知（实际 {len(after_retry['events'])} 条）")

    # Failed publish (origin refuses) must not produce an event.
    await reset(origin, tag("e2e-ev-fail"), "sha256:keep")
    await q1.get(f"/api/tag/{tag('e2e-ev-fail')}")
    await origin.post("/control", json={"fail_publish_next": 1})
    failed_pub = await q1.post(
        "/api/publish",
        json={"tag": tag("e2e-ev-fail"), "digest": "sha256:lost", "request_id": request_id("ev-fail")},
    )
    check(failed_pub.status_code >= 400, "源站拒绝发布时返回错误")
    failed_events = (await q2.get(f"/api/events/{tag('e2e-ev-fail')}?cursor=0")).json()
    check(failed_events["events"] == [], "发布失败不发送成功事件")
    check(failed_events["last_cursor"] == 0, "发布失败不推进游标")

    # Same for a timeout at the origin (unknown outcome): the publication is
    # not confirmed, so no success event may be delivered.
    await reset(origin, tag("e2e-ev-timeout"), "sha256:keep2")
    await q1.get(f"/api/tag/{tag('e2e-ev-timeout')}")
    await origin.post("/control", json={"publish_delay_ms": 6000})
    timed_out_pub = await q1.post(
        "/api/publish",
        json={
            "tag": tag("e2e-ev-timeout"),
            "digest": "sha256:slowlost",
            "request_id": request_id("ev-timeout"),
        },
    )
    check(timed_out_pub.status_code == 504, f"发布确认超时返回 504（实际 {timed_out_pub.status_code}）")
    timeout_events = (await q2.get(f"/api/events/{tag('e2e-ev-timeout')}?cursor=0")).json()
    check(timeout_events["events"] == [], "发布超时未确认时不发送成功事件")
    timeout_status = (await q2.get(f"/api/status/{tag('e2e-ev-timeout')}")).json()
    check(timeout_status["publication"]["status"] == "publishing",
          "发布超时后状态明确为未确认（publishing）")
    await origin.post("/control", json={"publish_delay_ms": 0})

    # Retention: 105 confirmed publications keep only the newest 100 events.
    ret_tag = tag("e2e-ev-retention")
    await reset(origin, ret_tag, "sha256:seed")
    for index in range(105):
        response = await q1.post(
            "/api/publish",
            json={"tag": ret_tag, "digest": f"sha256:r{index}", "request_id": f"ret-{index}-{run}"},
        )
        if response.status_code != 200:
            check(False, f"第 {index} 次发布用于保留测试时失败: {response.text}")
            break
    retention = (await q2.get(f"/api/events/{ret_tag}?cursor=0")).json()
    check(retention["retained"] == 100, f"每标签仅保留最近 100 条事件（实际 {retention['retained']}）")
    check(len(retention["events"]) == 100, f"读取返回 100 条（实际 {len(retention['events'])}）")
    check(retention["last_cursor"] == 105, f"最新游标为 105（实际 {retention['last_cursor']}）")
    check(retention["oldest_cursor"] == 6, f"最旧保留游标为 6（实际 {retention['oldest_cursor']}）")
    gap_body = retention
    check(gap_body["gap"] is not None and gap_body["gap"]["detected"] is True,
          "游标超出保留范围时明确报告缺口")
    check(gap_body["gap"]["missed_publications"] == 5,
          f"缺口数量明确为 5（实际 {gap_body['gap']['missed_publications']}）")
    check(gap_body["gap"]["oldest_available_cursor"] == 6, "缺口报告最早可用游标")
    check([e["cursor"] for e in gap_body["events"]] == list(range(6, 106)),
          "缺口中只返回仍保留的事件")
    check(gap_body["confirmation"]["digest"] == "sha256:r104", "缺口响应附带当前确认状态（最新摘要）")
    check(gap_body["confirmation"]["digest"] != gap_body["events"][0]["digest"],
          "未把中间发布说成已补齐：缺口与当前确认状态分开呈现")

    # A cursor exactly at the retention boundary has no gap.
    boundary = (await q1.get(f"/api/events/{ret_tag}?cursor=5")).json()
    check(boundary["gap"] is None and len(boundary["events"]) == 100,
          "游标刚好在保留边界时不报缺口")

    # Reconnect mid-stream: events after a saved cursor are the missing tail.
    reconnect = (await q1.get(f"/api/events/{ret_tag}?cursor=102")).json()
    check([e["cursor"] for e in reconnect["events"]] == [103, 104, 105],
          "断线重连按游标补收尾部事件")

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
