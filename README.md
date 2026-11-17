# 制品标签摘要查询（跨实例单飞缓存 + 游标续接的发布事件）

FastAPI + Redis 的制品标签查询服务：两个查询实例共享同一份 Redis 缓存，对同一个标签的一轮并发缓存未命中只会向源站回源一次；发布确认之后开始的查询一定能看到新摘要，晚到的旧回源结果不会覆盖它。

此外提供**发布事件订阅**：网页初次进入即可看到当前已确认摘要，随后及时收到另一个实例确认的新发布（新摘要、发布代号、可续接的游标）。断线/刷新后按保存的游标补收；每个标签只保留最近 100 条事件，游标超出保留范围时明确显示缺口并单独读取当前确认状态，绝不把中间发布说成已补齐。

## 组成

| 服务 | 说明 | 端口 |
| --- | --- | --- |
| `redis` | 缓存与协调状态 | 6379 |
| `origin` | 可设置响应延迟/失败的本地制品源站，自行统计请求数 | 9000 |
| `query-1` / `query-2` | 两个 FastAPI 查询实例（同一镜像） | 8001 / 8002 |

网页由任一查询实例直接提供：<http://localhost:8001/>（或 <http://localhost:8002/>）。

## 启动

```bash
docker compose up --build
```

首次启动后源站预置了 `v1.0.0=sha256:old1111`。

## 网页功能

* **发布事件订阅 · 游标续接**：选择标签（以及订阅经由哪个实例）后点「订阅」。首次进入从当前最新游标开始，只显示**当前已确认摘要**，不重放历史；订阅期间该标签的任何新确认发布（无论由哪个实例发布）都会在数百毫秒内到达，显示新摘要、发布代号与游标。收到的事件逐条累积并推进本地游标（按标签保存在 `localStorage`），刷新页面或点「断开」后重连会自动按游标补收遗漏事件；「清除本地游标」则回到“从最新开始”。发布数超过保留范围时显示**缺口**（缺多少条、最早可用游标），并单独展示当前确认状态。
* **发布标签摘要**：选择经由哪个实例发布，填写标签、新摘要和 `request_id`。发布 = 写源站 + 原子写缓存并递增发布代号 + 在同一次原子提交中登记事件；同一 `request_id` 重试是幂等的（不会重复通知）。
* **查询与并发测试**：「两实例并发查询」会同时向两个实例各发 N 个请求，并汇总请求数、实际回源数、等待者数、缓存命中数与摘要一致性。「仅使缓存失效」只把缓存标记为过期（保留发布代号），用来在不动源站的前提下制造一轮并发未命中——随后并发查询时源站计数应只增加 1。
* **源站模拟**：设置响应延迟、接下来 N 次查询失败。
* **发布状态**：查看已确认摘要、发布代号、源站版本、在途发布、最新事件游标。
* **统计**：源站的 `request_count` 由源站自己统计，是“回源次数”的权威依据；网关侧另有各实例的缓存命中/未命中/等待/回源调用计数。

## 一致性设计

所有协调状态在 Redis 中，键前缀 `artifact:{tag}:`：

| 键 | 用途 |
| --- | --- |
| `state` | 已确认摘要 + `gen`（发布代号）、`origin_revision`、`fetch_seq`、`result_seq`、`fresh_until` |
| `lock` | 跨实例单飞回源锁（UUID 值 + TTL） |
| `fetch_seq` | 回源写缓存的单调序号 |
| `result_seq` | 状态写入与错误记录共享的结果序号，供等待者判断“本轮是否有结果” |
| `fetch_error` | 最近一次失败回源，含结果序号，等待者据此得到同样的明确错误 |
| `pending` | 在途发布（短 TTL） |
| `pub_unconfirmed` | 结果未知的发布标记（源站可能已写入，网关未确认），读者会看到“旧摘要 + 明确标记” |
| `pub_seq` / `pub_lock` | 发布会话代号 / 发布串行锁 |
| `last_publish` | 最近一次已确认发布 |
| `events` | 发布事件 zset，score = 游标，最多保留最近 100 条 |
| `events_last` | 该标签最新事件游标（不设 TTL，保证游标可比） |

关键保证：

1. **单飞**：同轮未命中的请求竞争 Redis 锁，只有 1 个成为 leader 回源，其余等待 leader 写入的 `state` 或 `fetch_error`，不会各自回源。leader 拿到锁后会再检查一次缓存，避免“leader 刚释放锁、等待者补位”导致重复回源。
2. **发布栅栏**：leader 在调用源站**之前**记录当前发布代号；写缓存用 Lua 原子校验代号未变，且 `fetch_seq` 单调。因此旧回源晚到只会被拒绝，不会覆盖新发布的摘要。源站版本号只用于展示，不参与栅栏——演示源站重启后版本号会归零，若拿它当栅栏会让缓存永久写不进去。
3. **确认后可见**：发布提交用 Lua 原子完成“校验代号单调 → 编码并写入事件 → 写 `state` → 写 `last_publish` → 清在途标记 → 递增 `result_seq`”，发布确认后开始的查询直接命中新摘要。事件与确认同一次原子提交：不会出现“已确认但没有事件”或“有事件但未确认”。脚本在改动任何状态**之前**先校验事件键类型，类型不对则整次提交不生效（返回 `event_log_unavailable`，保持未确认标记），因此不会出现 Lua 已写入一部分后才失败的中间态。
4. **未确认不冒充新结果**：源站发布超时/网关提交失败时保留 `pub_unconfirmed`；读者仍能拿到最后已确认摘要，但响应带 `stale_due_to_pending: true`、`result_kind: "last_confirmed_while_publishing"`，发布状态显示 `publishing`。这些情况下**不产生事件**、不推进游标，订阅者收不到成功通知。
5. **重试不回退**：发布代号单调，且 `request_id` 有幂等记录；对旧发布的重试返回 `superseded` 而不是把较新摘要回滚，幂等重放也不会重复追加事件。
6. **明确错误**：源站超时 → `504 origin_timeout`；源站 5xx → `502 origin_error`；Redis 不可用/协调失败 → `503 cache_unavailable`；等待超时 → `504 cache_wait_timeout`；被并发写入拒绝 → `503 cache_write_rejected`；事件日志不可写 → `503 event_log_unavailable`。均可重试（非 404 标签不存在），等待者与 leader 得到同一错误，之后可以再查。

### 发布事件与游标

* **游标**：每个标签一条单调递增的计数（`events_last`），事件写入 zset 时以其为 score。事件内容含 `tag / type / cursor / digest / generation / origin_revision / request_id / published_at`。游标跨实例统一（都在 Redis），因此从实例 A 断开、经实例 B 重连也能对齐。
* **保留**：每次追加后 `ZREMRANGEBYRANK` 只保留最近 100 条（`EVENT_RETENTION`）；事件 zset 有 7 天 TTL，重新写入会续期；游标计数器**不设 TTL**（否则窗口过期后游标可能被重启编号误导为已追平）。
* **首次进入**：`GET /api/events/{tag}` 不带游标时，若该标签还从未建立确认状态，会先走正常查询路径（含单飞/回源）取到当前确认摘要；随后返回 `started_from: "latest"`、`events: []`、`last_cursor` 与确认状态——只展示当前确认摘要，不重放历史。标签不存在时明确 404。
* **续接**：带 `cursor=N` 返回 N 之后仍保留的事件（最多 `limit` 条，`has_more` 表示还有未读），没有新事件且 `wait_ms>0` 时长轮询等待（默认页面用 10s），因此新发布通常在数百毫秒内到达。
* **缺口**：若 `cursor` 早于保留范围（`cursor < oldest-1`，或整个窗口已过期且 `cursor < last`），响应带 `gap: {detected, requested_cursor, oldest_available_cursor, missed_publications, message}`，只返回仍保留的事件，并同时给出**单独读取**的当前确认状态（`confirmation`）。页面把缺口标为「中间发布未补齐」，不把当前状态冒充成缺失事件的逐条回放。
* **游标超前**：若本地游标大于服务端最新事件（例如事件日志被清空），响应 `cursor_ahead: true` 并给出说明，不重放、不静默重置编号；页面上可用「清除本地游标」重新开始。
* **损坏事件**：无法解析的 zset 成员会以 `type: "corrupt_event"` 单独标出（页面标红），不会冒充成一条成功发布。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/tag/{tag}?refresh=1` | 查询摘要（`refresh` 强制绕过新鲜度检查，仍走单飞） |
| POST | `/api/publish` | `{tag, digest, request_id?}`，发布并确认；响应含 `event_cursor` |
| GET | `/api/status/{tag}` | 发布状态、已确认摘要、代号、在途发布、`events_last_cursor` |
| GET | `/api/events/{tag}?cursor=&limit=&wait_ms=` | 游标之后的发布事件；省略 `cursor` 从最新开始并返回当前确认状态；`wait_ms>0` 长轮询 |
| POST | `/api/cache/invalidate` | `{tag}`，只让已有缓存过期，改源站与发布代号 |
| GET | `/api/stats` | 网关计数 + 源站计数 |
| POST | `/api/origin/control` | 设置源站 `delay_ms` / `fail_next` / `reset_counters` 等 |
| GET | `/api/health`, `/api/config` | 健康检查与实例配置 |

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `INSTANCE_NAME` | `query` | 实例名 |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis 地址 |
| `ORIGIN_URL` | `http://localhost:9000` | 源站地址 |
| `PUBLIC_INSTANCES` | `http://localhost:8001,http://localhost:8002` | 网页可直连的实例列表（也用于 CORS） |
| `ORIGIN_TIMEOUT_MS` | `2000` | 源站查询超时 |
| `CACHE_WAIT_TIMEOUT_MS` | `3500` | 等待 leader 结果的上限 |
| `LOCK_TTL_MS` | `12000` | 单飞锁 TTL |
| `PUBLISH_TIMEOUT_MS` | `5000` | 源站发布确认超时 |
| `PENDING_TTL_MS` | `15000` | 在途发布标记 TTL |
| `UNCONFIRMED_TTL_S` | `600` | 结果未知的发布标记 TTL |
| `REVALIDATE_TTL_S` | `300` | 缓存新鲜期 |
| `REDIS_SOCKET_TIMEOUT_MS` | `1500` | Redis 操作超时（超时即报协调失败） |

## 测试

```bash
# 1) 单元/交错测试（fakeredis，无需外部服务）
pip install -r requirements.txt fakeredis pytest pytest-asyncio
python3 -m pytest tests/test_cache.py

# 2a) Docker 栈
docker compose up --build -d
python3 tests/e2e_check.py --q1 http://localhost:8001 --q2 http://localhost:8002 --origin http://localhost:9000 --redis-port 6379
```

没有 Docker 时可用本地栈（需要本机 `redis-server`、`uvicorn`）：

```bash
scripts/dev_stack.sh     # 起 origin + query-1 + query-2（8101/8102/9100，Redis 6390）
python3 tests/e2e_check.py
```

`tests/e2e_check.py` 覆盖：16 并发请求单飞仅回源 1 次、缓存命中不回源、旧回源被发布栅栏拦截、确认后两实例都看到新摘要、重试旧发布不回退、发布失败/超时明确报错且未确认标记生效、源站超时 504 后可恢复、含 `/` 的标签、非法输入 422；以及事件订阅：首次进入只显示当前确认状态、跨实例长轮询及时收到事件（摘要/代号/request_id/游标）、按游标断线补收、幂等重试不重复通知、失败与超时发布不发成功事件、每标签保留最近 100 条、游标超出保留范围时明确缺口且单独给出当前确认状态、边界游标不误报缺口。

`tests/test_cache.py` 另外用 fakeredis 覆盖单元级交错：等待者优先返回“发布后”的新状态而不是上一轮错误、发布提交失败后保留未确认标记并可重试恢复、源站重启导致版本号倒退后仍可写入、request_id 复用校验、首次订阅建立确认状态、事件与确认的原子性（提交失败/事件键类型错误时无事件且不确认）、100 条保留与缺口计算、长轮询从另一实例收到事件、游标超前报告等。

## 实现说明与取舍

* 长轮询采用 **Redis 轮询**（200ms）而非 Pub/Sub 广播：不需要额外连接与订阅生命周期管理，且天然跨实例；在 200ms 粒度下，发布确认到订阅方可见通常在数百毫秒内。代价是空闲订阅者每 200ms 一次只读 Redis 轮询，演示规模下可接受。
* 事件游标用于**补收**，不承担去重职责：重复投递是幂等可接受的，客户端按 `cursor > 本地游标` 过滤并去重；`request_id` 幂等保证同一次重试不会追加第二个事件。
* 首次进入若标签从未被查询/发布，会先经正常查询路径（单飞/回源）建立确认状态，再返回事件流；标签不存在则返回明确 404，而不是显示空的“已确认”。
