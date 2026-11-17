"""Per-tag publication event log with bounded retention.

Every confirmed publication appends exactly one event to a Redis sorted set
scored by a monotonically increasing cursor.  The append happens inside the
same Lua script that commits the publication, so a publication is either both
confirmed in the cache and registered as an event, or neither.

Retention is ``EVENT_RETENTION`` events per tag (the newest N).  A subscriber
that reconnects with a cursor older than the retained window is told about the
gap explicitly; the missing events are never fabricated or silently skipped.

Keys (per tag)::

    artifact:{tag}:events       zset   cursor -> event JSON (newest 100)
    artifact:{tag}:events_last  string newest cursor ever appended

Both keys share ``EVENT_TTL_MS`` and the TTL is refreshed on every append, so
an idle tag's event window eventually expires.  The cursor counter is kept
without a TTL, so after the window expires a lagging cursor is still detected
as a gap instead of being silently compared against restarted numbering.

Reads use a single Lua script so retention metadata, the requested event range
and the confirmed publication state are taken atomically.  A concurrent trim
therefore cannot make a partial history look contiguous, and the confirmed
digest reported alongside events can never be older than the events returned.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

EVENT_RETENTION = 100
EVENT_TTL_MS = 7 * 24 * 3600 * 1000
EVENT_POLL_INTERVAL_S = 0.2
MAX_WAIT_MS = 25_000


def event_keys(tag: str) -> tuple[str, str]:
    return (f"artifact:{tag}:events", f"artifact:{tag}:events_last")


# KEYS[1]=events zset, 2=events_last counter, 3=state, 4=pending,
#      5=last_publish, 6=unconfirmed publication marker
# ARGV[1]=cursor, ARGV[2]=limit
# Returns {last cursor, retained count, oldest cursor (-1 when none),
#          [member, score, ...], state hash (flat), pending hash, last hash,
#          unconfirmed hash}
_READ_EVENTS_LUA = """
local last = tonumber(redis.call('GET', KEYS[2]) or '0')
local count = redis.call('ZCARD', KEYS[1])
local oldest = -1
if count > 0 then
  local first = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  oldest = tonumber(first[2])
  local tail = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  local tail_cursor = tonumber(tail[2])
  if tail_cursor > last then last = tail_cursor end
end
local entries = {}
if tonumber(ARGV[1]) < last then
  entries = redis.call(
    'ZRANGEBYSCORE', KEYS[1], '(' .. ARGV[1], '+inf',
    'WITHSCORES', 'LIMIT', 0, tonumber(ARGV[2]) + 1)
end
return {last, count, oldest, entries,
        redis.call('HGETALL', KEYS[3]),
        redis.call('HGETALL', KEYS[4]),
        redis.call('HGETALL', KEYS[5]),
        redis.call('HGETALL', KEYS[6])}
"""


class EventReadError(Exception):
    """Raised when Redis cannot serve the event log."""


def _hash(flat: list[str] | None) -> dict[str, str] | None:
    if not flat:
        return None
    return {flat[i]: flat[i + 1] for i in range(0, len(flat) - 1, 2)}


@dataclass
class EventSnapshot:
    last: int
    retained: int
    oldest: int | None
    entries: list[dict[str, Any]] = field(default_factory=list)
    has_more: bool = False
    state: dict[str, str] | None = None
    pending: dict[str, str] | None = None
    last_publish: dict[str, str] | None = None
    unconfirmed: dict[str, str] | None = None

    @property
    def oldest_available(self) -> int | None:
        return self.oldest


class EventStore:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def read(self, tag: str, cursor: int, limit: int) -> EventSnapshot:
        """Atomically read retention metadata, events after ``cursor`` and state."""
        zset_key, counter_key = event_keys(tag)
        try:
            raw = await self.redis.eval(
                _READ_EVENTS_LUA,
                6,
                zset_key,
                counter_key,
                f"artifact:{tag}:state",
                f"artifact:{tag}:pending",
                f"artifact:{tag}:last_publish",
                f"artifact:{tag}:pub_unconfirmed",
                str(cursor),
                str(limit),
            )
        except RedisError as exc:
            raise EventReadError(exc.__class__.__name__) from exc

        last = int(raw[0] or 0)
        retained = int(raw[1] or 0)
        oldest = int(raw[2]) if int(raw[2]) >= 0 else None
        flat = raw[3] or []
        entries = []
        for index in range(0, len(flat) - 1, 2):
            member = flat[index]
            score = int(float(flat[index + 1]))
            try:
                payload = json.loads(member)
            except (TypeError, ValueError):
                # A corrupt member must not masquerade as a usable event.
                payload = {"type": "corrupt_event"}
            if not isinstance(payload, dict):
                payload = {"type": "corrupt_event"}
            payload["cursor"] = score
            entries.append(payload)
        has_more = len(entries) > limit
        return EventSnapshot(
            last=last,
            retained=retained,
            oldest=oldest,
            entries=entries[:limit],
            has_more=has_more,
            state=_hash(raw[4]),
            pending=_hash(raw[5]),
            last_publish=_hash(raw[6]),
            unconfirmed=_hash(raw[7]),
        )
