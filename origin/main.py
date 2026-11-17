"""Delay-configurable artifact tag origin.

The origin is deliberately dumb: it only stores tag -> digest and can inject
latency per request.  It counts every HTTP request it serves so the tests can
prove that a round of concurrent cache misses hits it only once.
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logger = logging.getLogger("origin")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class PublishBody(BaseModel):
    tag: str = Field(min_length=1, max_length=200)
    digest: str = Field(min_length=1, max_length=200)


class Origin:
    def __init__(self) -> None:
        self.tags: dict[str, dict[str, Any]] = {}
        self.delay_ms = int(os.getenv("ORIGIN_DELAY_MS", "0") or 0)
        self.fail_next = 0
        self.slow_next_ms: int | None = None
        self.fail_publish_next = 0
        self.publish_delay_ms = 0
        self.request_count = 0
        self.tag_request_count = 0
        self.seq = 0
        self.lock = asyncio.Lock()

    def seed(self, tag: str, digest: str) -> None:
        self.seq += 1
        self.tags[tag] = {
            "digest": digest,
            "revision": self.seq,
            "updated_at": _now(),
        }

    def snapshot(self) -> dict[str, Any]:
        return {
            "delay_ms": self.delay_ms,
            "request_count": self.request_count,
            "tag_request_count": self.tag_request_count,
            "tags": {k: dict(v) for k, v in sorted(self.tags.items())},
            "fail_next": self.fail_next,
            "slow_next_ms": self.slow_next_ms,
            "fail_publish_next": self.fail_publish_next,
            "publish_delay_ms": self.publish_delay_ms,
        }


origin = Origin()


@asynccontextmanager
async def lifespan(_: FastAPI):
    seed = os.getenv("ORIGIN_SEED", "v1.0.0=sha256:old1111")
    for pair in filter(None, (p.strip() for p in seed.split(","))):
        tag, _, digest = pair.partition("=")
        if tag and digest:
            origin.seed(tag.strip(), digest.strip())
    logger.info("origin ready with %d tag(s), delay=%dms", len(origin.tags), origin.delay_ms)
    yield


app = FastAPI(title="Artifact tag origin", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/tags/{tag:path}")
async def get_tag(tag: str, request: Request) -> JSONResponse:
    # Count before any await so concurrent accounting is accurate.
    origin.request_count += 1
    origin.tag_request_count += 1
    entry = origin.tags.get(tag)
    if entry is None:
        raise HTTPException(status_code=404, detail={"code": "tag_not_found", "tag": tag})

    if origin.fail_next > 0:
        origin.fail_next -= 1
        delay = origin.slow_next_ms
        origin.slow_next_ms = None
        if delay:
            await asyncio.sleep(delay / 1000)
        raise HTTPException(
            status_code=503, detail={"code": "origin_unavailable", "tag": tag}
        )

    delay_ms = origin.delay_ms
    if origin.slow_next_ms is not None:
        try:
            delay_ms = origin.slow_next_ms
        finally:
            origin.slow_next_ms = None
    if delay_ms:
        await asyncio.sleep(delay_ms / 1000)

    body = {
        "tag": tag,
        "digest": entry["digest"],
        "revision": entry["revision"],
        "updated_at": entry["updated_at"],
        "served_at": _now(),
    }
    return JSONResponse(body, headers={"Cache-Control": "no-store"})


@app.get("/tags")
async def list_tags() -> dict[str, Any]:
    return origin.snapshot()


class Control(BaseModel):
    delay_ms: int | None = Field(default=None, ge=0, le=120_000)
    fail_next: int | None = Field(default=None, ge=0, le=100)
    slow_next_ms: int | None = Field(default=None, ge=0, le=120_000)
    fail_publish_next: int | None = Field(default=None, ge=0, le=100)
    publish_delay_ms: int | None = Field(default=None, ge=0, le=120_000)
    reset_counters: bool = False


@app.post("/control")
async def control(body: Control) -> dict[str, Any]:
    if body.delay_ms is not None:
        origin.delay_ms = body.delay_ms
    if body.fail_next is not None:
        origin.fail_next = body.fail_next
    if body.slow_next_ms is not None:
        origin.slow_next_ms = body.slow_next_ms
    if body.fail_publish_next is not None:
        origin.fail_publish_next = body.fail_publish_next
    if body.publish_delay_ms is not None:
        origin.publish_delay_ms = body.publish_delay_ms
    if body.reset_counters:
        origin.request_count = 0
        origin.tag_request_count = 0
    return origin.snapshot()


@app.get("/control")
async def control_state() -> dict[str, Any]:
    return origin.snapshot()


@app.post("/publish")
async def publish(body: PublishBody) -> dict[str, Any]:
    if origin.fail_publish_next > 0:
        origin.fail_publish_next -= 1
        if origin.publish_delay_ms:
            await asyncio.sleep(origin.publish_delay_ms / 1000)
        raise HTTPException(
            status_code=503, detail={"code": "origin_publish_unavailable"}
        )
    if origin.publish_delay_ms:
        await asyncio.sleep(origin.publish_delay_ms / 1000)
    origin.seed(body.tag, body.digest)
    entry = origin.tags[body.tag]
    logger.info("origin publish tag=%s digest=%s rev=%s", body.tag, body.digest, entry["revision"])
    return {"tag": body.tag, **entry}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "origin.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "9000")),
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )
