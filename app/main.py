from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Header, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from .cache import ServiceError, TagService
from .config import Settings, settings

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("gateway")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class PublishRequest(BaseModel):
    tag: str = Field(min_length=1, max_length=200)
    digest: str = Field(min_length=1, max_length=200)
    request_id: str | None = Field(default=None, max_length=120)


class TagBody(BaseModel):
    tag: str = Field(min_length=1, max_length=200)


class OriginControlRequest(BaseModel):
    delay_ms: int | None = Field(default=None, ge=0, le=120_000)
    fail_next: int | None = Field(default=None, ge=0, le=100)
    slow_next_ms: int | None = Field(default=None, ge=0, le=120_000)
    fail_publish_next: int | None = Field(default=None, ge=0, le=100)
    publish_delay_ms: int | None = Field(default=None, ge=0, le=120_000)
    reset_counters: bool = False


def _error_payload(exc: ServiceError, instance: str) -> dict[str, Any]:
    return {
        "error": {
            "code": exc.code,
            "message": exc.message,
            "retryable": exc.retryable,
            "status": exc.status_code,
            **exc.extra,
        },
        "instance": instance,
    }


def _install_error_handler(app: FastAPI) -> None:
    @app.exception_handler(ServiceError)
    async def _service_error(request: Request, exc: ServiceError) -> JSONResponse:
        headers = {"X-Instance": app.state.settings.instance_name}
        if exc.retryable:
            headers["Retry-After"] = "1"
        body = _error_payload(exc, app.state.settings.instance_name)
        body["requested_path"] = request.url.path
        return JSONResponse(body, status_code=exc.status_code, headers=headers)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg: Settings = app.state.settings
    redis = Redis.from_url(
        cfg.redis_url,
        decode_responses=True,
        socket_timeout=cfg.redis_socket_timeout_ms / 1000,
        socket_connect_timeout=cfg.redis_socket_timeout_ms / 1000,
        health_check_interval=15,
        retry_on_timeout=False,
    )
    origin = httpx.AsyncClient(
        base_url=cfg.origin_url,
        timeout=cfg.origin_timeout_ms / 1000,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )
    app.state.redis = redis
    app.state.origin = origin
    app.state.service = TagService(redis, origin, cfg)
    try:
        await redis.ping()
        logger.info(
            "instance %s connected to redis, origin=%s",
            cfg.instance_name,
            cfg.origin_url,
        )
    except Exception as exc:  # noqa: BLE001 - startup diagnostics only
        logger.warning("redis not reachable at startup: %r", exc)
    try:
        yield
    finally:
        await origin.aclose()
        await redis.aclose()


def create_app(cfg: Settings | None = None) -> FastAPI:
    cfg = cfg or settings
    app = FastAPI(title="制品标签查询", version="1.0.0", lifespan=lifespan)
    app.state.settings = cfg
    # The page served by one instance drives the other instance directly, so
    # the public instance origins must be allowed to call this API.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(
            dict.fromkeys(
                list(cfg.public_instances)
                + ["http://localhost:8001", "http://localhost:8002"]
            )
        ),
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "X-Request-Id"],
        allow_credentials=False,
    )
    _install_error_handler(app)
    _register_routes(app)
    return app


def _register_routes(app: FastAPI) -> None:
    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        service: TagService = app.state.service
        redis_ok = True
        redis_error = None
        try:
            await service.redis.ping()
        except Exception as exc:  # noqa: BLE001
            redis_ok = False
            redis_error = exc.__class__.__name__
        return {
            "status": "ok" if redis_ok else "degraded",
            "instance": app.state.settings.instance_name,
            "redis": {"ok": redis_ok, "error": redis_error},
            "public_instances": list(app.state.settings.public_instances),
        }

    @app.get("/api/config")
    async def config() -> dict[str, Any]:
        cfg: Settings = app.state.settings
        return {
            "instance": cfg.instance_name,
            "public_instances": list(cfg.public_instances),
            "origin_url": cfg.origin_url,
            "origin_timeout_ms": cfg.origin_timeout_ms,
            "cache_wait_timeout_ms": cfg.cache_wait_timeout_ms,
            "revalidate_ttl_s": cfg.revalidate_ttl_s,
        }

    @app.get("/api/tag/{tag:path}")
    async def get_tag(
        tag: str,
        refresh: bool = Query(default=False),
        x_request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, Any]:
        service: TagService = app.state.service
        result = await service.get_tag(tag, refresh=refresh)
        if x_request_id:
            result["gateway_request_id"] = x_request_id
        return result

    @app.get("/api/status/{tag:path}")
    async def get_status(tag: str) -> dict[str, Any]:
        service: TagService = app.state.service
        return await service.status(tag)

    @app.post("/api/publish")
    async def publish(
        body: PublishRequest,
        x_request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, Any]:
        service: TagService = app.state.service
        return await service.publish(
            body.tag,
            body.digest,
            body.request_id or x_request_id,
        )

    @app.post("/api/cache/invalidate")
    async def invalidate(body: TagBody) -> dict[str, Any]:
        service: TagService = app.state.service
        return await service.invalidate(body.tag)

    @app.get("/api/stats")
    async def stats() -> dict[str, Any]:
        service: TagService = app.state.service
        gateway = await service.gateway_stats()
        origin = await service.origin_stats()
        counts = origin.get("request_count")
        return {
            "instance": app.state.settings.instance_name,
            "gateway": gateway,
            "origin": origin,
            "origin_request_count": counts,
        }

    @app.post("/api/origin/control")
    async def origin_control(body: OriginControlRequest) -> dict[str, Any]:
        service: TagService = app.state.service
        payload = {k: v for k, v in body.model_dump().items() if v is not None}
        if body.reset_counters:
            payload["reset_counters"] = True
        return await service.origin_control(payload)

    @app.get("/api/origin/tags")
    async def origin_tags() -> dict[str, Any]:
        service: TagService = app.state.service
        return await service.origin_stats()


app = create_app()
