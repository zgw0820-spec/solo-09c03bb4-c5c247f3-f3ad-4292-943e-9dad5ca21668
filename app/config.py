from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int, minimum: int = 1, maximum: int = 3_600_000) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum or value > maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}, got {value}")
    return value


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


@dataclass(frozen=True)
class Settings:
    redis_url: str
    origin_url: str
    origin_timeout_ms: int
    cache_wait_timeout_ms: int
    lock_ttl_ms: int
    publish_timeout_ms: int
    redis_socket_timeout_ms: int
    pending_ttl_ms: int
    unconfirmed_ttl_s: int
    revalidate_ttl_s: int
    fetch_state_ttl_s: int
    publish_record_ttl_s: int
    instance_name: str
    public_instances: tuple[str, ...]

    @classmethod
    def from_env(cls) -> Settings:
        instances = tuple(
            item.strip().rstrip("/")
            for item in _env_str(
                "PUBLIC_INSTANCES", "http://localhost:8001,http://localhost:8002"
            ).split(",")
            if item.strip()
        )
        return cls(
            redis_url=_env_str("REDIS_URL", "redis://localhost:6379/0"),
            origin_url=_env_str("ORIGIN_URL", "http://localhost:9000").rstrip("/"),
            origin_timeout_ms=_env_int("ORIGIN_TIMEOUT_MS", 2_000, 100, 60_000),
            cache_wait_timeout_ms=_env_int(
                "CACHE_WAIT_TIMEOUT_MS", 3_500, 100, 60_000
            ),
            lock_ttl_ms=_env_int("LOCK_TTL_MS", 12_000, 500, 300_000),
            publish_timeout_ms=_env_int("PUBLISH_TIMEOUT_MS", 5_000, 200, 120_000),
            redis_socket_timeout_ms=_env_int(
                "REDIS_SOCKET_TIMEOUT_MS", 1_500, 100, 60_000
            ),
            pending_ttl_ms=_env_int("PENDING_TTL_MS", 15_000, 1_000, 300_000),
            unconfirmed_ttl_s=_env_int("UNCONFIRMED_TTL_S", 600, 30, 604_800),
            revalidate_ttl_s=_env_int("REVALIDATE_TTL_S", 300, 10, 86_400),
            fetch_state_ttl_s=_env_int("FETCH_STATE_TTL_S", 30, 5, 300),
            publish_record_ttl_s=_env_int(
                "PUBLISH_RECORD_TTL_S", 604_800, 60, 2_592_000
            ),
            instance_name=_env_str("INSTANCE_NAME", "query"),
            public_instances=instances,
        )


settings = Settings.from_env()
