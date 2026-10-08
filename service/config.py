"""Environment-backed limits shared by API and Worker processes."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _float(name: str, default: float | None) -> float | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    parsed = float(value)
    return parsed if parsed > 0 else None


@dataclass(frozen=True, slots=True)
class ServiceConfig:
    database_url: str = "postgresql://membot:membot@127.0.0.1:55432/membot"
    redis_url: str = "redis://127.0.0.1:56379/0"
    owner_id: str = "default"
    max_payload_bytes: int = 1_048_576
    max_iterations: int = 40
    max_unfinished: int = 1024
    queue_timeout_seconds: float | None = 300.0
    execution_timeout_seconds: float | None = 300.0
    llm_timeout_seconds: float | None = 120.0
    tool_timeout_seconds: float | None = 60.0
    worker_concurrency: int = 4
    worker_prefetch: int = 8
    redis_stream_max_depth: int = 1024
    worker_lease_seconds: float = 90.0
    worker_pending_idle_ms: int = 500
    outbox_batch_size: int = 64
    outbox_poll_seconds: float = 0.5
    queue_poll_seconds: float = 0.25
    workspace: str = "."
    model: str = "gpt-4o-mini"
    api_host: str = "127.0.0.1"
    api_port: int = 8080

    @classmethod
    def from_env(cls) -> "ServiceConfig":
        return cls(
            database_url=os.getenv("DATABASE_URL", cls.database_url),
            redis_url=os.getenv("REDIS_URL", cls.redis_url),
            owner_id=os.getenv("MEMBOT_OWNER_ID", cls.owner_id),
            max_payload_bytes=int(os.getenv("MEMBOT_MAX_PAYLOAD_BYTES", cls.max_payload_bytes)),
            max_iterations=int(os.getenv("MEMBOT_MAX_ITERATIONS", cls.max_iterations)),
            max_unfinished=int(os.getenv("MEMBOT_MAX_UNFINISHED", cls.max_unfinished)),
            queue_timeout_seconds=_float("MEMBOT_QUEUE_TIMEOUT_SECONDS", cls.queue_timeout_seconds),
            execution_timeout_seconds=_float("MEMBOT_EXECUTION_TIMEOUT_SECONDS", cls.execution_timeout_seconds),
            llm_timeout_seconds=_float("MEMBOT_LLM_TIMEOUT_SECONDS", cls.llm_timeout_seconds),
            tool_timeout_seconds=_float("MEMBOT_TOOL_TIMEOUT_SECONDS", cls.tool_timeout_seconds),
            worker_concurrency=max(1, int(os.getenv("MEMBOT_WORKER_CONCURRENCY", cls.worker_concurrency))),
            worker_prefetch=max(1, int(os.getenv("MEMBOT_WORKER_PREFETCH", cls.worker_prefetch))),
            redis_stream_max_depth=max(1, int(os.getenv("MEMBOT_REDIS_STREAM_MAX_DEPTH", cls.redis_stream_max_depth))),
            worker_lease_seconds=max(1.0, float(os.getenv("MEMBOT_WORKER_LEASE_SECONDS", cls.worker_lease_seconds))),
            worker_pending_idle_ms=max(0, int(os.getenv("MEMBOT_WORKER_PENDING_IDLE_MS", cls.worker_pending_idle_ms))),
            outbox_batch_size=max(1, int(os.getenv("MEMBOT_OUTBOX_BATCH_SIZE", cls.outbox_batch_size))),
            outbox_poll_seconds=max(0.05, float(os.getenv("MEMBOT_OUTBOX_POLL_SECONDS", cls.outbox_poll_seconds))),
            queue_poll_seconds=max(0.05, float(os.getenv("MEMBOT_QUEUE_POLL_SECONDS", cls.queue_poll_seconds))),
            workspace=os.getenv("MEMBOT_WORKSPACE", cls.workspace),
            model=os.getenv("MEMBOT_MODEL", cls.model),
            api_host=os.getenv("MEMBOT_API_HOST", cls.api_host),
            api_port=int(os.getenv("MEMBOT_API_PORT", cls.api_port)),
        )

    @property
    def worker_id(self) -> str:
        return f"worker:{socket.gethostname()}:{os.getpid()}"
