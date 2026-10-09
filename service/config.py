"""Environment-backed limits shared by API and Worker processes."""

from __future__ import annotations

import math
import os
import socket
from dataclasses import dataclass
from urllib.parse import urlsplit

from membot.agent.diagnostics import DiagnosticPolicy, config_version


def _float(name: str, default: float | None) -> float | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    if value.lower() == "none":
        return None
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be positive (or 'none' to disable)")
    return parsed


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
    diagnostic_payload_bytes: int = 65_536
    diagnostic_retention_days: int = 7
    code_version: str = "membot-0.2.0"
    instance_id: str = socket.gethostname()
    db_timeout_seconds: float = 5.0
    api_drain_seconds: float = 10.0
    worker_drain_seconds: float = 20.0
    cleanup_seconds: float = 10.0
    worker_heartbeat_seconds: float = 1.0
    worker_stale_seconds: float = 5.0
    redis_stream_key: str = "membot:invocations:stream"
    redis_group: str = "membot-workers"
    provider: str = "litellm"
    provider_base_url: str | None = None
    allow_fake_provider: bool = False

    def __post_init__(self) -> None:
        for name, schemes in (("database_url", {"postgres", "postgresql"}),
                              ("redis_url", {"redis", "rediss"})):
            if urlsplit(getattr(self, name)).scheme not in schemes:
                raise ValueError(f"{name} has an unsupported scheme")
        for name, ceiling in {"max_payload_bytes": 1048576, "max_iterations": 100,
                              "max_unfinished": 100000, "worker_concurrency": 64,
                              "worker_prefetch": 256, "redis_stream_max_depth": 100000,
                              "outbox_batch_size": 256, "api_port": 65535}.items():
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= ceiling:
                raise ValueError(f"{name} must be between 1 and {ceiling}")
        for name in ("queue_timeout_seconds", "execution_timeout_seconds", "llm_timeout_seconds",
                     "tool_timeout_seconds", "worker_lease_seconds", "outbox_poll_seconds",
                     "queue_poll_seconds", "db_timeout_seconds", "api_drain_seconds",
                     "worker_drain_seconds", "cleanup_seconds", "worker_heartbeat_seconds",
                     "worker_stale_seconds"):
            value = getattr(self, name)
            if value is None and name not in {"queue_timeout_seconds", "execution_timeout_seconds", "llm_timeout_seconds", "tool_timeout_seconds"}:
                raise ValueError(f"{name} is required")
            if value is not None and (not math.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be finite and positive")
        if self.worker_prefetch < self.worker_concurrency:
            raise ValueError("worker_prefetch must be >= worker_concurrency")
        if self.worker_pending_idle_ms < 0:
            raise ValueError("worker_pending_idle_ms cannot be negative")
        if self.worker_stale_seconds <= self.worker_heartbeat_seconds * 2:
            raise ValueError("worker_stale_seconds must exceed two heartbeat intervals")
        if not self.owner_id or not self.instance_id or len(self.instance_id) > 128:
            raise ValueError("owner_id and instance_id are required (instance_id <= 128 characters)")
        if self.provider not in {"litellm", "custom", "fake"}:
            raise ValueError("provider must be litellm, custom, or fake")
        if self.provider == "fake" and not self.allow_fake_provider:
            raise ValueError("fake provider requires MEMBOT_ALLOW_FAKE_PROVIDER=true")
        if self.provider == "custom" and (not self.provider_base_url or
            urlsplit(self.provider_base_url).scheme not in {"http", "https"}):
            raise ValueError("custom provider requires an HTTP(S) MEMBOT_PROVIDER_BASE_URL")
        self.diagnostic_policy

    @property
    def diagnostic_policy(self) -> DiagnosticPolicy:
        return DiagnosticPolicy(self.diagnostic_payload_bytes, self.diagnostic_retention_days)

    def diagnostic_snapshot(self) -> dict:
        # An allowlist: credentials/connection strings never enter the trace.
        data = {
            "codeVersion": self.code_version, "model": self.model,
            "maxIterations": self.max_iterations,
            "queueTimeoutSeconds": self.queue_timeout_seconds,
            "executionTimeoutSeconds": self.execution_timeout_seconds,
            "llmTimeoutSeconds": self.llm_timeout_seconds,
            "toolTimeoutSeconds": self.tool_timeout_seconds,
            "workerConcurrency": self.worker_concurrency,
            "memoryWindow": 100, "temperature": 0.1, "maxTokens": 4096,
            "diagnosticSchemaVersion": 1,
            "diagnosticPayloadBytes": self.diagnostic_payload_bytes,
            "diagnosticRetentionDays": self.diagnostic_retention_days,
        }
        return {**data, "configVersion": config_version(data)}

    @classmethod
    def from_env(cls) -> "ServiceConfig":
        defaults = cls()
        return cls(
            database_url=os.getenv("DATABASE_URL", defaults.database_url),
            redis_url=os.getenv("REDIS_URL", defaults.redis_url),
            owner_id=os.getenv("MEMBOT_OWNER_ID", defaults.owner_id),
            max_payload_bytes=int(os.getenv("MEMBOT_MAX_PAYLOAD_BYTES", defaults.max_payload_bytes)),
            max_iterations=int(os.getenv("MEMBOT_MAX_ITERATIONS", defaults.max_iterations)),
            max_unfinished=int(os.getenv("MEMBOT_MAX_UNFINISHED", defaults.max_unfinished)),
            queue_timeout_seconds=_float("MEMBOT_QUEUE_TIMEOUT_SECONDS", defaults.queue_timeout_seconds),
            execution_timeout_seconds=_float("MEMBOT_EXECUTION_TIMEOUT_SECONDS", defaults.execution_timeout_seconds),
            llm_timeout_seconds=_float("MEMBOT_LLM_TIMEOUT_SECONDS", defaults.llm_timeout_seconds),
            tool_timeout_seconds=_float("MEMBOT_TOOL_TIMEOUT_SECONDS", defaults.tool_timeout_seconds),
            worker_concurrency=int(os.getenv("MEMBOT_WORKER_CONCURRENCY", defaults.worker_concurrency)),
            worker_prefetch=int(os.getenv("MEMBOT_WORKER_PREFETCH", defaults.worker_prefetch)),
            redis_stream_max_depth=int(os.getenv("MEMBOT_REDIS_STREAM_MAX_DEPTH", defaults.redis_stream_max_depth)),
            worker_lease_seconds=float(os.getenv("MEMBOT_WORKER_LEASE_SECONDS", defaults.worker_lease_seconds)),
            worker_pending_idle_ms=int(os.getenv("MEMBOT_WORKER_PENDING_IDLE_MS", defaults.worker_pending_idle_ms)),
            outbox_batch_size=int(os.getenv("MEMBOT_OUTBOX_BATCH_SIZE", defaults.outbox_batch_size)),
            outbox_poll_seconds=float(os.getenv("MEMBOT_OUTBOX_POLL_SECONDS", defaults.outbox_poll_seconds)),
            queue_poll_seconds=float(os.getenv("MEMBOT_QUEUE_POLL_SECONDS", defaults.queue_poll_seconds)),
            workspace=os.getenv("MEMBOT_WORKSPACE", defaults.workspace),
            model=os.getenv("MEMBOT_MODEL", defaults.model),
            api_host=os.getenv("MEMBOT_API_HOST", defaults.api_host),
            api_port=int(os.getenv("MEMBOT_API_PORT", defaults.api_port)),
            diagnostic_payload_bytes=int(os.getenv("MEMBOT_DIAGNOSTIC_PAYLOAD_BYTES", defaults.diagnostic_payload_bytes)),
            diagnostic_retention_days=int(os.getenv("MEMBOT_DIAGNOSTIC_RETENTION_DAYS", defaults.diagnostic_retention_days)),
            code_version=os.getenv("MEMBOT_CODE_VERSION", defaults.code_version),
            instance_id=os.getenv("MEMBOT_INSTANCE_ID", defaults.instance_id),
            db_timeout_seconds=float(os.getenv("MEMBOT_DB_TIMEOUT_SECONDS", defaults.db_timeout_seconds)),
            api_drain_seconds=float(os.getenv("MEMBOT_API_DRAIN_SECONDS", defaults.api_drain_seconds)),
            worker_drain_seconds=float(os.getenv("MEMBOT_WORKER_DRAIN_SECONDS", defaults.worker_drain_seconds)),
            cleanup_seconds=float(os.getenv("MEMBOT_CLEANUP_SECONDS", defaults.cleanup_seconds)),
            worker_heartbeat_seconds=float(os.getenv("MEMBOT_WORKER_HEARTBEAT_SECONDS", defaults.worker_heartbeat_seconds)),
            worker_stale_seconds=float(os.getenv("MEMBOT_WORKER_STALE_SECONDS", defaults.worker_stale_seconds)),
            redis_stream_key=os.getenv("MEMBOT_REDIS_STREAM_KEY", defaults.redis_stream_key),
            redis_group=os.getenv("MEMBOT_REDIS_GROUP", defaults.redis_group),
            provider=os.getenv("MEMBOT_PROVIDER", defaults.provider),
            provider_base_url=os.getenv("MEMBOT_PROVIDER_BASE_URL") or None,
            allow_fake_provider=os.getenv("MEMBOT_ALLOW_FAKE_PROVIDER", "false").lower() == "true",
        )

    @property
    def worker_id(self) -> str:
        return f"worker:{socket.gethostname()}:{os.getpid()}"
