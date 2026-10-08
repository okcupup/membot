"""Environment-backed limits shared by API and Worker processes."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass

from membot.agent.diagnostics import DiagnosticPolicy, config_version


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
    diagnostic_payload_bytes: int = 65_536
    diagnostic_retention_days: int = 7
    code_version: str = "membot-0.2.0"

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
            worker_concurrency=max(1, int(os.getenv("MEMBOT_WORKER_CONCURRENCY", defaults.worker_concurrency))),
            worker_prefetch=max(1, int(os.getenv("MEMBOT_WORKER_PREFETCH", defaults.worker_prefetch))),
            redis_stream_max_depth=max(1, int(os.getenv("MEMBOT_REDIS_STREAM_MAX_DEPTH", defaults.redis_stream_max_depth))),
            worker_lease_seconds=max(1.0, float(os.getenv("MEMBOT_WORKER_LEASE_SECONDS", defaults.worker_lease_seconds))),
            worker_pending_idle_ms=max(0, int(os.getenv("MEMBOT_WORKER_PENDING_IDLE_MS", defaults.worker_pending_idle_ms))),
            outbox_batch_size=max(1, int(os.getenv("MEMBOT_OUTBOX_BATCH_SIZE", defaults.outbox_batch_size))),
            outbox_poll_seconds=max(0.05, float(os.getenv("MEMBOT_OUTBOX_POLL_SECONDS", defaults.outbox_poll_seconds))),
            queue_poll_seconds=max(0.05, float(os.getenv("MEMBOT_QUEUE_POLL_SECONDS", defaults.queue_poll_seconds))),
            workspace=os.getenv("MEMBOT_WORKSPACE", defaults.workspace),
            model=os.getenv("MEMBOT_MODEL", defaults.model),
            api_host=os.getenv("MEMBOT_API_HOST", defaults.api_host),
            api_port=int(os.getenv("MEMBOT_API_PORT", defaults.api_port)),
            diagnostic_payload_bytes=int(os.getenv("MEMBOT_DIAGNOSTIC_PAYLOAD_BYTES", defaults.diagnostic_payload_bytes)),
            diagnostic_retention_days=int(os.getenv("MEMBOT_DIAGNOSTIC_RETENTION_DAYS", defaults.diagnostic_retention_days)),
            code_version=os.getenv("MEMBOT_CODE_VERSION", defaults.code_version),
        )

    @property
    def worker_id(self) -> str:
        return f"worker:{socket.gethostname()}:{os.getpid()}"
