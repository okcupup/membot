"""Small, bounded diagnostic records shared by runtime and service code."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from membot.agent.redaction import redact_data

SCHEMA_VERSION = 1
EVENT_LOGGER = logging.getLogger("membot.events")


class DiagnosticPersistenceError(RuntimeError):
    """Strict service recording failed; execution must not silently continue."""


def json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


@dataclass(frozen=True, slots=True)
class DiagnosticPolicy:
    max_payload_bytes: int = 65_536
    retention_days: int = 7

    def __post_init__(self) -> None:
        if not 512 <= self.max_payload_bytes <= 1_048_576:
            raise ValueError("diagnostic payload limit must be between 512 and 1048576 bytes")
        if not 1 <= self.retention_days <= 365:
            raise ValueError("diagnostic retention must be between 1 and 365 days")

    def capture(self, value: dict[str, Any]) -> dict[str, Any]:
        """Keep independent visible payloads, never silent history truncation."""
        safe = redact_data(value)
        encoded = json_bytes(safe)
        metadata = {
            "truncated": False, "original_size": len(json_bytes(value)),
            "redacted_size": len(encoded), "size_unit": "utf8_bytes",
        }
        result = {**safe, **metadata}
        if len(json_bytes(result)) <= self.max_payload_bytes:
            return result
        # A preview cannot be used for deterministic replay. Never retain an
        # unredacted overflow elsewhere as a way around this storage budget.
        result = {**metadata, "truncated": True, "preview": ""}
        budget = self.max_payload_bytes - len(json_bytes(result)) - 16
        result["preview"] = encoded[:max(0, budget)].decode("utf-8", errors="ignore")
        # Quotes/newlines need JSON escaping; measure the final stored bytes.
        while len(json_bytes(result)) > self.max_payload_bytes:
            result["preview"] = result["preview"][:max(0, len(result["preview"]) * 3 // 4)]
        return result


def config_version(config: dict[str, Any]) -> str:
    return hashlib.sha256(json_bytes(redact_data(config))).hexdigest()[:16]


def iso_time(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


def event_record(row: Any) -> dict[str, Any]:
    """The wire/log shape; task IDs come from DB rows, never query headers."""
    data = dict(row)
    payload = data.get("payload", {})
    if isinstance(payload, str):
        payload = json.loads(payload)
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": data.get("event_id"), "sequence": data.get("event_seq"),
        "invocationId": data.get("invocation_id"), "traceId": data.get("trace_id"),
        "requestId": data.get("request_id"), "sessionId": data.get("session_id"),
        "worker": data.get("worker_id"), "attempt": data.get("attempt", 0),
        "time": iso_time(data.get("created_at")),
        "duration_ms": data.get("duration_ms"), "event_type": data.get("event_type"),
        "step": data.get("step"), "span_id": data.get("span_id"),
        "parent_span_id": data.get("parent_span_id"),
        "tool_call_id": data.get("tool_call_id"), "error_code": data.get("error_code"),
        "expires_at": iso_time(data.get("expires_at")), "payload": redact_data(payload),
    }


def log_event(event: dict[str, Any]) -> None:
    """Only called after the event transaction commits."""
    EVENT_LOGGER.info("invocation event", extra={"diagnostic_event": event})
