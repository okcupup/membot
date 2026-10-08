"""JSON stdout logging for service processes; CLI logging is unchanged."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, TextIO

from membot.agent.diagnostics import DiagnosticPolicy
from membot.agent.execution import get_execution_context
from membot.agent.redaction import redact_data


def correlation() -> dict[str, Any]:
    context = get_execution_context()
    if context is None:
        return {}
    return {
        "invocationId": context.invocation_id, "traceId": context.trace_id,
        "requestId": context.request_id, "sessionId": context.session_id,
        "worker": context.execution_owner, "attempt": context.attempt,
    }


class JsonFormatter(logging.Formatter):
    def __init__(self, *, component: str, policy: DiagnosticPolicy | None = None):
        super().__init__()
        self.component = component
        self.policy = policy or DiagnosticPolicy()

    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "diagnostic_event", None)
        if event is not None:
            # Already bounded at the storage boundary. Do not silently replace
            # event payloads with a second, different log truncation.
            data = {"component": self.component, "level": record.levelname, **event}
        else:
            data = {
                "time": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
                "component": self.component, "level": record.levelname,
                "logger": record.name, "process_id": os.getpid(), **correlation(),
                **getattr(record, "correlation", {}),
                "payload": self.policy.capture({"message": record.getMessage()}),
            }
            if record.exc_info:
                # No locals or request headers; exception text is redacted too.
                data["exception"] = self.policy.capture({"message": self.formatException(record.exc_info)})
        return json.dumps(redact_data(data), ensure_ascii=False, separators=(",", ":"), default=str)


def configure_json_logging(component: str, *, stream: TextIO | None = None,
                           policy: DiagnosticPolicy | None = None) -> None:
    destination = stream or sys.stdout
    formatter = JsonFormatter(component=component, policy=policy)
    handler = logging.StreamHandler(destination)
    handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    # AgentLoop/tools use Loguru. Route their ordinary messages through the
    # same redaction/formatting boundary, without changing CLI entry points.
    from loguru import logger

    def sink(message: Any) -> None:
        record = message.record
        converted = logging.LogRecord(
            record["name"], record["level"].no, record["file"].path,
            record["line"], record["message"], (), None,
        )
        destination.write(formatter.format(converted) + "\n")
        destination.flush()

    logger.remove()
    logger.add(sink, level="INFO", backtrace=False, diagnose=False)
