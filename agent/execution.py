"""Invocation-local execution context shared with tools through contextvars."""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """Routing and correlation data for one accepted invocation."""

    session_key: str
    channel: str
    chat_id: str
    message_id: str | None = None
    request_id: str | None = None
    trace_id: str | None = None
    invocation_id: str | None = None
    owner_id: str | None = None
    session_id: str | None = None
    session_seq: int | None = None
    execution_owner: str | None = None


_CURRENT_EXECUTION: ContextVar[ExecutionContext | None] = ContextVar(
    "membot_current_execution",
    default=None,
)


def get_execution_context() -> ExecutionContext | None:
    """Return the context of the currently executing invocation, if any."""

    return _CURRENT_EXECUTION.get()


def set_execution_context(context: ExecutionContext | None):
    """Set the current context and return a token for restoring it."""

    return _CURRENT_EXECUTION.set(context)


def reset_execution_context(token) -> None:
    """Restore the context represented by a token returned from ``set``."""

    _CURRENT_EXECUTION.reset(token)
