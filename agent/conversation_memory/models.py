"""Core models for the conversation memory lifecycle."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class MemoryScope(str, Enum):
    """Scope for a memory record or query."""

    SESSION = "session"
    WORKSPACE = "workspace"
    GLOBAL = "global"


@dataclass(slots=True)
class MemorySource:
    """Input envelope passed to extractors."""

    kind: str
    payload: dict[str, Any]
    scope: MemoryScope = MemoryScope.SESSION
    session_key: str | None = None
    source_id: str | None = None
    timestamp: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MemoryRecord:
    """Persistable memory envelope.

    `payload` is intentionally open. Its schema is determined by `kind`.
    """

    kind: str
    payload: dict[str, Any]
    scope: MemoryScope = MemoryScope.SESSION
    session_key: str | None = None
    record_id: str | None = None
    source_kind: str | None = None
    source_id: str | None = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MemoryQuery:
    """Query envelope for memory retrieval."""

    session_key: str | None = None
    text: str | None = None
    kinds: tuple[str, ...] = ()
    scope: MemoryScope = MemoryScope.SESSION
    filters: dict[str, Any] = field(default_factory=dict)
    limit: int | None = None
    since: datetime | None = None
    until: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RetrievedMemory:
    """A retrieved memory record plus retrieval metadata."""

    record: MemoryRecord
    score: float | None = None
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
