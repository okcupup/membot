"""Raw-message extractor."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence

from membot.agent.conversation_memory.extractors.base import MemoryExtractor
from membot.agent.conversation_memory.models import MemoryRecord, MemoryScope, MemorySource
from membot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND


def build_raw_message_record(
    message: Mapping[str, Any],
    *,
    source: MemorySource | None = None,
    record_id: str | None = None,
) -> MemoryRecord:
    """Build a raw-message record from a session-style message mapping."""

    timestamp = message.get("timestamp")
    if timestamp is None and source and source.timestamp:
        timestamp = source.timestamp.isoformat()
    if timestamp is None:
        timestamp = datetime.now().isoformat()

    payload: dict[str, Any] = dict(message)
    payload.setdefault("role", message.get("role"))
    payload.setdefault("content", message.get("content", ""))
    payload["timestamp"] = timestamp

    return MemoryRecord(
        kind=RAW_MESSAGE_KIND,
        payload=payload,
        scope=source.scope if source else MemoryScope.SESSION,
        session_key=source.session_key if source else None,
        record_id=record_id,
        source_kind=source.kind if source else None,
        source_id=source.source_id if source else None,
        metadata=dict(source.metadata) if source else {},
    )


class RawMessageExtractor(MemoryExtractor):
    """Extract a raw-message record from a mapping payload."""

    async def extract(self, source: MemorySource) -> Sequence[MemoryRecord]:
        if not isinstance(source.payload, Mapping):
            return ()
        return (build_raw_message_record(source.payload, source=source),)
