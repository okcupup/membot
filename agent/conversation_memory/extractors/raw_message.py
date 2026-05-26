"""Raw-message extractor."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence

from membot.agent.conversation_memory.extractors.base import MemoryExtractor
from membot.agent.conversation_memory.models import MemoryRecord, MemoryScope, MemorySource
from membot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND
from membot.agent.conversation_memory.ids import build_record_id


def build_raw_turn_record(
    source: MemorySource,
    *,
    record_id: str | None = None,
) -> MemoryRecord | None:
    """Build a raw-message record from a session-turn source."""

    payload = source.payload
    messages = payload.get("messages") if isinstance(payload, Mapping) else None
    if not isinstance(messages, list):
        return None

    turn_id = payload.get("turn_id") or source.source_id
    if not isinstance(turn_id, str) or not turn_id:
        return None

    turn_messages = [dict(message) for message in messages if isinstance(message, Mapping)]
    if not turn_messages:
        return None

    record_payload: dict[str, Any] = {
        "turn_id": turn_id,
        "messages": turn_messages,
        "timestamp": _record_timestamp(turn_messages, source),
        "time_range": _get_time_range(turn_messages, source),
    }
    record_id = record_id or build_record_id(RAW_MESSAGE_KIND, turn_id, payload=record_payload)

    return MemoryRecord(
        kind=RAW_MESSAGE_KIND,
        payload=record_payload,
        scope=source.scope,
        session_key=source.session_key,
        record_id=record_id,
        source_kind=source.kind,
        source_id=turn_id,
        metadata=dict(source.metadata),
    )


class RawMessageExtractor(MemoryExtractor):
    """Extract raw-message records from session turns."""

    async def extract(self, source: MemorySource) -> Sequence[MemoryRecord]:
        if not isinstance(source.payload, Mapping):
            return ()
        record = build_raw_turn_record(source)
        return (record,) if record else ()


def _record_timestamp(messages: Sequence[dict[str, Any]], source: MemorySource) -> str:
    time_range = _get_time_range(messages, source)
    return time_range["end"] or time_range["start"] or datetime.now().isoformat()


def _get_time_range(messages: Sequence[dict[str, Any]], source: MemorySource) -> dict[str, str | None]:
    timestamps = [
        value for message in messages
        if (value := _timestamp_string(message.get("timestamp"))) is not None
    ]
    if timestamps:
        return {"start": timestamps[0], "end": timestamps[-1]}

    fallback = source.timestamp.isoformat() if source.timestamp else None
    return {"start": fallback, "end": fallback}


def _timestamp_string(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
