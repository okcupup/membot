"""Schema registry for memory payload kinds."""

from __future__ import annotations

from dataclasses import dataclass, field

from nanobot.agent.conversation_memory.schemas.fact import FACT_KIND
from nanobot.agent.conversation_memory.schemas.preference import PREFERENCE_KIND
from nanobot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND
from nanobot.agent.conversation_memory.schemas.summary import SUMMARY_KIND
from nanobot.agent.conversation_memory.schemas.task import TASK_KIND


@dataclass(frozen=True, slots=True)
class MemorySchema:
    """Lightweight schema metadata for a MemoryRecord payload kind."""

    kind: str
    required_fields: tuple[str, ...]
    optional_fields: tuple[str, ...] = field(default_factory=tuple)


SCHEMAS: dict[str, MemorySchema] = {
    RAW_MESSAGE_KIND: MemorySchema(
        kind=RAW_MESSAGE_KIND,
        required_fields=("role", "content", "timestamp"),
        optional_fields=("tool_calls", "tool_call_id", "name"),
    ),
    SUMMARY_KIND: MemorySchema(
        kind=SUMMARY_KIND,
        required_fields=("summary", "covered_message_ids", "time_range"),
    ),
    FACT_KIND: MemorySchema(
        kind=FACT_KIND,
        required_fields=("subject", "predicate", "object", "confidence"),
    ),
    PREFERENCE_KIND: MemorySchema(
        kind=PREFERENCE_KIND,
        required_fields=("subject", "preference", "confidence"),
    ),
    TASK_KIND: MemorySchema(
        kind=TASK_KIND,
        required_fields=("title", "status", "confidence"),
    ),
}


def get_schema(kind: str) -> MemorySchema | None:
    return SCHEMAS.get(kind)
