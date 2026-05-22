"""Schema registry for memory payload kinds."""

from __future__ import annotations

from dataclasses import dataclass, field

from membot.agent.conversation_memory.schemas.fact import FACT_KIND
from membot.agent.conversation_memory.schemas.preference import PREFERENCE_KIND
from membot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND
from membot.agent.conversation_memory.schemas.summary import (
    OPTIONAL_FIELDS as SUMMARY_OPTIONAL_FIELDS,
    REQUIRED_FIELDS as SUMMARY_REQUIRED_FIELDS,
    SUMMARY_KIND,
)
from membot.agent.conversation_memory.schemas.task import TASK_KIND


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
        required_fields=SUMMARY_REQUIRED_FIELDS,
        optional_fields=SUMMARY_OPTIONAL_FIELDS,
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
