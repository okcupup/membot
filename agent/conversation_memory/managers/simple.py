"""Simple first-pass memory manager."""

from __future__ import annotations

from typing import Sequence

from membot.agent.conversation_memory.managers.base import MemoryManager
from membot.agent.conversation_memory.models import MemoryRecord


class SimpleMessageManager(MemoryManager):
    """Preserve order and keep records without summarization or merging."""

    def manage(self, records: Sequence[MemoryRecord]) -> Sequence[MemoryRecord]:
        return tuple(record for record in records if record.payload)

    def should_consolidate(self, records: Sequence[MemoryRecord]) -> bool:
        return False
