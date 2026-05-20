"""Manager protocol."""

from __future__ import annotations

from typing import Protocol, Sequence

from nanobot.agent.conversation_memory.models import MemoryRecord


class MemoryManager(Protocol):
    """Apply retention, filtering, and promotion policy before storage."""

    def manage(self, records: Sequence[MemoryRecord]) -> Sequence[MemoryRecord]:
        """Return records that should be stored."""
        ...

    def should_consolidate(self, records: Sequence[MemoryRecord]) -> bool:
        """Return whether these records should trigger consolidation."""
        ...


MemoryPolicy = MemoryManager
