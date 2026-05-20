"""Extractor protocol."""

from __future__ import annotations

from typing import Protocol, Sequence

from membot.agent.conversation_memory.models import MemoryRecord, MemorySource


class MemoryExtractor(Protocol):
    """Extract memory records from a source envelope."""

    async def extract(self, source: MemorySource) -> Sequence[MemoryRecord]:
        """Return zero or more memory records extracted from `source`."""
        ...
