"""Retriever protocol."""

from __future__ import annotations

from typing import Protocol, Sequence

from membot.agent.conversation_memory.models import MemoryQuery, RetrievedMemory


class MemoryRetriever(Protocol):
    """Retrieve memory records for prompt construction or consolidation."""

    async def retrieve(self, query: MemoryQuery) -> Sequence[RetrievedMemory]:
        """Return memories matching `query`."""
        ...
