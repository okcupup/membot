"""Backward-compatible imports for conversation memory protocols."""

from membot.agent.conversation_memory.extractors.base import MemoryExtractor
from membot.agent.conversation_memory.managers.base import MemoryManager, MemoryPolicy
from membot.agent.conversation_memory.retrievers.base import MemoryRetriever
from membot.agent.conversation_memory.stores.base import MemoryStore

__all__ = [
    "MemoryExtractor",
    "MemoryManager",
    "MemoryPolicy",
    "MemoryRetriever",
    "MemoryStore",
]
