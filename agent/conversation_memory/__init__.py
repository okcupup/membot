"""Conversation memory lifecycle abstractions.

This package is not wired into AgentLoop yet. It provides the first pass of
the current conversation-history lifecycle while leaving room for summaries,
Redis storage, and vector retrieval.
"""

from nanobot.agent.conversation_memory.engine import ConversationMemoryEngine
from nanobot.agent.conversation_memory.extractors.base import MemoryExtractor
from nanobot.agent.conversation_memory.extractors.raw_message import (
    RawMessageExtractor,
    build_raw_message_record,
)
from nanobot.agent.conversation_memory.managers.base import MemoryManager, MemoryPolicy
from nanobot.agent.conversation_memory.models import (
    MemoryQuery,
    MemoryRecord,
    MemoryScope,
    MemorySource,
    RetrievedMemory,
)
from nanobot.agent.conversation_memory.retrievers.base import MemoryRetriever
from nanobot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND
from nanobot.agent.conversation_memory.stores.base import MemoryStore

__all__ = [
    "ConversationMemoryEngine",
    "MemoryExtractor",
    "MemoryManager",
    "MemoryPolicy",
    "MemoryQuery",
    "MemoryRecord",
    "MemoryRetriever",
    "MemoryScope",
    "MemorySource",
    "MemoryStore",
    "RAW_MESSAGE_KIND",
    "RawMessageExtractor",
    "RetrievedMemory",
    "build_raw_message_record",
]
