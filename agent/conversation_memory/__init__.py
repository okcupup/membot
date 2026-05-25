"""Conversation memory lifecycle abstractions.

This package is not wired into AgentLoop yet. It provides the first pass of
the current conversation-history lifecycle while leaving room for summaries,
Redis storage, and vector retrieval.
"""

from membot.agent.conversation_memory.engine import ConversationMemoryEngine
from membot.agent.conversation_memory.extractors.base import MemoryExtractor
from membot.agent.conversation_memory.extractors.graph import GraphExtractionExtractor
from membot.agent.conversation_memory.extractors.raw_message import (
    RawMessageExtractor,
    build_raw_message_record,
)
from membot.agent.conversation_memory.extractors.summarization import SummarizationExtractor
from membot.agent.conversation_memory.managers.base import MemoryManager, MemoryPolicy
from membot.agent.conversation_memory.models import (
    MemoryQuery,
    MemoryRecord,
    MemoryScope,
    MemorySource,
    RetrievedMemory,
)
from membot.agent.conversation_memory.retrievers.base import MemoryRetriever
from membot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND
from membot.agent.conversation_memory.stores.base import MemoryStore

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
    "SummarizationExtractor",
    "build_raw_message_record",
]
