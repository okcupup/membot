"""Conversation memory lifecycle abstractions.

AgentLoop uses the JSONL engine for CLI operation and can be injected with the
PostgreSQL adapter for service execution. The abstractions also leave room for
summaries and other retrieval strategies.
"""

from membot.agent.conversation_memory.engine import ConversationMemoryEngine
from membot.agent.conversation_memory.extractors.base import MemoryExtractor
from membot.agent.conversation_memory.extractors.graph import GraphExtractionExtractor
from membot.agent.conversation_memory.extractors.raw_message import (
    RawMessageExtractor,
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
    "GraphExtractionExtractor",
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
]
