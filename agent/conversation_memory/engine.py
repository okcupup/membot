"""Composable engine for the conversation memory lifecycle."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from membot.agent.conversation_memory.extractors.base import MemoryExtractor
from membot.agent.conversation_memory.extractors.raw_message import RawMessageExtractor
from membot.agent.conversation_memory.extractors.summarization import SummarizationExtractor
from membot.agent.conversation_memory.formatters.openai_messages import OpenAIMessageFormatter
from membot.agent.conversation_memory.managers.base import MemoryManager
from membot.agent.conversation_memory.managers.simple import SimpleMessageManager
from membot.agent.conversation_memory.models import MemoryQuery, MemoryRecord, MemorySource
from membot.agent.conversation_memory.retrievers.base import MemoryRetriever
from membot.agent.conversation_memory.retrievers.recent import RecentMessageRetriever
from membot.agent.conversation_memory.sanitizer import (
    DEFAULT_TOOL_RESULT_MAX_CHARS,
    sanitize_messages_for_storage,
)
from membot.agent.conversation_memory.stores.base import MemoryStore
from membot.agent.conversation_memory.stores.jsonl import JsonlMessageStore
from membot.session.manager import SessionManager


class ConversationMemoryEngine:
    """Coordinate extract -> manage -> store and retrieve -> format."""

    def __init__(
        self,
        *,
        extractor: MemoryExtractor,
        manager: MemoryManager,
        store: MemoryStore,
        retriever: MemoryRetriever,
        formatter: OpenAIMessageFormatter,
        tool_result_max_chars: int = DEFAULT_TOOL_RESULT_MAX_CHARS,
    ):
        self.extractor = extractor
        self.manager = manager
        self.store = store
        self.retriever = retriever
        self.formatter = formatter
        self.tool_result_max_chars = tool_result_max_chars

    @classmethod
    def for_workspace(
        cls,
        workspace: Path,
        *,
        session_manager: SessionManager | None = None,
    ) -> "ConversationMemoryEngine":
        session_manager = session_manager or SessionManager(workspace)
        return cls(
            extractor=RawMessageExtractor(),
            manager=SimpleMessageManager(),
            store=JsonlMessageStore(workspace, session_manager=session_manager),
            retriever=RecentMessageRetriever(workspace, session_manager=session_manager),
            formatter=OpenAIMessageFormatter(),
        )

    async def get_history(self, session_key: str, limit: int) -> list[dict[str, Any]]:
        memories = await self.retriever.retrieve(MemoryQuery(session_key=session_key, limit=limit))
        return self.formatter.format(memories)

    async def save_turn(
        self,
        session_key: str,
        messages: Sequence[dict[str, Any]],
        skip: int,
    ) -> None:
        sanitized = sanitize_messages_for_storage(
            messages,
            skip=skip,
            tool_result_max_chars=self.tool_result_max_chars,
        )

        records: list[MemoryRecord] = []
        for message in sanitized:
            source = MemorySource(
                kind="session_message",
                payload=message,
                session_key=session_key,
                source_id=session_key,
            )
            records.extend(await self.extractor.extract(source))

        managed = self.manager.manage(records)
        await self.store.save_records(session_key, managed)

    async def clear(self, session_key: str) -> None:
        await self.store.clear(session_key)
