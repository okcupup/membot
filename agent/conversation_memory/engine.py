"""Composable engine for the conversation memory lifecycle."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from membot.agent.conversation_memory.extractors.base import MemoryExtractor
from membot.agent.conversation_memory.extractors.raw_message import RawMessageExtractor
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
from membot.agent.conversation_memory.ids import build_turn_id, infer_next_turn_index
from membot.agent.execution_result import ExecutionResult


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

    @classmethod
    def for_postgres(cls, repository: Any, *, owner_id: str) -> "ConversationMemoryEngine":
        """Build a stateless service engine over the PostgreSQL history store."""
        from membot.agent.persistence.memory import PostgresMessageRetriever, PostgresMessageStore

        return cls(
            extractor=RawMessageExtractor(),
            manager=SimpleMessageManager(),
            store=PostgresMessageStore(repository, owner_id),
            retriever=PostgresMessageRetriever(repository, owner_id),
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
        if not sanitized:
            return

        session = self.store.get_or_create_session(session_key)
        turn_index = infer_next_turn_index(session.messages, session_key)
        turn_id = build_turn_id(session_key, turn_index)
        source = MemorySource(
            kind="session_turn",
            payload={"turn_id": turn_id, "messages": sanitized},
            session_key=session_key,
            source_id=turn_id,
        )
        records: list[MemoryRecord] = []
        records.extend(await self.extractor.extract(source))

        managed = self.manager.manage(records)
        await self.store.save_records(session_key, managed)

    async def finish_invocation(
        self,
        invocation_id: str,
        session_key: str,
        result: ExecutionResult,
        messages: Sequence[dict[str, Any]],
        skip: int,
        *,
        execution_owner: str | None = None,
    ) -> Any:
        """Atomically store a successful turn and its terminal result."""
        if not hasattr(self.store, "complete_invocation"):
            raise RuntimeError("This memory store does not support durable invocations")
        records: list[MemoryRecord] = []
        if result.technical_success:
            sanitized = sanitize_messages_for_storage(
                messages,
                skip=skip,
                tool_result_max_chars=self.tool_result_max_chars,
            )
            if sanitized:
                turn_id = f"turn:{invocation_id}"
                source = MemorySource(
                    kind="session_turn",
                    payload={"turn_id": turn_id, "messages": sanitized},
                    session_key=session_key,
                    source_id=turn_id,
                )
                records.extend(await self.extractor.extract(source))
                records = list(self.manager.manage(records))
        return await self.store.complete_invocation(
            invocation_id, result, records, execution_owner=execution_owner,
        )

    async def archive_and_finish_new(
        self,
        invocation_id: str,
        *,
        execution_owner: str | None = None,
    ) -> Any:
        """Archive and reset a PostgreSQL Session as an ordered /new command."""
        archive = getattr(self.store, "archive_and_finish_new", None)
        if archive is None:
            raise RuntimeError("This memory store does not support ordered archival")
        return await archive(invocation_id, execution_owner=execution_owner)

    async def clear(self, session_key: str) -> None:
        await self.store.clear(session_key)
