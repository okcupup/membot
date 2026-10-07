"""ConversationMemoryEngine adapters backed by PostgreSQL."""

from __future__ import annotations

from typing import Any

from membot.agent.conversation_memory.models import MemoryQuery, MemoryRecord, RetrievedMemory
from membot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND


class PostgresMessageRetriever:
    def __init__(self, repository: Any, owner_id: str):
        self.repository = repository
        self.owner_id = owner_id

    async def retrieve(self, query: MemoryQuery) -> list[RetrievedMemory]:
        if not query.session_key:
            return []
        session = await self.repository.load_session(
            self.owner_id, query.session_key, message_limit=query.limit,
        )
        if session is None:
            return []
        messages = session.messages[-query.limit:] if query.limit else session.messages
        for index, message in enumerate(messages):
            if message.get("role") == "user":
                messages = messages[index:]
                break
        record = MemoryRecord(
            kind=RAW_MESSAGE_KIND,
            payload={"turn_id": f"recent:{session.session_id}", "messages": messages},
            session_key=query.session_key,
            record_id=f"recent:{session.session_id}",
            source_kind="session_messages",
            source_id=session.session_id,
        )
        return [RetrievedMemory(record=record)] if messages else []


class PostgresMessageStore:
    """Adapter exposing durable operations needed by ConversationMemoryEngine."""

    durable = True

    def __init__(self, repository: Any, owner_id: str):
        self.repository = repository
        self.owner_id = owner_id

    def get_or_create_session(self, session_key: str) -> Any:
        raise RuntimeError("PostgreSQL sessions are loaded asynchronously; use get_history")

    def load(self, session_key: str) -> Any:
        raise RuntimeError("PostgreSQL sessions are loaded asynchronously; use get_history")

    def append_messages(self, session_key: str, messages: Any) -> None:
        raise RuntimeError("PostgreSQL turns must be committed with complete_invocation")

    def save_messages(self, session_key: str, messages: Any) -> None:
        raise RuntimeError("PostgreSQL turns must be committed with complete_invocation")

    def get_recent(self, session_key: str, limit: int) -> list[dict[str, Any]]:
        raise RuntimeError("PostgreSQL history is asynchronous")

    def list_sessions(self) -> list[dict[str, Any]]:
        raise RuntimeError("PostgreSQL sessions are asynchronous")

    async def save_records(self, session_key: str, records: Any) -> None:
        raise RuntimeError("Use ConversationMemoryEngine.finish_invocation")

    async def clear(self, session_key: str) -> None:
        raise RuntimeError("Use ConversationMemoryEngine.archive_and_finish_new")

    async def complete_invocation(
        self,
        invocation_id: str,
        result: Any,
        records: Any = (),
        *,
        execution_owner: str | None = None,
    ) -> Any:
        return await self.repository.complete_invocation(
            invocation_id, result, records,
            owner_id=self.owner_id,
            execution_owner=execution_owner,
        )

    async def archive_and_finish_new(
        self,
        invocation_id: str,
        *,
        execution_owner: str | None = None,
    ) -> Any:
        return await self.repository.archive_and_finish_new(
            invocation_id, owner_id=self.owner_id, execution_owner=execution_owner,
        )

    async def finish_interrupted(
        self,
        invocation_id: str,
        outcome: Any,
        *,
        error_message: str | None = None,
        execution_owner: str | None = None,
    ) -> Any:
        return await self.repository.finish_interrupted(
            invocation_id,
            outcome,
            error_message=error_message,
            owner_id=self.owner_id,
            execution_owner=execution_owner,
        )
