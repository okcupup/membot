"""Recent raw-message retriever."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from membot.agent.conversation_memory.extractors.raw_message import build_raw_message_record
from membot.agent.conversation_memory.models import MemoryQuery, MemorySource, RetrievedMemory
from membot.agent.conversation_memory.retrievers.base import MemoryRetriever
from membot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND
from membot.session.manager import SessionManager


class RecentMessageRetriever(MemoryRetriever):
    """Retrieve recent session messages with current get_history alignment."""

    def __init__(self, workspace: Path, session_manager: SessionManager | None = None):
        self.sessions = session_manager or SessionManager(workspace)

    async def retrieve(self, query: MemoryQuery) -> list[RetrievedMemory]:
        if not query.session_key:
            return []

        session = self.sessions.get_or_create(query.session_key)
        unconsolidated = session.messages[session.last_consolidated:]
        sliced = unconsolidated[-query.limit:] if query.limit else list(unconsolidated)

        for index, message in enumerate(sliced):
            if message.get("role") == "user":
                sliced = sliced[index:]
                break

        results: list[RetrievedMemory] = []
        for message in sliced:
            source = MemorySource(
                kind=RAW_MESSAGE_KIND,
                payload=dict(message),
                session_key=query.session_key,
                source_id=query.session_key,
            )
            results.append(RetrievedMemory(record=build_raw_message_record(message, source=source)))
        return results
