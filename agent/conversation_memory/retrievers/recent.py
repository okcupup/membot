"""Recent raw-message retriever."""

from __future__ import annotations

from pathlib import Path

from membot.agent.conversation_memory.ids import build_record_id
from membot.agent.conversation_memory.models import MemoryQuery, MemoryRecord, RetrievedMemory
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
        for index, message in enumerate(sliced):
            payload = {
                "turn_id": message.get("turn_id") if isinstance(message.get("turn_id"), str) else f"recent:{index:06d}",
                "messages": [dict(message)],
                "timestamp": message.get("timestamp"),
            }
            results.append(RetrievedMemory(record=MemoryRecord(
                kind=RAW_MESSAGE_KIND,
                payload=payload,
                session_key=query.session_key,
                record_id=build_record_id(RAW_MESSAGE_KIND, payload["turn_id"], payload=payload),
                source_kind="session_turn",
                source_id=payload["turn_id"],
            )))
        return results
