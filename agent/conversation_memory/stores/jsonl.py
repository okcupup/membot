"""JSONL-backed raw-message store.

This adapter delegates to the existing SessionManager so the first integrated
version can preserve the current on-disk shape.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from membot.agent.conversation_memory.models import MemoryRecord
from membot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND
from membot.agent.conversation_memory.stores.base import MemoryStore
from membot.session.manager import Session, SessionManager


class JsonlMessageStore(MemoryStore):
    """Store raw-message records in the existing session JSONL files."""

    def __init__(self, workspace: Path, session_manager: SessionManager | None = None):
        self.sessions = session_manager or SessionManager(workspace)

    def get_or_create_session(self, session_key: str) -> Session:
        """Return an existing session or create an empty compatible Session."""
        return self.sessions.get_or_create(session_key)

    def load(self, session_key: str) -> Session | None:
        """Load a session from disk without creating a new one."""
        if session_key in self.sessions._cache:
            return self.sessions._cache[session_key]
        return self.sessions._load(session_key)

    def append_messages(self, session_key: str, messages: Sequence[dict[str, Any]]) -> None:
        """Append session-style messages and persist the session JSONL file."""
        session = self.get_or_create_session(session_key)
        for message in messages:
            session.messages.append(_normalize_session_message(message))
        session.updated_at = datetime.now()
        self.sessions.save(session)

    def save_messages(self, session_key: str, messages: Sequence[dict[str, Any]]) -> None:
        """Replace session messages and persist the session JSONL file."""
        session = self.get_or_create_session(session_key)
        session.messages = [_normalize_session_message(message) for message in messages]
        session.updated_at = datetime.now()
        self.sessions.save(session)

    def get_recent(self, session_key: str, limit: int) -> list[dict[str, Any]]:
        """Return recent history using the same behavior as Session.get_history."""
        session = self.get_or_create_session(session_key)
        return session.get_history(max_messages=limit)

    def list_sessions(self) -> list[dict[str, Any]]:
        """List persisted sessions using the existing SessionManager format."""
        return self.sessions.list_sessions()

    async def save_records(self, session_key: str, records: Sequence[MemoryRecord]) -> None:
        messages: list[dict[str, Any]] = []
        for record in records:
            if record.kind != RAW_MESSAGE_KIND:
                continue
            messages.append(_normalize_session_message(record.payload))
        if not messages:
            return
        self.append_messages(session_key, messages)

    async def clear(self, session_key: str) -> None:
        session = self.get_or_create_session(session_key)
        session.clear()
        self.sessions.save(session)
        self.sessions.invalidate(session_key)


def _normalize_session_message(message: dict[str, Any]) -> dict[str, Any]:
    message = dict(message)
    message.setdefault("timestamp", datetime.now().isoformat())
    return message
