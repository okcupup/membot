"""Store protocol."""

from __future__ import annotations

from typing import Any, Protocol, Sequence

from nanobot.agent.conversation_memory.models import MemoryRecord
from nanobot.session.manager import Session


class MemoryStore(Protocol):
    """Persist memory records."""

    def get_or_create_session(self, session_key: str) -> Session:
        """Get an existing session or create an empty one."""
        ...

    def load(self, session_key: str) -> Session | None:
        """Load a session if it exists."""
        ...

    def append_messages(self, session_key: str, messages: Sequence[dict[str, Any]]) -> None:
        """Append session-style message dictionaries."""
        ...

    def save_messages(self, session_key: str, messages: Sequence[dict[str, Any]]) -> None:
        """Replace and persist all messages for a session."""
        ...

    def get_recent(self, session_key: str, limit: int) -> list[dict[str, Any]]:
        """Return recent messages using current session history behavior."""
        ...

    def list_sessions(self) -> list[dict[str, Any]]:
        """List persisted sessions."""
        ...

    async def save_records(self, session_key: str, records: Sequence[MemoryRecord]) -> None:
        """Persist records for a session."""
        ...

    async def clear(self, session_key: str) -> None:
        """Clear records for a session."""
        ...
