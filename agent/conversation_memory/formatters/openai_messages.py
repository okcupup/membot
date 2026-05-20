"""Format retrieved memory records as OpenAI-style chat messages."""

from __future__ import annotations

from typing import Any, Sequence

from nanobot.agent.conversation_memory.models import RetrievedMemory
from nanobot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND


class OpenAIMessageFormatter:
    """Convert raw-message memories to chat-completion message dicts."""

    def format(self, memories: Sequence[RetrievedMemory]) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        for memory in memories:
            record = memory.record
            if record.kind != RAW_MESSAGE_KIND:
                continue

            payload = record.payload
            entry: dict[str, Any] = {
                "role": payload["role"],
                "content": payload.get("content", ""),
            }
            for field in ("tool_calls", "tool_call_id", "name"):
                if field in payload:
                    entry[field] = payload[field]
            messages.append(entry)
        return messages
