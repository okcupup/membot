"""Format retrieved memory records as OpenAI-style chat messages."""

from __future__ import annotations

from typing import Any, Sequence

from membot.agent.conversation_memory.models import RetrievedMemory
from membot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND


class OpenAIMessageFormatter:
    """Convert raw-message memories to chat-completion message dicts."""

    def format(self, memories: Sequence[RetrievedMemory]) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        for memory in memories:
            record = memory.record
            if record.kind != RAW_MESSAGE_KIND:
                continue

            payload = record.payload
            for message in payload.get("messages", []):
                if not isinstance(message, dict) or "role" not in message:
                    continue
                entry: dict[str, Any] = {
                    "role": message["role"],
                    "content": message.get("content", ""),
                }
                for field in ("tool_calls", "tool_call_id", "name"):
                    if field in message:
                        entry[field] = message[field]
                messages.append(entry)
        return messages
