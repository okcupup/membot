"""Sanitizers for session-style message persistence."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Iterable


RUNTIME_CONTEXT_PREFIX = "[Runtime Context"
DEFAULT_TOOL_RESULT_MAX_CHARS = 500


def sanitize_messages_for_storage(
    messages: Iterable[dict[str, Any]],
    *,
    skip: int = 0,
    tool_result_max_chars: int = DEFAULT_TOOL_RESULT_MAX_CHARS,
    now: Callable[[], datetime] = datetime.now,
) -> list[dict[str, Any]]:
    """Prepare LLM transcript messages for raw session storage.

    This mirrors the current JSONL persistence behavior and is shared with the
    PostgreSQL ConversationMemoryEngine adapter.
    """

    sanitized: list[dict[str, Any]] = []
    for message in list(messages)[skip:]:
        entry = dict(message)
        role = entry.get("role")
        content = entry.get("content")

        if role == "assistant" and not content and not entry.get("tool_calls"):
            continue

        if role == "tool" and isinstance(content, str) and len(content) > tool_result_max_chars:
            entry["content"] = content[:tool_result_max_chars] + "\n... (truncated)"
        elif role == "user":
            if isinstance(content, str) and content.startswith(RUNTIME_CONTEXT_PREFIX):
                continue
            if isinstance(content, list):
                entry["content"] = [_sanitize_content_part(part) for part in content]

        entry.setdefault("timestamp", now().isoformat())
        sanitized.append(entry)

    return sanitized


def _sanitize_content_part(part: Any) -> Any:
    if not isinstance(part, dict):
        return part
    image_url = part.get("image_url", {})
    if (
        part.get("type") == "image_url"
        and isinstance(image_url, dict)
        and image_url.get("url", "").startswith("data:image/")
    ):
        return {"type": "text", "text": "[image]"}
    return part
