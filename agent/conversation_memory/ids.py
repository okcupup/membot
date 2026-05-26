"""ID helpers for conversation memory sources and records."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Sequence


_SAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def safe_session_key(session_key: str) -> str:
    """Return a stable ID-safe representation of a session key."""
    safe = _SAFE_CHARS_RE.sub("_", session_key).strip("_")
    return safe or "session"


def build_turn_id(session_key: str, turn_index: int) -> str:
    """Build a sortable turn ID scoped by session."""
    return f"turn:{safe_session_key(session_key)}:{turn_index:06d}"


def build_segment_id(session_key: str, start_turn_index: int, end_turn_index: int) -> str:
    """Build a segment ID covering a contiguous turn range."""
    return f"segment:{safe_session_key(session_key)}:{start_turn_index:06d}-{end_turn_index:06d}"


def build_record_id(
    kind: str,
    source_id: str,
    extractor_version: str | None = None,
    payload: dict[str, Any] | None = None,
) -> str:
    """Build a stable record ID for a memory record."""
    version = extractor_version or "default"
    basis = {
        "kind": kind,
        "source_id": source_id,
        "extractor_version": version,
        "payload": payload or {},
    }
    digest = hashlib.sha256(
        json.dumps(basis, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:12]
    return f"{kind}:{source_id}:{version}:{digest}"


def infer_next_turn_index(messages: Sequence[dict[str, Any]], session_key: str) -> int:
    """Infer the next sortable turn index for a session.

    New data carries turn IDs. Legacy data may not, so fall back to counting
    user messages as a rough turn count.
    """
    safe_key = safe_session_key(session_key)
    turn_re = re.compile(rf"^turn:{re.escape(safe_key)}:(\d+)$")
    max_index = 0
    for message in messages:
        turn_id = message.get("turn_id")
        if not isinstance(turn_id, str):
            continue
        match = turn_re.match(turn_id)
        if match:
            max_index = max(max_index, int(match.group(1)))

    if max_index:
        return max_index + 1

    turns_without_ids = sum(1 for message in messages if message.get("role") == "user")
    return turns_without_ids + 1
