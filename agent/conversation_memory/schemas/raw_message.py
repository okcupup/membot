"""Schema metadata for raw-message records."""

from __future__ import annotations

from typing import Any


RAW_MESSAGE_KIND = "raw_message"
REQUIRED_FIELDS = ("turn_id", "messages", "timestamp")
OPTIONAL_FIELDS = ("time_range",)


def is_raw_message_payload(payload: dict[str, Any]) -> bool:
    return all(field in payload for field in REQUIRED_FIELDS) and isinstance(payload.get("messages"), list)

