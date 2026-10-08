"""Redaction shared by diagnostic storage, logs and exports without DB imports."""

from __future__ import annotations

import json
import re
from typing import Any

_SENSITIVE_KEY = re.compile(
    r"(?i)(?:api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|"
    r"password|passwd|secret|credential|cookie|private[_-]?key|client[_-]?secret|"
    r"database[_-]?url|redis[_-]?url|^token$)"
)
_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|password|"
    r"passwd|secret|token|cookie|client[_-]?secret)([\"']?\s*[:=]\s*)"
    r"(\"[^\"]*\"|'[^']*'|[^\s,;}]+)"
)
_BEARER = re.compile(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+")
_TOKEN = re.compile(r"\b(?:sk-[A-Za-z0-9_-]{12,}|xox[baprs]-[A-Za-z0-9-]{12,})\b")
_URL_PASSWORD = re.compile(r"(\b[a-z][a-z0-9+.-]*://[^\s/:@]+:)[^\s/@]+(@)", re.I)
_PEM = re.compile(r"-----BEGIN [^-]*PRIVATE KEY-----.*?(?:-----END [^-]*PRIVATE KEY-----|$)", re.S)
_THINK = re.compile(r"<(?:think|thinking|analysis)\b[^>]*>.*?(?:</(?:think|thinking|analysis)>|$)", re.I | re.S)
_PRIVATE_FIELDS = {"reasoning_content", "reasoning", "thinking_blocks", "chain_of_thought"}


def redact_text(value: str | None) -> str | None:
    """Mask common key/value, bearer, and provider token forms."""

    if value is None:
        return None
    redacted = _PEM.sub("[REDACTED]", value)
    # Mask multiword auth values before assignment masking consumes the
    # 'Bearer'/'Basic' prefix and leaves an opaque token behind.
    redacted = _BEARER.sub(r"\1 [REDACTED]", redacted)
    def mask_assignment(match: re.Match) -> str:
        value = match.group(3)
        quote = value[0] if value.startswith(('"', "'")) else ""
        return match.group(1) + match.group(2) + quote + "[REDACTED]" + quote

    redacted = _ASSIGNMENT.sub(mask_assignment, redacted)
    redacted = _URL_PASSWORD.sub(r"\1[REDACTED]\2", redacted)
    return _TOKEN.sub("[REDACTED]", redacted)


def visible_text(value: str | None) -> str | None:
    """Exclude model-private thinking from diagnostic records."""
    return _THINK.sub("", value).strip() if value is not None else None


def redact_data(value: Any) -> Any:
    """Copy JSON data, recursively masking credentials and omitting reasoning."""
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if _SENSITIVE_KEY.search(str(key)) else redact_data(item)
            for key, item in value.items() if str(key).lower() not in _PRIVATE_FIELDS
            and not (str(key) == "type" and item in ("thinking", "redacted_thinking", "reasoning"))
        }
    if isinstance(value, (list, tuple)):
        return [redact_data(item) for item in value if not (
            isinstance(item, dict) and item.get("type") in ("thinking", "redacted_thinking", "reasoning")
        )]
    if isinstance(value, str):
        # Tool arguments sometimes arrive as a JSON string rather than a dict.
        if value.lstrip().startswith(("{", "[")):
            try:
                return json.dumps(redact_data(json.loads(value)), ensure_ascii=False, sort_keys=True)
            except (ValueError, TypeError):
                pass
        return redact_text(visible_text(value))
    if value is None or isinstance(value, (int, float, bool)):
        return value
    return redact_text(str(value))
