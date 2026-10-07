"""Small persistence-boundary redaction for common credential formats."""

from __future__ import annotations

import re


_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|password|secret)"
    r"(\s*[:=]\s*)([\"']?)([^\s,;\"']+)"
)
_BEARER = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{8,}")
_TOKEN = re.compile(r"\b(?:sk-[A-Za-z0-9_-]{12,}|xox[baprs]-[A-Za-z0-9-]{12,})\b")


def redact_text(value: str | None) -> str | None:
    """Mask common key/value, bearer, and provider token forms."""

    if value is None:
        return None
    redacted = _ASSIGNMENT.sub(r"\1\2[REDACTED]", value)
    redacted = _BEARER.sub("Bearer [REDACTED]", redacted)
    return _TOKEN.sub("[REDACTED]", redacted)
