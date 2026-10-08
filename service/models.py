"""Shared wire representations, without HTTP or execution client imports."""

from __future__ import annotations

from typing import Any

from membot.agent.persistence.repository import Invocation
from membot.agent.redaction import redact_data


def invocation_payload(invocation: Invocation) -> dict[str, Any]:
    payload = {
        "invocationId": invocation.invocation_id, "sessionId": invocation.session_id,
        "sessionKey": invocation.session_key, "sessionSeq": invocation.session_seq,
        "status": invocation.status.value, "requestId": invocation.request_id,
        "traceId": invocation.trace_id, "submittedAt": invocation.submitted_at.isoformat(),
        "startedAt": invocation.started_at.isoformat() if invocation.started_at else None,
        "finishedAt": invocation.finished_at.isoformat() if invocation.finished_at else None,
    }
    if invocation.result is not None:
        # Legacy status blobs may contain entire transcripts. Timeline readers
        # use controlled invocation_events instead, including after retention.
        payload["result"] = redact_data({key: value for key, value in invocation.result.items() if key != "messages"})
    if invocation.error_code:
        payload["errorCode"] = invocation.error_code
    if invocation.error_message:
        payload["errorMessage"] = redact_data(invocation.error_message)
    return payload
