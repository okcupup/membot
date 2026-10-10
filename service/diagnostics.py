"""Read-only timelines, reviewed candidate cases and safe recorded replay.

Provider/AgentLoop imports are deliberately local to reproduce_recording().
API processes can use readers without constructing any execution runtime.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from membot.agent.diagnostics import json_bytes
from membot.agent.redaction import redact_data

MAX_BUNDLE_BYTES = 8 * 1_048_576
TERMINAL = {"SUCCEEDED", "FAILED", "TIMEOUT"}
_EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
_PHONE = re.compile(r"(?<!\w)(?:\+86[\s-]?)?1[3-9]\d{9}(?!\w)")


def _candidate_redaction(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _candidate_redaction(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_candidate_redaction(item) for item in value]
    if isinstance(value, str):
        return _PHONE.sub("[REDACTED_PHONE]", _EMAIL.sub("[REDACTED_EMAIL]", value))
    return value


class ReplayUnavailableError(ValueError):
    """A partial, expired or unsupported recording cannot be reproduced."""


def _time(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _elapsed(start: Any, end: Any) -> float | None:
    first, last = _time(start), _time(end)
    return max(0.0, (last - first).total_seconds() * 1000) if first and last else None


async def read_recording(repository: Any, invocation_id: str) -> dict[str, Any]:
    from membot.service.models import invocation_payload

    invocation = await repository.get_invocation(invocation_id)
    if invocation is None:
        raise KeyError(invocation_id)
    events: list[dict[str, Any]] = []
    cursor, size, limited = 0, 0, False
    while len(events) < 2048:
        page = await repository.events(invocation_id, after=cursor, limit=100)
        for event in page:
            size += len(json_bytes(event))
            if size > MAX_BUNDLE_BYTES - 65_536:
                limited = True
                break
            events.append(event)
        if limited or len(page) < 100:
            break
        cursor = page[-1]["sequence"]
    else:
        limited = True
    # Recheck status after a live read; never change the invocation's IDs.
    current = await repository.get_invocation(invocation_id)
    sequences = [event["sequence"] for event in events]
    gaps = bool(sequences and sequences != list(range(1, sequences[-1] + 1)))
    return redact_data({
        "schema_version": 1, "kind": "invocation_recording",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "invocation": invocation_payload(current or invocation), "events": events,
        "recording_limited": limited, "retention_gaps": gaps or not events,
    })


def timing_summary(recording: dict[str, Any]) -> dict[str, Any]:
    task, events = recording["invocation"], recording["events"]
    totals = {"llm_ms": 0.0, "tool_ms": 0.0, "history_read_ms": 0.0,
              "history_commit_ms": 0.0, "outbox_ms": 0.0}
    failures, opened, counted = [], {}, set()
    for event in events:
        category, step = event["event_type"].upper(), event.get("step")
        key = (category, event.get("span_id"))
        if category in {"LLM", "TOOL", "OUTBOX"}:
            if step == "start":
                opened[key] = event
            elif step in {"end", "error"}:
                opened.pop(key, None)
        duration = event.get("duration_ms")
        if duration is not None and key not in counted:
            target = {"LLM": "llm_ms", "TOOL": "tool_ms", "OUTBOX": "outbox_ms"}.get(category)
            if target and step in {"end", "error"}:
                totals[target] += duration
                counted.add(key)
            if category == "HISTORY":
                target = "history_read_ms" if step == "read" else "history_commit_ms"
                totals[target] += duration
        if event.get("error_code") or step == "error":
            failures.append({key: event.get(key) for key in (
                "sequence", "event_type", "step", "span_id", "tool_call_id", "error_code", "duration_ms",
            )})
    execution = _elapsed(task.get("startedAt"), task.get("finishedAt"))
    accounted = totals["llm_ms"] + totals["tool_ms"] + totals["history_read_ms"] + totals["history_commit_ms"]
    return {
        "status": task["status"], "error_code": task.get("errorCode"),
        "queue_ms": _elapsed(task.get("submittedAt"), task.get("startedAt") or task.get("finishedAt")),
        "execution_ms": execution, **{key: round(value, 3) for key, value in totals.items()},
        "other_execution_ms": round(max(0.0, execution - accounted), 3) if execution is not None else None,
        "failure_nodes": failures,
        "incomplete_spans": [{"event_type": key[0], "span_id": key[1], "sequence": event.get("sequence")}
                             for key, event in opened.items()],
        "retention_gaps": recording.get("retention_gaps", False),
        "recording_limited": recording.get("recording_limited", False),
    }


def render_timeline(recording: dict[str, Any], *, show_payload: bool = False) -> str:
    import json

    task = recording["invocation"]
    lines = [f"Invocation {task['invocationId']}  {task['status']}  trace={task['traceId']}",
             f"original request={task['requestId']}  session={task['sessionId']}"]
    for event in recording["events"]:
        payload = event.get("payload", {})
        label = f"{event['event_type'].upper()}.{event.get('step', 'recorded')}"
        duration = event.get("duration_ms")
        elapsed = f"{duration:.3f} ms" if duration is not None else "-"
        detail = payload.get("name") or event.get("error_code") or ""
        if event.get("tool_call_id"):
            detail += f" call={event['tool_call_id']}"
        if payload.get("truncated"):
            detail += f" [truncated: {payload['original_size']} UTF-8 bytes]"
        lines.append(f"{event.get('sequence', '?'):>4}  {event.get('time', '')}  {label:<18} {elapsed:>13}  {detail}")
        if show_payload:
            lines.append("      " + json.dumps(payload, ensure_ascii=False))
    lines.append("timing: " + json.dumps(timing_summary(recording), ensure_ascii=False))
    return "\n".join(lines)


def _payload(recording: dict[str, Any], category: str, step: str) -> dict[str, Any] | None:
    return next((event.get("payload", {}) for event in recording["events"]
                 if event["event_type"].upper() == category and event.get("step") == step), None)


def candidate_case(recording: dict[str, Any]) -> dict[str, Any]:
    task = recording["invocation"]
    if task["status"] not in {"FAILED", "TIMEOUT"}:
        raise ValueError("only failed or timed-out invocations can be exported as failure candidates")
    accepted = _payload(recording, "ACCEPTED", "committed") or {}
    context = _payload(recording, "CONTEXT", "snapshot")
    configured = _payload(recording, "CONFIG", "snapshot") or accepted
    # A failure answer is an observation, never an expected/golden answer.
    return _candidate_redaction(redact_data({
        "schema_version": 1, "kind": "candidate_regression_case",
        "case_id": f"candidate-{task['invocationId']}",
        "source": {key: task.get(key) for key in ("invocationId", "traceId", "requestId", "sessionId")},
        "input": accepted.get("input"), "input_truncated": accepted.get("truncated", False),
        "history_snapshot": context.get("history") if context else None,
        "context_complete": bool(context and not context.get("truncated")),
        "config": configured.get("config"),
        "config_version": (configured.get("config") or {}).get("configVersion"),
        "observed": {"status": task["status"], "error_code": task.get("errorCode")},
        "failure_summary": {"message": task.get("errorMessage"), **timing_summary(recording)},
        "observed_tools": sorted({event["payload"]["name"] for event in recording["events"]
                                  if event["event_type"] == "TOOL" and "name" in event["payload"]}),
        "expected": {"confirmed": False, "status": None, "answer": None,
                     "tool_constraints": {"allowed_tools": None, "forbidden_tools": None}},
        "review_required": True,
        "recording_limited": recording.get("recording_limited", False),
        "retention_gaps": recording.get("retention_gaps", False),
        "expires_at": min((event["expires_at"] for event in recording["events"] if event.get("expires_at")), default=None),
    }))


def confirm_expected(candidate: dict[str, Any], *, status: str, allowed_tools: list[str],
                     forbidden_tools: list[str], answer: str | None = None) -> dict[str, Any]:
    if status not in TERMINAL or set(allowed_tools) & set(forbidden_tools):
        raise ValueError("expected status/tool constraints are invalid")
    if not candidate.get("context_complete") and candidate["observed"]["error_code"] != "QUEUE_TIMEOUT":
        raise ValueError("review requires an intact input/context snapshot")
    if candidate.get("input_truncated") or not candidate.get("input"):
        raise ValueError("review requires an intact input")
    candidate_hash = hashlib.sha256(json_bytes(candidate)).hexdigest()
    result = copy.deepcopy(candidate)
    result["expected"] = {
        "confirmed": True, "status": status, "answer": _candidate_redaction(redact_data(answer)),
        "tool_constraints": {"allowed_tools": allowed_tools, "forbidden_tools": forbidden_tools},
        "candidate_hash": candidate_hash,
    }
    result["review_required"] = False
    # Explicit confirmation still does not register it in the standard suite.
    return result


async def reproduce_recording(recording: dict[str, Any]) -> dict[str, Any]:
    """Replay only recorded outputs in a fresh, disposable AgentLoop.

    Every Tool is a fixture adapter, including exec/write_file/network tools.
    No normal tool, provider, database, gateway, MCP, cron or subagent runs.
    """
    from membot.agent.execution_result import ExecutionOutcome, ExecutionResult
    from membot.agent.loop import AgentLoop
    from membot.agent.tools.base import Tool
    from membot.agent.tools.registry import ToolRegistry
    from membot.bus.queue import MessageBus
    from membot.providers.base import LLMProvider, LLMResponse, ToolCallRequest

    task = recording["invocation"]
    if task["status"] not in TERMINAL or recording.get("recording_limited") or recording.get("retention_gaps"):
        raise ReplayUnavailableError("replay needs a terminal, retained, complete recording")
    if task.get("errorCode") in {"WORKER_LOST", "CANCELLED"}:
        raise ReplayUnavailableError("Worker interruption/cancellation has no reproducible provider result; inspect the recorded timeline")
    context = _payload(recording, "CONTEXT", "snapshot")
    config = (_payload(recording, "CONFIG", "snapshot") or {}).get("config", {})
    if not context or context.get("truncated") or not config:
        raise ReplayUnavailableError("recorded context/config is absent or truncated")
    relevant = [event for event in recording["events"] if event["event_type"] in {"LLM", "TOOL"}]
    if len({event.get("attempt", 0) for event in relevant}) > 1:
        raise ReplayUnavailableError("multiple execution attempts require separate recordings")
    required = [event for event in recording["events"] if event["event_type"] in {"LLM", "TOOL", "CONTEXT", "CONFIG"}]
    now = datetime.now(timezone.utc)
    if any(event.get("payload", {}).get("truncated") or
           (_time(event.get("expires_at")) and _time(event["expires_at"]) <= now) for event in required):
        raise ReplayUnavailableError("required provider/tool payload is truncated or expired")
    starts = [event for event in relevant if event["event_type"] == "LLM" and event["step"] == "start"]
    endings = {event["span_id"]: event for event in relevant if event["step"] in {"end", "error"}}
    tool_starts = [event for event in relevant if event["event_type"] == "TOOL" and event["step"] == "start"]
    if not starts or any(event["span_id"] not in endings for event in [*starts, *tool_starts]):
        raise ReplayUnavailableError("an interrupted span has no recorded result")
    violations: list[str] = []
    calls = {"provider": 0, "tool": 0}

    class RecordedProvider(LLMProvider):
        def get_default_model(self) -> str:
            return config.get("model", "recorded")

        async def chat(self, messages, **kwargs):
            index = calls["provider"]
            calls["provider"] += 1
            if index >= len(starts) or redact_data(messages) != starts[index]["payload"].get("messages"):
                violations.append("provider request does not match the recording")
                raise RuntimeError(violations[-1])
            payload = endings[starts[index]["span_id"]]["payload"]
            if payload.get("failure_kind") == "timeout":
                raise asyncio.TimeoutError("recorded LLM timeout")
            if payload.get("failure_kind") == "cancelled":
                await asyncio.Event().wait()
            if "response" not in payload:
                raise RuntimeError(payload.get("message", "recorded provider failure"))
            response = payload["response"]
            return LLMResponse(
                content=response.get("content"), finish_reason=response.get("finish_reason", "stop"),
                usage=response.get("usage", {}),
                tool_calls=[ToolCallRequest(**call) for call in response.get("tool_calls", [])],
            )

    class RecordedTool(Tool):
        description = "Recorded response fixture; external operations are disabled."
        parameters = {"type": "object", "properties": {}}

        def __init__(self, name: str):
            self._name = name

        @property
        def name(self) -> str:
            return self._name

        def validate_params(self, params):
            return []

        async def execute(self, **params):
            index = calls["tool"]
            calls["tool"] += 1
            if index >= len(tool_starts) or self.name != tool_starts[index]["payload"].get("name") or \
               redact_data(params) != tool_starts[index]["payload"].get("arguments"):
                violations.append("tool request does not match the recording")
                raise RuntimeError(violations[-1])
            payload = endings[tool_starts[index]["span_id"]]["payload"]
            if payload.get("failure_kind") in {"timeout", "cancelled"}:
                await asyncio.Event().wait()
            return payload.get("result", "Error: " + payload.get("message", "recorded tool failure"))

    class RecordedRegistry(ToolRegistry):
        async def execute(self, name, params):
            adapter = self.get(name)
            if adapter is None:
                violations.append("tool has no recorded adapter")
                raise ReplayUnavailableError(violations[-1])
            # Results were recorded after normal ToolRegistry error wrapping.
            # Return that exact visible result, without adding a second hint.
            return await adapter.execute(**params)

    with TemporaryDirectory(prefix="membot-replay-") as temporary:
        agent = AgentLoop(
            bus=MessageBus(), provider=RecordedProvider(), workspace=Path(temporary),
            model=config.get("model"), max_iterations=config.get("maxIterations", 40),
            tool_timeout=0.01, enable_consolidation=False, enable_subagents=False,
            enable_cron=False, restrict_to_workspace=True,
        )
        agent.tools = RecordedRegistry()
        for name in {event["payload"]["name"] for event in tool_starts}:
            agent.tools.register(RecordedTool(name))
        try:
            if task.get("errorCode") == "EXECUTION_TIMEOUT":
                agent.tool_timeout = None
                try:
                    result = await asyncio.wait_for(agent._run_agent_loop(copy.deepcopy(context["messages"])), 0.02)
                except asyncio.TimeoutError:
                    result = ExecutionResult.failure(ExecutionOutcome.TIMEOUT, error_code="EXECUTION_TIMEOUT")
            else:
                result = await asyncio.wait_for(agent._run_agent_loop(copy.deepcopy(context["messages"])), 2.0)
        finally:
            await agent.shutdown()
        if violations or calls["provider"] != len(starts) or calls["tool"] != len(tool_starts):
            raise ReplayUnavailableError("; ".join(violations) or "recording was not fully consumed")
        status = "SUCCEEDED" if result.technical_success else "TIMEOUT" if result.outcome is ExecutionOutcome.TIMEOUT else "FAILED"
        final_payload = _payload(recording, "FINAL", "end") or {}
        return {
            "mode": "recorded_responses", "external_operations": False,
            "invocationId": task["invocationId"], "traceId": task["traceId"],
            "requestId": task["requestId"],
            "workspace": "temporary_discarded", "status": status, "error_code": result.error_code,
            "final": redact_data(result.final_content), "calls": calls,
            "matches_recorded_status": status == task["status"] and result.error_code == task.get("errorCode"),
            "matches_recorded_final": redact_data(result.final_content) == final_payload.get("content") if result.technical_success else None,
        }
