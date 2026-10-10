"""Promote an explicitly reviewed diagnostic candidate into a runnable Case."""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any

from membot.agent.redaction import redact_data
from membot.service.diagnostics import _candidate_redaction, candidate_case

from .schema import TERMINAL, Case, digest


def _events(recording: dict[str, Any], kind: str, step: str | None = None) -> list[dict[str, Any]]:
    return [event for event in recording.get("events", [])
            if str(event.get("event_type", "")).upper() == kind.upper()
            and (step is None or event.get("step") == step)]


def _accepted_input(candidate: dict[str, Any], recording: dict[str, Any]) -> str:
    value = candidate.get("input")
    if value is None:
        accepted = _events(recording, "ACCEPTED")
        value = accepted[0].get("payload", {}).get("input") if accepted else None
    if isinstance(value, dict):
        value = value.get("message", value.get("content"))
    if not isinstance(value, str) or not value.strip():
        raise ValueError("candidate must contain an intact textual input")
    return value


def _recorded_provider(recording: dict[str, Any], error_code: str | None) -> list[dict[str, Any]]:
    plans = []
    for event in _events(recording, "LLM"):
        payload = event.get("payload", {})
        if payload.get("truncated"):
            raise ValueError("LLM evidence is truncated and cannot become a runnable fixture")
        response = payload.get("response")
        if isinstance(response, dict):
            plans.append({"content": response.get("content"),
                          "finish_reason": response.get("finish_reason", "stop"),
                          "tool_calls": response.get("tool_calls", [])})
        elif event.get("step") == "error":
            if payload.get("failure_kind") in {"timeout", "cancelled"}:
                plans.append({"hang": True})
            else:
                plans.append({"raise": payload.get("message", error_code or "recorded provider failure")})
    if not plans:
        raise ValueError("candidate has no complete LLM response fixture")
    return plans


def _recorded_tools(recording: dict[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], set[str]]:
    result: dict[str, list[dict[str, Any]]] = {}
    observed = set()
    endings = {}
    for event in _events(recording, "TOOL"):
        payload = event.get("payload", {})
        if payload.get("truncated"):
            raise ValueError("Tool evidence is truncated and cannot become a runnable fixture")
        if event.get("step") == "start":
            name = payload.get("name")
            if not isinstance(name, str) or not isinstance(payload.get("arguments", {}), dict):
                raise ValueError("Tool start is missing a name or arguments")
            observed.add(name)
        elif event.get("step") in {"end", "error"}:
            endings[event.get("span_id")] = payload
    for event in _events(recording, "TOOL", "start"):
        payload = event["payload"]
        ending = endings.get(event.get("span_id"))
        if ending is None:
            raise ValueError("Tool fixture has an incomplete span")
        result.setdefault(payload["name"], []).append({
            "arguments": copy.deepcopy(payload["arguments"]),
            "result": ending.get("result", "Error: " + str(ending.get("message", "recorded Tool error"))),
            "hang": ending.get("failure_kind") in {"timeout", "cancelled"},
        })
    return result, observed


def register_candidate(candidate: dict[str, Any], recording: dict[str, Any], expectation: dict[str, Any], *, case_id: str) -> Case:
    """Require a confirmation artifact and never infer an answer from failure output."""
    if candidate.get("kind") != "candidate_regression_case" or not case_id:
        raise ValueError("invalid candidate regression artifact")
    if expectation.get("kind") != candidate.get("kind") or expectation.get("case_id") != candidate.get("case_id"):
        raise ValueError("reviewed expectations must come from confirmation of this candidate")
    if expectation.get("source") != candidate.get("source"):
        raise ValueError("reviewed expectation references a different Invocation")
    confirmed = expectation.get("expected", expectation)
    if confirmed.get("confirmed") is not True:
        raise ValueError("expected outcome requires explicit human confirmation")
    if confirmed.get("candidate_hash") != digest(candidate):
        raise ValueError("candidate changed after expected fields were reviewed")
    if not expectation.get("reviewer") or not expectation.get("review_reason"):
        raise ValueError("registration requires reviewer identity and expected-outcome rationale")
    status = confirmed.get("status")
    if status not in TERMINAL:
        raise ValueError("confirmed expected status is required")
    source = candidate.get("source", {})
    invocation = recording.get("invocation", {})
    if invocation.get("status") not in {"FAILED", "TIMEOUT"}:
        raise ValueError("source Invocation is not a failure")
    if source.get("invocationId") and source["invocationId"] != invocation.get("invocationId"):
        raise ValueError("candidate and recording Invocation IDs do not match")
    derived = candidate_case(recording)
    if any(candidate.get(key) != derived.get(key) for key in
           ("source", "input", "history_snapshot", "config_version", "observed")):
        raise ValueError("candidate input, history or identity differs from its recording")
    if candidate.get("recording_limited") or candidate.get("retention_gaps") or not candidate.get("context_complete") \
       or recording.get("recording_limited") or recording.get("retention_gaps"):
        raise ValueError("candidate recording is incomplete, expired or truncated")
    required_events = [event for event in recording.get("events", [])
                       if event.get("event_type", "").upper() in {"LLM", "TOOL", "CONTEXT", "CONFIG"}]
    if invocation.get("errorCode") in {"WORKER_LOST", "CANCELLED"}:
        raise ValueError("interruption without a recorded outcome is diagnostic only")
    if not _events(recording, "LLM", "start") or not _events(recording, "CONFIG", "snapshot"):
        raise ValueError("complete LLM start and configuration snapshots are required")
    if any(event.get("payload", {}).get("truncated") or event.get("expires_at") and
           datetime.fromisoformat(event["expires_at"]) <= datetime.now(timezone.utc)
           for event in required_events):
        raise ValueError("recorded execution evidence is expired or truncated")
    if len({event.get("attempt", 0) for event in required_events}) > 1:
        raise ValueError("multiple attempts cannot become one replay fixture")
    endings = {(event["event_type"].upper(), event.get("span_id")) for event in required_events
               if event.get("step") in {"end", "error"}}
    if any(event.get("step") == "start" and (not event.get("span_id") or
           (event["event_type"].upper(), event["span_id"]) not in endings) for event in required_events):
        raise ValueError("execution contains an incomplete span")
    recording = _candidate_redaction(redact_data(recording))
    input_text = _accepted_input(candidate, recording)
    if candidate.get("input_truncated"):
        raise ValueError("candidate input is truncated")
    allowed = confirmed.get("tool_constraints", {}).get("allowed_tools")
    forbidden = confirmed.get("tool_constraints", {}).get("forbidden_tools")
    required = expectation.get("required_tools")
    expected_calls = expectation.get("expected_calls")
    if not all(isinstance(value, list) for value in (allowed, forbidden, required, expected_calls)):
        raise ValueError("allowed/forbidden/required Tools and key-argument/order expectations must be explicit")
    if set(allowed) & set(forbidden) or not set(required) <= set(allowed):
        raise ValueError("invalid confirmed Tool constraints")
    for call in expected_calls:
        if call.get("name") not in allowed:
            raise ValueError("expected Tool call is outside the confirmed allowlist")
    observed_status = invocation.get("status") or candidate.get("observed", {}).get("status")
    error_code = expectation.get("error_code")
    if status in {"FAILED", "TIMEOUT"} and not error_code and not expectation.get("result_assertions"):
        raise ValueError("a failed Case needs an explicit expected error or result assertion")
    answer = confirmed.get("answer")
    assertions = copy.deepcopy(expectation.get("result_assertions", []))
    if answer is not None:
        if not isinstance(answer, str):
            raise ValueError("confirmed answer must be text")
        assertions.append({"turn": "t1", "kind": "equals", "value": answer})
    if error_code and status in {"FAILED", "TIMEOUT"}:
        assertions.append({"turn": "t1", "kind": "error_code", "value": error_code})
    if not assertions and not expectation.get("judge_rubric"):
        raise ValueError("registration requires a confirmed answer, assertions or Judge rubric")
    providers = _recorded_provider(recording, error_code)
    recorded_tools, observed_tools = _recorded_tools(recording)
    tool_schemas = {}
    for event in _events(recording, "LLM", "start"):
        for tool in event.get("payload", {}).get("tools", []):
            function = tool.get("function", {})
            if function.get("name") in observed_tools:
                tool_schemas[function["name"]] = function.get("parameters", {})
    if observed_tools - tool_schemas.keys():
        raise ValueError("recorded Tool schema is missing")
    # Observed names are fixtures only. The expected contract comes from the
    # explicit review artifact, so a bad trace cannot become its own oracle.
    history = candidate.get("history_snapshot")
    if not isinstance(history, list):
        raise ValueError("history snapshot must be a list")
    category = expectation.get("category") or (
        "exception_recovery" if status != "SUCCEEDED" else "tool_calling" if allowed else "basic"
    )
    turn = {"id": "t1", "input": input_text, "session": "candidate",
            "expected_status": status, "provider": providers}
    fixtures = {"recorded_tools": recorded_tools, "tool_schemas": tool_schemas,
                "observed_tools": sorted(observed_tools),
                "source_invocation_id": invocation.get("invocationId")}
    case = Case(id=case_id, category=category, version=int(expectation.get("version", 1)),
        description=expectation.get("description", f"Reviewed candidate from {invocation.get('invocationId')}"),
        input=input_text, history=history, fixtures=fixtures, expected_status=status, turns=[turn],
        tools={"required_tools": required, "allowed_tools": allowed, "forbidden_tools": forbidden,
               "expected_calls": expected_calls, "order": expectation.get("tool_order", "exact")},
        assertions=assertions, judge_rubric=expectation.get("judge_rubric"),
        business_task=bool(expectation.get("business_task", False)), real_model=False,
        budget=expectation.get("budget", {}),
        provenance={"kind": "reviewed_failure_candidate", "source_invocation_id": invocation.get("invocationId"),
                    "candidate_hash": digest(candidate), "observed_status": observed_status,
                    "observed_tools": sorted(observed_tools), "reviewer": expectation["reviewer"],
                    "review_reason": expectation["review_reason"], "config_version": candidate.get("config_version")})
    return case
