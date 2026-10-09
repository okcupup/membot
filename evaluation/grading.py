"""Deterministic gates and strict Tool contracts over actual visible evidence."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from .schema import TERMINAL, Case


def subset(expected: Any, actual: Any) -> bool:
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(key in actual and subset(value, actual[key])
                                               for key, value in expected.items())
    # Keep booleans distinct from integers in key argument assertions.
    return type(expected) is type(actual) and expected == actual


def tool_evidence(observation: dict) -> list[dict]:
    calls = []
    for invocation in observation["invocations"]:
        for event in invocation["events"]:
            if event["event_type"] == "TOOL" and event.get("step") == "start":
                payload = event.get("payload", {})
                calls.append({"turn": invocation["turn"], "name": payload.get("name"),
                              "arguments": payload.get("arguments"), "span_id": event.get("span_id")})
    return calls


def grade_tools(case: Case, observation: dict) -> dict:
    calls = tool_evidence(observation)
    expected = [call.model_dump() for call in case.tools.expected_calls]
    # Match occurrences per turn: interleaved Sessions have no global tool order.
    matched = []
    used = set()
    names_matched = 0
    for wanted in expected:
        name_matches = [index for index, call in enumerate(calls) if index not in used
                        and call["turn"] == wanted["turn"] and call["name"] == wanted["name"]]
        exact_matches = [index for index in name_matches
                         if subset(wanted["arguments"], calls[index]["arguments"])]
        if exact_matches:
            index = exact_matches[0]
            used.add(index)
            matched.append(index)
        else:
            matched.append(None)
    # Name-only recall has its own denominator and doesn't forgive wrong params.
    actual_names = Counter((call["turn"], call["name"]) for call in calls)
    for wanted in expected:
        key = wanted["turn"], wanted["name"]
        if actual_names[key]:
            names_matched += 1
            actual_names[key] -= 1
    reasons = []
    names = {call["name"] for call in calls}
    if not set(case.tools.required_tools) <= names:
        reasons.append("missing required Tool")
    if names - set(case.tools.allowed_tools):
        reasons.append("Tool outside allowlist")
    if names & set(case.tools.forbidden_tools):
        reasons.append("forbidden Tool executed")
    if any(index is None for index in matched):
        reasons.append("missing Tool call or incorrect key arguments")
    if case.tools.order == "exact" and len(calls) != len(expected):
        reasons.append("unexpected or missing call occurrence")
    for turn in case.turns:
        turn_matches = [index for wanted, index in zip(expected, matched, strict=True)
                        if wanted["turn"] == turn.id and index is not None]
        if turn_matches != sorted(turn_matches):
            reasons.append(f"incorrect Tool order for {turn.id}")
    return {"eligible": case.tools.evaluate, "passed": not reasons, "reasons": reasons,
            "calls": calls, "matched_calls": len(used), "observed_calls": len(calls),
            "expected_calls": len(expected), "name_matched_calls": names_matched}


def grade(case: Case, observation: dict, *, fault=None) -> dict:
    """Judge is a later gate; a model cannot override a failed deterministic gate."""
    observation = {**observation}
    checks = list(observation["contracts"])
    invocations = {row["turn"]: row for row in observation["invocations"]}
    for turn in case.turns:
        row = invocations.get(turn.id)
        status = row.get("status") if row else None
        if fault == "wrong_status" and status == "FAILED":
            status = "SUCCEEDED"
        checks.append({"name": f"status:{turn.id}", "passed": status == turn.expected_status,
                       "expected": turn.expected_status, "actual": status})
        if row:
            checks.append({"name": f"evidence:{turn.id}", "passed":
                bool(row.get("events")) and not row.get("harness_error")
                and not row.get("recording_limited") and not row.get("retention_gaps")
                and not any(event.get("payload", {}).get("truncated") for event in row["events"])})
            if status not in TERMINAL:
                checks.append({"name": f"terminal:{turn.id}", "passed": False})
    tools = grade_tools(case, observation)
    checks.append({"name": "tool_contract", "passed": tools["passed"], "reasons": tools["reasons"]})
    for assertion in case.assertions:
        row = invocations.get(assertion.turn, {})
        final = row.get("final") or ""
        kind, wanted = assertion.kind, assertion.value
        events = row.get("events", [])
        context = next((event["payload"] for event in events if event["event_type"] == "CONTEXT"), {})
        history = json.dumps(context.get("history", []), ensure_ascii=False)
        actual = final
        if kind == "equals":
            passed = final == wanted
        elif kind == "contains":
            passed = isinstance(wanted, str) and wanted in final
        elif kind == "not_contains":
            passed = isinstance(wanted, str) and wanted not in final
        elif kind == "json_equals":
            try:
                actual = json.loads(final)
                passed = actual == wanted
            except (ValueError, TypeError):
                passed = False
        elif kind in {"history_contains", "history_absent"}:
            actual = history
            passed = wanted in history if kind == "history_contains" else wanted not in history
        elif kind == "tool_result_contains":
            actual = [event.get("payload", {}).get("result", "") for event in events
                      if event["event_type"] == "TOOL" and event.get("step") in {"end", "error"}]
            passed = any(wanted in str(result) for result in actual)
        elif kind == "error_code":
            actual = row.get("error_code")
            passed = actual == wanted
        else:
            actual = row.get("delivery")
            passed = actual == wanted
        checks.append({"name": f"{kind}:{assertion.turn}", "passed": passed,
                       "expected": wanted, "actual": actual})
    deterministic_passed = all(check["passed"] for check in checks)
    # Real open-answer cases remain pending until a valid structured Judge verdict.
    needs_judge = observation["mode"] == "real" and bool(case.judge_rubric)
    observation.update(category=case.category, business_eligible=case.business_task,
                       checks=checks, tools=tools, deterministic_passed=deterministic_passed,
                       judge_required=needs_judge, judge=None,
                       passed=deterministic_passed and not needs_judge)
    return observation
