import copy

import pytest
from membot.evaluation.candidates import register_candidate
from membot.evaluation.pipeline import run_suite
from membot.evaluation.schema import Case
from membot.service.diagnostics import candidate_case, confirm_expected


def recording():
    invocation = {"invocationId": "inv-candidate", "traceId": "trace-candidate", "requestId": "request-candidate",
                  "sessionId": "session-candidate", "status": "FAILED", "errorCode": "PROVIDER_ERROR",
                  "errorMessage": "fixture provider failed"}
    events = [
        {"event_type": "accepted", "step": "committed", "payload": {"input": {"message": "candidate input"}}},
        {"event_type": "CONFIG", "step": "snapshot", "payload": {"config": {"configVersion": "test-v1"}}},
        {"event_type": "CONTEXT", "step": "snapshot", "payload": {"history": [], "messages": []}},
        {"event_type": "LLM", "step": "start", "payload": {"tools": []}, "span_id": "llm"},
        {"event_type": "LLM", "step": "error", "payload": {"message": "fixture provider failed"}, "span_id": "llm"},
    ]
    return {"invocation": invocation, "events": events, "recording_limited": False, "retention_gaps": False}


def test_candidate_registration_requires_confirmation_and_never_uses_observed_answer():
    data = recording()
    candidate = candidate_case(data)
    with pytest.raises(ValueError, match="confirmation"):
        register_candidate(candidate, data, candidate, case_id="candidate-provider-error")
    confirmed = confirm_expected(candidate, status="FAILED", allowed_tools=[], forbidden_tools=[])
    reviewed = confirmed | {"error_code": "PROVIDER_ERROR",
        "description": "Provider failure remains a regression contract.",
        "reviewer": "test-review-fixture", "review_reason": "Provider failures must be FAILED, never Final.",
        "required_tools": [], "expected_calls": []}
    case = register_candidate(candidate, data, reviewed,
        case_id="candidate-provider-error")
    assert isinstance(case, Case)
    assert case.expected_status == "FAILED"
    assert case.assertions[0].value == "PROVIDER_ERROR"
    assert case.turns[0].provider[0].get("raise") == "fixture provider failed"


def test_candidate_registration_rejects_incomplete_or_implicit_tool_contract():
    data = recording()
    candidate = candidate_case(data)
    confirmed = confirm_expected(candidate, status="FAILED", allowed_tools=[], forbidden_tools=[])
    changed = {**candidate, "context_complete": False}
    with pytest.raises(ValueError, match="changed"):
        register_candidate(changed, data, confirmed | {"error_code": "PROVIDER_ERROR"},
                           case_id="candidate-incomplete")


@pytest.mark.asyncio
async def test_reviewed_failure_candidate_runs_in_actual_agent_loop():
    data = recording()
    candidate = candidate_case(data)
    reviewed = confirm_expected(candidate, status="FAILED", allowed_tools=[], forbidden_tools=[])
    reviewed.update(reviewer="test-review-fixture", review_reason="Provider error must be FAILED.",
                    required_tools=[], expected_calls=[], error_code="PROVIDER_ERROR")
    case = register_candidate(candidate, data, reviewed, case_id="candidate-runnable-error")
    result = await run_suite([case])
    assert result["passed"]
    assert result["metrics"]["counts"]["expected_failed"] == 1
    assert result["results"][0]["invocations"][0]["status"] == "FAILED"


@pytest.mark.parametrize("fault", ["missing_span", "truncated", "expired", "context", "source", "history"])
def test_registration_rejects_partial_or_different_recordings(fault):
    data = recording()
    candidate = candidate_case(data)
    reviewed = confirm_expected(candidate, status="FAILED", allowed_tools=[], forbidden_tools=[])
    reviewed.update(reviewer="test-only-review", review_reason="Validate fail-closed registration.",
                    required_tools=[], expected_calls=[], error_code="PROVIDER_ERROR")
    broken = copy.deepcopy(data)
    if fault == "missing_span":
        broken["events"].pop()
    elif fault == "truncated":
        broken["events"][-1]["payload"]["truncated"] = True
    elif fault == "expired":
        broken["events"][-1]["expires_at"] = "2020-01-01T00:00:00+00:00"
    elif fault == "context":
        broken["events"] = [event for event in broken["events"] if event["event_type"] != "CONTEXT"]
    elif fault == "source":
        broken["invocation"]["traceId"] = "different-trace"
    else:
        next(event for event in broken["events"] if event["event_type"] == "CONTEXT")["payload"]["history"] = [
            {"role": "user", "content": "unrelated private history"}]
    with pytest.raises(ValueError):
        register_candidate(candidate, broken, reviewed, case_id="candidate-refused")


@pytest.mark.asyncio
async def test_imported_external_write_tool_only_returns_recorded_result(tmp_path, monkeypatch):
    data = recording()
    data["invocation"].update(errorCode="TOOL_ERROR", errorMessage="recorded refusal")
    sentinel = tmp_path / "external-write-must-not-happen"
    arguments = {"command": f"touch {sentinel}"}
    visible_result = "Error executing exec: refused\n\n[Analyze the error above and try a different approach.]"
    data["events"][-2:] = [
        {"event_type": "LLM", "step": "start", "span_id": "llm", "payload": {"tools": [{
            "type": "function", "function": {"name": "exec", "parameters": {
                "type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]}}}]}},
        {"event_type": "LLM", "step": "end", "span_id": "llm", "payload": {"response": {
            "tool_calls": [{"id": "call-exec", "name": "exec", "arguments": arguments}]}}},
        {"event_type": "TOOL", "step": "start", "span_id": "tool", "payload": {"name": "exec", "arguments": arguments}},
        {"event_type": "TOOL", "step": "error", "span_id": "tool", "payload": {"result": visible_result}},
    ]
    candidate = candidate_case(data)
    reviewed = confirm_expected(candidate, status="FAILED", allowed_tools=["exec"], forbidden_tools=["write_file"])
    reviewed.update(reviewer="test-only-review", review_reason="Recorded exec must remain inert.",
                    required_tools=["exec"], expected_calls=[{"turn": "t1", "name": "exec", "arguments": arguments}],
                    error_code="TOOL_ERROR")
    case = register_candidate(candidate, data, reviewed, case_id="candidate-inert-exec")

    async def prohibited(*args, **kwargs):
        pytest.fail("candidate replay ran a real external write Tool")

    monkeypatch.setattr("membot.agent.tools.shell.ExecTool.execute", prohibited)
    monkeypatch.setattr("membot.agent.tools.filesystem.WriteFileTool.execute", prohibited)
    result = await run_suite([case])
    assert result["passed"] and not sentinel.exists()
    tool_end = next(event for event in result["results"][0]["invocations"][0]["events"]
                    if event["event_type"] == "TOOL" and event["step"] == "error")
    assert tool_end["payload"]["result"] == visible_result
