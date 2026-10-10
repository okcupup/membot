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
        {"event_type": "CONTEXT", "step": "snapshot", "payload": {"history": [], "messages": []}},
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
