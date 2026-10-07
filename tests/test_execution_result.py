from membot.agent.execution_result import ExecutionOutcome, ExecutionResult


def test_empty_final_is_classified_as_provider_failure():
    result = ExecutionResult(ExecutionOutcome.FINAL, "  ", [])

    assert not result.technical_success
    assert result.outcome is ExecutionOutcome.PROVIDER_ERROR
    assert result.error_code == "MISSING_FINAL"


def test_technical_success_does_not_assert_business_task_success():
    result = ExecutionResult(ExecutionOutcome.FINAL, "I cannot do that", [])

    assert result.technical_success
    assert result.to_dict()["technical_success"] is True
