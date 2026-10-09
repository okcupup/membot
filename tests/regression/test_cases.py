from collections import Counter

import pytest
from membot.evaluation.grading import grade
from membot.evaluation.kernel import run_kernel
from membot.evaluation.metrics import summarize
from membot.evaluation.schema import CATEGORIES, load_cases

CASES = load_cases()


def test_standard_suite_has_eight_runnable_cases_in_each_category():
    assert len(CASES) == 32
    assert Counter(case.category for case in CASES) == {category: 8 for category in CATEGORIES}


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.id)
async def test_real_agent_loop_fixture_contract(case):
    result = grade(case, await run_kernel(case))
    assert result["passed"], [check for check in result["checks"] if not check["passed"]]
    assert all(row["e2e_ms"] >= 0 for row in result["invocations"])


@pytest.mark.asyncio
@pytest.mark.parametrize("case_id,fault", [
    ("tool-read", "tool_error"), ("context-multi-turn", "context_leak"),
    ("error-tool", "wrong_status"), ("tool-notify", "missing_tool"),
])
async def test_intentional_defects_fail_then_correct_cases_pass(case_id, fault):
    case = next(case for case in CASES if case.id == case_id)
    broken = grade(case, await run_kernel(case, fault=fault), fault=fault)
    fixed = grade(case, await run_kernel(case))
    assert not broken["passed"]
    assert fixed["passed"]
    summary = summarize([broken, fixed])
    assert summary["case_contract_pass_rate"] == {"numerator": 1, "denominator": 2, "value": .5}
