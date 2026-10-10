import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest
from membot.evaluation.artifacts import read_json, write_json
from membot.evaluation.baseline import (
    IncompatibleBaselineError,
    compare,
    make_baseline,
    validate_report,
)
from membot.evaluation.grading import grade, grade_tools
from membot.evaluation.judge import calibrate, evaluate
from membot.evaluation.kernel import run_kernel
from membot.evaluation.metrics import distribution, summarize
from membot.evaluation.pipeline import run_suite
from membot.evaluation.real import (
    BudgetExceededError,
    Ledger,
    MeteredProvider,
    RealConfig,
    RealRuntime,
)
from membot.evaluation.schema import load_cases
from membot.providers.base import LLMProvider, LLMResponse

CASES = {case.id: case for case in load_cases()}


@pytest.mark.asyncio
async def test_full_suite_report_can_be_validated_and_fixed_as_baseline():
    report = await run_suite(list(CASES.values()))
    assert report["passed"]
    assert report["metrics"]["business_task_success_rate"]["denominator"] == sum(case.business_task for case in CASES.values())
    assert report["metrics"]["case_contract_pass_rate"]["denominator"] == len(CASES)
    assert compare(make_baseline(report), report)["passed"]


@pytest.mark.asyncio
async def test_redacted_baseline_round_trip_keeps_integrity_and_raw_samples(tmp_path):
    report = await run_suite([CASES["tool-read"]], repetitions=2)
    report_path, baseline_path = tmp_path / "run.json", tmp_path / "baseline.json"
    write_json(report_path, report)
    write_json(baseline_path, make_baseline(report))
    saved = read_json(report_path)
    assert saved == report
    assert report_path.stat().st_mode & 0o777 == 0o600
    assert compare(read_json(baseline_path), saved)["passed"]
    assert len(saved["results"]) == 2
    saved["results"][0]["case_hash"] = "tampered"
    with pytest.raises(ValueError, match="provenance"):
        validate_report(saved)


def test_cli_reports_injected_failure_then_fixed_run_and_missing_credentials(tmp_path, monkeypatch):
    output = tmp_path / "cli.json"
    command = [sys.executable, "scripts/evaluate_cases.py", "--ids", "tool-read", "--output", str(output)]
    broken = subprocess.run([*command, "--inject-fault", "tool_error"], capture_output=True, text=True, timeout=30)
    assert broken.returncode == 1
    assert not json.loads(broken.stdout)["passed"] and not read_json(output)["passed"]
    fixed = subprocess.run(command, capture_output=True, text=True, timeout=30)
    assert fixed.returncode == 0 and json.loads(fixed.stdout)["passed"]
    assert read_json(output)["passed"]
    config = tmp_path / "real.json"
    write_json(config, real_config().model_dump())
    monkeypatch.delenv("MEMBOT_EVAL_API_KEY", raising=False)
    monkeypatch.delenv("MEMBOT_JUDGE_API_KEY", raising=False)
    real = subprocess.run([sys.executable, "scripts/evaluate_cases.py", "--mode", "real",
        "--real-config", str(config), "--output", str(output)], capture_output=True, text=True, timeout=30)
    assert real.returncode == 1 and "missing MEMBOT_EVAL_API_KEY" in real.stderr
    assert read_json(output)["kind"] == "eval_error" and not read_json(output)["passed"]


class JudgeFixture(LLMProvider):
    def __init__(self, answers):
        super().__init__()
        self.answers = iter(answers)
        self.calls = 0

    def get_default_model(self):
        return "judge-fixture-v1"

    async def chat(self, **kwargs):
        self.calls += 1
        value = next(self.answers)
        if isinstance(value, Exception):
            raise value
        return LLMResponse(content=value, usage={"prompt_tokens": 2, "completion_tokens": 2})


def verdict(passed, evidence="quoted visible evidence"):
    return json.dumps({"passed": passed, "score": 1.0 if passed else 0.0,
                       "reason": "verified actual evidence", "evidence": [evidence]})


def real_config(**overrides):
    value = json.loads(Path("evaluation/real.example.json").read_text())
    value.update(agent_model="agent-snapshot-v1", judge_model="judge-snapshot-v1", model_version_confirmed=True)
    value.update(overrides)
    return RealConfig.model_validate(value)


@pytest.mark.asyncio
async def test_pairing_reports_failures_recovery_and_raw_latency():
    case = CASES["tool-read"]
    good = await run_suite([case], repetitions=2)
    bad = await run_suite([case], repetitions=2, fault="tool_error")
    comparison = compare(make_baseline(good), bad)
    assert comparison["new_failures"] == [case.id]
    assert not comparison["passed"]
    assert len(comparison["pairs"]) == 2
    assert all(pair["latency_delta_ms"] is not None for pair in comparison["pairs"])
    recovery = compare(make_baseline(bad), good)
    assert recovery["recovered_cases"] == [case.id]
    assert recovery["passed"]
    changed = copy.deepcopy(good)
    changed["provenance"]["prompt_hash"] = "new-prompt"
    assert "prompt_hash" in compare(make_baseline(good), changed)["changes"]
    for key in ("model", "case_hashes", "tool_schema_hash", "rubric_hash", "repetitions"):
        changed = copy.deepcopy(good)
        if key in {"case_hashes", "repetitions"}:
            # These malformed samples are rejected before compatibility checks.
            changed["provenance"][key] = {} if key == "case_hashes" else 3
            with pytest.raises(ValueError):
                compare(make_baseline(good), changed)
        else:
            changed["provenance"][key] = "changed"
            with pytest.raises(IncompatibleBaselineError, match="rebuild baseline"):
                compare(make_baseline(good), changed)
    incomplete = copy.deepcopy(good)
    incomplete["results"].pop()
    with pytest.raises(ValueError, match="missing"):
        validate_report(incomplete)


@pytest.mark.asyncio
async def test_tool_accuracy_checks_arguments_order_extra_and_forbidden_calls():
    case = CASES["tool-write-read"]
    original = await run_kernel(case)
    assert grade_tools(case, original)["passed"]
    broken = copy.deepcopy(original)
    starts = [e for e in broken["invocations"][0]["events"] if e["event_type"] == "TOOL" and e["step"] == "start"]
    starts[0]["payload"]["arguments"]["path"] = "wrong.txt"
    metric = grade_tools(case, broken)
    assert not metric["passed"]
    assert metric["matched_calls"] == 1 and metric["name_matched_calls"] == 2
    broken = copy.deepcopy(original)
    events = broken["invocations"][0]["events"]
    indices = [i for i, e in enumerate(events) if e["event_type"] == "TOOL" and e["step"] == "start"]
    events[indices[0]], events[indices[1]] = events[indices[1]], events[indices[0]]
    assert not grade_tools(case, broken)["passed"]
    events.append({"event_type": "TOOL", "step": "start", "payload": {"name": "exec", "arguments": {}}})
    assert "forbidden Tool executed" in grade_tools(case, broken)["reasons"]


def test_metric_denominators_and_missing_durations_are_explicit():
    assert distribution([1, 2, None, 3, 4, 100])["p95_ms"] == 100
    assert distribution([])["mean_ms"] is None
    result = {"mode": "queue", "passed": False, "business_eligible": True,
              "tools": {"eligible": True, "passed": False, "matched_calls": 0,
                        "observed_calls": 0, "expected_calls": 1, "name_matched_calls": 0},
              "admission": {"requests": 3, "accepted_responses": 2, "rejected_responses": 1,
                            "duplicate_responses": 1},
              "invocations": [{"accepted": True, "status": "QUEUED"},
                              {"accepted": False, "status": "REJECTED"}]}
    metric = summarize([result])
    assert metric["counts"]["nonterminal"] == 1
    assert metric["counts"]["accepted"] == metric["counts"]["rejected"] == 1
    assert metric["call_precision"]["value"] is None
    assert metric["business_task_success_rate"]["value"] == 0
    assert metric["e2e"]["samples"] == 0
    assert metric["admission"]["requests"] == 3 and metric["admission"]["duplicate_responses"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", ["not json", '{"passed":true}',
    '{"passed":true,"score":0.1,"reason":"wrong","evidence":["x"]}',
    '{"passed":true,"score":1,"reason":"ok","evidence":[]}', RuntimeError("provider down")])
async def test_judge_errors_do_not_pass(answer):
    result = await evaluate(JudgeFixture([answer]), rubric="test", input="test", final="done", events=[])
    assert not result["passed"] and result["error"]


@pytest.mark.asyncio
async def test_calibration_requires_review_and_false_completion_negative():
    labels = json.loads(Path("evaluation/judge_labels.json").read_text())
    proposed = copy.deepcopy(labels)
    proposed.update(label_status="proposed_manual_labels", reviewer=None)
    with pytest.raises(ValueError, match="human labels"):
        await calibrate(JudgeFixture([]), proposed)
    labels.update(label_status="human_confirmed", reviewer="test-only-synthetic-review")
    provider = JudgeFixture([verdict(sample["expected_passed"], sample["final"]) for sample in labels["samples"]])
    calibrated = await calibrate(provider, labels)
    assert calibrated["passed"] and provider.calls == 4
    wrong = await calibrate(JudgeFixture([verdict(True, sample["final"]) for sample in labels["samples"]]), labels)
    assert not wrong["passed"]
    assert not next(row for row in wrong["samples"] if row["id"] == "notify-false-completion")["matched"]


@pytest.mark.asyncio
async def test_real_budget_fails_before_sending_extra_request_and_missing_usage():
    config = real_config(max_requests=1)
    raw = JudgeFixture([verdict(True), verdict(True)])
    provider = MeteredProvider(raw, Ledger(config), "judge")
    await provider.chat(messages=[{"role": "user", "content": "test"}])
    with pytest.raises(BudgetExceededError):
        await provider.chat(messages=[{"role": "user", "content": "test"}])
    assert raw.calls == 1
    low = MeteredProvider(raw, Ledger(real_config(max_cost_usd=.000001)), "judge")
    with pytest.raises(BudgetExceededError):
        await low.chat(messages=[{"role": "user", "content": "test"}])
    assert raw.calls == 1


def test_real_missing_credentials_and_unpinned_models_fail_explicitly(monkeypatch):
    monkeypatch.delenv("MEMBOT_EVAL_API_KEY", raising=False)
    monkeypatch.delenv("MEMBOT_JUDGE_API_KEY", raising=False)
    with pytest.raises(ValueError, match="missing MEMBOT_EVAL_API_KEY"):
        RealRuntime(real_config())
    with pytest.raises(ValueError, match="pin/confirm"):
        real_config(model_version_confirmed=False)


@pytest.mark.asyncio
async def test_real_judge_cannot_override_failed_tool_gate_or_uncalibrated_judge():
    case = CASES["tool-notify"]
    observation = await run_kernel(case, fault="missing_tool")
    observation["mode"] = "real"
    result = grade(case, observation)
    raw = JudgeFixture([verdict(True)])
    runtime = RealRuntime(real_config(), agent=raw, judge_provider=raw)
    runtime.calibration = {"passed": True}
    await runtime.judge(case, result)
    assert not result["passed"] and raw.calls == 0
    assert result["judge"]["error"] == "DETERMINISTIC_GATE_FAILED"
    intact = await run_kernel(case)
    intact["mode"] = "real"
    result = grade(case, intact)
    runtime.calibration = None
    await runtime.judge(case, result)
    assert not result["passed"] and result["judge"]["error"] == "JUDGE_NOT_CALIBRATED"
