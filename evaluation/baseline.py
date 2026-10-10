"""Compare paired Case/repetition samples without silently changing baselines."""

from __future__ import annotations

import statistics

from .metrics import summarize
from .schema import digest


class IncompatibleBaselineError(ValueError):
    pass


def validate_report(report):
    if report.get("schema_version") != 1 or report.get("kind") != "eval_run":
        raise ValueError("not a version-1 evaluation report")
    provenance = report["provenance"]
    keys = {(case_id, repeat) for case_id in provenance["case_hashes"]
            for repeat in range(provenance["repetitions"])}
    rows = report["results"]
    observed = {(row["case_id"], row["repeat"]) for row in rows}
    if len(observed) != len(rows) or observed != keys:
        raise ValueError("report has missing, duplicate or unexpected samples")
    if any(row["case_hash"] != provenance["case_hashes"][row["case_id"]]
           or row["mode"] != provenance["mode"] for row in rows):
        raise ValueError("raw sample provenance mismatch")
    if report["metrics"] != summarize(rows) or report["passed"] != all(row["passed"] for row in rows):
        raise ValueError("report summary differs from raw samples")
    return report


def make_baseline(report):
    validate_report(report)
    # A reference run may contain business failures: those are measured results,
    # never new expected answers. Case oracles remain separately versioned.
    if any(row.get("judge", {}).get("error") for row in report["results"] if row.get("judge")):
        raise ValueError("Judge errors cannot become a model baseline")
    return {"schema_version": 1, "kind": "eval_baseline", "run": report,
            "integrity_hash": digest(report)}


def compare(baseline, current):
    if baseline.get("kind") != "eval_baseline" or baseline.get("integrity_hash") != digest(baseline.get("run")):
        raise ValueError("baseline integrity mismatch")
    old = validate_report(baseline["run"])
    current = validate_report(current)
    a, b = old["provenance"], current["provenance"]
    # Code and prompt edits are the intended comparison. Tool schema, rubric,
    # model or Case changes alter the experiment and need an explicit new run.
    incompatible = [key for key in ("schema_version", "mode", "case_hashes", "model", "judge_model",
        "parameters", "rubric_hash", "judge_prompt_hash", "judge_label_hash", "tool_schema_hash", "environment", "repetitions")
        if a.get(key) != b.get(key)]
    if incompatible:
        raise IncompatibleBaselineError("rebuild baseline explicitly; incompatible: " + ", ".join(incompatible))
    previous = {(row["case_id"], row["repeat"]): row for row in old["results"]}
    paired = []
    for row in current["results"]:
        before = previous[row["case_id"], row["repeat"]]
        prior_values = [value["e2e_ms"] for value in before["invocations"] if value.get("e2e_ms") is not None]
        now_values = [value["e2e_ms"] for value in row["invocations"] if value.get("e2e_ms") is not None]
        prior = statistics.mean(prior_values) if prior_values else None
        now = statistics.mean(now_values) if now_values else None
        paired.append({"case_id": row["case_id"], "repeat": row["repeat"],
            "before_passed": before["passed"], "after_passed": row["passed"],
            "new_failure": before["passed"] and not row["passed"],
            "recovered": not before["passed"] and row["passed"],
            "before_e2e_mean_ms": prior, "after_e2e_mean_ms": now,
            "latency_delta_ms": now - prior if prior is not None and now is not None else None,
            "latency_delta_fraction": (now - prior) / prior if prior and now is not None else None})
    return {"schema_version": 1, "kind": "baseline_comparison",
            "new_failures": sorted({row["case_id"] for row in paired if row["new_failure"]}),
            "recovered_cases": sorted({row["case_id"] for row in paired if row["recovered"]}),
            "pairs": paired, "changes": {key: {"before": a[key], "after": b[key]}
                for key in ("code_commit", "effective_code_hash", "prompt_hash") if a[key] != b[key]},
            "passed": current["passed"] and not any(row["new_failure"] for row in paired),
            "interpretation": "paired raw repeats; latency differences are descriptive, no significance claim"}
