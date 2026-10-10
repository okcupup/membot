"""One report contract for deterministic, durable queue and real-model runs."""

from __future__ import annotations

from datetime import datetime, timezone

from membot.agent.redaction import redact_data

from .artifacts import provenance
from .grading import grade
from .kernel import run_kernel
from .metrics import summarize


async def run_suite(cases, *, mode="deterministic", repetitions=1, fault=None,
                    queue_runner=None, real_runtime=None):
    if not 1 <= repetitions <= 10:
        raise ValueError("repetitions must be between 1 and 10")
    if mode not in {"deterministic", "queue", "real"}:
        raise ValueError("unknown execution mode")
    if mode == "queue" and queue_runner is None or mode == "real" and real_runtime is None:
        raise ValueError(f"{mode} runtime must be explicitly configured")
    metadata = provenance(cases, mode, repetitions,
                          real_config=real_runtime.config if real_runtime else None)
    if real_runtime and real_runtime.calibration:
        metadata["judge_label_hash"] = real_runtime.calibration["label_hash"]
    if queue_runner and getattr(queue_runner, "__self__", None):
        metadata["environment"].update(queue_runner.__self__.environment)
    results = []
    for repeat in range(repetitions):
        for case in cases:
            if mode == "queue":
                observed = await queue_runner(case, repeat=repeat)
            elif mode == "real":
                observed = await real_runtime.execute(case, repeat=repeat)
            else:
                observed = await run_kernel(case, repeat=repeat, fault=fault)
            result = grade(case, observed)
            if mode == "real" and result["judge_required"]:
                await real_runtime.judge(case, result)
            results.append(result)
    # Seal baselines only after redaction: saving must not change the hashed run.
    return redact_data({"schema_version": 1, "kind": "eval_run", "created_at": datetime.now(timezone.utc).isoformat(),
            "provenance": metadata, "results": results, "metrics": summarize(results),
            "passed": len(results) == len(cases) * repetitions and all(result["passed"] for result in results)})
