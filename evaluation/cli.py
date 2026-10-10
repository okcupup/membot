"""Runnable regression/evaluation, baseline and reviewed Case registration CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from membot.evaluation.artifacts import read_json, write_json
from membot.evaluation.baseline import (
    IncompatibleBaselineError,
    compare,
    make_baseline,
    validate_report,
)
from membot.evaluation.schema import CATEGORIES, load_cases


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--mode", choices=["deterministic", "queue", "real", "baseline", "import-case", "confirm-labels"], default="deterministic")
    result.add_argument("--cases", type=Path)
    result.add_argument("--ids", nargs="*")
    result.add_argument("--category", choices=CATEGORIES)
    result.add_argument("--assert-count", type=int, help="minimum selected runnable Case count")
    result.add_argument("--repeat", type=int, default=1)
    result.add_argument("--output", type=Path, default=Path("evaluation/reports/latest.json"))
    result.add_argument("--baseline", type=Path)
    result.add_argument("--report", type=Path)
    result.add_argument("--write-baseline", type=Path)
    result.add_argument("--replace-baseline", action="store_true")
    result.add_argument("--assert-report", action="store_true")
    result.add_argument("--real-config", type=Path)
    result.add_argument("--database-url")
    result.add_argument("--redis-url")
    result.add_argument("--inject-fault", choices=["tool_error", "context_leak", "wrong_status", "missing_tool"])
    result.add_argument("--candidate", type=Path)
    result.add_argument("--recording", type=Path)
    result.add_argument("--expectation", type=Path)
    result.add_argument("--case-id")
    result.add_argument("--labels", type=Path, default=Path("evaluation/judge_labels.json"))
    result.add_argument("--reviewer")
    return result


async def execute(args):
    from membot.evaluation.pipeline import run_suite
    if args.mode == "baseline":
        if not args.baseline or not args.report:
            raise ValueError("baseline comparison requires --baseline and --report")
        report = compare(read_json(args.baseline), read_json(args.report))
        write_json(args.output, report)
        print(json.dumps({key: report[key] for key in ("passed", "new_failures", "recovered_cases", "changes")}, indent=2))
        return 0 if report["passed"] else 1
    if args.mode == "confirm-labels":
        if not args.reviewer:
            raise ValueError("explicit human --reviewer identity is required")
        labels = read_json(args.labels)
        labels.update(label_status="human_confirmed", reviewer=args.reviewer)
        write_json(args.labels, labels)
        print("Human-confirmed calibration labels saved; no model request was made.")
        return 0
    if args.mode == "import-case":
        from membot.evaluation.candidates import register_candidate
        if not all((args.candidate, args.recording, args.expectation, args.case_id)):
            raise ValueError("import-case requires --candidate --recording --expectation --case-id")
        case = register_candidate(read_json(args.candidate), read_json(args.recording),
                                  read_json(args.expectation), case_id=args.case_id)
        if args.output.exists():
            raise ValueError("registered Case destination already exists")
        write_json(args.output, case.model_dump())
        print(f"Registered reviewed runnable Case {case.id}; observed failure is a fixture, not an answer oracle.")
        return 0
    cases = load_cases(args.cases)
    if args.ids:
        if set(args.ids) - {case.id for case in cases}:
            raise ValueError("unknown requested Case ID")
        cases = [case for case in cases if case.id in args.ids]
    if args.category:
        cases = [case for case in cases if case.category == args.category]
    excluded = []
    queue_runtime = real_runtime = None
    output_written = False
    try:
        if args.mode == "real":
            from membot.evaluation.real import RealConfig, RealRuntime
            excluded = [case.id for case in cases if not case.real_model]
            cases = [case for case in cases if case.real_model]
            if not args.real_config:
                raise ValueError("real evaluation requires --real-config, pinned models, credentials and budget")
            real_runtime = RealRuntime(RealConfig.model_validate(read_json(args.real_config)))
            await real_runtime.preflight()
        elif args.mode == "queue":
            from membot.evaluation.queue import QUEUE_CASE_IDS, QueueRuntime
            if args.ids and set(args.ids) - QUEUE_CASE_IDS:
                raise ValueError("requested Case isn't enabled in the queue suite")
            excluded = [case.id for case in cases if case.id not in QUEUE_CASE_IDS]
            cases = [case for case in cases if case.id in QUEUE_CASE_IDS]
            queue_runtime = QueueRuntime(args.database_url, args.redis_url)
            await queue_runtime.start()
        if not cases or args.assert_count is not None and len(cases) < args.assert_count:
            raise ValueError(f"selected {len(cases)} Cases, expected {args.assert_count}")
        report = await run_suite(cases, mode=args.mode, repetitions=args.repeat, fault=args.inject_fault,
                                 queue_runner=queue_runtime.execute if queue_runtime else None,
                                 real_runtime=real_runtime)
        report["selection"] = {"executed_case_ids": [case.id for case in cases], "excluded_case_ids": excluded,
            "reason": "explicit mode capability selection; excluded cases aren't counted as passing"}
        if real_runtime:
            report["calibration"] = real_runtime.calibration
            report["spending"] = real_runtime.ledger.snapshot()
        write_json(args.output, report)
        output_written = True
        if args.assert_report:
            validate_report(report)
        if args.write_baseline:
            if args.write_baseline.exists() and not args.replace_baseline:
                raise ValueError("baseline exists; rebuilding requires --replace-baseline")
            write_json(args.write_baseline, make_baseline(report))
        print(json.dumps({"passed": report["passed"], "cases": len(cases), "repetitions": args.repeat,
                          "metrics": report["metrics"], "report": str(args.output)}, ensure_ascii=False, indent=2))
        return 0 if report["passed"] else 1
    except Exception as exc:
        evidence = {"kind": "eval_error", "passed": False, "error": f"{type(exc).__name__}: {exc}", "mode": args.mode}
        if real_runtime:
            evidence.update(calibration=real_runtime.calibration, spending=real_runtime.ledger.snapshot())
        error_path = args.output.with_name(args.output.stem + ".error.json") if output_written else args.output
        write_json(error_path, evidence)
        raise
    finally:
        if queue_runtime:
            await queue_runtime.close()
        if real_runtime:
            await real_runtime.close()


def main():
    args = parser().parse_args()
    os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    from loguru import logger
    logger.remove()
    try:
        return asyncio.run(execute(args))
    except IncompatibleBaselineError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        from membot.agent.redaction import redact_text
        print(redact_text(f"{type(exc).__name__}: {exc}"), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
