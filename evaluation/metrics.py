"""Explicit counts and denominators; no mock rate is a real-model claim."""

from __future__ import annotations

import math
import statistics

from .schema import TERMINAL


def ratio(numerator, denominator):
    return {"numerator": numerator, "denominator": denominator,
            "value": numerator / denominator if denominator else None}


def distribution(values):
    values = sorted(value for value in values if value is not None)
    return {"samples": len(values), "mean_ms": statistics.mean(values) if values else None,
            "p95_ms": values[max(0, math.ceil(len(values) * .95) - 1)] if values else None,
            "p95_method": "nearest_rank"}


def summarize(results):
    invocations = [row for result in results for row in result["invocations"]]
    business = [result for result in results if result["business_eligible"]]
    tools = [result["tools"] for result in results if result["tools"]["eligible"]]
    matched = sum(result["tools"]["matched_calls"] for result in results)
    expected = sum(result["tools"]["expected_calls"] for result in results)
    observed = sum(result["tools"]["observed_calls"] for result in results)
    terminal = [row for row in invocations if row["status"] in TERMINAL]
    counts = {"attempted": len(invocations), "accepted": sum(row.get("accepted", False) for row in invocations),
              "rejected": sum(not row.get("accepted", False) for row in invocations),
              "failed": sum(row["status"] == "FAILED" for row in invocations),
              "timeout": sum(row["status"] == "TIMEOUT" for row in invocations),
              "succeeded": sum(row["status"] == "SUCCEEDED" for row in invocations),
              "nonterminal": sum(row["status"] not in TERMINAL and row.get("accepted", False) for row in invocations)}
    return {"counts": counts, "case_contract_pass_rate": ratio(sum(r["passed"] for r in results), len(results)),
            "business_task_success_rate": ratio(sum(r["passed"] for r in business), len(business)),
            "strict_tool_accuracy": ratio(sum(t["passed"] for t in tools), len(tools)),
            "call_precision": ratio(matched, observed), "call_recall": ratio(matched, expected),
            "call_name_recall": ratio(sum(r["tools"]["name_matched_calls"] for r in results), expected),
            "timeout_rate": ratio(counts["timeout"], counts["accepted"]),
            "e2e": distribution(row.get("e2e_ms") for row in terminal),
            "queue_wait": distribution(row.get("queue_wait_ms") for row in terminal),
            "execution": distribution(row.get("execution_ms") for row in terminal),
            "judge_errors": sum(bool(r.get("judge", {}).get("error")) for r in results if r.get("judge")),
            "interpretation": "engineering fixture contract" if all(r["mode"] != "real" for r in results)
                              else "real model task evaluation; repeated samples, not a stable improvement claim"}
