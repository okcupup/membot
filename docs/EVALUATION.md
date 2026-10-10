# M6 Eval/Regression

`evaluation/cases/` contains 32 executable versioned Cases: eight each in
`basic`, `tool_calling`, `context_concurrency`, and `exception_recovery`.
The fixtures are injected into the production `AgentLoop`; they are not mocks
around a different runner. A Case's status contract, visible result assertions,
history assertions, and Tool trace contract are independent checks. A normal
technical `SUCCEEDED` therefore does not imply business-task success.

## Deterministic engineering run

```bash
LITELLM_LOCAL_MODEL_COST_MAP=True \
  ./.venv/bin/python scripts/evaluate_cases.py \
  --mode deterministic --assert-count 32 --repeat 2 \
  --output evaluation/reports/deterministic.json \
  --write-baseline evaluation/baselines/deterministic.json
```

The report is an atomic, redacted JSON artifact. It contains every raw
repetition, Case hash, code/working-tree hash, Prompt hash, Tool schema hash,
fixture model/parameters, environment, status counts, queue wait, execution and
end-to-end mean/p95 durations. These timings describe this run and hardware;
they are not a capacity claim. `--inject-fault` is used by regression tests to
prove Tool errors, context leakage, missing Tools and false terminal status fail.
`membot-eval` is the equivalent installed-wheel entrypoint. `--assert-count` is
a minimum, allowing reviewed Cases to be added. Each Case has strict schema,
versioned input/history/fixtures, expected status, allowed/required/forbidden
Tools, key-argument and call-order assertions, result/rubric and timeouts.
The default suite uses Events to observe concurrency and order, not a
wall-clock overlap threshold. Reports are limited to 32 MiB; suites to 256
Cases, ten repetitions, and 16 turns/iterations per Case. All workspaces,
runtime tasks and buses are bounded and disposed between Cases.

Metrics use explicit numerator/denominator and sample counts:

- Case Contract covers all selected Cases; business task success covers only
  Cases marked `business_task` (24 of the standard 32). Recovery Cases grade
  correct failure/recovery behavior, not successful user tasks.
- Strict Tool Accuracy includes all Cases with Tool constraints, including
  prohibitions. `tool_call_case_accuracy` separately reports the 14 Cases with
  expected calls; `no_tool_policy_accuracy` reports the other 18. Required
  names, argument subsets, forbidden/extra calls and per-turn exact/subsequence
  order must all match. Booleans are distinct from numeric arguments.
- Call precision is matched key-argument occurrences / observed occurrences;
  call recall is matched / required occurrences. Name-only recall is separate.
  Call metrics do not replace Case-level ordering or Tool-result checks.
- Status counts concern unique logical invocations. `admission` counts HTTP
  requests/responses and idempotent duplicates separately; the in-process
  runner makes no HTTP requests. Timeout Rate includes intentionally timed-out
  invocations; expected/unexpected timeout counts are separate.
- End-to-end, queue wait and execution report average/p95 with nearest-rank
  p95. A queue timeout has no execution sample. Queue mode measures POST start
  to terminal observation; durable queue/execution times come from PostgreSQL.
  Missing/truncated/gapped evidence is a failed contract, not a guessed pass.

## Queue integration

Twelve selected Cases use two production HTTP API apps with separate repository
pools, PostgreSQL acceptance/Outbox, Redis Streams, ACK and one `AsyncWorker`.
They cover basic/read/multi-step Tools, three serial turns, parallel Sessions,
hot Session fairness, Provider/Tool errors, missing Final, iteration exhaustion,
and LLM/Tool timeouts. API1 submits; API2 queries original IDs and events with
its own requestId. HTTP idempotent duplicates and Redis terminal redelivery do
not re-execute Tools. Each Case/repetition has a new owner/Session scope and
isolated workspace. The remaining 20 Cases are explicitly excluded in this
mode; cancellation/native-bus contracts and queue expiry remain in the kernel
and the existing real-service recovery tests. Use a dedicated evaluation DB:
retained invocation/event rows are intentional, not production data cleanup.

```bash
DATABASE_URL=postgresql://membot:membot@127.0.0.1:55432/membot_m6_20261010 \
REDIS_URL=redis://127.0.0.1:56379/0 \
  ./.venv/bin/python scripts/evaluate_cases.py --mode queue \
  --assert-count 12 --repeat 2 --assert-report \
  --output evaluation/reports/queue.json
```

## Real model and Judge

Create a private config from `evaluation/real.example.json`, pin and manually
confirm both model versions, set explicit per-role prices and a request/token/
cost budget, then provide `MEMBOT_EVAL_API_KEY` and
`MEMBOT_JUDGE_API_KEY`. The default labels are deliberately
`proposed_manual_labels`; run `--mode confirm-labels --reviewer <human-id>`
only after review. Calibration includes positive and negative samples,
including a response claiming completion with no Tool call. Invalid JSON,
missing evidence, provider errors, score/pass contradictions, and uncalibrated
Judges fail closed. Provider retries are disabled and reservations are shared
between Agent and Judge calls.
Nine Cases are explicitly enabled for real Agent models; seven use hard
assertions and two also use the structured Judge. Tools remain isolated fixture
implementations, so this measures model reasoning/calling in the tested Tool
environment, not live external-system reliability. The default calibration
labels have not been human-confirmed in this checkout. Review all four labels,
including the false-completion negative, before running `confirm-labels` on a
private labels file and configuring `labels_file` to point to it.

Budgets reserve UTF-8 request bytes plus a 512-unit input margin and bounded
output tokens, retaining reservations for failed/uncertain calls. This is a
conservative bound for ordinary BPE use, not a universal tokenizer guarantee or
provider invoice. Supply correct prices; usage must be present. Exhausted
budgets, malformed Judge JSON, fabricated evidence and missing prerequisites
fail the requested run. Judge cannot override a deterministic Tool failure.

```bash
./.venv/bin/python scripts/evaluate_cases.py --mode real \
  --real-config evaluation/real.private.json --repeat 3 \
  --output evaluation/reports/real.json
```

Results are not comparable if Case hash, Tool schema, rubric/Judge prompt or
labels, model/parameters, environment, mode or repetition count changes.
Code and Agent Prompt edits are the intended experiment: comparison records
their old/new hashes. Baseline comparison
pairs `(case_id, repeat)`, reports new failures/recoveries and descriptive
latency deltas. A small sample is retained as raw evidence and never presented
as a stable improvement claim.

```bash
./.venv/bin/python scripts/evaluate_cases.py --mode deterministic --repeat 2 \
  --output evaluation/reports/current.json
./.venv/bin/python scripts/evaluate_cases.py --mode baseline --assert-report \
  --baseline evaluation/baselines/deterministic.json \
  --report evaluation/reports/current.json \
  --output evaluation/reports/comparison.json
```

Baseline files seal all raw samples and provenance with an integrity hash.
Existing baselines require explicit `--replace-baseline`. A failed business
sample can be a reference observation, never a new oracle; Judge errors cannot
be baselines. Exit 1 means failed/error; exit 2 means incompatible baseline.
An error after writing a completed report goes to a separate `.error.json`.

## Failed Invocation candidates

`trace_probe.py --export-candidate` creates a redacted, review-required
candidate. A failure answer is never assigned as the expected answer. After a
human confirms status, Tool constraints and any expected result, register a
runnable recorded-response Case:

```bash
mkdir -p evaluation/candidates
./.venv/bin/python scripts/trace_probe.py <invocation-id> \
  --database-url "$DATABASE_URL" \
  --export-recording evaluation/candidates/recording.json \
  --export-candidate evaluation/candidates/candidate.json
```

For example, after human review of a Provider-error candidate, create explicit
expected fields and record the reviewer/rationale. These constants express
the intended contract; they are not copied from the observed answer:

```bash
./.venv/bin/python - "$REVIEWER" <<'PY'
import sys
from pathlib import Path
from membot.evaluation.artifacts import read_json, write_json
from membot.service.diagnostics import confirm_expected
root = Path("evaluation/candidates")
candidate = read_json(root / "candidate.json")
reviewed = confirm_expected(candidate, status="FAILED", allowed_tools=[],
                            forbidden_tools=["exec", "spawn", "cron"])
reviewed.update(reviewer=sys.argv[1], review_reason="Provider errors must fail without Tool side effects.",
                required_tools=[], expected_calls=[], error_code="PROVIDER_ERROR")
write_json(root / "reviewed.json", reviewed)
PY
./.venv/bin/python scripts/evaluate_cases.py --mode import-case \
  --candidate evaluation/candidates/candidate.json \
  --recording evaluation/candidates/recording.json \
  --expectation evaluation/candidates/reviewed.json --case-id candidate-example \
  --output evaluation/cases/exception_recovery/candidate-example.json
./.venv/bin/python scripts/evaluate_cases.py --ids candidate-example --assert-report
make regression
```

Registration rejects truncated/expired/gapped recordings, unconfirmed
expectations, conflicting Tool lists, changed candidate hashes, mismatched
input/history/IDs, missing configuration/Tool schemas, incomplete spans and
implicit gold answers. Expected calls need explicit turn/name/key arguments;
review can set `tool_order`, `budget`, `result_assertions` and error code.
Worker-lost/cancelled records without reproducible outcomes remain diagnostic
candidates. A fixed success expectation against recorded failing responses
stays red until a reviewed new fixture demonstrates the intended behavior.
Recorded adapters execute in a temporary isolated
workspace without normal network, shell, MCP or external write tools.

## CI

`.github/workflows/regression.yml` runs deterministic engineering Cases and
their tests on every push/PR; a second job runs real PostgreSQL/Redis, 12x2
Queue Cases and the full old/new test suite. Raw samples/comparisons are uploaded
on success or failure with 30-day retention. The deterministic reference is
`MEMBOT_EVAL_BASELINE_SHA`, or the PR base/push predecessor. A reference predating
M6 produces explicit bootstrap evidence, not a claimed comparison. Incompatible
Cases require a reviewed new reference SHA.

With `MEMBOT_RUN_REAL_EVAL=true`, changes to Agent Prompt/Tool/Provider/Case
paths trigger three repeated real runs; `workflow_dispatch` can explicitly
request one. Fork PRs never receive credentials. Configure the secrets
`MEMBOT_EVAL_API_KEY`, `MEMBOT_JUDGE_API_KEY`, `MEMBOT_REAL_EVAL_CONFIG_JSON`,
and `MEMBOT_JUDGE_LABELS_JSON`. An optional committed/redacted baseline path is
`MEMBOT_REAL_BASELINE_FILE`. Real jobs require pinned config, human Judge labels,
credentials and an explicit budget; missing credentials are a failed requested
run, never a passing mock result. CI is configured and locally checked, but
GitHub-hosted runs and real-model/Judge calibration were not executed here.
