# Delivery Plan

Each phase has a concrete gate. Commands are run from the repository root with
`.venv` active by path; commands referring to future files become valid when
that phase lands.

## M0: review and baseline (complete)

Review the current commit, call paths, mutable state, memory behavior, package
layout, dependencies, tests, and wheel. Add the deterministic Fake Provider and
record native-bus and `process_direct` behavior. Do not add a service.

Acceptance:

```bash
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python -m pytest -q
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python scripts/m0_baseline.py --output /tmp/membot-m0-baseline.json
./.venv/bin/python -m compileall -q agent bus channels cli config cron heartbeat providers session utils membot tests scripts
```

## M1: runtime kernel (complete)

Introduced an explicit invocation context, per-session acceptance sequence,
per-session serial executor, Worker semaphore, bounded queues, cancellation, and
separate queue/run deadlines while preserving the CLI path. Provider, tool, and
memory context is invocation-owned. Consolidation is inline and service-mode
background capabilities have explicit runtime switches and budgets.

Acceptance:

```bash
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python -m pytest -q tests/test_runtime_kernel.py tests/test_runtime_timeouts.py
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python -m pytest -q
./.venv/bin/ruff check --select E4,E7,E9,F,N bus/queue.py agent/tools/base.py agent/execution.py agent/loop.py agent/subagent.py agent/tools/message.py agent/tools/spawn.py agent/tools/cron.py agent/tools/registry.py config/schema.py cli/commands.py tests/test_runtime_kernel.py tests/test_runtime_timeouts.py
./.venv/bin/python -m compileall -q agent bus channels cli config cron heartbeat providers session utils membot tests scripts
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python scripts/m0_baseline.py --output /tmp/membot-m1-baseline.json
```

## M2: persistence and delivery

Add PostgreSQL migrations and repositories for owner-scoped sessions, ordered
messages, invocations, events, leases, and Outbox rows. Reuse
ConversationMemoryEngine with a stateless PostgreSQL adapter while leaving CLI
JSONL behavior intact. Add idempotency hash checks, atomic turn/terminal commits,
ordered `/new` archival, a bounded Redis envelope transport, and a retrying
Outbox relay. Delivery remains at least once. Verify restart and recovery with
real PostgreSQL/Redis services.

Acceptance:

```bash
docker compose -f deploy/docker-compose.test.yml up -d postgres redis
./.venv/bin/python -m pip install -e '.[service]'
DATABASE_URL=postgresql://membot:membot@127.0.0.1:55432/membot \
REDIS_URL=redis://127.0.0.1:56379/0 \
  ./.venv/bin/python -m pytest -q tests/test_persistence.py tests/test_outbox.py
docker compose -f deploy/docker-compose.test.yml down -v
```

The integration tests skip with an explicit reason when either local service is
unavailable. The Compose file exposes PostgreSQL on host port `55432` and Redis
on `56379` to avoid colliding with developer services.

## M3: asynchronous API and Worker

Add the `202` submission endpoint, status and event queries, one Worker process,
bounded shutdown, request validation, and idempotency handling. Keep API
instances stateless and route all durable history through PostgreSQL.

Acceptance:

```bash
./.venv/bin/python -m pytest -q tests/test_api_contract.py tests/test_worker.py
./.venv/bin/python scripts/api_smoke.py --assert-202 --assert-timeline
```

## M4: diagnostics and timeline

Emit structured logs and PostgreSQL events for LLM, Tool, Result, and Final
steps. Add correlation propagation, redaction, error classification, and an
invocation timeline export.

Acceptance:

```bash
./.venv/bin/python -m pytest -q tests/test_diagnostics.py
./.venv/bin/python scripts/trace_probe.py --assert-correlation --assert-redaction
```

## M5: deployment

Add Dockerfiles, Compose, Nginx HTTPS configuration, `least_conn`, rate limits,
live/ready probes, dependency checks, and graceful shutdown. Keep exactly two
API instances and one Worker in the first topology.

Acceptance:

```bash
docker compose -f deploy/docker-compose.yml config
docker compose -f deploy/docker-compose.yml up -d --build
./.venv/bin/python scripts/deploy_smoke.py --assert-ready --assert-https
docker compose -f deploy/docker-compose.yml down
```

## M6: regression and evaluation

Define at least 32 standard cases with deterministic engineering assertions,
real-model evaluation, baseline comparison, and export of failed traces as
candidate cases. Keep provider credentials and network calls out of the default
deterministic job.

Acceptance:

```bash
./.venv/bin/python -m pytest -q tests/regression
./.venv/bin/python scripts/evaluate_cases.py --mode deterministic --assert-count 32
./.venv/bin/python scripts/evaluate_cases.py --mode baseline --assert-report
```

## M7: load and concurrency

Measure queue saturation, per-session ordering, cross-session parallelism,
semaphore limits, bounded memory, and API rate limiting. Publish measured data
with workload, hardware, and commit; never invent performance numbers.

Acceptance:

```bash
./.venv/bin/python scripts/load_test.py --sessions 4 --invocations 32 --assert-order
./.venv/bin/python scripts/load_test.py --assert-bounded-memory --assert-semaphore
```

## M8: failure drills

Exercise provider timeout, queue timeout, tool timeout, API restart, Worker kill,
Redis interruption, PostgreSQL interruption, duplicate delivery, and graceful
shutdown. Verify every terminal state and exported timeline.

Acceptance:

```bash
./.venv/bin/python scripts/failure_drill.py --all --assert-terminal-states
docker compose -f deploy/docker-compose.yml down -v
```
