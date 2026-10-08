# Project Status

Date: 2026-10-08

M0, M1, and M2 are complete. M3 API/Queue/Worker execution is implemented on
the feature branch; deployment topology and Nginx remain planned for M5.

The reviewed starting commit was:

```text
8756befe872bbb3c52dc2d1307a45280cab39189
feat:对每个轮次的message重新定义
```

The local M0 checkpoint is `906c59f`. The M1 checkpoint is the local commit
containing the runtime kernel and regression tests described below. Commits are
local only; nothing was pushed remotely.

Environment and checks:

- System `/usr/bin/python` is Python 3.6.8 and does not satisfy `>=3.11`.
- `.venv` was created from `/root/miniconda3/envs/nanobot/bin/python` 3.11.15.
- `pip install -e '.[dev]'` succeeded after the sandbox-only attempt was blocked
  by DNS access to the configured package mirror.
- Before the M0 hygiene fix, the existing suite was `5 passed, 1 failed` because
  `recent.py` used `MemoryRecord` without importing it.
- After that minimal import fix, `LITELLM_LOCAL_MODEL_COST_MAP=True
  ./.venv/bin/python -m pytest -q` reports `6 passed`.
- `scripts/m0_baseline.py` records bus dispatch and direct-call behavior with no
  model credentials or network calls.
- A wheel builds successfully, but a clean directory install contains only the
  `membot` shell and bridge files. `membot.agent` and `membot.cli` are absent;
  this is a code/packaging defect, not an environment dependency failure.

M1 implementation and checks:

- `_processing_lock` was replaced by a bounded per-Session queue and drain task.
  The queue head owns the complete history-read, context-build, LLM/tool, and
  turn-save range.
- Different Sessions can overlap; the Worker Semaphore is acquired only at the
  queue head, so queued work does not consume execution capacity.
- `process_direct`, native bus `_dispatch`, and future service calls share the
  same scheduler entrance. Effective keys normalize system callbacks and custom
  session overrides before touching memory or tools.
- `ExecutionContext` is carried by a `contextvars` value. Message, Spawn, and
  Cron tools are cloned per invocation; Subagent tasks have bounded total and
  concurrent budgets. MCP initialization shares and awaits one in-flight task.
- Queue and execution timeout exceptions release Session state and Semaphore
  capacity. Empty Session queues are removed. Consolidation is inline and
  serialized by a Worker-level long-term-memory lock; service deployments can
  disable consolidation, spawn, and cron with `agents.runtime` flags.
- The native `MessageBus` now has positive, configurable inbound and outbound
  bounds (default 256 each), and injected Tools have a
  `clone_for_execution()` contract. The default is a shallow compatibility copy;
  Tools with mutable invocation state can override it, as Message/Spawn/Cron do.
- New tests use `asyncio.Event` barriers and cover three-turn ordering,
  cross-Session overlap, semaphore limits, backlog bypass, callback key
  normalization, tool routing/final-marker isolation, MCP initialization,
  timeout, cancellation recovery, shutdown draining, and queue bounds.

The M1 acceptance result is:

```text
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python -m pytest -q
21 passed in 3.57s
```

The focused M1 suite reports `15 passed`; the selected Ruff checks and
repository bytecode compilation also pass. A repeat of `scripts/m0_baseline.py`
observes `provider_max_active=2` for the native bus (different Sessions
overlap) and `provider_max_active=1` for two direct calls targeting one Session
(that Session remains serial). These are behavioral observations for this
commit, not performance claims.

M2 implementation:

- Added asyncpg migrations and repositories for `sessions`, `session_messages`,
  `invocations`, `invocation_events`, `outbox`, and archived Session messages.
  Session ordering allocation, invocation creation, initial event, and Outbox
  insertion share one short transaction.
- Idempotency keys are owner-scoped. Canonical payload hashes return the
  original invocation for the same request and raise a conflict for a changed
  body. Failed invocations retain their sequence; later work continues at the
  next sequence after the earlier invocation becomes terminal.
- PostgreSQL implements the ConversationMemoryEngine store/retriever boundary.
  It reads only the requested history window, owns no cross-process Session
  cache, and commits successful messages with invocation terminal state in one
  short transaction. CLI continues using the existing JSONL engine.
- `/new` archives current messages with their original ordered message IDs and
  clears current history transactionally while keeping invocation and message
  sequences monotonic. Provider errors, tool exceptions, iteration
  exhaustion, cancellation, and timeout have structured technical outcomes;
  only a normal Final is `SUCCEEDED`. Failed/partial protocol messages do not
  enter session history.
- Added a bounded Redis list transport with an unacknowledged processing list,
  an Outbox retry relay, and expired-lease recovery. Delivery is at least once.
  The M2 development Compose file contains only PostgreSQL and Redis.
- PostgreSQL and Redis drivers are a `service` extra, so CLI-only installs do
  not acquire database clients. Completion, `/new`, interruption, and lease
  renewal are fenced by the current Worker owner and an unexpired lease.
- Invocation errors pass a persistence-boundary redactor for common key/value,
  bearer, and provider-token forms. Failed invocation result JSON omits partial
  transcripts while preserving the structured outcome and redacted error.
- Service construction injects `ConversationMemoryEngine.for_postgres(...)`.
  Consolidation, subagents, and cron must be disabled until M3 owns their
  durable lifecycle. CLI JSONL behavior remains the default.

M2 verification environment and results:

- The development host had no Docker/Podman and no installed database drivers.
  `asyncpg` and `redis` were installed in `.venv`; PostgreSQL 13 and Redis 6
  packages were installed on the host after disabling only the broken MySQL
  package repositories for that command. Temporary local instances ran at
  ports `55432` and `56379`. Docker Compose itself could not be run here.
- A first ordinary sandbox run skipped five integration tests because loopback
  access was denied. The elevated run first exposed PostgreSQL's default
  `ident` TCP auth; tests were then run through a local peer-authenticated
  PostgreSQL socket. These were environment setup issues, not code failures.
- `LITELLM_LOCAL_MODEL_COST_MAP=True DATABASE_URL='postgresql:///membot?user=root&port=55432' REDIS_URL=redis://127.0.0.1:56379/0 ./.venv/bin/python -m pytest -q tests/test_persistence.py tests/test_outbox.py`
  reports `8 passed`. This used real PostgreSQL 13 and Redis 6, including
  concurrent sequence allocation, idempotency/conflict, repository-to-repository
  visibility, AgentLoop history across instances, failure isolation, `/new`,
  lease recovery, bounded Redis claims, and Outbox relay publication.
- The complete test suite with both real services enabled reports `31 passed`;
  the deterministic suite without those service URLs reports `23 passed,
  8 skipped` because database integration cases require PostgreSQL/Redis.
- Selected Ruff checks, `git diff --check`, repository bytecode compilation,
  and YAML parsing of the Compose file pass.

M3 implementation:

- Added stateless aiohttp admission/query endpoints. Submission persists the
  invocation, session sequence, and Outbox row before returning `202` with
  `Location`, request/trace/invocation identifiers. API processes never build
  an AgentLoop. Owner headers are restricted to the Worker-configured owner in
  the single-Worker topology.
- Added Redis Streams consumer-group transport with bounded prefetch, ACK,
  pending recovery (`XAUTOCLAIM`/fallback), stream depth limits, and Outbox
  retry/backoff. A Worker reconciles durable QUEUED rows when Redis loses data.
- Added a PostgreSQL advisory-lock singleton Worker. It fences RUNNING work as
  `WORKER_LOST` on startup, claims Session heads only after readiness and the
  global execution semaphore, persists LLM/Tool/Result/Final events, ACKs only
  after a terminal commit, and cancels local tasks when ownership is lost.
- Queue deadlines are applied by the database maintenance transition, so
  waiting work does not consume execution capacity. Terminal rows are fenced
  by execution owner and lease; duplicate deliveries only ACK.

M3 verification:

```text
./.venv/bin/ruff check service/worker.py service/api.py: passed
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python -m pytest -q: 24 passed, 14 skipped
./.venv/bin/python -m compileall -q agent service bus membot tests scripts: passed
```

The local PostgreSQL/Redis instances used by M2 were stopped when M3 was
verified, and this sandbox cannot open loopback sockets. Consequently the four
`tests/test_worker.py` cases were explicitly skipped for unavailable
integration services; the deterministic API and runtime tests passed. Before
the M3 checkpoint, rerun the integration command from `docs/PLAN.md` with
PostgreSQL and Redis available.

The wheel packaging defect recorded in M0 remains a deployment blocker for M5.
