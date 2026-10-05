# Membot Engineering Rules

This file is the repository development guide. `templates/AGENTS.md` is a user
workspace template and is not a Codex or repository rule file.

## Scope and current boundary

- Keep the existing CLI commands and their observable test semantics.
- Use Python `asyncio` and the existing `AgentLoop`, `LLMProvider`, tools, and
  conversation-memory interfaces as the runtime foundation.
- M0 is review, baseline, and documentation only. Do not add an HTTP service,
  Redis protocol, PostgreSQL schema, or deployment stack until the corresponding
  plan phase is started.
- The service target is one host, two stateless API processes, one asyncio Worker
  process, and one execution process inside that Worker.

## Source layout

The implementation packages (`agent`, `bus`, `channels`, `cli`, `config`,
`cron`, `heartbeat`, `providers`, `session`, and `utils`) currently live at the
repository root. `membot/__init__.py` extends the package path so source-tree
imports such as `membot.agent.loop` work. The wheel currently does not include
those root packages; this is a tracked deployment blocker in `docs/BASELINE.md`.

## Working rules

- Treat mutable state as owned by one Session, one invocation, or one process.
  Do not make a process-global singleton the owner of request context.
- A Session is ordered by acceptance sequence. Different Sessions may execute
  concurrently. A Worker-level semaphore limits main invocation concurrency.
- Queue delivery is at-least-once. Never describe it as exactly-once.
- PostgreSQL is the service history and state source of truth. Redis is a bounded
  transport, not the durable record.
- Persist task creation and its Outbox record in one database transaction, then
  retry publication and make Worker handling idempotent.
- Keep both waiting and execution deadlines explicit. Queue waiting must not
  consume the execution budget.
- Errors, cancellations, Worker interruptions, and timeouts must not become
  `SUCCEEDED`.
- Keep bounded in-memory queues and bounded Worker memory. Make backpressure and
  rejection observable.
- Carry `requestId`, `traceId`, and `invocationId` through API, queue, Worker,
  provider, tool, persistence, and diagnostic events.
- Prefer structured data and existing local helpers. Add a focused regression
  test for each state, ordering, timeout, or recovery contract.

## Commands

Use the repository virtual environment, not the system Python 3.6:

```bash
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python -m pytest -q
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python scripts/m0_baseline.py
./.venv/bin/python -m compileall -q agent bus channels cli config cron heartbeat providers session utils membot tests scripts
```

The LiteLLM environment variable keeps the deterministic baseline from making
an unrelated network request for a model-price map. Real-provider tests must be
explicitly marked and must never run as part of the deterministic default suite.

Before a release candidate, build and inspect a wheel from outside the source
checkout; source-tree imports are not evidence that the wheel is complete.

## Review checklist

For changes touching execution, inspect both native bus dispatch and
`process_direct`. For changes touching tools, check MessageTool per-turn state,
MCP lifecycle, SpawnTool background tasks, and routing context. For changes
touching memory, check SessionManager cache ownership, JSONL writes, long-term
memory files, and consolidation tasks. Record commands and real outcomes in
`docs/STATUS.md` and update `docs/BASELINE.md` when the baseline changes.
