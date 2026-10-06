# Project Status

Date: 2026-10-06

M0 and M1 are complete. No HTTP service, Redis queue, PostgreSQL schema, Docker
Compose topology, or Nginx configuration has been implemented yet.

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
commit, not performance claims. M2 is next:
durable PostgreSQL state and transactional Outbox/Redis delivery. The wheel
packaging defect remains a deployment blocker and is intentionally carried into
M2/M5 rather than hidden by source-tree imports.
