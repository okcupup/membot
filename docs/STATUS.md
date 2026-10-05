# Project Status

Date: 2026-10-06

M0 is complete. No HTTP service, Redis queue, PostgreSQL schema, Docker Compose
topology, or Nginx configuration has been implemented yet.

The reviewed starting commit was:

```text
8756befe872bbb3c52dc2d1307a45280cab39189
feat:对每个轮次的message重新定义
```

The local M0 checkpoint is the commit containing this document set and the
deterministic baseline support. It is local only; nothing was pushed remotely.

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

The next gate is M1. It must first preserve the measured M0 semantics while
replacing the global dispatch lock with per-session ordering and an explicit
Worker concurrency limit. The wheel packaging defect must be fixed before any
deployment acceptance gate can pass.
