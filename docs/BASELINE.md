# M0 Baseline

## Provenance

The baseline review started at commit
`8756befe872bbb3c52dc2d1307a45280cab39189` on branch `service-backend`.
The source tree had no user changes at review start. A minimal import fix was
made after the first test run so the deterministic baseline could execute:
`agent/conversation_memory/retrievers/recent.py` now imports `MemoryRecord`.

## Entry points and locking

| Path | Current behavior | M0 implication |
| --- | --- | --- |
| `AgentLoop.run()` (`agent/loop.py:261`) | Consumes the native inbound bus and creates one `_dispatch` task per non-`/stop` message. | The bus is the canonical asynchronous entry point. |
| `AgentLoop._dispatch()` (`agent/loop.py:296`) | Holds one `self._processing_lock` across `_process_message()` and outbound publication. | All Sessions are globally serialized; there is no per-Session acceptance queue. |
| `self._active_tasks` (`agent/loop.py:113`, `276-278`) | Tracks tasks by session key for `/stop`, but does not order or gate execution. | Tracking is not a scheduler. |
| `AgentLoop._handle_stop()` (`agent/loop.py:280`) | Cancels all tracked tasks for the session and its SubagentManager tasks. | Cancellation is best-effort and has no durable state. |
| `AgentLoop.process_direct()` (`agent/loop.py:484`) | Calls `_connect_mcp()` and `_process_message()` directly. | It bypasses the bus, `_dispatch`, and `_processing_lock`. It must not be mixed with bus measurements. |
| `AgentLoop._process_message()` (`agent/loop.py:332`) | Builds context, runs provider/tool iterations, saves memory, and returns an outbound message. | It has no invocation identity, timeout envelope, or durable event stream. |

The only current lock is the global `_processing_lock`. The consolidation path
has one per-session `asyncio.Lock` (`agent/loop.py:365-403`), but it protects
memory archival only and is not an invocation execution lock.

## Tool and context audit

- `_set_tool_context()` (`agent/loop.py:157-162`) mutates the singleton
  `message`, `spawn`, and optional `cron` tools before every turn.
- `MessageTool` stores routing fields and `_sent_in_turn` on the one registered
  tool instance (`agent/tools/message.py:19-37`). `start_turn()` resets the flag
  (`agent/loop.py:413-416`), and `execute()` sets it when the default target is
  used (`agent/tools/message.py:92-106`). Concurrent Sessions would race on
  routing and the per-turn flag.
- MCP is lazy and process-scoped: `_connect_mcp()` creates one `AsyncExitStack`,
  initializes each configured server, and registers wrappers once
  (`agent/loop.py:135-155`, `agent/tools/mcp.py:56-101`). It retries after a
  failed connection but has no invocation-scoped lifecycle.
- `SpawnTool` creates background `asyncio.Task` objects through
  `SubagentManager.spawn()` (`agent/subagent.py:50-80`). Tasks are tracked per
  Session for cancellation, but they are not bounded by a Worker semaphore or a
  durable queue.

## Session, JSONL, and memory audit

- `SessionManager` caches mutable `Session` objects in one process
  (`session/manager.py:79-113`). The cache is not shared across API processes
  and has no cross-process lock.
- `SessionManager.save()` rewrites a session JSONL file synchronously
  (`session/manager.py:162-179`). It is not a PostgreSQL history and is not a
  transactionally coupled queue record.
- `ConversationMemoryEngine` uses `JsonlMessageStore` and
  `RecentMessageRetriever` by default (`agent/conversation_memory/engine.py:48-61`).
  `RedisMessageStore` is an explicit `NotImplementedError` placeholder.
- Long-term memory is workspace-wide `memory/MEMORY.md` and `HISTORY.md`
  (`agent/memory.py:45-67`). Automatic consolidation starts detached tasks
  (`agent/loop.py:395-411`) and can share mutable Session/workspace state with a
  turn.

## Dependency and test checks

The system interpreter is Python 3.6.8, so it cannot satisfy the project
requirement `>=3.11`. The project `.venv` uses Python 3.11.15 and was installed
with:

```bash
./.venv/bin/python -m pip install -e '.[dev]'
```

The first sandbox attempt could not resolve `mirrors.cloud.aliyuncs.com`; an
authorized retry completed the install. With the local LiteLLM model-cost map:

```bash
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python -m pytest -q
```

Result after the one-line import fix:

```text
6 passed in 3.22s
```

The initial run at the reviewed commit was `5 passed, 1 failed`: the failing
test was `ConversationMemoryEngineTest.test_recent_history_matches_session_get_history`
and the exception was `NameError: name 'MemoryRecord' is not defined`. The same
run emitted a LiteLLM model-cost-map DNS warning; that warning is an environment
network issue and not the assertion failure.

## Wheel check

Build command:

```bash
./.venv/bin/python -m pip wheel . --no-deps -w /tmp/membot-wheel-m0
```

The wheel builds (`membot_ai-0.2.0-py3-none-any.whl`). Its archive contains
`membot/__init__.py`, `membot/__main__.py`, bridge files, and metadata, but not
the root implementation packages (`agent`, `bus`, `cli`, `providers`, and so
on). A clean-directory check was run from `/tmp` with `PYTHONPATH` unset:

```text
membot: OK
membot.agent: FAIL ModuleNotFoundError: No module named 'membot.agent'
membot.agent.loop: FAIL ModuleNotFoundError: No module named 'membot.agent'
membot.cli.commands: FAIL ModuleNotFoundError: No module named 'membot.cli'
```

This is a packaging configuration defect. A check run from the source checkout
can appear to pass because `membot/__init__.py` extends its path to the checkout.

## Deterministic execution baseline

`tests/fakes.py` provides a delayed `FakeProvider` with no network access and an
inspectable `llm_start`/`llm_end` event list. Run:

```bash
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python scripts/m0_baseline.py \
  --output /tmp/membot-m0-baseline.json
```

The observed result was:

```text
native_message_bus: provider_max_active=1,
  responses=alpha-1, alpha-2, beta-1, beta-2,
  observed_global_dispatch_serial=true
process_direct: provider_max_active=2,
  responses=direct-1, direct-2,
  bus_outbound_size=0,
  observed_dispatch_lock_bypassed=true
```

The bus case submitted two messages to `baseline:alpha` followed by two to
`baseline:beta`; completions matched that order because the global lock covers
all dispatches. The direct case issued two concurrent calls for
`baseline:direct`; both entered the provider concurrently and published no bus
response. These are observations of the current implementation, not target
performance numbers.

## M1 follow-up observation

After the M0 checkpoint, the deterministic Fake Provider was run again through
the same two entry points. The runtime kernel now gives each effective Session
its own queue and drain task, while a Worker semaphore limits provider
admission:

```text
native_message_bus: provider_max_active=2,
  alpha-1, alpha-2 and beta-1, beta-2 preserve per-session order
process_direct: provider_max_active=1,
  two concurrent calls for baseline:direct remain serial
```

The output is saved by:

```bash
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python scripts/m0_baseline.py \
  --output /tmp/membot-m1-baseline.json
```

This is a deterministic behavior probe, not a throughput benchmark. The
original M0 observations above remain the historical baseline for comparison.

## M4 diagnostic baseline

The M4 deterministic and real-service checks use the same Fake Provider/Tool
fixtures as the runtime tests. The diagnostic path now records bounded,
redacted event payloads in PostgreSQL and emits the committed event record as
JSON on service stdout. The complete acceptance command was:

```bash
DATABASE_URL=postgresql://membot:membot@127.0.0.1:55432/membot_m4_20261008 \
REDIS_URL=redis://127.0.0.1:56379/0 \
LITELLM_LOCAL_MODEL_COST_MAP=True \
./.venv/bin/python -m pytest -q -rs
```

It reported `69 passed in 17.02s`. This is a regression result, not a
throughput measurement. The tests verify event sequence uniqueness, original
ID propagation, redaction, explicit UTF-8 truncation metadata, retention,
failure/timeout nodes, read-only export, safe replay isolation, and candidate
cases with unconfirmed expectations. PostgreSQL 13.23 was queried directly;
Docker was unavailable, so deployment behavior is not part of this baseline.

## M5 deployment baseline

The M5 deployment branch fixed the wheel packaging blocker. A wheel built from
the source checkout was installed into a fresh non-editable Python 3.11
environment; `pip check` passed and imports resolved from `site-packages` for
`membot.agent.loop`, `membot.cli.commands`, `membot.service.entrypoints`, and
the packaged migration files. No source checkout was on `PYTHONPATH`.

The isolated Docker daemon used Docker Engine 27.5.1 and Compose 2.32.4. The
verified manifest digests are recorded in `deploy/images.env`; local pulls used
the explicitly recorded public cache transport because Docker Hub timed out on
this host. The final image was built from the pinned Python base with the
locked runtime/build requirements and `python -m pip check` passed in-image.

The deterministic local deployment used a self-signed seven-day localhost
certificate and the explicit fake Provider. It started PostgreSQL 16.8,
Redis 7.4.2, the migration service, two API containers, one Worker, and Nginx.
Only `127.0.0.1:8088` and `127.0.0.1:8443` were published by Nginx. Smoke
accepted `202`, verified Location and stable idempotency, observed a successful
Final with 15 events, and queried it by one invocation ID.

Real local deployment evidence on 2026-10-09 UTC (phase crossed into 2026-10-10 CST):

```text
LITELLM_LOCAL_MODEL_COST_MAP=True DATABASE_URL=...membot_m5_20261009 REDIS_URL=... ./.venv/bin/python -m pytest -q -rs
89 passed in 30.97s
./.venv/bin/python scripts/wheel_check.py
fresh non-editable install: passed; pip check: no broken requirements
make deploy-smoke ENV_FILE=.env.m5-local
HTTP 202; duplicate invocation ID stable; changed payload HTTP 409; HTTPS verified; eventCount=15
make deploy-api-drill ENV_FILE=.env.m5-local
oldIP=172.29.55.4 newIP=172.29.55.8; nginxUnchanged=true; one observed passive 504; API2 served during failure
make deploy-worker-drill ENV_FILE=.env.m5-local
WORKER_LOST recovery: passed; graceful drain: SUCCEEDED; WORKER_DRAIN_TIMEOUT: FAILED; successor stayed QUEUED then SUCCEEDED
make deploy-restart-check ENV_FILE=.env.m5-local
terminal row persisted; next turn returned history_turns=1
make deploy-rate-check ENV_FILE=.env.m5-local
138 HTTP 429 responses; health/live remained available
make deploy-backup / make deploy-restore-check ENV_FILE=.env.m5-local
random isolated database restored; six table fingerprints matched; live database untouched
```

The local deployment used no real model credentials and makes no public
availability claim. Cloud validation is still missing a target host, DNS,
firewall policy, and publicly trusted certificate. A production operator must
provide those and run smoke with a real Provider before publishing externally.

## M6 executable evaluation baseline

The standard suite is 32 executable Cases, eight per category. It injects
safe Provider/Tool fixtures into the actual AgentLoop; the queue subset uses
real HTTP/PG/Redis/Worker production paths. Fixtures measure engine contracts,
not actual model quality. Commands, metrics, candidate review and CI setup are
in `docs/EVALUATION.md`; exact acceptance is in `docs/STATUS.md`.

Final local artifacts pin runtime commit
`3cb1f61f9d0ae4885c9f088439bad6fbaaf64562`, Python 3.11.15/Linux/x86_64 and
the explicit dependency versions in each report. Queue reports also record
PostgreSQL 13.23/Redis 6.2.24. Provenance pins all individual Case versions and
hashes, model `m6-fixture-v1`, temperature 0, max tokens 1024, two repetitions,
Tool definitions and rubric hashes. The principal hashes are:

```text
effective_code_hash=f5f17680c043a8d662116546397ddfb51e1278193114feb0dc4da525cdfe7915
prompt_hash=fe0c448945eb7cca7ee8baea4d25b3810e9eff8f649264e04c06ce4c89bef0bc
kernel_tool_schema_hash=279d13b2eb43c40893751430864b7d461e4f60205d80b7f52fb2fad116becf4b
queue_tool_schema_hash=ffb2b3fe753eb353d79f535edfe75b6e3f821810b87560124ed828364a6d706e
```

`evaluation/baselines/m6-deterministic.json` seals the raw reference run and
integrity hash. `evaluation/reports/m6-current.json` is a separate second run;
`m6-comparison.json` pairs all 64 samples with no new failures or recoveries.
Both runs have 102 logical invocations: 84 succeeded, 12 intentionally failed,
six intentionally timed out, zero unfinished. Strict Tool Accuracy is 64/64,
business-eligible fixture Case success 48/48, and matched call precision/recall
42/42. Separate denominators prevent expected faults or no-Tool tasks from
being misrepresented as successful model tool tasks.

`m6-queue.json` runs 12x2 Cases: 24/24 contracts, 42 unique invocations
(30 succeeded/eight expected failed/four expected timeout). It observes 66
HTTP 202 responses including 24 idempotent duplicates, without additional
logical tasks or terminal Tool re-execution. Strict Tool Accuracy is 24/24,
matched call precision/recall 16/16. Twenty Cases are explicitly excluded from
queue mode, not counted as passing there. Full old/new tests with both real
services report `163 passed in 45.00s`, no skips.

The independent kernel run E2E mean/p95 is 14.056/46.574 ms; queue mode is
137.976/268.466 ms. These are local fixture observations with intentional
faults and harness/query overhead; they imply no capacity or stable latency
improvement. Raw repetitions, queue/execution samples and paired deltas are
retained. Code/Agent Prompt changes are comparable; changed Case/schema/model/
rubric/Judge/configured parameters/mode/environment/repetition count requires
an explicit new baseline. Report directories/private configs are git-ignored
and excluded from the installed wheel/Docker build context.

Four injected CLI defects each return 1 and pass with exit 0 after removal;
failure recordings from PostgreSQL become runnable reviewed fixtures without
executing external writes. `scripts/m0_baseline.py` separately still observes
native bus parallelism 2 and one-Session process_direct serialism 1.

There is no real-model baseline yet. Agent/Judge credentials, pinned private
configuration and human confirmation of the four proposed Judge labels are
missing. Requested real preflight fails explicitly. No fixture rate or
synthetic calibration result is presented as actual model/Judge performance.
