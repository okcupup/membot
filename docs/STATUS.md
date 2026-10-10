# Project Status

Date: 2026-10-10 (Asia/Shanghai)

M0 through M5 are complete locally. M6 implementation and local engineering
acceptance are complete: executable Cases, durable queue evaluation, metrics,
paired baselines, reviewed failure candidates and CI. Actual real-model/Judge
calibration remains unverified because model credentials, pinned private
configuration and human-confirmed labels are absent. The local TLS/Fake
Provider results do not represent public HTTPS deployment or real task success.
See `docs/DEPLOYMENT.md` and `docs/EVALUATION.md` for runnable commands.

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

M4 implementation:

- Added `0002_diagnostics.sql` fields and retention index. Every Invocation gets
  a PostgreSQL-allocated event sequence and immutable correlation IDs. Event
  writes and durable state changes use short transactions; staged JSON logs are
  emitted only after commit.
- Recorded API admission, Outbox publication/error, Queue consume/ACK and
  pending recovery, RUNNING, configuration/context/history, LLM start/end/error,
  Tool start/end/error, Result, Final, timeout, failure, and Worker interruption
  events. Each record includes step, worker/attempt, spans, tool call ID,
  duration, error code, expiry, and bounded redacted payload.
- Added JSON stdout logging for service processes and separated query
  `requestId` from the Invocation's original request ID. API event queries are
  paged and owner-scoped.
- Added `scripts/trace_probe.py` for read-only timeline rendering, timing/error
  summaries, recording export, reviewed failure candidates, and safe replay with
  Recorded Provider/Tool adapters in a disposable workspace. Replay refuses
  incomplete, truncated, expired, tampered, multi-attempt, Worker-lost, and
  cancelled recordings. Candidate `expected` remains unconfirmed by default.
- Kept status result blobs compact and separately bounded Final/error captures;
  large visible payloads carry explicit truncation metadata. Private reasoning,
  credentials, authorization headers, and common contact identifiers are
  redacted before persistence or stdout.

M4 verification:

```text
DATABASE_URL=postgresql://membot:membot@127.0.0.1:55432/membot_m4_20261008 \
REDIS_URL=redis://127.0.0.1:56379/0 \
LITELLM_LOCAL_MODEL_COST_MAP=True \
./.venv/bin/python -m pytest -q -rs
69 passed in 17.02s
```

The run used the real local PostgreSQL 13.23 and Redis service. It covered
multi-round LLM/Tool timelines, provider and tool errors, LLM/Tool/execution
timeouts, iteration exhaustion, queue expiry, ownership loss, duplicate ACK,
redaction and retention, two API processes, migration backfill, safe replay,
candidate export, and stdout JSON logs. Ruff, bytecode compilation, and
`git diff --check` also passed. Docker was unavailable during M4; M5 installed
isolated tooling for the later deployment checks below. No performance number
is claimed.

M5 implementation and decisions:

- Installed `membot-service api|worker|migrate|check-config` entrypoints run
  directly as the container process. Each API has one aiohttp process; the
  Worker has one asyncio execution process, a singleton PostgreSQL advisory
  lock, and bounded main invocation/prefetch budgets. Healthcheck commands may
  start short probe processes; they are not execution workers.
- Fixed wheel inclusion of the root implementation packages and migration,
  template and skill resources. Source-tree package extension is conditional,
  so a clean installed wheel no longer depends on a sibling checkout. Added
  exact transitive/build dependency locks and pinned official image manifests.
- Compose includes Nginx/api1/api2/Redis/PostgreSQL/Worker and one-shot
  migration. Only Nginx publishes ports. Named volumes persist DB, Redis AOF,
  and tool workspace; application containers are nonroot/read-only with
  tmpfs, resource/PID limits, restart policies and rotated JSON logs.
- Nginx terminates TLS, uses least_conn and passive max_fails/fail_timeout,
  request limits, bounded connect/read/write timeouts, upstream zones and
  Docker DNS resolve. POST retries do not enable non_idempotent. Client retries
  require identical payload and Idempotency-Key. Explicit network IPAM enables
  an address-change drill without restarting Nginx.
- API live is process responsiveness. Ready checks DB/schema, durable capacity
  and drain state. Redis availability, queue depth, unpublished Outbox and
  fenced Worker heartbeat are separate diagnostics; admission can remain ready
  while Worker/Redis are absent. Docker healthcheck does not actively remove
  Nginx upstreams or automatically restart an unhealthy container.
- API SIGTERM rejects new mutations and completes admitted short transactions.
  Worker stops receiving/starting work, renews its current ownership during a
  20-second drain, then cancels remaining executions, closes resources and
  records FAILED/WORKER_DRAIN_TIMEOUT. Unclaimed tasks remain QUEUED. Kill
  recovery records FAILED/WORKER_LOST; uncertain write tools are not replayed.
  API/Worker stop grace is 40/45 seconds and the application cleanup is bounded.
- Shell cancellation kills the POSIX process group, including descendants.
  MCP shared initialization is cancelled and cleaned up at process shutdown;
  Provider clients expose async close. Service env does not enable MCP servers,
  consolidation, spawn, cron or Agent heartbeat background tasks.
- Makefile operations validate configuration, start the stack, check health,
  show doctor diagnostics, smoke, drill API/Worker failure, test rate limits,
  restart persistence, backup and restore. Backup and its manifest share an
  exported PostgreSQL snapshot; restore validation uses a disposable database,
  never the live DB. `.env` is parsed as data, not shell code.

M5 environment distinctions:

- Ordinary sandbox runs cannot resolve dependencies or connect to loopback;
  these failures/skips are environmental. The elevated full regression used
  real local PostgreSQL 13.23/Redis and a separate `membot_m5_20261009` database.
- Docker Engine 27.5.1 ran in an isolated `/tmp` data/socket directory; the
  host's existing Nginx was untouched. GitHub Compose binary download stalled,
  and the official EL9 package required newer glibc. Compose 2.32.4 and Buildx
  0.20.0 were therefore run in an isolated compatible Python container.
- Docker Hub timed out. Verified fixed official images were pulled via
  `docker.m.daocloud.io/library`, with the actual manifest digests recorded in
  `deploy/images.env`. Container PyPI download later timed out on a large wheel;
  the successful build used the same fixed requirements from a downloaded
  wheelhouse with MEMBOT_BUILD_OFFLINE=1. This fallback is documented and tested.
- The first full run found an obsolete test assertion of two migrations; it
  now checks the migration directory count (three). The first address-change
  drill found that Docker auto-subnet networks reject a requested IP; an
  explicit configurable subnet corrected the drill. Both were fixed before
  acceptance; neither failure is counted as a pass.

M5 actual regression commands and results:

```bash
LITELLM_LOCAL_MODEL_COST_MAP=True \
DATABASE_URL=postgresql://membot:membot@127.0.0.1:55432/membot_m5_20261009 \
REDIS_URL=redis://127.0.0.1:56379/0 \
./.venv/bin/python -m pytest -q -rs
# 89 passed in 30.97s; no skipped integration cases

./.venv/bin/python scripts/wheel_check.py
# built wheel; fresh non-editable install; site-packages imports, CLI and migration resources passed
# python -m pip check: No broken requirements found

./.venv/bin/ruff check agent/loop.py agent/persistence service scripts \
  tests/test_service_lifecycle.py tests/test_deploy_client.py \
  tests/test_worker.py tests/test_diagnostics_integration.py
# All checks passed
./.venv/bin/python -m compileall -q agent bus channels cli config cron heartbeat \
  providers session utils membot service tests scripts
git diff --check
# passed
```

The exact local deployment command prefix was:

```bash
export DOCKER_HOST=unix:///tmp/membot-m5-docker.sock
export DOCKER_BIN=/tmp/membot-docker-tools/docker/docker
export COMPOSE_BIN=/tmp/membot-compose
./.venv/bin/python scripts/deploy.py --env-file .env.m5-local init-local
make deploy-config ENV_FILE=.env.m5-local
make deploy-up ENV_FILE=.env.m5-local
make deploy-doctor ENV_FILE=.env.m5-local
make deploy-smoke ENV_FILE=.env.m5-local
make deploy-worker-drill ENV_FILE=.env.m5-local
make deploy-restart-check ENV_FILE=.env.m5-local
make deploy-api-drill ENV_FILE=.env.m5-local
make deploy-rate-check ENV_FILE=.env.m5-local
make deploy-backup ENV_FILE=.env.m5-local
make deploy-restore-check ENV_FILE=.env.m5-local \
  BACKUP=deploy/backups/membot-1791560902697833714.dump
```

Real results (local fixture, 2026-10-09 UTC; phase finished on 2026-10-10 CST):

- Image built and imported the installed AgentLoop/CLI/service/migrations;
  pip check passed. The migration exited 0 and all six running services became
  healthy. Inspection confirmed only Nginx host-port mappings and one main
  Python process in each API/Worker container.
- HTTPS smoke verified localhost certificate/hostname, 202 + Location, same
  key/body returning the same Invocation, different body 409, Final SUCCEEDED
  and 15 diagnostic events. `model=not_probed` in doctor is deliberate.
- API1 Kill produced one 504 among 30 sampled requests; the subsequent 29
  responses came from API2. Existing task succeeded, duplicate submission kept
  its ID. API1 changed from 172.29.55.4 to 172.29.55.8; both instance IDs were
  observed after recovery, with the Nginx container unchanged. This is an
  observed error window, not an availability/performance promise.
- Worker Kill marked the original RUNNING Invocation FAILED/WORKER_LOST;
  accepted QUEUED work resumed. Normal SIGTERM allowed the running fixture to
  finish SUCCEEDED. Drain deadline yielded FAILED/WORKER_DRAIN_TIMEOUT; its
  same-Session successor stayed QUEUED and subsequently SUCCEEDED.
- Whole-stack down/up kept volumes and the queried terminal result identical;
  the next turn at session_seq=2 returned `history_turns=1`.
- Rate check observed 62 HTTP 404 and 138 HTTP 429 responses; health/live was
  still available. These are test outcomes, not benchmark measurements.
- Consistent backup restored into a random isolated database and matched all
  six table count/content fingerprints: sessions=8, messages=14,
  invocations=10, archives=0, events=141, outbox=10. The backup SHA256 was
  checked; the isolated database was dropped and live data was unchanged.

Evidence reports and backup files remain local in ignored deploy/reports and
deploy/backups. Secrets, local env and certificates are ignored. Development
commits are 6d28811 (lifecycle), 45feb6c (wheel/image/Compose), and 6ca4c93
(scripts/drills/fixes); the fourth commit closes documentation. The phase is
merged to local main with --no-ff after acceptance, without a remote push.

Remaining risks at the M5 checkpoint:

1. No target cloud host, domain/DNS, firewall/access scope or public certificate
   was supplied. Local self-signed HTTPS is verified; public HTTPS deployment
   and real-provider smoke are unverified. Runnable production configuration is
   ready for review in docs/DEPLOYMENT.md.
2. M6 still needs the 32-case regression/evaluation corpus, baselines and
   explicit real-model assessment. Existing tests use deterministic providers.
3. Load/memory capacity and broader Redis/DB failure drills remain M7/M8. M5
   verifies resource configuration and controlled failures, not host capacity.
4. The single-owner API has no user login/auth layer. External exposure needs
   the intended caller access policy; tool workspace files require their own
   backup alongside PostgreSQL if real write tools are used.

## M6 implementation and acceptance

Branch: `test/m6-regression`, starting from the M5 implementation checkpoint
`fcef948`. Runtime/evaluator acceptance artifacts pin
`3cb1f61f9d0ae4885c9f088439bad6fbaaf64562`; the fourth development commit closes
documentation. The three implementation commits are:

- `f3d0995`: 32 versioned runnable Cases and actual AgentLoop fixture execution.
- `951285d`: HTTP/Queue evaluation, structured Judge/budget, paired Baselines
  and explicitly reviewed failure candidate registration.
- `3cb1f61`: evidence/replay checks, installed evaluator, package whitelist and
  CI gates retaining raw repeated samples.

Each standard category has eight Cases. Fixtures inject Provider/Tool adapters
into the real AgentLoop, scheduler, memory and Tool iteration loop. Event gates
check serial three turns, parallel Sessions, limits, cold progress behind a hot
Session, routing/Final isolation, timeout and cancellation recovery. Case
expected fields and observed outputs remain separate. The four intentional
defects (Tool error, context leak, false terminal status and missing Tool with
a completion claim) fail the pipeline and pass after removing the injection.

The queue runner uses two production API apps with separate PostgreSQL pools,
one production AsyncWorker, real Redis Streams and durable Outbox/events. It
runs 12 selected Cases, including errors/timeouts and concurrent ordering,
queries through API2 after API1 submission, and checks HTTP duplicate admission
and terminal redelivery without re-execution. Dedicated owners/Sessions and a
random stream isolate each run; retained database records support diagnosis.
It does not claim 32 Queue Cases: the other 20 are explicitly excluded there
and covered by the kernel/previous integration suites.

Strict Tool Accuracy checks required/allowed/forbidden names, key arguments,
extra occurrences and per-turn order. Its denominator includes prohibition
Cases; call-bearing and no-call Case accuracy are also separate. JSON boolean
and integer assertions cannot match accidentally. Business success, technical
SUCCEEDED and expected recovery failures have distinct counts/denominators.
Incomplete/truncated evidence, missing context, null/mismatched spans and IDs
cannot become a pass. Reports have a 32 MiB limit and private atomic writes.

Baselines fix Case/version/schema/model/parameters/environment/repetition
metadata and seal the raw report integrity hash. Code/Agent Prompt changes can
be compared; changed Case/schema/rubric/Judge/model/mode/environment requires
an explicit new baseline. Comparisons pair Case/repetition and preserve every
latency sample; there is no significance or stable-improvement claim.

Failure candidate tests read actual PostgreSQL Provider/Tool error recordings,
export/redact input/history/config, require explicit expected review, register
Cases and execute them in a fresh AgentLoop. Shell/write/network operations are
recorded adapters only; an external sentinel remains absent. Tampered,
expired, partial, unconfirmed or implicit expectations are rejected. The test
reviewer is a synthetic fixture identity, not a claim of human dataset review.

Exact final checks:

```bash
LITELLM_LOCAL_MODEL_COST_MAP=True \
DATABASE_URL=postgresql://membot:membot@127.0.0.1:55432/membot_m6_20261010 \
REDIS_URL=redis://127.0.0.1:56379/0 \
./.venv/bin/python -m pytest -q -rs
# 163 passed in 45.00s, no skipped integration cases

LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python scripts/evaluate_cases.py \
  --mode deterministic --assert-count 32 --repeat 2 --assert-report \
  --output evaluation/reports/m6-baseline-run.json \
  --write-baseline evaluation/baselines/m6-deterministic.json --replace-baseline
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python scripts/evaluate_cases.py \
  --mode deterministic --assert-count 32 --repeat 2 --assert-report \
  --output evaluation/reports/m6-current.json
LITELLM_LOCAL_MODEL_COST_MAP=True ./.venv/bin/python scripts/evaluate_cases.py \
  --mode baseline --assert-report \
  --baseline evaluation/baselines/m6-deterministic.json \
  --report evaluation/reports/m6-current.json \
  --output evaluation/reports/m6-comparison.json
# both 64/64 Case Contracts; comparison has 64 pairs, zero new failures/recoveries

LITELLM_LOCAL_MODEL_COST_MAP=True \
DATABASE_URL=postgresql://membot:membot@127.0.0.1:55432/membot_m6_20261010 \
REDIS_URL=redis://127.0.0.1:56379/0 \
./.venv/bin/python scripts/evaluate_cases.py \
  --mode queue --assert-count 12 --repeat 2 --assert-report \
  --output evaluation/reports/m6-queue.json
# 24/24 Case Contracts; 42 unique Invocations; 66 HTTP 202, including 24 duplicates

./.venv/bin/python scripts/wheel_check.py
# fresh locked-dependency install; pip check, site-packages imports, CLI,
# all Case resources and installed membot-eval basic-echo execution passed
# wheel excludes reports/baselines/candidates/private configs
./.venv/bin/ruff check evaluation scripts/evaluate_cases.py \
  scripts/wheel_check.py tests/regression service/diagnostics.py
./.venv/bin/python -m compileall -q agent bus channels cli config cron heartbeat \
  providers session utils membot service evaluation tests scripts
git diff --check
# passed
```

Actual engineering metrics (two repetitions, Fake Provider/isolated Tools):

| Metric | Kernel | Real HTTP/PG/Redis Queue |
| --- | --- | --- |
| Case Contract | 64/64 | 24/24 |
| Business-eligible fixture Case | 48/48 | 12/12 |
| Strict Tool Accuracy | 64/64 | 24/24 |
| Call-bearing Case Accuracy | 28/28 | 10/10 |
| No-Tool Policy Accuracy | 36/36 | 14/14 |
| Call precision / recall | 42/42 each | 16/16 each |
| Unique Invocations accepted / rejected | 102 / 0 | 42 / 0 |
| SUCCEEDED / expected FAILED / expected TIMEOUT | 84 / 12 / 6 | 30 / 8 / 4 |
| Nonterminal / unexpected failure / unexpected timeout | 0 / 0 / 0 | 0 / 0 / 0 |
| Timeout Rate | 6/102 | 4/42 |
| E2E mean / p95, ms | 14.056 / 46.574 | 137.976 / 268.466 |
| Queue wait mean / p95, ms | 2.534 / 14.055 | 64.145 / 209.677 |
| Execution mean / p95, ms | 11.752 / 46.441 | 51.998 / 93.485 |

Kernel E2E/queue has 102 samples and execution 100 because two intentionally
expired queued turns never started. Queue timing has 42 samples. The fixture
timeouts make the Timeout Rate nonzero by design. These local measurements
include harness/HTTP/query overhead and deliberate faults; they are not a
real-model quality, throughput, capacity or stable performance claim. Baseline
and independent current raw results remain in ignored `evaluation/reports/`
and `evaluation/baselines/` with Case hashes and provenance.

The host test used Python 3.11.15, PostgreSQL 13.23 and Redis 6.2.24; CI services
pin PostgreSQL 16.8/Redis 7.4.2. Ordinary sandbox sockets/DNS are restricted;
real local service and wheel dependency checks were run with authorized
external access. The final tests contain no unavailable-service skips.
An initial Outbox trace assertion used the wrong event step; candidate replay
also duplicated the production Tool error hint. Both were fixed and tested.
Wheel inspection additionally found local reports included by directory force
inclusion; source/resource whitelisting and a negative package check fixed it.

CLI fault probes for `tool_error`, `context_leak`, `wrong_status` and
`missing_tool` each exited 1; corresponding fixed runs exited 0. Their raw
reports and `m6-fault-gates.json` are retained. The native bus baseline still
observes max active 2 across Sessions; process_direct observes 1 for one
Session and outbound queue size 0, recorded separately at
`/tmp/membot-m6-native-baseline.json`.

CI is configured for every push/PR engineering regression, default real queue
integration, repeated samples/artifact upload and paired reference comparison.
With reviewed credentials/config/budget, Prompt/Tool/Provider/Case changes
trigger real-model runs; fork PRs receive no secrets. Workflow YAML and local
commands passed. No remote CI run or remote push was performed.

Real-model acceptance is still pending, explicitly:

- `MEMBOT_EVAL_API_KEY`, `MEMBOT_JUDGE_API_KEY`, `MEMBOT_LLM_API_KEY` and
  `OPENAI_API_KEY` were absent (only presence was inspected, no values printed).
- No pinned private Agent/Judge model/endpoint/pricing configuration was
  provided. `--mode real` without it exited 1 and saved `eval_error` in
  `evaluation/reports/m6-real-preflight.json`.
- Four proposed calibration labels include pass/fail examples and the
  completion-without-Tool negative. No human confirmation was received;
  `evaluation/judge_labels.json` remains `proposed_manual_labels`.
- Tests validate structured verdicts, literal evidence, missing credentials,
  bounded spending, uncalibrated Judges and deterministic gate priority using
  fixtures. They do not measure actual model task success or real Judge accuracy.

The local engineering checkpoint is merged to main with `--no-ff` after these
checks; `learning_docs/M6.md` and the user's AGENTS.md edits remain outside all
phase commits. M7/M8 capacity and broader fault drills are still future phases.
