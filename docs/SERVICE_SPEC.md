# Agent Service Specification

This is the service contract for the backend. M1 implements the in-process
runtime kernel. M2 adds PostgreSQL history/invocation persistence, migrations,
and bounded Redis Outbox transport. M3 adds the asynchronous HTTP admission,
query API, and single-process Redis Streams Worker. M4 adds durable
Invocation diagnostics, structured stdout logs, timeline viewing, and safe
recorded replay. M5 supplies the single-host deployment; M6 adds executable
regression/evaluation Cases, metrics, paired baselines and reviewed candidates.

## Scope

The first deployable service runs on one host with Nginx, two stateless API
instances, Redis, PostgreSQL, and one single-process asyncio Worker. The API
accepts work and returns quickly; the Worker owns execution. The existing CLI
continues to call the existing AgentLoop path until an explicit migration phase.

## Submission contract

`POST /v1/sessions` creates an owner-scoped Session. `POST /v1/invocations`
accepts a bounded JSON body containing `sessionId` and
the user message, optional media and metadata, and optional client request ID.
The API generates or validates:

- `requestId`: the submission request identity;
- `traceId`: the end-to-end trace identity;
- `invocationId`: the durable execution identity;
- `sessionId`: the ordering key.

On durable acceptance it returns HTTP `202`:

```json
{
  "invocationId": "...",
  "sessionId": "...",
  "status": "QUEUED",
  "requestId": "...",
  "traceId": "..."
}
```

The API must not return `202` before the invocation row and its Outbox record are
committed. A duplicate client idempotency key returns the original invocation
identity and does not create a second logical task.

`GET /v1/invocations/{invocationId}` returns the durable status, timestamps,
error code/message (when terminal), and final result when available.
`GET /v1/invocations/{invocationId}/events` returns the ordered diagnostic
timeline. It accepts `after` and `limit` cursors (at most 1000 per response)
and returns `nextAfter` when more events remain. A query has its own HTTP
`X-Request-ID`; the response and every event retain the original submission
`requestId` and `traceId`. The timeline contains queue, `LLM`, `TOOL`, history,
`result`, and `FINAL` categories and carries `invocationId`, `traceId`, the
original `requestId`, and `sessionId`.

## State machine

The only execution states are `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, and
`TIMEOUT`.

```text
QUEUED --claim--> RUNNING --final result--> SUCCEEDED
   |                 |  \
   |                 |   +--deadline------> TIMEOUT
   |                 +------error---------> FAILED
   +--queue deadline----------------------> TIMEOUT
RUNNING --worker interruption------------> FAILED (WORKER_LOST)
```

The Worker uses an atomic claim/lease so redelivery cannot run the same
invocation concurrently. A timeout or cancellation is terminal only after the
database records the reason. An exception, missing result, or provider error can
never be written as `SUCCEEDED`.

`queueTimeout` starts at acceptance and ends at the Worker claim. `runTimeout`
starts only after a successful claim. Waiting time therefore does not consume
the execution budget. The Worker records which deadline fired.

## Ordering and concurrency

- Acceptance assigns a monotonic per-session sequence in PostgreSQL.
- One Session executes accepted invocations strictly in that sequence.
- Different Sessions may run in parallel.
- A Worker semaphore bounds the number of main invocations admitted to provider
  execution. Tool calls spawned by an invocation remain attributed to it and
  obey their own bounded policy.
- Queue length, Worker memory, and in-process task registries are bounded. A full
  queue causes a visible retryable rejection or leaves the durable Outbox pending.

The M1 in-process settings are under `agents.runtime`:

```yaml
agents:
  runtime:
    maxConcurrentInvocations: 4
    maxPendingInvocations: 256
    inboundQueueSize: 256
    outboundQueueSize: 256
    queueTimeoutSeconds: null
    executionTimeoutSeconds: null
    maxSubagentTasks: 16
    maxConcurrentSubagents: 4
    enableConsolidation: true
    enableSubagents: true
    enableCron: true
```

The native bus applies backpressure when either queue reaches its limit.
`queueTimeoutSeconds` measures acceptance-to-admission waiting, while
`executionTimeoutSeconds` starts only after the Worker semaphore is acquired.
For a service process, disable `enableConsolidation`, `enableSubagents`, and
`enableCron` until their durable lifecycle is implemented; the existing CLI
gateway may enable them with the configured subagent cap. Heartbeat remains
controlled by `gateway.heartbeat.enabled` and is disabled in a service process
until a durable scheduler owns its lifecycle.

The service must preserve at-least-once delivery. Idempotency keys and
conditional invocation claims reduce duplicate effects; they do not change the
delivery guarantee to exactly-once.

## Persistence and recovery

PostgreSQL stores sessions, invocations, invocation context, state transitions,
Outbox rows, and diagnostic events. The acceptance payload is immutable. The
effective history/context snapshot is recorded at execution, after earlier
Session turns become terminal; subsequent turns do not rewrite that snapshot.

The API acceptance transaction inserts the invocation and Outbox row together.
M2 implements the transaction in `PostgresRepository.accept_invocation`; the
relay retries pending Outbox rows until Redis accepts the envelope. The service
uses a bounded Redis Stream with consumer-group pending recovery and ACK after
the durable terminal commit. Delivery is at least once: publication can precede
a crash before the Outbox row is marked. PostgreSQL remains the ordering/state
authority. Lost RUNNING work becomes FAILED/WORKER_LOST instead of automatic
re-execution; recovered QUEUED work keeps its sequence and can be republished.

M2 migrations live in `agent/persistence/migrations/`. `sessions` owns
`next_session_seq` and `next_message_seq`; owner-scoped uniqueness constraints
protect both. `invocations` stores the immutable acceptance payload and hash,
timeouts, execution owner/lease, timestamps, result, and terminal error. The
same owner-scoped idempotency key plus canonical payload hash returns the
original invocation; a different hash raises a conflict. The transaction that
allocates a Session sequence also inserts `invocations`, its initial event, and
its Outbox row. Failed invocations keep their sequence; the next accepted
invocation receives the next sequence and can run once earlier entries are
terminal.

`session_messages` stores only complete successful turns. Service history is
read by the PostgreSQL ConversationMemoryEngine adapter, with the requested
memory window applied in SQL and no cross-process Session cache. Provider or
tool failures and iteration exhaustion keep structured invocation results and
events but do not append partial tool-call protocol or error text to history.
The short completion transaction commits successful turn messages and terminal
result together; no database transaction spans an LLM or Tool await. `/new` is
an ordered invocation: its transaction archives current messages, clears the
current history, and completes the invocation while preserving both monotonic
invocation and message sequence identifiers.

`ExecutionResult` separates `FINAL`, `PROVIDER_ERROR`, `TOOL_ERROR`,
`ITERATION_LIMIT`, `CANCELLED`, `TIMEOUT`, and `INTERNAL_ERROR`. Only a normal
`FINAL` maps to technical `SUCCEEDED`; evaluation of whether the requested
business task was actually accomplished belongs to the regression/evaluation
layer.

CLI construction continues to select the JSONL-backed memory engine. A service
process opts into PostgreSQL by injecting
`ConversationMemoryEngine.for_postgres(repository, owner_id=...)` into
`AgentLoop`. The M5 Worker sets `enable_consolidation`, `enable_subagents`, and
`enable_cron` to false; these capabilities are not exposed through service env.
Install service-only drivers with `python -m pip install '.[service]'`.
`DATABASE_URL` and `REDIS_URL` are used by service processes and migration/test
tooling. M2 masks common key/value, bearer, and provider-token forms at the
persistence boundary; M4 applies the same policy to structured logs, event
payloads, status blobs, timeline exports, and candidate cases.

After Worker interruption, the replacement takes the exclusive advisory lock
and marks uncertain RUNNING rows FAILED/WORKER_LOST, preserving their Trace.
Recovery is visible and does not automatically replay tools with unknown side
effects. Unclaimed QUEUED work remains eligible after earlier terminal rows.

## Diagnostics

PostgreSQL `invocation_events` is the diagnostic source of truth. Migration
`0002_diagnostics.sql` adds a per-Invocation `event_seq`, `worker_id`,
`attempt`, `step`, `span_id`, `parent_span_id`, `tool_call_id`, `duration_ms`,
`error_code`, and `expires_at`. The event row itself supplies the immutable
`invocationId`, `traceId`, original submission `requestId`, and `sessionId`.
Event sequence allocation updates the Invocation row in the same short
transaction, so concurrent API, Outbox, queue, and Worker writers cannot reuse
an ordinal. A failed transaction emits no staged event log.

The normal timeline is:

```text
accepted -> OUTBOX.publish -> QUEUE.consume -> running
  -> CONFIG.snapshot -> CONTEXT/HISTORY.read
  -> LLM.start/end (zero or more)
  -> TOOL.start/end (zero or more)
  -> HISTORY.commit -> result.end -> FINAL.end
  -> QUEUE.ack_start/ack
```

Errors and deadlines close the relevant span with `step=error` and an
`error_code`; they never produce a `FINAL` or technical `SUCCEEDED`. Provider
and Tool payloads include visible requests/responses, controlled errors, and
durations. Model private reasoning fields are excluded. API HTTP logs use the
query request ID separately from the Invocation's original request ID.

Payload policy is explicit and bounded. The default is 65,536 UTF-8 JSON bytes
per captured payload and seven days retention; configure with
`MEMBOT_DIAGNOSTIC_PAYLOAD_BYTES` (512..1048576) and
`MEMBOT_DIAGNOSTIC_RETENTION_DAYS` (1..365). Every captured payload records
`truncated`, `original_size`, `redacted_size`, and `size_unit`; an overflow is
a redacted preview and is never replayable. A background Worker purge removes
expired event rows. Status blobs retain compact structured outcomes and do not
store a complete transcript; complete visible diagnostic data belongs to the
event rows.

Service entry points configure JSON logging on stdout. The formatter applies
the same redaction policy and does not include logger locals or model private
reasoning. Redis messages contain IDs and the accepted payload only; ContextVar
values are never assumed to cross a process boundary.

Use the read-only viewer:

```bash
./.venv/bin/python scripts/trace_probe.py <invocation-id> \
  --database-url "$DATABASE_URL" --assert-correlation --assert-redaction
./.venv/bin/python scripts/trace_probe.py <invocation-id> \
  --api-url http://127.0.0.1:8080 --export-recording /tmp/invocation.json
```

The viewer prints `LLM -> Tool -> Result -> Final`, failure nodes, incomplete
spans, queue/execution/tool/LLM timing, retention gaps, and truncation markers.
`--json` is a machine-readable form. Its default path only reads PostgreSQL or
the GET API and does not execute tools.

`--reproduce` is an opt-in safe replay. It creates a disposable workspace,
injects only Provider/Tool adapters backed by recorded visible responses, and
refuses missing, tampered, expired, truncated, incomplete, multi-attempt,
`WORKER_LOST`, or `CANCELLED` recordings. It cannot call normal network,
filesystem, shell, MCP, cron, subagent, or database tools. Replay reproduces
the state/result contract, not original wall-clock timing or private model
reasoning.

Failed or timed-out records can be exported as candidate regression cases:

```bash
./.venv/bin/python scripts/trace_probe.py <invocation-id> \
  --export-candidate candidate.json
```

Candidates include redacted input, an intact history snapshot when available,
configuration/version, observed status and failure summary, tool observations,
and retention metadata. `expected.confirmed` is false with no answer by
default. `--confirm-expected` requires an explicit status and tool constraints
and still does not register the case in the standard suite; an observed wrong
answer is never promoted automatically.

## Deployment and health

The first deployment is one host with Nginx, two stateless API containers, one
single-process asyncio Worker, Redis Streams, and PostgreSQL. Only Nginx maps a
host port. `deploy/docker-compose.yml` runs a one-shot migration before API or
Worker startup, uses named volumes, resource limits, restart policy, read-only
application filesystems, JSON log rotation, and pinned image manifests.

Nginx terminates HTTPS, applies request limits, and routes API traffic with
`least_conn`. Its open-source passive upstream checks use `max_fails`,
`fail_timeout`, short timeouts, Docker DNS refresh, and no `non_idempotent`
retry. A POST is retried by a client only with the same Idempotency-Key and
payload. Nginx does not actively consume API readiness; a healthcheck changes
Docker's diagnostic state and does not update upstream membership.

API instances expose `/health/live` and `/health/ready` (with `/live` and
`/ready` compatibility aliases). Live means the process responds. Ready checks
PostgreSQL schema availability, draining state, and durable unfinished-task
capacity. Redis and Worker heartbeat/backlog are reported independently by
`/health/doctor` and `membot-health doctor`; API admission readiness is not an
assertion that the Agent can currently finish a task. SIGTERM makes API
readiness fail, rejects new mutations, and drains admitted short transactions.
Worker SIGTERM stops consuming and starting new work, waits within its drain
budget, closes Provider/MCP/shell resources, and persists interruption state.
SIGKILL recovery fences old RUNNING rows as `WORKER_LOST` on the next Worker.

## Evaluation contract

`evaluation/cases/` has at least eight executable Cases per category:
basic, Tool Calling, context/concurrency and exception/recovery. Versioned
expectations are separate from Provider/Tool fixtures. Real AgentLoop kernel
and real API/PostgreSQL/Redis/Worker runners produce the same bounded report
contract; selected capabilities and excluded Cases are explicit.

Technical status, deterministic result/Tool assertions and business task
success are distinct. Strict Tool Accuracy includes required/allowed/forbidden
names, key arguments and per-turn order; call metrics and no-Tool policies have
their own denominators. Reports retain accepted/rejected/failed/timed-out/
nonterminal counts and queue/execution/end-to-end average/p95.

Open answers use a versioned structured Judge only after deterministic gates.
Agent/Judge models and budgets must be configured and human calibration labels
confirmed. Missing credentials, missing evidence or Judge errors fail closed.
Baselines fix Case/schema/model/parameters/environment/repetition hashes;
paired code/Prompt comparisons report new failures, recoveries and descriptive
latency differences. Recorded failures require explicit expected review before
registration and run only isolated adapters. See `docs/EVALUATION.md` for
commands, CI gates and the current real-model verification limitations.

## Compatibility and non-goals

The existing CLI and Provider/Tool/ConversationMemoryEngine APIs remain supported.
The first service does not add Kubernetes, a full metrics platform, or multi-agent
orchestration. Subagents remain an existing tool capability and are not a new
service scheduling primitive.
