# Agent Service Specification

This is the service contract for the planned backend. M1 implements the
in-process runtime kernel. M2 adds PostgreSQL history/invocation persistence,
migrations, and bounded Redis Outbox transport. The HTTP API and long-running
Worker process remain M3 work.

## Scope

The first deployable service runs on one host with Nginx, two stateless API
instances, Redis, PostgreSQL, and one single-process asyncio Worker. The API
accepts work and returns quickly; the Worker owns execution. The existing CLI
continues to call the existing AgentLoop path until an explicit migration phase.

## Submission contract

`POST /v1/sessions/{sessionId}/invocations` accepts a bounded JSON body containing
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
timeline. The timeline contains `LLM`, `TOOL`, `RESULT`, and `FINAL` event
categories and carries all three IDs.

## State machine

The only execution states are `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, and
`TIMEOUT`.

```text
QUEUED --claim--> RUNNING --final result--> SUCCEEDED
   |                 |  \
   |                 |   +--deadline------> TIMEOUT
   |                 +------error---------> FAILED
   +--queue deadline----------------------> TIMEOUT
   +--worker interruption-----------------> QUEUED or FAILED (policy-defined)
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
Outbox rows, and diagnostic events. The context snapshot used by an invocation
is immutable after acceptance; later Session turns do not rewrite it.

The API acceptance transaction inserts the invocation and Outbox row together.
M2 implements the transaction in `PostgresRepository.accept_invocation`; the
relay retries pending Outbox rows until Redis accepts the envelope. Redis
transport is bounded and uses a ready list plus an unacknowledged processing
list. Delivery is at least once: publication can precede a crash before the
Outbox row is marked, and expired Worker leases are returned to `QUEUED` with
the Outbox made publishable again.

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
`AgentLoop`. Until M3 owns background-task lifecycle, service configuration must
set `enable_consolidation`, `enable_subagents`, and `enable_cron` to false.
Install service-only drivers with `python -m pip install '.[service]'`.
`DATABASE_URL` and `REDIS_URL` are used by migration/test tooling; M3 will
centralize process configuration. M2 masks common key/value, bearer, and
provider-token forms at the persistence boundary; M4 broadens and audits
redaction across structured logs and timeline exports.

On Worker interruption, an expired lease is recovered by a sweeper according to
the retry policy. Recovery is visible in the event timeline and never silently
reports success.

## Diagnostics

Every API, queue, Worker, provider, tool, and persistence log line includes
`requestId`, `traceId`, and `invocationId` when an invocation exists. Event
payloads are structured JSON, redact secrets, and preserve provider/tool error
details without turning an error text into a result.

The minimum timeline is:

```text
accepted -> queued -> claimed/running -> llm.start/end
         -> tool.start/end (zero or more) -> result -> final
```

## Deployment and health

Nginx terminates HTTPS, applies request limits, and routes API traffic with
`least_conn`. API instances expose `/live` and `/ready`; readiness depends on
PostgreSQL and Redis connectivity and a healthy Worker handoff path. Shutdown
marks the instance unready, stops new acceptance, drains bounded work, closes
MCP/provider resources, and exits within a configured grace period.

## Compatibility and non-goals

The existing CLI and Provider/Tool/ConversationMemoryEngine APIs remain supported.
The first service does not add Kubernetes, a full metrics platform, or multi-agent
orchestration. Subagents remain an existing tool capability and are not a new
service scheduling primitive.
