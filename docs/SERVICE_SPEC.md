# Agent Service Specification

This is the service contract for the planned backend. M1 implements the
in-process runtime kernel used by the existing CLI; the HTTP, Redis, and
PostgreSQL endpoints remain planned for M2/M3.

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

The service must preserve at-least-once delivery. Idempotency keys, invocation
leases, and event uniqueness prevent duplicate effects; they do not change the
delivery guarantee to exactly-once.

## Persistence and recovery

PostgreSQL stores sessions, invocations, invocation context, state transitions,
Outbox rows, and diagnostic events. The context snapshot used by an invocation
is immutable after acceptance; later Session turns do not rewrite it.

The API transaction inserts the invocation and Outbox row together. A relay
retries pending Outbox rows until Redis accepts the envelope. The Worker updates
the lease and state in PostgreSQL, appends events transactionally, and can safely
reprocess a redelivered envelope.

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
