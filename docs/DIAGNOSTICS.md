# Invocation Diagnostics

M4 gives every accepted Invocation one durable, ordered timeline. PostgreSQL is
the source of truth; stdout is a searchable copy of events after their database
transaction commits. Redis carries the IDs and accepted payload needed for
execution, while `ContextVar` is only an in-process convenience.

## Event shape

Each `invocation_events` row and API event response contains:

| Field | Meaning |
| --- | --- |
| `invocationId`, `traceId`, `requestId`, `sessionId` | IDs from the original submission; a GET request's ID is separate |
| `sequence`, `time`, `expires_at` | Per-Invocation order, UTC timestamp, and retention deadline |
| `worker`, `attempt` | Execution owner and delivery attempt |
| `event_type`, `step` | Category and lifecycle point, such as `LLM.start` or `TOOL.error` |
| `span_id`, `parent_span_id`, `tool_call_id` | Correlation for an LLM/Tool call and its parent |
| `duration_ms`, `error_code` | Measured operation duration and structured failure code |
| `payload` | Redacted, bounded visible input/output or metadata |

The common path is `accepted`, `OUTBOX`, `QUEUE.consume`, `running`, context
and history snapshots, configuration, LLM/Tool spans, history commit, `result`,
`FINAL`, and queue ACK. Failure and timeout close the relevant span with an
error code and never create a success Final. A transaction rollback discards
both event rows and their staged JSON log records.

## Payload policy

`DiagnosticPolicy` defaults to 65,536 UTF-8 JSON bytes and seven days. The
service settings are `MEMBOT_DIAGNOSTIC_PAYLOAD_BYTES` (512 through 1 MiB) and
`MEMBOT_DIAGNOSTIC_RETENTION_DAYS` (1 through 365). Captures recursively mask
keys and values that look like credentials, bearer/basic authorization, URL
passwords, PEM data, email addresses, and phone numbers in exported candidate
cases. Private reasoning fields and thinking blocks are dropped.

Every capture records `truncated`, `original_size`, `redacted_size`, and
`size_unit`. An oversized value keeps only a redacted preview. The complete
status result is compact and the event payload is the only place to inspect
the controlled visible response. Expired events are removed by the Worker at a
bounded maintenance interval, so an old recording may intentionally have
retention gaps.

## Reading and replaying

The API endpoint is paged:

```bash
curl -H 'X-Request-ID: query-123' \
  'http://127.0.0.1:8080/v1/invocations/INV/events?after=0&limit=100'
```

The response's `X-Request-ID` is `query-123`; the body keeps the Invocation's
original `requestId`. The read-only CLI renders timing and failure nodes:

```bash
./.venv/bin/python scripts/trace_probe.py INV \
  --database-url "$DATABASE_URL" --assert-correlation --assert-redaction
```

`--export-recording` writes a bounded private JSON artifact. `--reproduce` uses
only recorded Provider and Tool adapters in a temporary workspace. It refuses
truncated, expired, missing, tampered, incomplete, or multi-attempt recordings,
and does not execute normal shell, filesystem, network, MCP, cron, subagent, or
database tools. Worker-loss and cancellation records are diagnostic-only and
are not replayed as if a result were known.

## Candidate regression cases

`--export-candidate` creates a redacted candidate for a `FAILED` or `TIMEOUT`
Invocation. It includes the accepted input, history snapshot, config version,
observed tools, failure summary, and retention metadata. It sets:

```json
{"expected":{"confirmed":false,"status":null,"answer":null}}
```

An observed wrong answer is never promoted automatically. A reviewer must use
`--confirm-expected` with an explicit terminal status and allowed/forbidden tool
constraints; confirmation still only edits the candidate and does not register
it in the M6 standard corpus.
