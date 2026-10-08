"""Async PostgreSQL repository for service sessions and invocations.

The repository deliberately keeps transactions short: it locks rows while
allocating sequence numbers or committing a completed turn, but never while
awaiting an LLM or tool.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Sequence

from membot.agent.diagnostics import DiagnosticPolicy, event_record, iso_time, log_event
from membot.agent.execution_result import ExecutionOutcome, ExecutionResult
from membot.agent.persistence.redaction import redact_data, redact_text


class InvocationStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"


class IdempotencyConflictError(ValueError):
    """The same idempotency key was used for a different request payload."""


class CapacityError(RuntimeError):
    """The durable unfinished-invocation limit has been reached."""


@dataclass(slots=True)
class SessionSnapshot:
    owner_id: str
    session_id: str
    session_key: str
    messages: list[dict[str, Any]]
    next_session_seq: int


@dataclass(slots=True)
class Invocation:
    invocation_id: str
    owner_id: str
    session_id: str
    session_key: str
    session_seq: int
    request_id: str
    trace_id: str
    status: InvocationStatus
    submitted_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    queue_timeout_seconds: float | None
    execution_timeout_seconds: float | None
    execution_owner: str | None
    result: dict[str, Any] | None
    error_code: str | None
    error_message: str | None
    idempotency_key: str | None
    payload_hash: str
    payload: dict[str, Any]
    attempt_count: int = 0


def payload_hash(payload: dict[str, Any]) -> str:
    """Hash canonical JSON so idempotency is independent of key order."""

    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _decode_json(value: Any) -> Any:
    """Decode asyncpg's default JSON/JSONB text values."""

    return json.loads(value) if isinstance(value, str) else value


def _result_payload(result: ExecutionResult, policy: DiagnosticPolicy) -> dict[str, Any]:
    """Compact durable status; controlled events own diagnostic transcripts."""
    final = policy.capture({"content": result.final_content})
    error = policy.capture({"content": result.error_message})

    def text(record: dict, source: str | None) -> str | None:
        if not record["truncated"]:
            return record.get("content")
        safe = redact_data(source) or ""
        return safe.encode("utf-8")[:policy.max_payload_bytes // 4].decode("utf-8", errors="ignore")

    return {
        "schema_version": 1,
        "outcome": result.outcome.value, "technical_success": result.technical_success,
        "messages": [], "tools_used": redact_data(result.tools_used),
        "final_content": text(final, result.final_content), "final_payload": final,
        "error_code": result.error_code,
        "error_message": text(error, result.error_message), "error_payload": error,
    }


def _row_to_invocation(row: Any) -> Invocation:
    return Invocation(
        invocation_id=row["invocation_id"],
        owner_id=row["owner_id"],
        session_id=row["session_id"],
        session_key=row["session_key"],
        session_seq=row["session_seq"],
        request_id=row["request_id"],
        trace_id=row["trace_id"],
        status=InvocationStatus(row["status"]),
        submitted_at=row["submitted_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        queue_timeout_seconds=row["queue_timeout_seconds"],
        execution_timeout_seconds=row["execution_timeout_seconds"],
        execution_owner=row["execution_owner"],
        result=_decode_json(row["result"]) if row["result"] is not None else None,
        error_code=row["error_code"],
        error_message=row["error_message"],
        idempotency_key=row["idempotency_key"],
        payload_hash=row["payload_hash"],
        payload=_decode_json(row["payload"]),
        attempt_count=row["attempt_count"],
    )


class _DiagnosticConnection:
    """Transaction-local log buffer; a rollback never logs a committed event."""

    def __init__(self, connection: Any, policy: DiagnosticPolicy):
        self.connection = connection
        self.policy = policy
        self.emitted: list[dict[str, Any]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.connection, name)


async def _insert_event(connection: _DiagnosticConnection, row: Any, event_type: str, event_payload: dict[str, Any]) -> None:
    # Updating the invocation row serializes event allocation across API,
    # publisher and execution connections, including concurrent appends.
    current = await connection.fetchrow(
        "UPDATE invocations SET next_event_seq=next_event_seq+1 WHERE invocation_id=$1 RETURNING *",
        row["invocation_id"],
    )
    data = dict(event_payload)
    step = data.get("step") or {
        "accepted": "accepted", "running": "start", "result": "end",
        "FINAL": "end", "worker_lost": "error", "worker_interrupted": "error",
    }.get(event_type, "recorded")
    event = await connection.fetchrow(
        """
        INSERT INTO invocation_events
          (invocation_id, owner_id, session_id, request_id, trace_id, event_type, payload,
           event_seq, worker_id, attempt, step, span_id, parent_span_id, tool_call_id,
           duration_ms, error_code, expires_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                clock_timestamp()+$17 * interval '1 day') RETURNING *
        """,
        current["invocation_id"], current["owner_id"], current["session_id"],
        current["request_id"], current["trace_id"], event_type,
        json.dumps(connection.policy.capture(data), ensure_ascii=False),
        current["next_event_seq"] - 1,
        data.get("worker") or current["execution_owner"], current["attempt_count"], step,
        data.get("span_id") or f"invocation:{current['invocation_id']}",
        f"invocation:{current['invocation_id']}" if data.get("parent_span_id") == "invocation" else data.get("parent_span_id"),
        data.get("tool_call_id"), data.get("duration_ms"),
        data.get("error_code") or data.get("errorCode"), connection.policy.retention_days,
    )
    connection.emitted.append(event_record(event))


class PostgresRepository:
    """Database access used by API/Worker processes and durable memory."""

    def __init__(self, pool: Any, *, diagnostics: DiagnosticPolicy | None = None):
        self.pool = pool
        self.diagnostics = diagnostics or DiagnosticPolicy()

    @asynccontextmanager
    async def _event_transaction(self, connection: Any):
        scoped = _DiagnosticConnection(connection, self.diagnostics)
        async with connection.transaction():
            yield scoped
        for event in scoped.emitted:
            log_event(event)

    @classmethod
    async def connect(cls, dsn: str, *, min_size: int = 1, max_size: int = 8,
                      diagnostics: DiagnosticPolicy | None = None) -> "PostgresRepository":
        try:
            import asyncpg
        except ImportError as exc:  # pragma: no cover - environment diagnostic
            raise RuntimeError("asyncpg is required for PostgreSQL service mode") from exc
        return cls(await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size), diagnostics=diagnostics)

    async def close(self) -> None:
        await self.pool.close()

    async def migrate(self, migrations_dir: Path | None = None) -> None:
        """Apply ordered SQL migrations once, recording each filename."""

        path = migrations_dir or Path(__file__).with_name("migrations")
        async with self.pool.acquire() as connection:
            await connection.execute("SELECT pg_advisory_lock(843199322540217)")
            try:
                await connection.execute(
                    "CREATE TABLE IF NOT EXISTS schema_migrations "
                    "(version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
                )
                applied = {
                    row["version"]
                    for row in await connection.fetch("SELECT version FROM schema_migrations")
                }
                for migration in sorted(path.glob("*.sql")):
                    if migration.name in applied:
                        continue
                    async with connection.transaction():
                        await connection.execute(migration.read_text(encoding="utf-8"))
                        await connection.execute(
                            "INSERT INTO schema_migrations(version) VALUES ($1)", migration.name
                        )
            finally:
                await connection.execute("SELECT pg_advisory_unlock(843199322540217)")

    async def accept_invocation(
        self,
        *,
        owner_id: str,
        session_id: str,
        session_key: str,
        request_id: str,
        trace_id: str,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
        invocation_id: str | None = None,
        queue_timeout_seconds: float | None = None,
        execution_timeout_seconds: float | None = None,
        max_unfinished: int | None = None,
        diagnostic_config: dict[str, Any] | None = None,
    ) -> tuple[Invocation, bool]:
        """Atomically allocate a Session sequence and create invocation+Outbox."""

        accepted_started = time.monotonic()
        invocation_id = invocation_id or str(uuid.uuid4())
        digest = payload_hash({
            "sessionId": session_id,
            "sessionKey": session_key,
            "payload": payload,
        })
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                if idempotency_key:
                    await connection.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"{owner_id}:{idempotency_key}")
                    existing = await connection.fetchrow(
                        "SELECT * FROM invocations WHERE owner_id = $1 AND idempotency_key = $2",
                        owner_id, idempotency_key,
                    )
                    if existing:
                        if existing["payload_hash"] != digest:
                            raise IdempotencyConflictError("idempotency key is already bound to another request")
                        return _row_to_invocation(existing), True

                if max_unfinished is not None:
                    if max_unfinished < 1:
                        raise ValueError("max_unfinished must be positive")
                    # Serialize the count across API instances.  The lock is
                    # held only for this short admission transaction.
                    await connection.execute("SELECT pg_advisory_xact_lock($1)", 843199322540218)
                    unfinished = await connection.fetchval(
                        "SELECT count(*) FROM invocations "
                        "WHERE status IN ('QUEUED','RUNNING')"
                    )
                    if unfinished >= max_unfinished:
                        raise CapacityError(
                            f"unfinished invocation capacity is full ({max_unfinished})"
                        )

                await connection.execute(
                    """
                    INSERT INTO sessions(owner_id, session_id, session_key)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (owner_id, session_id) DO NOTHING
                    """,
                    owner_id, session_id, session_key,
                )
                actual_key = await connection.fetchval(
                    "SELECT session_key FROM sessions WHERE owner_id=$1 AND session_id=$2",
                    owner_id, session_id,
                )
                if actual_key != session_key:
                    raise ValueError("session_id is already bound to another session_key")
                session = await connection.fetchrow(
                    "UPDATE sessions SET next_session_seq = next_session_seq + 1, updated_at = now() "
                    "WHERE owner_id = $1 AND session_id = $2 RETURNING next_session_seq - 1 AS session_seq",
                    owner_id, session_id,
                )
                seq = session["session_seq"]
                row = await connection.fetchrow(
                    """
                    INSERT INTO invocations(
                      invocation_id, owner_id, session_id, session_key, session_seq,
                      request_id, trace_id, idempotency_key, payload_hash, payload,
                      queue_timeout_seconds, execution_timeout_seconds
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12)
                    RETURNING *
                    """,
                    invocation_id, owner_id, session_id, session_key, seq,
                    request_id, trace_id, idempotency_key, digest,
                    json.dumps(payload, ensure_ascii=False), queue_timeout_seconds, execution_timeout_seconds,
                )
                envelope = {
                    "invocationId": invocation_id, "ownerId": owner_id, "sessionId": session_id,
                    "sessionSeq": seq, "requestId": request_id, "traceId": trace_id,
                }
                await connection.execute(
                    "INSERT INTO outbox(invocation_id, envelope) VALUES ($1, $2::jsonb)",
                    invocation_id, json.dumps(envelope, ensure_ascii=False),
                )
                await _insert_event(connection, row, "accepted", {
                    "status": InvocationStatus.QUEUED.value, "sessionSeq": seq,
                    "input": payload, "config": diagnostic_config or {},
                    "step": "committed", "duration_ms": (time.monotonic() - accepted_started) * 1000,
                })
                return _row_to_invocation(row), False

    async def create_session(
        self, *, owner_id: str, session_id: str, session_key: str,
    ) -> SessionSnapshot:
        """Create or return an owner-scoped Session without local caching."""
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                await connection.execute(
                    """
                    INSERT INTO sessions(owner_id, session_id, session_key)
                    VALUES ($1,$2,$3)
                    ON CONFLICT (owner_id, session_id) DO NOTHING
                    """, owner_id, session_id, session_key,
                )
                row = await connection.fetchrow(
                    "SELECT * FROM sessions WHERE owner_id=$1 AND session_id=$2",
                    owner_id, session_id,
                )
                if row is None:
                    raise KeyError(session_id)
                if row["session_key"] != session_key:
                    raise ValueError("session_id is already bound to another session_key")
                return SessionSnapshot(
                    owner_id=owner_id, session_id=row["session_id"],
                    session_key=row["session_key"], messages=[],
                    next_session_seq=row["next_session_seq"],
                )

    async def get_invocation(self, invocation_id: str) -> Invocation | None:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow("SELECT * FROM invocations WHERE invocation_id = $1", invocation_id)
            return _row_to_invocation(row) if row else None

    async def load_session(
        self,
        owner_id: str,
        session_ref: str,
        *,
        message_limit: int | None = None,
    ) -> SessionSnapshot | None:
        async with self.pool.acquire() as connection:
            session = await connection.fetchrow(
                "SELECT * FROM sessions WHERE owner_id = $1 AND (session_id = $2 OR session_key = $2) "
                "ORDER BY CASE WHEN session_key = $2 THEN 0 ELSE 1 END LIMIT 1",
                owner_id, session_ref,
            )
            if session is None:
                return None
            if message_limit is None:
                rows = await connection.fetch(
                    "SELECT message FROM session_messages WHERE owner_id = $1 AND session_id = $2 ORDER BY message_seq",
                    owner_id, session["session_id"],
                )
            else:
                rows = await connection.fetch(
                    "SELECT message FROM session_messages WHERE owner_id=$1 AND session_id=$2 "
                    "ORDER BY message_seq DESC LIMIT $3",
                    owner_id, session["session_id"], max(0, message_limit),
                )
                rows = list(reversed(rows))
            return SessionSnapshot(
                owner_id=owner_id, session_id=session["session_id"], session_key=session["session_key"],
                messages=[_decode_json(row["message"]) for row in rows],
                next_session_seq=session["next_session_seq"],
            )

    async def claim_invocation(
        self,
        invocation_id: str,
        execution_owner: str,
        *,
        lease_seconds: float = 60.0,
    ) -> Invocation:
        """Claim only when no earlier invocation in this Session is unfinished."""

        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")

        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                row = await connection.fetchrow("SELECT * FROM invocations WHERE invocation_id = $1 FOR UPDATE", invocation_id)
                if row is None:
                    raise KeyError(invocation_id)
                if row["status"] != InvocationStatus.QUEUED.value:
                    raise RuntimeError(f"invocation is not claimable from {row['status']}")
                if row["queue_timeout_seconds"] is not None:
                    expired = await connection.fetchval(
                        "SELECT submitted_at + queue_timeout_seconds * interval '1 second' <= now() "
                        "FROM invocations WHERE invocation_id=$1", invocation_id,
                    )
                    if expired:
                        raise RuntimeError("invocation queue deadline has expired")
                earlier = await connection.fetchval(
                    """
                    SELECT 1 FROM invocations
                    WHERE owner_id = $1 AND session_id = $2 AND session_seq < $3
                      AND status IN ('QUEUED','RUNNING') LIMIT 1
                    """,
                    row["owner_id"], row["session_id"], row["session_seq"],
                )
                if earlier:
                    raise RuntimeError("session predecessor is not terminal")
                updated = await connection.fetchrow(
                    """
                    UPDATE invocations SET status='RUNNING', started_at=COALESCE(started_at, clock_timestamp()),
                      execution_owner=$2, execution_lease_until=now()+($3 * interval '1 second'),
                      attempt_count=attempt_count+1 WHERE invocation_id=$1 RETURNING *
                    """, invocation_id, execution_owner, lease_seconds,
                )
                await _insert_event(connection, updated, "running", {
                    "executionOwner": execution_owner,
                    "queue_duration_ms": max(0.0, (updated["started_at"] - updated["submitted_at"]).total_seconds() * 1000),
                })
                return _row_to_invocation(updated)

    async def complete_invocation(
        self,
        invocation_id: str,
        result: ExecutionResult,
        records: Sequence[Any] = (),
        *,
        owner_id: str | None = None,
        execution_owner: str | None = None,
    ) -> Invocation:
        """Commit messages and terminal result in one short transaction."""

        if result.technical_success:
            status = InvocationStatus.SUCCEEDED.value
        elif result.outcome is ExecutionOutcome.TIMEOUT:
            status = InvocationStatus.TIMEOUT.value
        else:
            status = InvocationStatus.FAILED.value
        persisted_result = _result_payload(result, self.diagnostics)
        result_json = json.dumps(persisted_result, ensure_ascii=False)
        error_message = persisted_result["error_message"]
        commit_started = time.monotonic()
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                row = await connection.fetchrow("SELECT * FROM invocations WHERE invocation_id = $1 FOR UPDATE", invocation_id)
                if row is None:
                    raise KeyError(invocation_id)
                if owner_id is not None and row["owner_id"] != owner_id:
                    raise PermissionError("invocation belongs to a different owner")
                if row["status"] in {InvocationStatus.SUCCEEDED.value, InvocationStatus.FAILED.value, InvocationStatus.TIMEOUT.value}:
                    return _row_to_invocation(row)
                if row["status"] != InvocationStatus.RUNNING.value:
                    raise RuntimeError("invocation must be claimed before it can be completed")
                lease_valid = await connection.fetchval(
                    "SELECT execution_lease_until > now() FROM invocations WHERE invocation_id=$1",
                    invocation_id,
                )
                if (
                    execution_owner is None
                    or row["execution_owner"] != execution_owner
                    or not lease_valid
                ):
                    raise PermissionError("Worker lease is no longer owned by this executor")
                if result.technical_success:
                    await connection.execute(
                        "SELECT 1 FROM sessions WHERE owner_id = $1 AND session_id = $2 FOR UPDATE",
                        row["owner_id"], row["session_id"],
                    )
                    turn_index = 0
                    for record in records:
                        if record.session_key and record.session_key != row["session_key"]:
                            raise ValueError("memory record belongs to a different Session")
                        messages = record.payload.get("messages", [])
                        for message in messages:
                            seq = await connection.fetchval(
                                "UPDATE sessions SET next_message_seq=next_message_seq+1, updated_at=now() "
                                "WHERE owner_id=$1 AND session_id=$2 RETURNING next_message_seq-1",
                                row["owner_id"], row["session_id"],
                            )
                            await connection.execute(
                                """
                                INSERT INTO session_messages(owner_id,session_id,message_seq,invocation_id,session_seq,turn_index,role,message)
                                VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
                                """,
                                row["owner_id"], row["session_id"], seq, invocation_id,
                                row["session_seq"], turn_index, message.get("role", "unknown"),
                                json.dumps(message, ensure_ascii=False),
                            )
                            turn_index += 1
                    await _insert_event(connection, row, "HISTORY", {
                        "step": "commit", "message_count": turn_index,
                        "duration_ms": (time.monotonic() - commit_started) * 1000,
                    })
                updated = await connection.fetchrow(
                    """
                    UPDATE invocations SET status=$2, finished_at=now(), result=$3::jsonb,
                      execution_lease_until=NULL, error_code=$4, error_message=$5
                    WHERE invocation_id=$1 RETURNING *
                    """, invocation_id, status, result_json, result.error_code, error_message,
                )
                await _insert_event(
                    connection,
                    updated,
                    "result",
                    {
                        "outcome": result.outcome.value,
                        "status": status,
                        "errorCode": result.error_code,
                        "errorMessage": error_message,
                        "duration_ms": max(0.0, (updated["finished_at"] - updated["started_at"]).total_seconds() * 1000),
                    },
                )
                if result.technical_success:
                    await _insert_event(connection, updated, "FINAL", {"content": result.final_content or ""})
                else:
                    await _insert_event(connection, updated, "TIMEOUT" if status == "TIMEOUT" else "FAILURE", {
                        "step": "error", "error_code": result.error_code, "message": error_message,
                        "outcome": result.outcome.value,
                    })
                return _row_to_invocation(updated)

    async def archive_and_finish_new(
        self,
        invocation_id: str,
        *,
        owner_id: str | None = None,
        execution_owner: str | None = None,
    ) -> Invocation:
        """Archive current messages and complete an ordered ``/new`` invocation."""

        result = ExecutionResult(
            outcome=ExecutionOutcome.FINAL,
            final_content="New session started.",
            messages=[],
        )
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                row = await connection.fetchrow(
                    "SELECT * FROM invocations WHERE invocation_id=$1 FOR UPDATE", invocation_id,
                )
                if row is None:
                    raise KeyError(invocation_id)
                if owner_id is not None and row["owner_id"] != owner_id:
                    raise PermissionError("invocation belongs to a different owner")
                if row["status"] in {
                    InvocationStatus.SUCCEEDED.value,
                    InvocationStatus.FAILED.value,
                    InvocationStatus.TIMEOUT.value,
                }:
                    return _row_to_invocation(row)
                if row["status"] != InvocationStatus.RUNNING.value:
                    raise RuntimeError("invocation must be claimed before /new can complete")
                lease_valid = await connection.fetchval(
                    "SELECT execution_lease_until > now() FROM invocations WHERE invocation_id=$1",
                    invocation_id,
                )
                if execution_owner is None or row["execution_owner"] != execution_owner or not lease_valid:
                    raise PermissionError("Worker lease is no longer owned by this executor")
                await connection.execute(
                    "SELECT 1 FROM sessions WHERE owner_id=$1 AND session_id=$2 FOR UPDATE",
                    row["owner_id"], row["session_id"],
                )
                messages = await connection.fetch(
                    "SELECT message_seq, message FROM session_messages "
                    "WHERE owner_id=$1 AND session_id=$2 ORDER BY message_seq",
                    row["owner_id"], row["session_id"],
                )
                await connection.execute(
                    """
                    INSERT INTO session_archives(owner_id, session_id, archive_seq, invocation_id, messages)
                    VALUES($1,$2,$3,$4,$5::jsonb)
                    """,
                    row["owner_id"], row["session_id"], row["session_seq"], invocation_id,
                    json.dumps(
                        [
                            {
                                "messageSeq": message["message_seq"],
                                "message": _decode_json(message["message"]),
                            }
                            for message in messages
                        ],
                        ensure_ascii=False,
                    ),
                )
                await connection.execute(
                    "DELETE FROM session_messages WHERE owner_id=$1 AND session_id=$2",
                    row["owner_id"], row["session_id"],
                )
                await connection.execute(
                    "UPDATE sessions SET updated_at=now() WHERE owner_id=$1 AND session_id=$2",
                    row["owner_id"], row["session_id"],
                )
                updated = await connection.fetchrow(
                    """
                    UPDATE invocations SET status='SUCCEEDED', finished_at=now(),
                      execution_lease_until=NULL, result=$2::jsonb
                    WHERE invocation_id=$1 RETURNING *
                    """,
                    invocation_id, json.dumps(_result_payload(result, self.diagnostics), ensure_ascii=False),
                )
                await _insert_event(
                    connection, updated,
                    "result", {"outcome": "FINAL", "status": "SUCCEEDED"},
                )
                await _insert_event(connection, updated, "HISTORY", {
                    "step": "archive", "archived_message_count": len(messages),
                })
                await _insert_event(connection, updated, "FINAL", {"content": result.final_content})
                return _row_to_invocation(updated)

    async def finish_interrupted(
        self,
        invocation_id: str,
        outcome: ExecutionOutcome,
        *,
        error_message: str | None = None,
        owner_id: str | None = None,
        execution_owner: str | None = None,
        error_code: str | None = None,
    ) -> Invocation:
        if error_code is None:
            if outcome is ExecutionOutcome.TIMEOUT:
                error_code = "QUEUE_TIMEOUT" if error_message and "queue" in error_message.lower() else "EXECUTION_TIMEOUT"
            else:
                error_code = outcome.value
        error_message = redact_text(error_message)
        result = ExecutionResult.failure(outcome, error_code=error_code, error_message=error_message)
        persisted_result = _result_payload(result, self.diagnostics)
        error_message = persisted_result["error_message"]
        status = InvocationStatus.TIMEOUT.value if outcome is ExecutionOutcome.TIMEOUT else InvocationStatus.FAILED.value
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                row = await connection.fetchrow("SELECT * FROM invocations WHERE invocation_id=$1 FOR UPDATE", invocation_id)
                if row is None:
                    raise KeyError(invocation_id)
                if owner_id is not None and row["owner_id"] != owner_id:
                    raise PermissionError("invocation belongs to a different owner")
                if row["status"] in {InvocationStatus.SUCCEEDED.value, InvocationStatus.FAILED.value, InvocationStatus.TIMEOUT.value}:
                    return _row_to_invocation(row)
                if row["status"] == InvocationStatus.RUNNING.value:
                    lease_valid = await connection.fetchval(
                        "SELECT execution_lease_until > now() FROM invocations WHERE invocation_id=$1",
                        invocation_id,
                    )
                    if execution_owner is None or row["execution_owner"] != execution_owner or not lease_valid:
                        raise PermissionError("Worker lease is no longer owned by this executor")
                updated = await connection.fetchrow(
                    "UPDATE invocations SET status=$2, finished_at=now(), execution_lease_until=NULL, result=$3::jsonb,error_code=$4,error_message=$5 WHERE invocation_id=$1 RETURNING *",
                    invocation_id, status, json.dumps(persisted_result), result.error_code, error_message,
                )
                await _insert_event(
                    connection,
                    updated,
                    "result",
                    {
                        "outcome": outcome.value,
                        "status": status,
                        "errorCode": error_code,
                        "errorMessage": error_message,
                    },
                )
                await _insert_event(connection, updated, "TIMEOUT" if outcome is ExecutionOutcome.TIMEOUT else "FAILURE", {
                    "step": "error", "error_code": error_code, "message": error_message,
                })
                return _row_to_invocation(updated)

    async def renew_lease(
        self,
        invocation_id: str,
        execution_owner: str,
        *,
        lease_seconds: float = 60.0,
    ) -> bool:
        """Extend a live Worker claim; returns false after reassignment/expiry."""

        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        async with self.pool.acquire() as connection:
            status = await connection.execute(
                """
                UPDATE invocations SET execution_lease_until=now()+($3 * interval '1 second')
                WHERE invocation_id=$1 AND execution_owner=$2 AND status='RUNNING'
                  AND execution_lease_until > now()
                """,
                invocation_id, execution_owner, lease_seconds,
            )
            return status == "UPDATE 1"

    async def recover_expired_invocations(self, *, limit: int = 100) -> int:
        """Return expired RUNNING claims to QUEUED and republish through Outbox."""

        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                rows = await connection.fetch(
                    """
                    SELECT * FROM invocations
                    WHERE status='RUNNING' AND execution_lease_until < now()
                    ORDER BY execution_lease_until FOR UPDATE SKIP LOCKED LIMIT $1
                    """,
                    limit,
                )
                for row in rows:
                    queued = await connection.fetchrow(
                        """
                        UPDATE invocations SET status='QUEUED', execution_owner=NULL,
                          execution_lease_until=NULL
                        WHERE invocation_id=$1 RETURNING *
                        """,
                        row["invocation_id"],
                    )
                    await connection.execute(
                        "UPDATE outbox SET published_at=NULL, available_at=now(), last_error='worker lease expired' "
                        "WHERE invocation_id=$1",
                        row["invocation_id"],
                    )
                    await _insert_event(
                        connection,
                        queued,
                        "worker_interrupted",
                        {"previousExecutionOwner": row["execution_owner"], "attempt": row["attempt_count"]},
                    )
                return len(rows)

    async def events(self, invocation_id: str, *, after: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        if after < 0 or not 1 <= limit <= 1000:
            raise ValueError("event cursor/limit out of range")
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT * FROM invocation_events WHERE invocation_id=$1 AND event_seq>$2 "
                "AND expires_at>now() ORDER BY event_seq LIMIT $3", invocation_id, after, limit,
            )
            result = []
            for row in rows:
                event = event_record(row)
                # Pre-M4 rows are also redacted/bounded on read; they lack the
                # recording metadata required for safe reproducible replay.
                if "truncated" not in event["payload"]:
                    event["payload"] = self.diagnostics.capture(event["payload"])
                result.append({
                    **{key: iso_time(value) for key, value in dict(row).items() if key != "payload"},
                    **event,
                })
            return result

    async def append_event(self, invocation_id: str, event_type: str, payload: dict[str, Any],
                           *, execution_owner: str | None = None) -> None:
        """Append a diagnostic event using the invocation's original IDs."""
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                row = await connection.fetchrow(
                    "SELECT * FROM invocations WHERE invocation_id=$1 FOR UPDATE", invocation_id,
                )
                if row is None:
                    raise KeyError(invocation_id)
                if execution_owner is not None:
                    valid = await connection.fetchval(
                        "SELECT status='RUNNING' AND execution_owner=$2 AND execution_lease_until>now() "
                        "FROM invocations WHERE invocation_id=$1", invocation_id, execution_owner,
                    )
                    if not valid:
                        raise PermissionError("diagnostic writer no longer owns execution")
                await _insert_event(connection, row, event_type, payload)

    async def purge_expired_events(self, *, limit: int = 500) -> int:
        """Bounded retention cleanup; never erase service history/status."""
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                "WITH expired AS (SELECT event_id FROM invocation_events WHERE expires_at<=now() "
                "ORDER BY expires_at FOR UPDATE SKIP LOCKED LIMIT $1) "
                "DELETE FROM invocation_events e USING expired WHERE e.event_id=expired.event_id RETURNING e.event_id",
                limit,
            )
            return len(rows)

    async def expire_queued(self, *, limit: int = 100, worker_id: str | None = None) -> int:
        """Move queue-expired work to TIMEOUT and leave a diagnostic event."""
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                rows = await connection.fetch(
                    """
                    SELECT * FROM invocations
                    WHERE status='QUEUED' AND queue_timeout_seconds IS NOT NULL
                      AND submitted_at + queue_timeout_seconds * interval '1 second' < now()
                    ORDER BY submitted_at FOR UPDATE SKIP LOCKED LIMIT $1
                    """, limit,
                )
                for row in rows:
                    result = _result_payload(ExecutionResult.failure(
                        ExecutionOutcome.TIMEOUT, error_code="QUEUE_TIMEOUT",
                        error_message="invocation expired in queue",
                    ), self.diagnostics)
                    updated = await connection.fetchrow(
                        """UPDATE invocations SET status='TIMEOUT', finished_at=now(),
                           error_code='QUEUE_TIMEOUT', error_message='invocation expired in queue',
                           result=$2::jsonb WHERE invocation_id=$1 AND status='QUEUED' RETURNING *""",
                        row["invocation_id"], json.dumps(result),
                    )
                    if updated:
                        await _insert_event(
                            connection, updated, "result",
                            {"outcome": "TIMEOUT", "status": "TIMEOUT", "errorCode": "QUEUE_TIMEOUT", "worker": worker_id},
                        )
                        await _insert_event(connection, updated, "TIMEOUT", {
                            "step": "error", "error_code": "QUEUE_TIMEOUT",
                            "worker": worker_id,
                            "duration_ms": max(0.0, (updated["finished_at"] - updated["submitted_at"]).total_seconds() * 1000),
                        })
                        await connection.execute(
                            "UPDATE outbox SET published_at=COALESCE(published_at, now()) WHERE invocation_id=$1",
                            row["invocation_id"],
                        )
                return len(rows)

    async def fail_running_worker_lost(self, execution_owner: str, *, limit: int = 1000) -> int:
        """Fence RUNNING work owned by a lost Worker; never touch terminal rows."""
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                rows = await connection.fetch(
                    """SELECT * FROM invocations WHERE status='RUNNING' AND execution_owner=$1
                       ORDER BY started_at FOR UPDATE SKIP LOCKED LIMIT $2""",
                    execution_owner, limit,
                )
                for row in rows:
                    updated = await connection.fetchrow(
                        """UPDATE invocations SET status='FAILED', finished_at=now(),
                           execution_lease_until=NULL, error_code='WORKER_LOST',
                           error_message='Worker execution process stopped'
                           WHERE invocation_id=$1 AND status='RUNNING' RETURNING *""",
                        row["invocation_id"],
                    )
                    if updated:
                        await _insert_event(
                            connection, updated, "worker_lost",
                            {"status": "FAILED", "errorCode": "WORKER_LOST"},
                        )
                return len(rows)

    async def fail_all_running_worker_lost(self, *, limit: int = 1000,
                                          owner_id: str | None = None) -> int:
        """Fence every RUNNING claim during single-Worker startup recovery."""
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                rows = await connection.fetch(
                    """SELECT * FROM invocations WHERE status='RUNNING' AND ($2::text IS NULL OR owner_id=$2)
                       ORDER BY started_at FOR UPDATE SKIP LOCKED LIMIT $1""", limit, owner_id,
                )
                for row in rows:
                    updated = await connection.fetchrow(
                        """UPDATE invocations SET status='FAILED', finished_at=now(),
                           execution_lease_until=NULL, error_code='WORKER_LOST',
                           error_message='Worker execution process stopped'
                           WHERE invocation_id=$1 AND status='RUNNING' RETURNING *""",
                        row["invocation_id"],
                    )
                    if updated:
                        await _insert_event(
                            connection, updated, "worker_lost",
                            {"status": "FAILED", "errorCode": "WORKER_LOST"},
                        )
                return len(rows)

    async def reclassify_worker_cancellations(self, execution_owner: str, *, limit: int = 1000) -> int:
        """Turn cancellation caused by Worker shutdown into WORKER_LOST."""
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                rows = await connection.fetch(
                    """SELECT * FROM invocations
                       WHERE status='FAILED' AND execution_owner=$1
                         AND error_code='CANCELLED'
                       ORDER BY finished_at FOR UPDATE SKIP LOCKED LIMIT $2""",
                    execution_owner, limit,
                )
                for row in rows:
                    result = _result_payload(ExecutionResult.failure(
                        ExecutionOutcome.CANCELLED, error_code="WORKER_LOST",
                        error_message="Worker execution process stopped",
                    ), self.diagnostics)
                    updated = await connection.fetchrow(
                        """UPDATE invocations SET error_code='WORKER_LOST',
                           error_message='Worker execution process stopped', result=$2::jsonb
                           WHERE invocation_id=$1 AND status='FAILED'
                             AND error_code='CANCELLED' RETURNING *""",
                        row["invocation_id"], json.dumps(result),
                    )
                    if updated:
                        await _insert_event(
                            connection, updated, "worker_lost",
                            {"status": "FAILED", "errorCode": "WORKER_LOST"},
                        )
                return len(rows)

    async def unfinished_count(self) -> int:
        async with self.pool.acquire() as connection:
            return int(await connection.fetchval(
                "SELECT count(*) FROM invocations WHERE status IN ('QUEUED','RUNNING')"
            ))

    async def session_predecessor_ready(self, invocation_id: str) -> bool:
        """Check ordered eligibility without claiming or consuming execution quota."""
        async with self.pool.acquire() as connection:
            return bool(await connection.fetchval(
                """
                SELECT NOT EXISTS (
                  SELECT 1
                  FROM invocations current
                  JOIN invocations earlier
                    ON earlier.owner_id=current.owner_id
                   AND earlier.session_id=current.session_id
                   AND earlier.session_seq < current.session_seq
                   AND earlier.status IN ('QUEUED','RUNNING')
                  WHERE current.invocation_id=$1 AND current.status='QUEUED'
                )
                """, invocation_id,
            ))

    async def unfinished_invocations(self, *, limit: int = 1000) -> list[Invocation]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """SELECT * FROM invocations WHERE status='QUEUED'
                   ORDER BY session_id, session_seq LIMIT $1""", limit,
            )
            return [_row_to_invocation(row) for row in rows]

    async def queued_envelopes(self, *, limit: int = 1000, owner_id: str | None = None) -> list[dict[str, Any]]:
        """Return durable QUEUED envelopes for Redis-loss reconciliation."""
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """SELECT invocation_id, owner_id, session_id, session_seq,
                          request_id, trace_id
                   FROM invocations WHERE status='QUEUED' AND ($2::text IS NULL OR owner_id=$2)
                   ORDER BY submitted_at, session_seq LIMIT $1""", limit, owner_id,
            )
            return [
                {
                    "invocationId": row["invocation_id"], "ownerId": row["owner_id"],
                    "sessionId": row["session_id"], "sessionSeq": row["session_seq"],
                    "requestId": row["request_id"], "traceId": row["trace_id"],
                }
                for row in rows
            ]

    async def pending_outbox(self, *, limit: int = 100, owner_id: str | None = None) -> list[dict[str, Any]]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT o.* FROM outbox o JOIN invocations i USING(invocation_id) "
                "WHERE o.published_at IS NULL AND o.available_at <= now() "
                "AND i.status IN ('QUEUED','RUNNING') AND ($2::text IS NULL OR i.owner_id=$2) "
                "ORDER BY o.outbox_id LIMIT $1", limit, owner_id,
            )
            return [{**dict(row), "envelope": _decode_json(row["envelope"])} for row in rows]

    async def record_outbox_start(self, outbox_id: int, *, worker: str | None = None,
                                  span_id: str | None = None) -> None:
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                row = await connection.fetchrow(
                    "SELECT i.*,o.attempts AS publication_attempt FROM invocations i "
                    "JOIN outbox o USING(invocation_id) WHERE o.outbox_id=$1", outbox_id,
                )
                if row is None:
                    raise KeyError(outbox_id)
                await _insert_event(connection, row, "OUTBOX", {
                    "step": "start", "worker": worker, "span_id": span_id,
                    "parent_span_id": "invocation", "outbox_id": outbox_id,
                    "publication_attempt": row["publication_attempt"] + 1,
                })

    async def mark_outbox_published(self, outbox_id: int, *, worker: str | None = None,
                                    span_id: str | None = None, entry_id: str | None = None,
                                    duration_ms: float | None = None) -> None:
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                # Match terminal/deadline transactions' lock order: invocation
                # first, Outbox second. Event-sequence allocation also locks
                # the invocation, so reversing this order could deadlock.
                locked = await connection.fetchrow(
                    "SELECT i.invocation_id FROM invocations i JOIN outbox o USING(invocation_id) "
                    "WHERE o.outbox_id=$1 FOR UPDATE OF i", outbox_id,
                )
                if locked is None:
                    raise KeyError(outbox_id)
                outbox = await connection.fetchrow(
                    "UPDATE outbox SET published_at=now(), attempts=attempts+1,last_error=NULL "
                    "WHERE outbox_id=$1 RETURNING invocation_id,attempts", outbox_id,
                )
                if outbox is None:
                    raise KeyError(outbox_id)
                await _insert_event(connection, outbox, "OUTBOX", {
                    "step": "end", "worker": worker, "span_id": span_id,
                    "parent_span_id": "invocation", "entry_id": entry_id,
                    "duration_ms": duration_ms, "outbox_id": outbox_id,
                    "publication_attempt": outbox["attempts"],
                })

    async def mark_outbox_failed(self, outbox_id: int, error: str, *, retry_seconds: float = 1.0,
                                 worker: str | None = None, span_id: str | None = None,
                                 duration_ms: float | None = None,
                                 error_code: str = "OUTBOX_PUBLISH_ERROR") -> None:
        async with self.pool.acquire() as connection:
            async with self._event_transaction(connection) as connection:
                locked = await connection.fetchrow(
                    "SELECT i.invocation_id FROM invocations i JOIN outbox o USING(invocation_id) "
                    "WHERE o.outbox_id=$1 FOR UPDATE OF i", outbox_id,
                )
                if locked is None:
                    raise KeyError(outbox_id)
                outbox = await connection.fetchrow(
                    "UPDATE outbox SET attempts=attempts+1,last_error=$2,available_at=now()+($3 * interval '1 second') "
                    "WHERE outbox_id=$1 RETURNING invocation_id,attempts",
                    outbox_id, redact_text(error)[:1000], retry_seconds,
                )
                if outbox is None:
                    raise KeyError(outbox_id)
                await _insert_event(connection, outbox, "OUTBOX", {
                    "step": "error", "worker": worker, "span_id": span_id,
                    "parent_span_id": "invocation", "error_code": error_code,
                    "duration_ms": duration_ms, "message": error,
                    "publication_attempt": outbox["attempts"], "retry_seconds": retry_seconds,
                })
