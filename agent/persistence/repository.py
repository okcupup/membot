"""Async PostgreSQL repository for service sessions and invocations.

The repository deliberately keeps transactions short: it locks rows while
allocating sequence numbers or committing a completed turn, but never while
awaiting an LLM or tool.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Sequence

from membot.agent.execution_result import ExecutionOutcome, ExecutionResult
from membot.agent.persistence.redaction import redact_text


class InvocationStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"


class IdempotencyConflictError(ValueError):
    """The same idempotency key was used for a different request payload."""


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


def payload_hash(payload: dict[str, Any]) -> str:
    """Hash canonical JSON so idempotency is independent of key order."""

    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _decode_json(value: Any) -> Any:
    """Decode asyncpg's default JSON/JSONB text values."""

    return json.loads(value) if isinstance(value, str) else value


def _result_payload(result: ExecutionResult) -> dict[str, Any]:
    """Persist structured outcome without failed partial transcripts."""

    payload = result.to_dict()
    payload["error_message"] = redact_text(result.error_message)
    if not result.technical_success:
        payload["final_content"] = redact_text(result.final_content)
        payload["messages"] = []
    return payload


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
    )


async def _insert_event(connection: Any, row: Any, event_type: str, event_payload: dict[str, Any]) -> None:
    await connection.execute(
        """
        INSERT INTO invocation_events
          (invocation_id, owner_id, session_id, request_id, trace_id, event_type, payload)
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        """,
        row["invocation_id"], row["owner_id"], row["session_id"],
        row["request_id"], row["trace_id"], event_type,
        json.dumps(event_payload, ensure_ascii=False),
    )


class PostgresRepository:
    """Database access used by API/Worker processes and durable memory."""

    def __init__(self, pool: Any):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str, *, min_size: int = 1, max_size: int = 8) -> "PostgresRepository":
        try:
            import asyncpg
        except ImportError as exc:  # pragma: no cover - environment diagnostic
            raise RuntimeError("asyncpg is required for PostgreSQL service mode") from exc
        return cls(await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size))

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
    ) -> tuple[Invocation, bool]:
        """Atomically allocate a Session sequence and create invocation+Outbox."""

        invocation_id = invocation_id or str(uuid.uuid4())
        digest = payload_hash({
            "sessionId": session_id,
            "sessionKey": session_key,
            "payload": payload,
        })
        async with self.pool.acquire() as connection:
            async with connection.transaction():
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
                await _insert_event(connection, row, "accepted", {"status": InvocationStatus.QUEUED.value, "sessionSeq": seq})
                return _row_to_invocation(row), False

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
            async with connection.transaction():
                row = await connection.fetchrow("SELECT * FROM invocations WHERE invocation_id = $1 FOR UPDATE", invocation_id)
                if row is None:
                    raise KeyError(invocation_id)
                if row["status"] != InvocationStatus.QUEUED.value:
                    raise RuntimeError(f"invocation is not claimable from {row['status']}")
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
                    UPDATE invocations SET status='RUNNING', started_at=COALESCE(started_at, now()),
                      execution_owner=$2, execution_lease_until=now()+($3 * interval '1 second'),
                      attempt_count=attempt_count+1 WHERE invocation_id=$1 RETURNING *
                    """, invocation_id, execution_owner, lease_seconds,
                )
                await _insert_event(connection, updated, "running", {"executionOwner": execution_owner})
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

        status = InvocationStatus.SUCCEEDED.value if result.technical_success else InvocationStatus.FAILED.value
        result_json = json.dumps(_result_payload(result), ensure_ascii=False)
        error_message = redact_text(result.error_message)
        async with self.pool.acquire() as connection:
            async with connection.transaction():
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
                    },
                )
                if result.technical_success:
                    await _insert_event(connection, updated, "final", {"content": result.final_content or ""})
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
            async with connection.transaction():
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
                    invocation_id, json.dumps(_result_payload(result), ensure_ascii=False),
                )
                await _insert_event(
                    connection, updated,
                    "result", {"outcome": "FINAL", "status": "SUCCEEDED"},
                )
                await _insert_event(connection, updated, "final", {"content": result.final_content})
                return _row_to_invocation(updated)

    async def finish_interrupted(
        self,
        invocation_id: str,
        outcome: ExecutionOutcome,
        *,
        error_message: str | None = None,
        owner_id: str | None = None,
        execution_owner: str | None = None,
    ) -> Invocation:
        if outcome is ExecutionOutcome.TIMEOUT:
            error_code = "QUEUE_TIMEOUT" if error_message and "queue" in error_message.lower() else "EXECUTION_TIMEOUT"
        else:
            error_code = outcome.value
        error_message = redact_text(error_message)
        result = ExecutionResult.failure(outcome, error_code=error_code, error_message=error_message)
        status = InvocationStatus.TIMEOUT.value if outcome is ExecutionOutcome.TIMEOUT else InvocationStatus.FAILED.value
        async with self.pool.acquire() as connection:
            async with connection.transaction():
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
                    invocation_id, status, json.dumps(_result_payload(result)), result.error_code, error_message,
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
            async with connection.transaction():
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

    async def events(self, invocation_id: str) -> list[dict[str, Any]]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT event_id, event_type, payload, created_at, request_id, trace_id, invocation_id "
                "FROM invocation_events WHERE invocation_id=$1 ORDER BY event_id", invocation_id,
            )
            return [{**dict(row), "payload": _decode_json(row["payload"])} for row in rows]

    async def pending_outbox(self, *, limit: int = 100) -> list[dict[str, Any]]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT * FROM outbox WHERE published_at IS NULL AND available_at <= now() ORDER BY outbox_id LIMIT $1",
                limit,
            )
            return [{**dict(row), "envelope": _decode_json(row["envelope"])} for row in rows]

    async def mark_outbox_published(self, outbox_id: int) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute("UPDATE outbox SET published_at=now(), attempts=attempts+1 WHERE outbox_id=$1", outbox_id)

    async def mark_outbox_failed(self, outbox_id: int, error: str, *, retry_seconds: float = 1.0) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                "UPDATE outbox SET attempts=attempts+1,last_error=$2,available_at=now()+($3 * interval '1 second') WHERE outbox_id=$1",
                outbox_id, error[:1000], retry_seconds,
            )
