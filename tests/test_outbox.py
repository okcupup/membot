from __future__ import annotations

import os
import uuid

import pytest
from membot.agent.persistence.redis_transport import OutboxRelay, QueueFullError, RedisTransport
from membot.agent.persistence.repository import PostgresRepository

asyncpg = pytest.importorskip("asyncpg")
redis = pytest.importorskip("redis.asyncio")
RedisError = pytest.importorskip("redis.exceptions").RedisError


REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:56379/0")
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://membot:membot@127.0.0.1:55432/membot"
)


@pytest.mark.asyncio
async def test_bounded_queue_and_unacknowledged_claim_recovery():
    client = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except (OSError, RedisError) as exc:
        await client.aclose()
        pytest.skip(f"Redis integration service unavailable: {exc}")
    queue_key = f"membot:test:{uuid.uuid4().hex}"
    transport = RedisTransport(client, queue_key=queue_key, max_depth=1)
    try:
        envelope = {"invocationId": "inv-test", "traceId": "trace-test"}
        await transport.publish(envelope)
        with pytest.raises(QueueFullError):
            await transport.publish({"invocationId": "inv-overflow"})
        assert await transport.claim(timeout=1) == envelope
        assert await transport.requeue_processing() == 1
        assert await transport.claim(timeout=1) == envelope
        await transport.acknowledge(envelope)
        assert await transport.claim(timeout=1) is None
    finally:
        await client.delete(queue_key, f"{queue_key}:processing")
        await transport.close()


@pytest.mark.asyncio
async def test_committed_outbox_is_published_and_marked():
    try:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"PostgreSQL integration service unavailable: {exc}")
    client = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except (OSError, RedisError) as exc:
        await pool.close()
        await client.aclose()
        pytest.skip(f"Redis integration service unavailable: {exc}")

    repository = PostgresRepository(pool)
    await repository.migrate()
    queue_key = f"membot:outbox-test:{uuid.uuid4().hex}"
    transport = RedisTransport(client, queue_key=queue_key, max_depth=4)
    try:
        suffix = uuid.uuid4().hex
        invocation, duplicate = await repository.accept_invocation(
            owner_id=f"owner-{suffix}",
            session_id=f"session-{suffix}",
            session_key=f"test:{suffix}",
            request_id=f"request-{suffix}",
            trace_id=f"trace-{suffix}",
            payload={"message": "relay"},
        )
        assert not duplicate
        target_row = await pool.fetchrow(
            "SELECT * FROM outbox WHERE invocation_id=$1", invocation.invocation_id,
        )
        assert target_row is not None

        class _SingleOutbox:
            async def pending_outbox(self, *, limit=100):
                row = await pool.fetchrow(
                    "SELECT * FROM outbox WHERE outbox_id=$1 AND published_at IS NULL LIMIT $2",
                    target_row["outbox_id"], limit,
                )
                return [dict(row)] if row else []

            async def mark_outbox_published(self, outbox_id, **kwargs):
                await repository.mark_outbox_published(outbox_id, **kwargs)

            async def mark_outbox_failed(self, outbox_id, error, **kwargs):
                await repository.mark_outbox_failed(outbox_id, error, **kwargs)

        relay = OutboxRelay(_SingleOutbox(), transport)
        assert await relay.publish_once() == 1
        assert await relay.publish_once() == 0
        envelope = await transport.claim(timeout=1)
        assert envelope == {
            "invocationId": invocation.invocation_id,
            "ownerId": invocation.owner_id,
            "sessionId": invocation.session_id,
            "sessionSeq": invocation.session_seq,
            "requestId": invocation.request_id,
            "traceId": invocation.trace_id,
        }
        await transport.acknowledge(envelope)
    finally:
        await client.delete(queue_key, f"{queue_key}:processing")
        await transport.close()
        await repository.close()
