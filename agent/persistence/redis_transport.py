"""Small Redis transport used by the PostgreSQL Outbox relay and Worker.

The queue is deliberately at-least-once: a claimed envelope remains in the
processing list until the Worker acknowledges it. On Worker restart the
unacknowledged envelope can be returned to the ready list; PostgreSQL Outbox
remains the durable source of work.
"""

from __future__ import annotations

import json
from typing import Any


class QueueFullError(RuntimeError):
    """The bounded Redis transport cannot accept another envelope."""


class RedisTransport:
    def __init__(self, client: Any, *, queue_key: str = "membot:invocations", max_depth: int = 1024):
        self.client = client
        self.queue_key = queue_key
        self.processing_key = f"{queue_key}:processing"
        self.max_depth = max(1, max_depth)

    @classmethod
    async def connect(cls, url: str, *, queue_key: str = "membot:invocations", max_depth: int = 1024) -> "RedisTransport":
        try:
            from redis.asyncio import Redis
        except ImportError as exc:  # pragma: no cover - environment diagnostic
            raise RuntimeError("redis is required for service queue mode") from exc
        return cls(Redis.from_url(url, decode_responses=True), queue_key=queue_key, max_depth=max_depth)

    async def close(self) -> None:
        await self.client.aclose()

    async def publish(self, envelope: dict[str, Any]) -> None:
        """Append one envelope if the bounded queue has capacity."""

        encoded = json.dumps(envelope, ensure_ascii=False, sort_keys=True)
        accepted = await self.client.eval(
            "local depth=redis.call('LLEN', KEYS[1]); "
            "local processing=redis.call('LLEN', KEYS[2]); "
            "if depth+processing >= tonumber(ARGV[1]) then return 0 end; "
            "redis.call('LPUSH', KEYS[1], ARGV[2]); return 1",
            2, self.queue_key, self.processing_key, self.max_depth, encoded,
        )
        if not accepted:
            raise QueueFullError(f"Redis invocation queue is full ({self.max_depth})")

    async def claim(self, timeout: int = 1) -> dict[str, Any] | None:
        """Move one item to the processing list before returning it."""

        item = await self.client.brpoplpush(self.queue_key, self.processing_key, timeout=timeout)
        if item is None:
            return None
        return json.loads(item)

    async def acknowledge(self, envelope: dict[str, Any]) -> None:
        encoded = json.dumps(envelope, ensure_ascii=False, sort_keys=True)
        await self.client.lrem(self.processing_key, 1, encoded)

    async def requeue_processing(self) -> int:
        """Move all unacknowledged envelopes back to the ready queue on startup."""

        moved = 0
        while True:
            item = await self.client.rpoplpush(self.processing_key, self.queue_key)
            if item is None:
                return moved
            moved += 1


class OutboxRelay:
    """Publish committed Outbox rows and leave failures pending for retry."""

    def __init__(self, repository: Any, transport: RedisTransport, *, batch_size: int = 100):
        self.repository = repository
        self.transport = transport
        self.batch_size = max(1, batch_size)

    async def publish_once(self) -> int:
        published = 0
        for row in await self.repository.pending_outbox(limit=self.batch_size):
            try:
                envelope = row["envelope"]
                if isinstance(envelope, str):
                    envelope = json.loads(envelope)
                await self.transport.publish(envelope)
            except Exception as exc:
                await self.repository.mark_outbox_failed(row["outbox_id"], str(exc))
                continue
            await self.repository.mark_outbox_published(row["outbox_id"])
            published += 1
        return published
