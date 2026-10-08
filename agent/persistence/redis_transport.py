"""Small Redis transport used by the PostgreSQL Outbox relay and Worker.

The queue is deliberately at-least-once: a claimed envelope remains in the
processing list until the Worker acknowledges it. On Worker restart the
unacknowledged envelope can be returned to the ready list; PostgreSQL Outbox
remains the durable source of work.
"""

from __future__ import annotations

import json
import time
import uuid
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


class RedisStreamTransport:
    """Bounded Redis Streams transport with consumer-group delivery.

    Redis is only a transport.  A stream entry remains pending until the
    Worker has committed the PostgreSQL terminal state and calls ``ack``.
    Publication is therefore at-least-once and duplicate entries are expected.
    """

    def __init__(
        self,
        client: Any,
        *,
        stream_key: str = "membot:invocations:stream",
        group: str = "membot-workers",
        consumer: str | None = None,
        max_depth: int = 1024,
        prefetch: int = 8,
    ):
        self.client = client
        self.stream_key = stream_key
        self.group = group
        self.consumer = consumer or f"worker-{uuid.uuid4().hex}"
        self.max_depth = max(1, max_depth)
        self.prefetch = max(1, prefetch)
        self._group_ready = False

    @classmethod
    async def connect(
        cls,
        url: str,
        *,
        stream_key: str = "membot:invocations:stream",
        group: str = "membot-workers",
        consumer: str | None = None,
        max_depth: int = 1024,
        prefetch: int = 8,
    ) -> "RedisStreamTransport":
        try:
            from redis.asyncio import Redis
        except ImportError as exc:  # pragma: no cover - environment diagnostic
            raise RuntimeError("redis is required for service queue mode") from exc
        return cls(
            Redis.from_url(url, decode_responses=True), stream_key=stream_key,
            group=group, consumer=consumer, max_depth=max_depth, prefetch=prefetch,
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def ensure_group(self) -> None:
        if self._group_ready:
            return
        try:
            await self.client.xgroup_create(
                name=self.stream_key, groupname=self.group, id="0-0", mkstream=True,
            )
        except Exception as exc:
            # Redis-py does not expose a stable exception class across major
            # versions; BUSYGROUP is the only expected creation race.
            if "BUSYGROUP" not in str(exc):
                raise
        self._group_ready = True

    @staticmethod
    def _encode(envelope: dict[str, Any]) -> str:
        return json.dumps(envelope, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _decode(entry: tuple[str, dict[str, Any]]) -> tuple[str, dict[str, Any]]:
        entry_id, fields = entry
        raw = fields.get("payload") or fields.get(b"payload")
        if raw is None:
            raw = fields.get("envelope") or fields.get(b"envelope")
        return entry_id, json.loads(raw)

    async def depth(self) -> int:
        await self.ensure_group()
        return int(await self.client.xlen(self.stream_key))

    async def publish(self, envelope: dict[str, Any]) -> str:
        """Publish one bounded envelope and return its Redis stream ID."""
        await self.ensure_group()
        depth = await self.client.xlen(self.stream_key)
        if int(depth) >= self.max_depth:
            raise QueueFullError(f"Redis invocation stream is full ({self.max_depth})")
        return await self.client.xadd(
            self.stream_key,
            {"payload": self._encode(envelope)},
            maxlen=self.max_depth,
            approximate=False,
        )

    async def read(self, *, timeout: float = 1.0, count: int | None = None) -> list[tuple[str, dict[str, Any]]]:
        """Read new entries with a bounded prefetch."""
        await self.ensure_group()
        result = await self.client.xreadgroup(
            groupname=self.group, consumername=self.consumer,
            streams={self.stream_key: ">"}, count=min(count or self.prefetch, self.prefetch),
            block=max(1, int(timeout * 1000)),
        )
        if not result:
            return []
        return [self._decode(entry) for _, entries in result for entry in entries]

    async def ack(self, entry_id: str) -> int:
        await self.ensure_group()
        acknowledged = int(await self.client.xack(self.stream_key, self.group, entry_id))
        if acknowledged:
            delete = getattr(self.client, "xdel", None)
            if delete is not None:
                await delete(self.stream_key, entry_id)
        return acknowledged

    async def recover_pending(
        self, *, min_idle_ms: int = 0, count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Claim pending entries from a previous Worker and return them."""
        await self.ensure_group()
        limit = min(count or self.prefetch, self.prefetch)
        try:
            result = await self.client.xautoclaim(
                self.stream_key, self.group, self.consumer, min_idle_ms,
                start_id="0-0", count=limit,
            )
            entries = result[1] if len(result) > 1 else []
            return [self._decode(entry) for entry in entries]
        except (AttributeError, NotImplementedError):
            pending = await self.client.xpending_range(
                self.stream_key, self.group, min="-", max="+", count=limit,
            )
            if not pending:
                return []
            ids = [item["message_id"] for item in pending]
            claimed = await self.client.xclaim(
                self.stream_key, self.group, self.consumer,
                min_idle_time=min_idle_ms, message_ids=ids,
            )
            return [self._decode(entry) for entry in claimed]

    async def reclaim_loop(self, *, min_idle_ms: int = 500, count: int | None = None) -> list[tuple[str, dict[str, Any]]]:
        """Alias used by Worker recovery code."""
        return await self.recover_pending(min_idle_ms=min_idle_ms, count=count)


class OutboxRelay:
    """Publish committed Outbox rows and leave failures pending for retry."""

    def __init__(
        self, repository: Any, transport: Any, *, batch_size: int = 100,
        max_retry_seconds: float = 30.0,
        owner_id: str | None = None,
        worker_id: str | None = None,
    ):
        self.repository = repository
        self.transport = transport
        self.batch_size = max(1, batch_size)
        self.max_retry_seconds = max(0.1, max_retry_seconds)
        self.owner_id = owner_id
        self.worker_id = worker_id

    async def publish_once(self) -> int:
        published = 0
        query = {"limit": self.batch_size}
        if self.owner_id is not None:
            query["owner_id"] = self.owner_id
        for row in await self.repository.pending_outbox(**query):
            span_id = uuid.uuid4().hex
            record_start = getattr(self.repository, "record_outbox_start", None)
            if record_start is not None:
                await record_start(row["outbox_id"], worker=self.worker_id, span_id=span_id)
            started = time.monotonic()
            try:
                envelope = row["envelope"]
                if isinstance(envelope, str):
                    envelope = json.loads(envelope)
                entry_id = await self.transport.publish(envelope)
            except Exception as exc:
                attempts = int(row.get("attempts") or 0)
                retry_seconds = min(self.max_retry_seconds, max(0.1, 2 ** min(attempts, 8)))
                await self.repository.mark_outbox_failed(
                    row["outbox_id"], str(exc), retry_seconds=retry_seconds,
                    worker=self.worker_id, span_id=span_id,
                    duration_ms=(time.monotonic() - started) * 1000,
                    error_code="QUEUE_FULL" if isinstance(exc, QueueFullError) else "OUTBOX_PUBLISH_ERROR",
                )
                continue
            await self.repository.mark_outbox_published(
                row["outbox_id"], worker=self.worker_id, span_id=span_id,
                entry_id=entry_id, duration_ms=(time.monotonic() - started) * 1000,
            )
            published += 1
        return published
