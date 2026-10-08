from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path

import pytest
from membot.agent.persistence.redis_transport import RedisStreamTransport
from membot.agent.persistence.repository import InvocationStatus, PostgresRepository
from membot.providers.base import LLMProvider, LLMResponse
from membot.service.config import ServiceConfig
from membot.service.worker import AsyncWorker, WorkerAlreadyRunningError

asyncpg = pytest.importorskip("asyncpg")
redis_async = pytest.importorskip("redis.asyncio")
RedisError = pytest.importorskip("redis.exceptions").RedisError

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://membot:membot@127.0.0.1:55432/membot"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:56379/0")


class WorkerFakeProvider(LLMProvider):
    """Deterministic provider with barriers for Worker overlap assertions."""

    def __init__(self, *, delay: float = 0.0):
        super().__init__(api_key="worker-test")
        self.delay = delay
        self.active = 0
        self.max_active = 0
        self.starts: list[str] = []
        self.ends: list[str] = []
        self.started = asyncio.Event()

    async def chat(self, messages, tools=None, model=None, max_tokens=4096, temperature=0.7, reasoning_effort=None):
        del tools, model, max_tokens, temperature, reasoning_effort
        prompt = str(messages[-1].get("content", ""))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.starts.append(prompt)
        self.started.set()
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            return LLMResponse(content=f"answer:{prompt}")
        finally:
            self.active -= 1
            self.ends.append(prompt)

    def get_default_model(self) -> str:
        return "worker-fake"


class BlockingProvider(WorkerFakeProvider):
    def __init__(self):
        super().__init__()
        self.release = asyncio.Event()

    async def chat(self, messages, **kwargs):
        del kwargs
        prompt = str(messages[-1].get("content", ""))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.starts.append(prompt)
        self.started.set()
        try:
            await self.release.wait()
            return LLMResponse(content=f"answer:{prompt}")
        finally:
            self.active -= 1
            self.ends.append(prompt)


@pytest.fixture
async def service_stack(tmp_path: Path):
    try:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=6)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"PostgreSQL integration service unavailable: {exc}")
    repository = PostgresRepository(pool)
    await repository.migrate()
    redis = redis_async.from_url(REDIS_URL, decode_responses=True)
    try:
        await redis.ping()
    except (OSError, RedisError) as exc:
        await repository.close()
        await redis.aclose()
        pytest.skip(f"Redis integration service unavailable: {exc}")
    suffix = uuid.uuid4().hex
    stream = f"membot:m3:{suffix}"
    group = f"membot-m3:{suffix}"
    transport = RedisStreamTransport(
        redis, stream_key=stream, group=group, consumer=f"consumer:{suffix}",
        max_depth=32, prefetch=4,
    )
    try:
        yield repository, transport, redis, tmp_path
    finally:
        await redis.delete(stream)
        await transport.close()
        await repository.close()


def _config(tmp_path: Path, **overrides) -> ServiceConfig:
    values = {
        "owner_id": f"owner-{uuid.uuid4().hex}",
        "workspace": str(tmp_path),
        "model": "worker-fake",
        "worker_concurrency": 2,
        "worker_prefetch": 4,
        "queue_timeout_seconds": 5.0,
        "execution_timeout_seconds": 5.0,
        "llm_timeout_seconds": 2.0,
        "tool_timeout_seconds": 2.0,
        "outbox_poll_seconds": 0.02,
        "queue_poll_seconds": 0.02,
        "worker_pending_idle_ms": 0,
    }
    values.update(overrides)
    return ServiceConfig(**values)


async def _accept(repo, config, session_id, session_key, message, *, queue_timeout=None):
    return await repo.accept_invocation(
        owner_id=config.owner_id,
        session_id=session_id,
        session_key=session_key,
        request_id=f"request-{uuid.uuid4().hex}",
        trace_id=f"trace-{uuid.uuid4().hex}",
        payload={"message": message, "channel": "service", "chatId": session_id, "maxIterations": 4},
        queue_timeout_seconds=queue_timeout if queue_timeout is not None else config.queue_timeout_seconds,
        execution_timeout_seconds=config.execution_timeout_seconds,
    )


async def _wait_terminal(repo, invocation_id, timeout=8.0):
    async def poll():
        while True:
            value = await repo.get_invocation(invocation_id)
            if value and value.status in {
                InvocationStatus.SUCCEEDED, InvocationStatus.FAILED, InvocationStatus.TIMEOUT,
            }:
                return value
            await asyncio.sleep(0.02)

    return await asyncio.wait_for(poll(), timeout)


@pytest.mark.asyncio
async def test_worker_runs_outbox_stream_and_preserves_session_order(service_stack):
    repo, transport, _, tmp_path = service_stack
    config = _config(tmp_path)
    provider = WorkerFakeProvider(delay=0.03)
    worker = AsyncWorker(repo, transport, config, provider=provider)
    await worker.start()
    task = asyncio.create_task(worker.run())
    try:
        first, _ = await _accept(repo, config, "session-order", "service:order", "one")
        second, _ = await _accept(repo, config, "session-order", "service:order", "two")
        first_done, second_done = await asyncio.gather(
            _wait_terminal(repo, first.invocation_id), _wait_terminal(repo, second.invocation_id),
        )
        assert first_done.status is InvocationStatus.SUCCEEDED
        assert second_done.status is InvocationStatus.SUCCEEDED
        assert provider.max_active == 1
        assert provider.starts[:2] == ["one", "two"]
        assert await repo.pool.fetchval(
            "SELECT count(*) FROM invocation_events WHERE invocation_id=$1 AND event_type='LLM'",
            first.invocation_id,
        ) == 1
        assert await repo.pool.fetchval(
            "SELECT count(*) FROM invocation_events WHERE invocation_id=$1 AND event_type='FINAL'",
            first.invocation_id,
        ) == 1
    finally:
        await worker.stop(worker_lost=False)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_worker_overlaps_sessions_and_duplicate_delivery_only_acks(service_stack):
    repo, transport, _, tmp_path = service_stack
    config = _config(tmp_path, worker_concurrency=2)
    provider = WorkerFakeProvider(delay=0.08)
    worker = AsyncWorker(repo, transport, config, provider=provider)
    await worker.start()
    task = asyncio.create_task(worker.run())
    try:
        first, _ = await _accept(repo, config, "session-a", "service:a", "a")
        second, _ = await _accept(repo, config, "session-b", "service:b", "b")
        await asyncio.gather(
            _wait_terminal(repo, first.invocation_id), _wait_terminal(repo, second.invocation_id),
        )
        assert provider.max_active == 2
        # A second Redis entry for the already terminal invocation is safely
        # acknowledged and does not call the provider again.
        duplicate_id = await transport.publish({
            "invocationId": first.invocation_id, "sessionId": first.session_id,
        })
        await worker._consume_once()
        await asyncio.sleep(0.05)
        assert (await transport.client.xpending(transport.stream_key, transport.group))["pending"] == 0
        assert provider.starts.count("a") == 1
        assert duplicate_id
    finally:
        await worker.stop(worker_lost=False)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_worker_llm_timeout_and_queue_timeout_are_terminal(service_stack):
    repo, transport, _, tmp_path = service_stack
    config = _config(tmp_path, worker_concurrency=1, llm_timeout_seconds=0.03)
    provider = BlockingProvider()
    worker = AsyncWorker(repo, transport, config, provider=provider)
    await worker.start()
    task = asyncio.create_task(worker.run())
    try:
        timed, _ = await _accept(repo, config, "session-timeout", "service:timeout", "slow")
        terminal = await _wait_terminal(repo, timed.invocation_id)
        assert terminal.status is InvocationStatus.TIMEOUT
        assert terminal.error_code == "LLM_TIMEOUT"

        queued, _ = await _accept(
            repo, config, "session-queue-timeout", "service:queue-timeout", "never",
            queue_timeout=0.01,
        )
        await asyncio.sleep(0.1)
        assert await repo.expire_queued() == 1 or (await repo.get_invocation(queued.invocation_id)).status is InvocationStatus.TIMEOUT
        queued_done = await _wait_terminal(repo, queued.invocation_id)
        assert queued_done.status is InvocationStatus.TIMEOUT
        assert queued_done.error_code == "QUEUE_TIMEOUT"
    finally:
        provider.release.set()
        await worker.stop(worker_lost=False)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_second_worker_is_rejected_by_postgres_lock(service_stack):
    repo, transport, _, tmp_path = service_stack
    config = _config(tmp_path)
    first = AsyncWorker(repo, transport, config, provider=WorkerFakeProvider())
    await first.start()
    second = AsyncWorker(repo, transport, config, provider=WorkerFakeProvider())
    try:
        with pytest.raises(WorkerAlreadyRunningError):
            await second.start()
    finally:
        await first.stop(worker_lost=False)
        await second.stop(worker_lost=False)
