from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from membot.agent.loop import (
    AgentLoop,
    InvocationExecutionTimeoutError,
    InvocationQueueFullError,
    InvocationQueueTimeoutError,
)
from membot.bus.queue import MessageBus
from membot.providers.base import LLMProvider, LLMResponse


class SwitchProvider(LLMProvider):
    def __init__(self):
        super().__init__(api_key="test")
        self.hold = True
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()

    async def chat(self, messages, tools=None, model=None, max_tokens=4096, temperature=0.7, reasoning_effort=None):
        del tools, model, max_tokens, temperature, reasoning_effort
        prompt = messages[-1].get("content", "")
        self.started.set()
        if self.hold:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                raise
        return LLMResponse(content=f"ok:{prompt}")

    def get_default_model(self) -> str:
        return "fake"


def make_loop(tmp_path: Path, provider: LLMProvider, **kwargs) -> AgentLoop:
    return AgentLoop(
        bus=MessageBus(), provider=provider, workspace=tmp_path,
        model="fake", max_iterations=2, memory_window=100, **kwargs,
    )


@pytest.mark.asyncio
async def test_execution_timeout_releases_worker_capacity(tmp_path: Path):
    provider = SwitchProvider()
    loop = make_loop(tmp_path, provider, max_concurrent_invocations=1, execution_timeout=0.01)

    with pytest.raises(InvocationExecutionTimeoutError):
        await loop.process_direct("slow", session_key="timeout:one", chat_id="one")
    assert provider.cancelled.is_set()
    assert loop._execution_semaphore._value == 1

    provider.hold = False
    assert await loop.process_direct("fast", session_key="timeout:two", chat_id="two") == "ok:fast"
    assert loop._pending_invocations == 0


@pytest.mark.asyncio
async def test_pending_invocation_bound_rejects_new_work(tmp_path: Path):
    provider = SwitchProvider()
    loop = make_loop(
        tmp_path, provider, max_concurrent_invocations=1, max_pending_invocations=1,
    )
    first = asyncio.create_task(loop.process_direct("slow", session_key="bound:one", chat_id="one"))
    await asyncio.wait_for(provider.started.wait(), timeout=1)

    with pytest.raises(InvocationQueueFullError):
        await loop.process_direct("rejected", session_key="bound:two", chat_id="two")

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert loop._pending_invocations == 0
    assert loop._execution_semaphore._value == 1


@pytest.mark.asyncio
async def test_queue_timeout_does_not_consume_execution_budget_or_capacity(tmp_path: Path):
    provider = SwitchProvider()
    loop = make_loop(
        tmp_path, provider, max_concurrent_invocations=1,
        queue_timeout=0.01, execution_timeout=1,
    )
    first = asyncio.create_task(loop.process_direct("slow", session_key="queue:one", chat_id="one"))
    await asyncio.wait_for(provider.started.wait(), timeout=1)

    with pytest.raises(InvocationQueueTimeoutError):
        await loop.process_direct("queued", session_key="queue:two", chat_id="two")
    assert loop._execution_semaphore._value == 0
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert loop._execution_semaphore._value == 1


@pytest.mark.asyncio
async def test_cancelled_head_allows_next_turn_to_run(tmp_path: Path):
    provider = SwitchProvider()
    loop = make_loop(tmp_path, provider, max_concurrent_invocations=1)
    first = asyncio.create_task(loop.process_direct("first", session_key="cancel:one", chat_id="one"))
    await asyncio.wait_for(provider.started.wait(), timeout=1)
    second = asyncio.create_task(loop.process_direct("second", session_key="cancel:one", chat_id="one"))

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    await asyncio.wait_for(provider.cancelled.wait(), timeout=1)

    provider.release.set()
    provider.hold = False
    assert await second == "ok:second"
    assert loop._execution_semaphore._value == 1
    assert loop._pending_invocations == 0
    assert loop._session_states == {}


@pytest.mark.asyncio
async def test_shutdown_cancels_direct_backlog_without_starting_it(tmp_path: Path):
    provider = SwitchProvider()
    loop = make_loop(tmp_path, provider, max_concurrent_invocations=1)
    first = asyncio.create_task(loop.process_direct("first", session_key="shutdown:one", chat_id="one"))
    await asyncio.wait_for(provider.started.wait(), timeout=1)
    second = asyncio.create_task(loop.process_direct("second", session_key="shutdown:one", chat_id="one"))
    await asyncio.sleep(0)
    assert loop._pending_invocations == 2

    await loop.shutdown()
    results = await asyncio.gather(first, second, return_exceptions=True)

    assert all(isinstance(result, asyncio.CancelledError) for result in results)
    assert provider.cancelled.is_set()
    assert loop._pending_invocations == 0
    assert loop._session_states == {}
    assert loop._execution_semaphore._value == 1
