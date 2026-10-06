from __future__ import annotations

import asyncio
from collections import defaultdict
from pathlib import Path

import pytest

from membot.agent.execution import get_execution_context
from membot.agent.loop import AgentLoop
from membot.agent.tools.base import Tool
from membot.bus.events import InboundMessage
from membot.bus.queue import MessageBus
from membot.providers.base import LLMProvider, LLMResponse, ToolCallRequest


class EventProvider(LLMProvider):
    """Deterministic provider with explicit entry/release barriers."""

    def __init__(self, *, hold: bool = False, final: str = "done"):
        super().__init__(api_key="test")
        self.hold = hold
        self.final = final
        self.started = asyncio.Event()
        self.all_started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.active = 0
        self.max_active = 0
        self.start_order: list[str] = []
        self.finish_order: list[str] = []
        self._start_count = 0

    async def chat(self, messages, tools=None, model=None, max_tokens=4096, temperature=0.7, reasoning_effort=None):
        del tools, model, max_tokens, temperature, reasoning_effort
        prompt = messages[-1].get("content", "") if messages else ""
        if not isinstance(prompt, str):
            prompt = str(prompt)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self._start_count += 1
        self.start_order.append(prompt)
        self.started.set()
        if self._start_count >= 2:
            self.all_started.set()
        try:
            if self.hold:
                await self.release.wait()
            else:
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        finally:
            self.active -= 1
            self.finish_order.append(prompt)
        return LLMResponse(content=f"answer:{prompt}")

    def get_default_model(self) -> str:
        return "fake"


class ContextTool(Tool):
    name = "record_context"
    description = "Record the invocation context."
    parameters = {"type": "object", "properties": {}, "required": []}

    def __init__(self):
        self.calls: list[tuple[str, str, str, str | None]] = []

    async def execute(self, **kwargs):
        del kwargs
        context = get_execution_context()
        assert context is not None
        self.calls.append((context.session_key, context.channel, context.chat_id, context.message_id))
        return f"context:{context.session_key}"


class ToolScriptProvider(LLMProvider):
    """Ask for one tool call per initial user message, then return a final."""

    def __init__(self, tool_name: str):
        super().__init__(api_key="test")
        self.tool_name = tool_name
        self.calls: defaultdict[str, int] = defaultdict(int)

    async def chat(self, messages, tools=None, model=None, max_tokens=4096, temperature=0.7, reasoning_effort=None):
        del tools, model, max_tokens, temperature, reasoning_effort
        user = next(m for m in reversed(messages) if m.get("role") == "user")
        prompt = user.get("content", "")
        self.calls[prompt] += 1
        if self.calls[prompt] == 1:
            return LLMResponse(
                content=None,
                tool_calls=[ToolCallRequest(id=f"call-{prompt}", name=self.tool_name, arguments={})],
            )
        return LLMResponse(content=f"final:{prompt}")

    def get_default_model(self) -> str:
        return "fake"


class MessageScriptProvider(LLMProvider):
    def __init__(self):
        super().__init__(api_key="test")
        self.calls: defaultdict[str, int] = defaultdict(int)

    async def chat(self, messages, tools=None, model=None, max_tokens=4096, temperature=0.7, reasoning_effort=None):
        del tools, model, max_tokens, temperature, reasoning_effort
        prompt = next(m for m in reversed(messages) if m.get("role") == "user").get("content", "")
        self.calls[prompt] += 1
        if self.calls[prompt] == 1:
            return LLMResponse(
                content=None,
                tool_calls=[ToolCallRequest(
                    id=f"call-{prompt}", name="message",
                    arguments={"content": f"sent:{prompt}"},
                )],
            )
        return LLMResponse(content=f"final:{prompt}")

    def get_default_model(self) -> str:
        return "fake"


def make_loop(tmp_path: Path, provider: LLMProvider, **kwargs) -> AgentLoop:
    return AgentLoop(
        bus=MessageBus(),
        provider=provider,
        workspace=tmp_path,
        model="fake",
        max_iterations=4,
        memory_window=100,
        **kwargs,
    )


def test_message_bus_queues_are_bounded():
    bus = MessageBus(inbound_maxsize=2, outbound_maxsize=3)

    assert bus.inbound.maxsize == 2
    assert bus.outbound.maxsize == 3
    with pytest.raises(ValueError):
        MessageBus(inbound_maxsize=0)
    with pytest.raises(ValueError):
        MessageBus(outbound_maxsize=0)


@pytest.mark.asyncio
async def test_three_turns_same_session_are_serial_and_preserve_acceptance_order(tmp_path: Path):
    provider = EventProvider()
    loop = make_loop(tmp_path, provider, max_concurrent_invocations=3)

    results = await asyncio.gather(
        loop.process_direct("one", session_key="chat:one", chat_id="one"),
        loop.process_direct("two", session_key="chat:one", chat_id="one"),
        loop.process_direct("three", session_key="chat:one", chat_id="one"),
    )

    assert results == ["answer:one", "answer:two", "answer:three"]
    assert provider.max_active == 1
    assert provider.start_order == ["one", "two", "three"]
    assert provider.finish_order == ["one", "two", "three"]
    assert loop._session_states == {}


@pytest.mark.asyncio
async def test_different_sessions_overlap_without_exceeding_worker_limit(tmp_path: Path):
    provider = EventProvider(hold=True)
    loop = make_loop(tmp_path, provider, max_concurrent_invocations=2)

    first = asyncio.create_task(loop.process_direct("a", session_key="chat:a", chat_id="a"))
    second = asyncio.create_task(loop.process_direct("b", session_key="chat:b", chat_id="b"))
    await asyncio.wait_for(provider.all_started.wait(), timeout=1)
    assert provider.max_active == 2
    provider.release.set()

    assert await asyncio.gather(first, second) == ["answer:a", "answer:b"]
    assert loop._pending_invocations == 0
    assert loop._session_states == {}


@pytest.mark.asyncio
async def test_worker_semaphore_limits_cross_session_invocations(tmp_path: Path):
    provider = EventProvider(hold=True)
    loop = make_loop(tmp_path, provider, max_concurrent_invocations=2)
    tasks = [
        asyncio.create_task(loop.process_direct(str(i), session_key=f"chat:{i}", chat_id=str(i)))
        for i in range(4)
    ]

    await asyncio.wait_for(provider.all_started.wait(), timeout=1)
    await asyncio.sleep(0)
    assert provider.max_active == 2
    assert loop._pending_invocations == 4
    provider.release.set()

    assert len(await asyncio.gather(*tasks)) == 4
    assert provider.max_active == 2
    assert loop._pending_invocations == 0


@pytest.mark.asyncio
async def test_large_session_backlog_does_not_block_another_session(tmp_path: Path):
    provider = EventProvider(hold=True)
    loop = make_loop(tmp_path, provider, max_concurrent_invocations=2)

    hot1 = asyncio.create_task(loop.process_direct("hot-1", session_key="chat:hot", chat_id="hot"))
    await asyncio.wait_for(provider.started.wait(), timeout=1)
    hot2 = asyncio.create_task(loop.process_direct("hot-2", session_key="chat:hot", chat_id="hot"))
    hot3 = asyncio.create_task(loop.process_direct("hot-3", session_key="chat:hot", chat_id="hot"))
    cold = asyncio.create_task(loop.process_direct("cold", session_key="chat:cold", chat_id="cold"))

    await asyncio.wait_for(provider.all_started.wait(), timeout=1)
    assert provider.start_order[:2] == ["hot-1", "cold"]
    provider.release.set()
    assert await asyncio.gather(hot1, hot2, hot3, cold) == [
        "answer:hot-1", "answer:hot-2", "answer:hot-3", "answer:cold"
    ]


@pytest.mark.asyncio
async def test_tool_context_isolated_between_sessions(tmp_path: Path):
    provider = ToolScriptProvider("record_context")
    loop = make_loop(tmp_path, provider, max_concurrent_invocations=2)
    tool = ContextTool()
    loop.tools.register(tool)

    first, second = await asyncio.gather(
        loop.process_direct(
            "first", session_key="thread:first", channel="telegram", chat_id="100",
            metadata={"message_id": "m-1"},
        ),
        loop.process_direct(
            "second", session_key="thread:second", channel="discord", chat_id="200",
            metadata={"message_id": "m-2"},
        ),
    )

    assert first == "final:first"
    assert second == "final:second"
    assert sorted(tool.calls) == [
        ("thread:first", "telegram", "100", "m-1"),
        ("thread:second", "discord", "200", "m-2"),
    ]


@pytest.mark.asyncio
async def test_system_callback_uses_actual_target_session(tmp_path: Path):
    provider = EventProvider()
    bus = MessageBus()
    loop = AgentLoop(
        bus=bus, provider=provider, workspace=tmp_path, model="fake", max_iterations=2,
    )
    await loop._dispatch(InboundMessage(
        channel="system", sender_id="callback", chat_id="telegram:42", content="callback",
    ))
    outbound = await bus.consume_outbound()
    assert outbound.channel == "telegram"
    assert outbound.chat_id == "42"
    assert (tmp_path / "sessions" / "telegram_42.jsonl").exists()


@pytest.mark.asyncio
async def test_message_tool_final_marker_is_invocation_local(tmp_path: Path):
    provider = MessageScriptProvider()
    bus = MessageBus()
    loop = AgentLoop(
        bus=bus, provider=provider, workspace=tmp_path, model="fake", max_iterations=3,
        max_concurrent_invocations=2,
    )

    first, second = await asyncio.gather(
        loop.process_direct("first", session_key="message:first", channel="telegram", chat_id="1"),
        loop.process_direct("second", session_key="message:second", channel="discord", chat_id="2"),
    )

    assert first == ""
    assert second == ""
    sent = []
    while len(sent) < 2:
        message = await bus.consume_outbound()
        if not message.metadata.get("_progress"):
            sent.append(message)
    assert sorted((message.channel, message.chat_id, message.content) for message in sent) == [
        ("discord", "2", "sent:second"),
        ("telegram", "1", "sent:first"),
    ]


@pytest.mark.asyncio
async def test_tool_messages_keep_invocation_correlation_ids(tmp_path: Path):
    provider = MessageScriptProvider()
    bus = MessageBus()
    loop = AgentLoop(
        bus=bus, provider=provider, workspace=tmp_path, model="fake", max_iterations=3,
    )

    await loop.process_direct(
        "correlated",
        session_key="message:correlated",
        channel="telegram",
        chat_id="99",
        metadata={
            "message_id": "m-99",
            "requestId": "req-99",
            "traceId": "trace-99",
            "invocationId": "inv-99",
        },
    )

    while True:
        message = await bus.consume_outbound()
        if not message.metadata.get("_progress"):
            assert message.metadata == {
                "message_id": "m-99",
                "request_id": "req-99",
                "trace_id": "trace-99",
                "invocation_id": "inv-99",
            }
            break


@pytest.mark.asyncio
async def test_mcp_initialization_is_shared_and_waited_for(tmp_path: Path, monkeypatch):
    provider = EventProvider()
    loop = make_loop(tmp_path, provider, max_concurrent_invocations=2, mcp_servers={"fake": object()})
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def fake_connect(servers, registry, stack):
        nonlocal calls
        del servers, registry, stack
        calls += 1
        started.set()
        await release.wait()

    monkeypatch.setattr("membot.agent.tools.mcp.connect_mcp_servers", fake_connect)
    first = asyncio.create_task(loop._connect_mcp())
    second = asyncio.create_task(loop._connect_mcp())
    await asyncio.wait_for(started.wait(), timeout=1)
    assert not first.done()
    assert not second.done()
    assert calls == 1
    release.set()
    await asyncio.gather(first, second)
    assert loop._mcp_connected
    await loop.close_mcp()
