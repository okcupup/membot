from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from membot.agent.conversation_memory.models import MemoryRecord
from membot.agent.conversation_memory.engine import ConversationMemoryEngine
from membot.agent.execution_result import ExecutionOutcome, ExecutionResult
from membot.agent.loop import AgentLoop
from membot.agent.tools.base import Tool
from membot.agent.persistence.repository import (
    IdempotencyConflictError,
    InvocationStatus,
    PostgresRepository,
)
from membot.bus.queue import MessageBus
from membot.providers.base import LLMProvider, LLMResponse, ToolCallRequest

asyncpg = pytest.importorskip("asyncpg")


DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://membot:membot@127.0.0.1:55432/membot"
)


@pytest.fixture
async def repositories():
    try:
        pool_a = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"PostgreSQL integration service unavailable: {exc}")
    pool_b = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    first, second = PostgresRepository(pool_a), PostgresRepository(pool_b)
    await first.migrate()
    try:
        yield first, second
    finally:
        await first.close()
        await second.close()


def _ids() -> tuple[str, str, str]:
    suffix = uuid.uuid4().hex
    return f"owner-{suffix}", f"session-{suffix}", f"channel:{suffix}"


async def _accept(repo, owner, session, key, *, payload=None, idem=None):
    return await repo.accept_invocation(
        owner_id=owner,
        session_id=session,
        session_key=key,
        request_id=f"request-{uuid.uuid4().hex}",
        trace_id=f"trace-{uuid.uuid4().hex}",
        payload=payload or {"message": "hello"},
        idempotency_key=idem,
    )


@pytest.mark.asyncio
async def test_acceptance_is_idempotent_and_allocates_ordered_sequences(repositories):
    first, second = repositories
    owner, session, key = _ids()

    accepted, duplicate = await first.accept_invocation(
        owner_id=owner, session_id=session, session_key=key,
        request_id="request-original", trace_id="trace-original",
        payload={"message": "same"}, idempotency_key="same-key",
    )
    repeated, was_duplicate = await second.accept_invocation(
        owner_id=owner, session_id=session, session_key=key,
        request_id="request-retry", trace_id="trace-retry",
        payload={"message": "same"}, idempotency_key="same-key",
    )
    assert not duplicate and was_duplicate
    assert repeated.invocation_id == accepted.invocation_id
    assert repeated.trace_id == "trace-original"

    with pytest.raises(IdempotencyConflictError):
        await second.accept_invocation(
            owner_id=owner, session_id=session, session_key=key,
            request_id="request-conflict", trace_id="trace-conflict",
            payload={"message": "different"}, idempotency_key="same-key",
        )
    with pytest.raises(IdempotencyConflictError):
        await second.accept_invocation(
            owner_id=owner, session_id=f"other-{session}", session_key=f"other:{key}",
            request_id="request-other-session", trace_id="trace-other-session",
            payload={"message": "same"}, idempotency_key="same-key",
        )

    results = await asyncio.gather(*[
        _accept(first if index % 2 else second, owner, session, key, payload={"message": str(index)})
        for index in range(12)
    ])
    seqs = sorted([accepted.session_seq, *(result.session_seq for result, _ in results)])
    assert seqs == list(range(1, 14))


@pytest.mark.asyncio
async def test_restart_visibility_and_atomic_turn_terminal_commit(repositories):
    first, second = repositories
    owner, session, key = _ids()
    invocation, _ = await _accept(first, owner, session, key)
    await first.claim_invocation(invocation.invocation_id, "worker-a")
    records = [MemoryRecord(
        kind="raw_message",
        session_key=key,
        payload={"turn_id": "turn-1", "messages": [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "world"},
        ]},
    )]
    completed = await first.complete_invocation(
        invocation.invocation_id,
        ExecutionResult(ExecutionOutcome.FINAL, "world", records[0].payload["messages"]),
        records,
        execution_owner="worker-a",
    )
    assert completed.status is InvocationStatus.SUCCEEDED

    visible = await second.load_session(owner, key)
    assert visible is not None
    assert [message["content"] for message in visible.messages] == ["hello", "world"]
    row = await second.get_invocation(invocation.invocation_id)
    assert row is not None and row.status is InvocationStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_failure_records_diagnostics_without_committing_partial_history(repositories):
    first, second = repositories
    owner, session, key = _ids()
    invocation, _ = await _accept(first, owner, session, key)
    await first.claim_invocation(invocation.invocation_id, "worker-b")
    partial = [{"role": "assistant", "tool_calls": [{"id": "call-1"}]}]
    failed = await first.complete_invocation(
        invocation.invocation_id,
        ExecutionResult.failure(
            ExecutionOutcome.PROVIDER_ERROR,
            messages=partial,
            error_code="PROVIDER_ERROR",
            error_message="upstream 502 api_key=sk-12345678901234567890",
        ),
        execution_owner="worker-b",
    )
    assert failed.status is InvocationStatus.FAILED
    assert failed.error_code == "PROVIDER_ERROR"
    assert failed.error_message == "upstream 502 api_key=[REDACTED]"
    assert failed.result["messages"] == []
    assert (await second.load_session(owner, key)).messages == []
    event_types = [event["event_type"] for event in await second.events(invocation.invocation_id)]
    assert "result" in event_types
    assert "final" not in event_types
    result_event = next(event for event in await second.events(invocation.invocation_id) if event["event_type"] == "result")
    assert result_event["payload"]["errorMessage"] == "upstream 502 api_key=[REDACTED]"

    next_invocation, _ = await _accept(second, owner, session, key)
    assert next_invocation.session_seq == invocation.session_seq + 1

    await second.claim_invocation(next_invocation.invocation_id, "worker-b")
    exhausted = await second.complete_invocation(
        next_invocation.invocation_id,
        ExecutionResult.failure(
            ExecutionOutcome.ITERATION_LIMIT,
            messages=partial,
            error_code="ITERATION_LIMIT",
        ),
        execution_owner="worker-b",
    )
    assert exhausted.status is InvocationStatus.FAILED


@pytest.mark.asyncio
async def test_ordered_new_archives_old_history_and_lease_recovery(repositories):
    first, second = repositories
    owner, session, key = _ids()
    original, _ = await _accept(first, owner, session, key)
    await first.claim_invocation(original.invocation_id, "worker-c")
    messages = [{"role": "user", "content": "old"}]
    await first.complete_invocation(
        original.invocation_id,
        ExecutionResult(ExecutionOutcome.FINAL, "ok", messages),
        [MemoryRecord(kind="raw_message", payload={"turn_id": "turn-old", "messages": messages})],
        execution_owner="worker-c",
    )
    reset, _ = await _accept(second, owner, session, key, payload={"message": "/new"})
    await second.claim_invocation(reset.invocation_id, "worker-c")
    await second.archive_and_finish_new(reset.invocation_id, execution_owner="worker-c")
    assert (await first.load_session(owner, key)).messages == []
    assert await first.pool.fetchval(
        "SELECT count(*) FROM session_archives WHERE invocation_id=$1", reset.invocation_id,
    ) == 1
    assert await first.pool.fetchval(
        "SELECT next_message_seq FROM sessions WHERE owner_id=$1 AND session_id=$2",
        owner, session,
    ) == 2
    next_turn, _ = await _accept(first, owner, session, key)
    assert next_turn.session_seq == reset.session_seq + 1
    await first.claim_invocation(next_turn.invocation_id, "worker-c")
    new_messages = [{"role": "user", "content": "after reset"}]
    await first.complete_invocation(
        next_turn.invocation_id,
        ExecutionResult(ExecutionOutcome.FINAL, "next", new_messages),
        [MemoryRecord(kind="raw_message", payload={"turn_id": "turn-new", "messages": new_messages})],
        execution_owner="worker-c",
    )
    assert await first.pool.fetchval(
        "SELECT message_seq FROM session_messages WHERE invocation_id=$1", next_turn.invocation_id,
    ) == 2

    interrupted, _ = await _accept(first, owner, session, key)
    await first.claim_invocation(interrupted.invocation_id, "worker-crashed", lease_seconds=0.01)
    await asyncio.sleep(0.03)
    assert await second.recover_expired_invocations() == 1
    recovered = await second.get_invocation(interrupted.invocation_id)
    assert recovered is not None and recovered.status is InvocationStatus.QUEUED
    await second.claim_invocation(interrupted.invocation_id, "worker-retry")
    with pytest.raises(PermissionError):
        await first.complete_invocation(
            interrupted.invocation_id,
            ExecutionResult(ExecutionOutcome.FINAL, "stale", []),
            execution_owner="worker-crashed",
        )
    assert await second.renew_lease(interrupted.invocation_id, "worker-retry", lease_seconds=30)
    await second.complete_invocation(
        interrupted.invocation_id,
        ExecutionResult(ExecutionOutcome.FINAL, "recovered", []),
        execution_owner="worker-retry",
    )

    timed_out, _ = await _accept(second, owner, session, key)
    timeout_result = await first.finish_interrupted(
        timed_out.invocation_id,
        ExecutionOutcome.TIMEOUT,
        error_message="invocation expired while waiting in queue",
    )
    assert timeout_result.status is InvocationStatus.TIMEOUT
    assert timeout_result.error_code == "QUEUE_TIMEOUT"
    cancelled, _ = await _accept(second, owner, session, key)
    cancellation_result = await first.finish_interrupted(
        cancelled.invocation_id,
        ExecutionOutcome.CANCELLED,
        error_message="invocation cancelled",
    )
    assert cancellation_result.status is InvocationStatus.FAILED
    assert cancellation_result.result["outcome"] == "CANCELLED"


class _StorageProvider(LLMProvider):
    def __init__(self):
        super().__init__(api_key="test")
        self.prompts: list[list[dict]] = []
        self.fail_next = False

    async def chat(self, messages, tools=None, model=None, max_tokens=4096, temperature=0.7, reasoning_effort=None):
        del tools, model, max_tokens, temperature, reasoning_effort
        self.prompts.append([dict(message) for message in messages])
        if self.fail_next:
            self.fail_next = False
            return LLMResponse(content="provider secret failure", finish_reason="error")
        user = next(message for message in reversed(messages) if message.get("role") == "user")
        return LLMResponse(content=f"answer:{user['content']}")

    def get_default_model(self) -> str:
        return "fake-storage"


class _ToolFailureProvider(LLMProvider):
    def __init__(self):
        super().__init__(api_key="test")
        self.calls = 0

    async def chat(self, messages, tools=None, model=None, max_tokens=4096, temperature=0.7, reasoning_effort=None):
        del messages, tools, model, max_tokens, temperature, reasoning_effort
        self.calls += 1
        if self.calls == 1:
            return LLMResponse(
                content=None,
                tool_calls=[ToolCallRequest(id="call-failure", name="failing_tool", arguments={})],
            )
        return LLMResponse(content="the model tried to recover")

    def get_default_model(self) -> str:
        return "fake-tool-failure"


class _ErrorTool(Tool):
    name = "failing_tool"
    description = "Returns a standard ToolRegistry error result."
    parameters = {"type": "object", "properties": {}, "required": []}

    async def execute(self, **kwargs):
        del kwargs
        return "Error: test tool failed"


@pytest.mark.asyncio
async def test_agent_loop_uses_postgres_history_after_restart_and_skips_failed_turns(repositories, tmp_path):
    first, second = repositories
    owner, session, key = _ids()
    provider = _StorageProvider()

    async def run_turn(repository, text, *, fail=False):
        invocation, _ = await _accept(repository, owner, session, key, payload={"message": text})
        await repository.claim_invocation(invocation.invocation_id, "test-worker")
        provider.fail_next = fail
        engine = ConversationMemoryEngine.for_postgres(repository, owner_id=owner)
        loop = AgentLoop(
            bus=MessageBus(), provider=provider, workspace=tmp_path,
            model="fake-storage", max_iterations=2, memory_engine=engine,
            enable_consolidation=False, enable_subagents=False, enable_cron=False,
        )
        try:
            response = await loop.process_direct(
                text,
                session_key=key,
                chat_id=key.split(":", 1)[1],
                metadata={
                    "ownerId": owner,
                    "sessionId": session,
                    "invocationId": invocation.invocation_id,
                    "requestId": invocation.request_id,
                    "traceId": invocation.trace_id,
                    "executionOwner": "test-worker",
                },
            )
        finally:
            await loop.shutdown()
        return invocation, response

    first_invocation, first_response = await run_turn(first, "first")
    assert first_response == "answer:first"
    assert (await first.get_invocation(first_invocation.invocation_id)).status is InvocationStatus.SUCCEEDED

    failed_invocation, _ = await run_turn(second, "do not remember", fail=True)
    assert (await first.get_invocation(failed_invocation.invocation_id)).status is InvocationStatus.FAILED
    assert (await first.load_session(owner, key)).messages[-1]["content"] == "answer:first"

    _, third_response = await run_turn(second, "third")
    assert third_response == "answer:third"
    prompt = provider.prompts[-1]
    history_users = [
        message["content"] for message in prompt
        if message.get("role") == "user"
        and not str(message.get("content", "")).startswith("[Runtime Context")
    ]
    assert history_users == ["first", "third"]
    assert all(message.get("content") != "provider secret failure" for message in prompt)


@pytest.mark.asyncio
async def test_tool_error_text_is_a_failed_invocation_not_a_successful_final(repositories, tmp_path):
    first, second = repositories
    owner, session, key = _ids()
    invocation, _ = await _accept(first, owner, session, key)
    await first.claim_invocation(invocation.invocation_id, "tool-worker")
    engine = ConversationMemoryEngine.for_postgres(first, owner_id=owner)
    loop = AgentLoop(
        bus=MessageBus(), provider=_ToolFailureProvider(), workspace=tmp_path,
        model="fake-tool-failure", max_iterations=2, memory_engine=engine,
        enable_consolidation=False, enable_subagents=False, enable_cron=False,
    )
    loop.tools.register(_ErrorTool())
    try:
        await loop.process_direct(
            "exercise the tool",
            session_key=key,
            chat_id=key.split(":", 1)[1],
            metadata={
                "ownerId": owner,
                "sessionId": session,
                "invocationId": invocation.invocation_id,
                "requestId": invocation.request_id,
                "traceId": invocation.trace_id,
                "executionOwner": "tool-worker",
            },
        )
    finally:
        await loop.shutdown()
    failed = await second.get_invocation(invocation.invocation_id)
    assert failed is not None and failed.status is InvocationStatus.FAILED
    assert failed.error_code == "TOOL_ERROR"
    assert (await second.load_session(owner, key)).messages == []
