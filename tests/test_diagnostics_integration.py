from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import uuid
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer
from membot.agent.conversation_memory.models import MemoryRecord
from membot.agent.diagnostics import DiagnosticPolicy, json_bytes
from membot.agent.execution import get_execution_context
from membot.agent.execution_result import ExecutionOutcome, ExecutionResult
from membot.agent.persistence.redis_transport import OutboxRelay, RedisStreamTransport
from membot.agent.persistence.repository import InvocationStatus, PostgresRepository
from membot.agent.tools.base import Tool
from membot.providers.base import LLMProvider, LLMResponse, ToolCallRequest
from membot.service.api import create_api_app
from membot.service.config import ServiceConfig
from membot.service.diagnostics import (
    ReplayUnavailableError,
    candidate_case,
    read_recording,
    reproduce_recording,
    timing_summary,
)

asyncpg = pytest.importorskip("asyncpg")
redis_async = pytest.importorskip("redis.asyncio")
RedisError = pytest.importorskip("redis.exceptions").RedisError

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://membot:membot@127.0.0.1:55432/membot")
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:56379/0")
SECRET = "sk-m4Private12345678901234567890"
PRIVATE_REASONING = "PRIVATE_REASONING_MUST_NOT_BE_RECORDED"
FULL_RESULT = "visible-" * 200 + "FULL_DIAGNOSTIC_TAIL"


class DiagnosticProvider(LLMProvider):
    def __init__(self, sentinel: Path):
        super().__init__(api_key=SECRET)
        self.sentinel = sentinel
        self.calls = defaultdict(int)
        self.contexts = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.gate = False

    def get_default_model(self):
        return "diagnostic-fake"

    async def chat(self, messages, **kwargs):
        context = get_execution_context()
        assert context and context.invocation_id and context.trace_id and context.request_id
        self.contexts.append(context)
        self.calls[context.invocation_id] += 1
        iteration = self.calls[context.invocation_id]
        mode = next(message["content"] for message in reversed(messages) if message["role"] == "user").split()[0]
        self.entered.set()
        if self.gate:
            await self.release.wait()
        if mode in {"llm_timeout", "execution_timeout"}:
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled.set()
        if mode == "llm_error":
            raise RuntimeError(f"provider failed api_key={SECRET}")
        if mode == "llm_error_response":
            return LLMResponse(content=f"incorrect answer password={SECRET}", finish_reason="error")
        if mode == "llm_error_with_tool":
            return LLMResponse(content="provider error", finish_reason="error", tool_calls=[
                ToolCallRequest("invalid-call", "write_file", {"path": str(self.sentinel), "content": "must not write"}),
            ])
        if mode == "huge_final":
            return LLMResponse(content="H" * 100_000)
        if mode in {"tool_error", "tool_timeout", "iteration_limit"}:
            return LLMResponse(content=None, tool_calls=[ToolCallRequest(
                id=f"call-{iteration}", name="diagnostic_tool", arguments={"action": mode},
            )])
        if mode == "multi":
            if iteration == 1:
                return LLMResponse(
                    content=f"<think>{PRIVATE_REASONING}</think>Writing visible output.",
                    reasoning_content=PRIVATE_REASONING,
                    thinking_blocks=[{"type": "thinking", "thinking": PRIVATE_REASONING}],
                    tool_calls=[ToolCallRequest("write-call", "write_file", {
                        "path": str(self.sentinel), "content": "one real write",
                        "api_key": SECRET,
                    })],
                )
            if iteration == 2:
                return LLMResponse(content="Checking result.", tool_calls=[ToolCallRequest(
                    "probe-call", "diagnostic_tool", {"action": "probe", "authorization": "Bearer " + SECRET},
                )])
        return LLMResponse(content="visible final", usage={"prompt_tokens": 3, "completion_tokens": 2})


class DiagnosticTool(Tool):
    description = "Controlled diagnostic fixture running inside the actual Worker."
    parameters = {"type": "object", "properties": {}}

    def __init__(self, name, sentinel, cancelled):
        self._name = name
        self.sentinel = sentinel
        self.cancelled = cancelled

    @property
    def name(self):
        return self._name

    async def execute(self, **params):
        context = get_execution_context()
        assert context and context.invocation_id and context.attempt == 1
        if self.name == "write_file":
            self.sentinel.write_text("one real write")
            return "write completed"
        if params.get("action") == "tool_error":
            raise RuntimeError(f"tool refused password={SECRET}")
        if params.get("action") == "tool_timeout":
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled.set()
        return FULL_RESULT


@pytest.fixture
async def diagnostic_stack(tmp_path, request):
    try:
        pool_a = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=8)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"PostgreSQL integration service unavailable: {exc}")
    pool_b = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    repo_a, repo_b = PostgresRepository(pool_a), PostgresRepository(pool_b)
    await repo_a.migrate()
    client = redis_async.from_url(REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except (OSError, RedisError) as exc:
        await repo_a.close()
        await repo_b.close()
        await client.aclose()
        pytest.skip(f"Redis integration service unavailable: {exc}")
    suffix = uuid.uuid4().hex
    values = dict(owner_id=f"m4-owner-{suffix}", workspace=str(tmp_path), model="diagnostic-fake",
                  worker_prefetch=4, worker_concurrency=2, max_iterations=8, max_unfinished=4096,
                  execution_timeout_seconds=5.0, llm_timeout_seconds=2.0, tool_timeout_seconds=2.0,
                  queue_timeout_seconds=5.0, outbox_poll_seconds=0.02, queue_poll_seconds=0.02)
    values.update(getattr(request, "param", {}))
    config = ServiceConfig(**values)
    transport = RedisStreamTransport(client, stream_key=f"membot:m4:{suffix}", group=f"m4:{suffix}",
                                     max_depth=32, prefetch=4)
    from membot.service.worker import AsyncWorker

    sentinel = tmp_path / "sentinel"
    provider = DiagnosticProvider(sentinel)
    worker = AsyncWorker(repo_a, transport, config, provider=provider)
    await worker.start()
    tool_cancelled = asyncio.Event()
    for name in ("write_file", "diagnostic_tool"):
        worker.agent.tools.register(DiagnosticTool(name, sentinel, tool_cancelled))
    api_a, api_b = TestClient(TestServer(create_api_app(repo_a, config))), TestClient(TestServer(create_api_app(repo_b, config)))
    stack = SimpleNamespace(a=repo_a, b=repo_b, redis=client, transport=transport, config=config,
                            worker=worker, provider=provider, sentinel=sentinel, tool_cancelled=tool_cancelled,
                            api_a=api_a, api_b=api_b, runner=None, directory=tmp_path)
    try:
        try:
            await api_a.start_server()
            await api_b.start_server()
        except PermissionError as exc:
            pytest.skip(f"API socket access unavailable: {exc}")
        yield stack
    finally:
        provider.release.set()
        await worker.stop(worker_lost=False)
        if stack.runner:
            stack.runner.cancel()
            await asyncio.gather(stack.runner, return_exceptions=True)
        await api_a.close()
        await api_b.close()
        await client.delete(transport.stream_key)
        await transport.close()
        await repo_a.close()
        await repo_b.close()


async def _submit(stack, mode, *, iterations=8, session_id=None):
    response = await stack.api_a.post("/v1/invocations", json={
        "sessionId": session_id or f"session-{uuid.uuid4().hex}", "maxIterations": iterations,
        "message": f"{mode} password={SECRET}",
    }, headers={"X-Request-ID": f"request-{uuid.uuid4().hex}", "X-Trace-ID": f"trace-{uuid.uuid4().hex}"})
    assert response.status == 202, await response.text()
    return await response.json()


async def _finished(stack, invocation_id):
    async def poll():
        while True:
            current = await stack.b.get_invocation(invocation_id)
            if current and current.status.value in {"SUCCEEDED", "FAILED", "TIMEOUT"}:
                events = await stack.b.events(invocation_id)
                if any(event["event_type"] == "QUEUE" and event["step"] == "ack" for event in events):
                    return await read_recording(stack.b, invocation_id)
            await asyncio.sleep(0.01)
    return await asyncio.wait_for(poll(), 10)


@pytest.mark.asyncio
async def test_multiround_api_outbox_queue_trace_full_results_and_safe_replay(diagnostic_stack, caplog):
    stack = diagnostic_stack
    caplog.set_level(logging.INFO, logger="membot.events")
    stack.provider.gate = True
    accepted = await _submit(stack, "multi")
    assert accepted["status"] == "QUEUED" and not stack.provider.calls
    stack.runner = asyncio.create_task(stack.worker.run())
    await asyncio.wait_for(stack.provider.entered.wait(), 5)
    query = await stack.api_b.get(f"/v1/invocations/{accepted['invocationId']}",
                                 headers={"X-Request-ID": "query-request", "X-Trace-ID": "query-trace"})
    assert query.status == 200 and (await query.json())["status"] == "RUNNING"
    assert query.headers["X-Request-ID"] == "query-request"
    assert (await query.json())["requestId"] == accepted["requestId"]
    stack.provider.release.set()
    recording = await _finished(stack, accepted["invocationId"])
    events = recording["events"]
    assert recording["invocation"]["status"] == "SUCCEEDED"
    assert [event["sequence"] for event in events] == list(range(1, len(events) + 1))
    for event in events:
        for key in ("invocationId", "traceId", "requestId", "sessionId"):
            assert event[key] == accepted[key]
        assert event["time"] and event["expires_at"]
        if event["event_type"] in {"LLM", "TOOL", "FINAL"}:
            assert event["worker"] == stack.worker.execution_owner and event["attempt"] == 1
    assert [(event["event_type"], event["step"]) for event in events if event["event_type"] in {"LLM", "TOOL"}] == [
        ("LLM", "start"), ("LLM", "end"), ("TOOL", "start"), ("TOOL", "end"),
        ("LLM", "start"), ("LLM", "end"), ("TOOL", "start"), ("TOOL", "end"),
        ("LLM", "start"), ("LLM", "end"),
    ]
    assert {event["event_type"] for event in events} >= {"accepted", "OUTBOX", "QUEUE", "running", "HISTORY", "result", "FINAL"}
    probe = next(event for event in events if event["event_type"] == "TOOL" and event["step"] == "end" and event["tool_call_id"] == "probe-call")
    assert probe["payload"]["result"] == FULL_RESULT and not probe["payload"]["truncated"]
    history = await stack.b.load_session(stack.config.owner_id, accepted["sessionId"])
    assert "FULL_DIAGNOSTIC_TAIL" not in next(message["content"] for message in history.messages if message.get("tool_call_id") == "probe-call")
    stored = await stack.a.pool.fetchval("SELECT jsonb_agg(payload)::text FROM invocation_events WHERE invocation_id=$1", accepted["invocationId"])
    assert SECRET not in stored and PRIVATE_REASONING not in stored
    assert timing_summary(recording)["incomplete_spans"] == []
    assert all(event["duration_ms"] >= 0 for event in events if event["duration_ms"] is not None)

    cursor, wire_events = 0, []
    while True:
        response = await stack.api_b.get(f"/v1/invocations/{accepted['invocationId']}/events?after={cursor}&limit=3",
                                        headers={"X-Request-ID": "timeline-query"})
        assert response.status == 200
        body = await response.json()
        assert body["traceId"] == accepted["traceId"] and body["requestId"] == accepted["requestId"]
        assert response.headers["X-Request-ID"] == "timeline-query"
        wire_events.extend(body["events"])
        if body["nextAfter"] is None:
            break
        cursor = body["nextAfter"]
    assert [event["sequence"] for event in wire_events] == [event["sequence"] for event in events]
    persisted_logs = [record.diagnostic_event for record in caplog.records if hasattr(record, "diagnostic_event")]
    assert any(record["event_type"] == "FINAL" and record["invocationId"] == accepted["invocationId"] for record in persisted_logs)
    replay = await reproduce_recording(recording)
    assert replay["matches_recorded_status"] and replay["calls"] == {"provider": 3, "tool": 2}
    assert stack.sentinel.read_text() == "one real write"


@pytest.mark.parametrize("diagnostic_stack,mode,status,error,node,iterations", [
    ({}, "llm_error", "FAILED", "PROVIDER_ERROR", "LLM", 8),
    ({}, "llm_error_response", "FAILED", "PROVIDER_ERROR", "LLM", 8),
    ({}, "llm_error_with_tool", "FAILED", "PROVIDER_ERROR", "LLM", 8),
    ({}, "tool_error", "FAILED", "TOOL_ERROR", "TOOL", 8),
    ({"llm_timeout_seconds": 0.04}, "llm_timeout", "TIMEOUT", "LLM_TIMEOUT", "LLM", 8),
    ({"tool_timeout_seconds": 0.04}, "tool_timeout", "TIMEOUT", "TOOL_TIMEOUT", "TOOL", 8),
    ({"execution_timeout_seconds": 0.08}, "execution_timeout", "TIMEOUT", "EXECUTION_TIMEOUT", "LLM", 8),
    ({}, "iteration_limit", "FAILED", "ITERATION_LIMIT", "FAILURE", 2),
], indirect=["diagnostic_stack"])
@pytest.mark.asyncio
async def test_failures_timeouts_are_queryable_with_nodes_and_reviewed_candidates(diagnostic_stack, mode, status, error, node, iterations):
    stack = diagnostic_stack
    accepted = await _submit(stack, mode, iterations=iterations)
    stack.runner = asyncio.create_task(stack.worker.run())
    recording = await _finished(stack, accepted["invocationId"])
    assert recording["invocation"]["status"] == status and recording["invocation"]["errorCode"] == error
    assert any(event["event_type"] == node and event["step"] == "error" for event in recording["events"])
    assert not any(event["event_type"] == "FINAL" for event in recording["events"])
    assert (await stack.b.load_session(stack.config.owner_id, accepted["sessionId"])).messages == []
    response = await stack.api_b.get(f"/v1/invocations/{accepted['invocationId']}/events")
    assert response.status == 200 and len((await response.json())["events"]) == len(recording["events"])
    assert SECRET not in json.dumps(recording)
    case = candidate_case(recording)
    assert case["expected"]["status"] is None and case["expected"]["answer"] is None
    assert not case["expected"]["confirmed"] and case["config_version"]
    assert case["input"]["message"].startswith(mode) and case["history_snapshot"] == []
    replay = await reproduce_recording(recording)
    assert replay["matches_recorded_status"] and replay["error_code"] == error
    if mode == "llm_timeout" or mode == "execution_timeout":
        assert stack.provider.cancelled.is_set()
    if mode == "tool_timeout":
        assert stack.tool_cancelled.is_set()
    if mode == "llm_error_with_tool":
        assert not stack.sentinel.exists()


@pytest.mark.asyncio
async def test_context_snapshot_survives_new_and_cli_export_does_not_mutate(diagnostic_stack):
    stack = diagnostic_stack
    seed = await _submit(stack, "seed")
    stack.runner = asyncio.create_task(stack.worker.run())
    await _finished(stack, seed["invocationId"])
    failed = await _submit(stack, "llm_error_response", session_id=seed["sessionId"])
    recording = await _finished(stack, failed["invocationId"])
    reset = await stack.api_a.post("/v1/invocations", json={"sessionId": seed["sessionId"], "message": "/new"})
    assert reset.status == 202
    await _finished(stack, (await reset.json())["invocationId"])
    assert (await stack.b.load_session(stack.config.owner_id, seed["sessionId"])).messages == []
    recording = await read_recording(stack.b, failed["invocationId"])
    case = candidate_case(recording)
    assert any(message.get("content", "").startswith("seed") for message in case["history_snapshot"])
    recorded_file, case_file = stack.directory / "recording.json", stack.directory / "candidate.json"
    before = len(await stack.b.events(failed["invocationId"]))
    result = subprocess.run([sys.executable, "scripts/trace_probe.py", failed["invocationId"], "--database-url", DATABASE_URL,
                             "--export-recording", str(recorded_file), "--export-candidate", str(case_file),
                             "--assert-correlation", "--assert-redaction"], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert "LLM.error" in result.stdout and "PROVIDER_ERROR" in result.stdout
    assert len(await stack.b.events(failed["invocationId"])) == before
    exported = json.loads(case_file.read_text())
    assert exported["expected"]["answer"] is None and not exported["expected"]["confirmed"]
    assert SECRET not in recorded_file.read_text() and case_file.stat().st_mode & 0o077 == 0


@pytest.mark.asyncio
async def test_concurrent_event_sequences_rollback_logging_and_retention(diagnostic_stack, caplog):
    stack = diagnostic_stack
    caplog.set_level(logging.INFO, logger="membot.events")
    accepted = await _submit(stack, "seed")
    identifier = accepted["invocationId"]
    await stack.a.claim_invocation(identifier, stack.worker.execution_owner)
    await asyncio.gather(*[(stack.a if index % 2 else stack.b).append_event(identifier, "TEST", {
        "step": "record", "index": index, "api_key": SECRET,
    }) for index in range(20)])
    events = await stack.b.events(identifier)
    assert [event["sequence"] for event in events] == list(range(1, len(events) + 1))

    class FailFinalPolicy(DiagnosticPolicy):
        def capture(self, value):
            if value.get("content") == "rollback-final":
                raise ValueError("controlled final recording failure")
            return super().capture(value)

    stack.a.diagnostics = FailFinalPolicy()
    result = ExecutionResult(ExecutionOutcome.FINAL, "rollback-final", [])
    records = [MemoryRecord(kind="raw_message", payload={"messages": [{"role": "user", "content": "seed"}]})]
    before_log_count = len(caplog.records)
    with pytest.raises(ValueError):
        await stack.a.complete_invocation(identifier, result, records, execution_owner=stack.worker.execution_owner)
    assert len(await stack.b.events(identifier)) == len(events)
    assert len(caplog.records) == before_log_count
    assert (await stack.b.get_invocation(identifier)).status is InvocationStatus.RUNNING
    assert (await stack.b.load_session(stack.config.owner_id, accepted["sessionId"])).messages == []
    stack.a.diagnostics = DiagnosticPolicy()
    await stack.a.complete_invocation(identifier, ExecutionResult(ExecutionOutcome.FINAL, "ok", []), records,
                                      execution_owner=stack.worker.execution_owner)
    all_events = await stack.b.events(identifier)
    last = all_events[-1]["sequence"]
    await stack.a.pool.execute("UPDATE invocation_events SET expires_at=now()-interval '1 second' WHERE invocation_id=$1", identifier)
    assert await stack.b.events(identifier) == []
    assert await stack.a.purge_expired_events(limit=1000) >= len(all_events)
    await stack.a.append_event(identifier, "TEST", {"step": "after_retention"})
    assert (await stack.b.events(identifier))[0]["sequence"] == last + 1
    assert (await stack.b.get_invocation(identifier)).status is InvocationStatus.SUCCEEDED
    assert (await stack.b.load_session(stack.config.owner_id, accepted["sessionId"])).messages


@pytest.mark.asyncio
async def test_outbox_publication_failure_and_bounded_payload_have_diagnostics(diagnostic_stack):
    stack = diagnostic_stack
    accepted = await _submit(stack, "seed")

    class FailingTransport:
        async def publish(self, envelope):
            assert envelope["traceId"] == accepted["traceId"] and envelope["requestId"] == accepted["requestId"]
            raise ConnectionError(f"queue unavailable password={SECRET}")

    relay = OutboxRelay(stack.a, FailingTransport(), owner_id=stack.config.owner_id,
                        worker_id=stack.worker.execution_owner)
    assert await relay.publish_once() == 0
    events = await stack.b.events(accepted["invocationId"])
    assert any(event["event_type"] == "OUTBOX" and event["step"] == "error" and
               event["error_code"] == "OUTBOX_PUBLISH_ERROR" for event in events)
    pending = await stack.a.pool.fetchrow("SELECT * FROM outbox WHERE invocation_id=$1", accepted["invocationId"])
    assert pending["published_at"] is None and SECRET not in pending["last_error"]
    stack.a.diagnostics = DiagnosticPolicy(max_payload_bytes=512)
    await stack.a.append_event(accepted["invocationId"], "TOOL", {"step": "end", "result": FULL_RESULT * 10, "api_key": SECRET})
    captured = (await stack.b.events(accepted["invocationId"]))[-1]["payload"]
    assert captured["truncated"] and captured["original_size"] > 512 and len(json_bytes(captured)) <= 512
    with pytest.raises(ReplayUnavailableError):
        await reproduce_recording(await read_recording(stack.b, accepted["invocationId"]))


@pytest.mark.parametrize("diagnostic_stack", [{"worker_concurrency": 1}], indirect=True)
@pytest.mark.asyncio
async def test_queue_expiry_records_timeout_without_starting_the_provider(diagnostic_stack):
    stack = diagnostic_stack
    stack.provider.gate = True
    first = await _submit(stack, "multi")
    stack.runner = asyncio.create_task(stack.worker.run())
    await asyncio.wait_for(stack.provider.entered.wait(), 5)
    queued = await _submit(stack, "seed")
    identifier = queued["invocationId"]
    # Move only this waiting invocation's acceptance clock, while a barrier
    # holds the one execution slot. No timing threshold asserts concurrency.
    await stack.a.pool.execute("UPDATE invocations SET submitted_at=now()-interval '10 seconds' WHERE invocation_id=$1", identifier)

    async def expired():
        while (await stack.b.get_invocation(identifier)).status is not InvocationStatus.TIMEOUT:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(expired(), 5)
    assert (await stack.b.get_invocation(identifier)).started_at is None
    assert identifier not in stack.provider.calls
    stack.provider.release.set()
    await _finished(stack, first["invocationId"])
    recording = await _finished(stack, identifier)
    assert any(event["event_type"] == "TIMEOUT" and event["error_code"] == "QUEUE_TIMEOUT" for event in recording["events"])
    assert not any(event["event_type"] in {"running", "LLM", "TOOL", "FINAL"} for event in recording["events"])
    assert candidate_case(recording)["expected"]["confirmed"] is False


@pytest.mark.asyncio
async def test_worker_ownership_loss_keeps_original_trace_and_pending_recovery_only_acks(diagnostic_stack):
    from membot.service.worker import AsyncWorker

    stack = diagnostic_stack
    accepted = await _submit(stack, "execution_timeout")
    stack.runner = asyncio.create_task(stack.worker.run())
    await asyncio.wait_for(stack.provider.entered.wait(), 5)
    old_owner = stack.worker.execution_owner
    backend = await stack.worker._lock_connection.fetchval("SELECT pg_backend_pid()")
    assert await stack.b.pool.fetchval("SELECT pg_terminate_backend($1)", backend)
    await asyncio.wait_for(stack.worker._stop_event.wait(), 5)
    assert stack.provider.cancelled.is_set()
    interrupted = await stack.b.get_invocation(accepted["invocationId"])
    assert interrupted.status is InvocationStatus.FAILED and interrupted.error_code == "WORKER_LOST"
    before = await stack.b.events(accepted["invocationId"])
    assert any(event["event_type"] == "LLM" and event["step"] == "start" for event in before)
    assert any(event["error_code"] == "WORKER_LOST" for event in before)
    provider = DiagnosticProvider(stack.sentinel)
    replacement = AsyncWorker(stack.a, stack.transport, stack.config, provider=provider)
    assert replacement.execution_owner != old_owner
    replacement_task = asyncio.create_task(replacement.run())
    try:
        recording = await _finished(stack, accepted["invocationId"])
        assert provider.calls == {}  # uncertain external writes are never re-run
        assert any(event["event_type"] == "QUEUE" and event["step"] == "consume" and
                   event["payload"]["pending_recovery"] for event in recording["events"])
        assert all(event["traceId"] == accepted["traceId"] for event in recording["events"])
        assert not any(event["event_type"] == "FINAL" for event in recording["events"])
        assert recording["invocation"]["result"]["error_code"] == "WORKER_LOST"
        with pytest.raises(ReplayUnavailableError):
            await reproduce_recording(recording)
    finally:
        await replacement.stop(worker_lost=False)
        replacement_task.cancel()
        await asyncio.gather(replacement_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_legacy_migration_backfills_sequences_and_masks_read_payloads(diagnostic_stack, tmp_path):
    stack = diagnostic_stack
    schema = "m4_upgrade_" + uuid.uuid4().hex
    await stack.a.pool.execute(f'CREATE SCHEMA "{schema}"')
    upgrade_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2,
                                            server_settings={"search_path": schema})
    repository = PostgresRepository(upgrade_pool)
    old_migrations = tmp_path / "old-migrations"
    old_migrations.mkdir()
    migrations = Path(__file__).parents[1] / "agent/persistence/migrations"
    (old_migrations / "0001_initial.sql").write_text((migrations / "0001_initial.sql").read_text())
    try:
        await repository.migrate(old_migrations)
        await upgrade_pool.execute("INSERT INTO sessions(owner_id,session_id,session_key) VALUES('owner','session','service:legacy')")
        await upgrade_pool.execute(
            "INSERT INTO invocations(invocation_id,owner_id,session_id,session_key,session_seq,request_id,trace_id,payload_hash,payload) "
            "VALUES('legacy','owner','session','service:legacy',1,'original-request','original-trace',$1,'{}')", "0" * 64,
        )
        for event_type in ("accepted", "result"):
            await upgrade_pool.execute(
                "INSERT INTO invocation_events(invocation_id,owner_id,session_id,request_id,trace_id,event_type,payload) "
                "VALUES('legacy','owner','session','original-request','original-trace',$1,$2::jsonb)",
                event_type, json.dumps({"errorMessage": f"password={SECRET}"}),
            )
        await repository.migrate()
        await repository.migrate()  # applying the upgrade again is a no-op
        events = await repository.events("legacy")
        assert [event["sequence"] for event in events] == [1, 2]
        assert SECRET not in json.dumps(events)
        await repository.append_event("legacy", "TEST", {"step": "after_upgrade"})
        assert (await repository.events("legacy"))[-1]["sequence"] == 3
        assert await upgrade_pool.fetchval("SELECT count(*) FROM schema_migrations") == 2
    finally:
        await repository.close()
        # The generated schema is entirely owned by this test and has no
        # service/session data from other tests or the development database.
        await stack.a.pool.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.asyncio
async def test_two_api_processes_and_worker_propagate_ids_and_emit_json(diagnostic_stack, unused_tcp_port_factory):
    import httpx

    stack = diagnostic_stack
    await stack.worker.stop(worker_lost=False)  # subprocess owns the sole execution lock
    ports = [unused_tcp_port_factory(), unused_tcp_port_factory()]
    environment = {**os.environ, "DATABASE_URL": DATABASE_URL, "REDIS_URL": REDIS_URL,
                   "LITELLM_LOCAL_MODEL_COST_MAP": "True", "MEMBOT_OWNER_ID": stack.config.owner_id,
                   "MEMBOT_WORKSPACE": str(stack.directory), "MEMBOT_MODEL": "diagnostic-fake",
                   "MEMBOT_QUEUE_TIMEOUT_SECONDS": "30", "MEMBOT_MAX_UNFINISHED": "4096"}
    processes, readers = [], []
    try:
        for port in ports:
            process = await asyncio.create_subprocess_exec(
                sys.executable, "scripts/api_server.py", "--port", str(port),
                env=environment, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            processes.append(process)
            readers.append(asyncio.create_task(process.communicate()))
        async with httpx.AsyncClient(timeout=2.0, trust_env=False) as client:
            async def ready(port):
                while True:
                    try:
                        response = await client.get(f"http://127.0.0.1:{port}/ready")
                        if response.status_code == 200:
                            return
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.03)
            await asyncio.wait_for(asyncio.gather(*(ready(port) for port in ports)), 10)
            response = await client.post(f"http://127.0.0.1:{ports[0]}/v1/invocations", json={
                "sessionId": "cross-process-session", "message": f"multi password={SECRET}", "maxIterations": 8,
            }, headers={"X-Request-ID": "cross-process-original-request", "X-Trace-ID": "cross-process-original-trace"})
            assert response.status_code == 202
            accepted = response.json()
            query_url = f"http://127.0.0.1:{ports[1]}/v1/invocations/{accepted['invocationId']}"
            query = await client.get(query_url)
            assert query.status_code == 200 and query.json()["status"] == "QUEUED"
            worker = await asyncio.create_subprocess_exec(
                sys.executable, "tests/support/m4_worker.py", env={
                    **environment, "M4_TEST_STREAM": stack.transport.stream_key,
                    "M4_TEST_GROUP": stack.transport.group, "M4_TEST_SENTINEL": str(stack.sentinel),
                    "M4_TEST_INVOCATION": accepted["invocationId"],
                }, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            processes.append(worker)
            worker_reader = asyncio.create_task(worker.communicate())
            readers.append(worker_reader)
            stdout, stderr = await asyncio.wait_for(asyncio.shield(worker_reader), 20)
            assert worker.returncode == 0, stderr.decode()
            lines = [json.loads(line) for line in stdout.decode().splitlines()]
            task_lines = [line for line in lines if line.get("event_id") and line["invocationId"] == accepted["invocationId"]]
            assert task_lines and {line["event_type"] for line in task_lines} >= {"OUTBOX", "QUEUE", "LLM", "TOOL", "FINAL"}
            assert all(line["invocationId"] == accepted["invocationId"] and
                       line["requestId"] == "cross-process-original-request" and
                       line["traceId"] == "cross-process-original-trace" for line in task_lines)
            for identifier in {line["invocationId"] for line in lines if line.get("event_id")}:
                stored = await stack.b.get_invocation(identifier)
                assert all(line["requestId"] == stored.request_id and line["traceId"] == stored.trace_id
                           for line in lines if line.get("event_id") and line["invocationId"] == identifier)
            assert SECRET.encode() not in stdout and PRIVATE_REASONING.encode() not in stdout
            query = await client.get(query_url, headers={"X-Request-ID": "cross-process-query-request", "X-Trace-ID": "query-trace"})
            assert query.json()["status"] == "SUCCEEDED" and query.headers["X-Request-ID"] == "cross-process-query-request"
            assert query.json()["traceId"] == accepted["traceId"] and query.json()["requestId"] == accepted["requestId"]
            assert stack.sentinel.read_text() == "one real write"
    finally:
        for process in processes:
            if process.returncode is None:
                process.terminate()
        for process, reader in zip(processes, readers, strict=True):
            try:
                await asyncio.wait_for(asyncio.shield(reader), 5)
            except asyncio.TimeoutError:
                process.kill()
                await reader


@pytest.mark.asyncio
async def test_recording_failure_before_tool_start_prevents_the_external_write(diagnostic_stack, monkeypatch):
    stack = diagnostic_stack
    original = stack.a.append_event

    async def recording_fails(invocation_id, category, payload, **kwargs):
        if category == "TOOL" and payload["step"] == "start":
            raise ConnectionError("controlled diagnostic write failure")
        await original(invocation_id, category, payload, **kwargs)

    monkeypatch.setattr(stack.a, "append_event", recording_fails)
    accepted = await _submit(stack, "multi")
    stack.runner = asyncio.create_task(stack.worker.run())
    recording = await _finished(stack, accepted["invocationId"])
    assert recording["invocation"]["status"] == "FAILED"
    assert recording["invocation"]["errorCode"] == "INTERNAL_ERROR"
    assert not stack.sentinel.exists()
    assert any(event["event_type"] == "FAILURE" and event["error_code"] == "INTERNAL_ERROR" for event in recording["events"])


@pytest.mark.asyncio
async def test_large_final_and_status_transcripts_cannot_bypass_diagnostic_limits(diagnostic_stack):
    stack = diagnostic_stack
    accepted = await _submit(stack, "huge_final")
    stack.runner = asyncio.create_task(stack.worker.run())
    recording = await _finished(stack, accepted["invocationId"])
    result = (await stack.b.get_invocation(accepted["invocationId"])).result
    assert result["messages"] == [] and result["final_payload"]["truncated"]
    assert result["final_payload"]["original_size"] > stack.a.diagnostics.max_payload_bytes
    assert len(result["final_content"].encode()) <= stack.a.diagnostics.max_payload_bytes
    final = next(event for event in recording["events"] if event["event_type"] == "FINAL")
    assert final["payload"]["truncated"] and len(json_bytes(final["payload"])) <= stack.a.diagnostics.max_payload_bytes
    with pytest.raises(ReplayUnavailableError):
        await reproduce_recording(recording)
