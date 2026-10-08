from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from membot.agent.diagnostics import DiagnosticPolicy, event_record, json_bytes
from membot.agent.execution import ExecutionContext, reset_execution_context, set_execution_context
from membot.agent.loop import AgentLoop
from membot.agent.redaction import redact_data
from membot.bus.queue import MessageBus
from membot.providers.base import LLMProvider, LLMResponse, ToolCallRequest
from membot.service.config import ServiceConfig
from membot.service.diagnostics import (
    ReplayUnavailableError,
    candidate_case,
    confirm_expected,
    render_timeline,
    reproduce_recording,
    timing_summary,
)
from membot.service.logging import JsonFormatter

SECRET = "sk-private12345678901234567890"


def test_nested_redaction_excludes_credentials_and_private_reasoning():
    data = {
        "arguments": {"apiKey": SECRET, "nested": {"Authorization": "Bearer " + SECRET}},
        "parameters_json": json.dumps({"api_key": SECRET, "safe": "visible"}),
        "content": f"<think>PRIVATE_REASONING</think>visible password='{SECRET}'",
        "error": "postgresql://user:pass@127.0.0.1/db",
        "reasoning_content": "PRIVATE_REASONING", "thinking_blocks": [{"thinking": "PRIVATE_REASONING"}],
        "blocks": [{"type": "thinking", "thinking": "PRIVATE_REASONING"}, {"type": "text", "text": "safe"}],
    }
    safe = redact_data(data)
    assert SECRET not in json.dumps(safe) and "PRIVATE_REASONING" not in json.dumps(safe)
    assert "user:pass@" not in safe["error"]
    assert safe["arguments"]["nested"]["Authorization"] == "[REDACTED]"
    assert "visible" in safe["content"] and safe["blocks"] == [{"type": "text", "text": "safe"}]
    assert redact_data(safe) == safe


@pytest.mark.parametrize("header", ["Authorization: Bearer opaqueJwt", "authorization=Basic dXNlcjpwYXNz", "Bearer tiny"])
def test_plain_auth_headers_are_masked_before_losing_the_scheme(header):
    masked = redact_data(header)
    assert header.split()[-1] not in masked
    assert "[REDACTED]" in masked and redact_data(masked) == masked


def test_payload_budget_marks_utf8_truncation_without_a_secret_overflow():
    policy = DiagnosticPolicy(max_payload_bytes=512, retention_days=3)
    source = {"result": "汉\"字\n" * 1000, "api_key": SECRET}
    captured = policy.capture(source)
    assert captured["truncated"] is True
    assert captured["original_size"] == len(json_bytes(source))
    assert captured["redacted_size"] == len(json_bytes(redact_data(source)))
    assert len(json_bytes(captured)) <= 512
    assert SECRET not in json.dumps(captured)
    assert policy.capture({"result": "full visible"})["truncated"] is False
    with pytest.raises(ValueError):
        DiagnosticPolicy(retention_days=0)


@pytest.mark.asyncio
async def test_json_logging_correlates_concurrent_tasks_and_redacts():
    both = asyncio.Event()
    entered = []
    formatter = JsonFormatter(component="test")

    async def log_for(number):
        token = set_execution_context(ExecutionContext(
            session_key=f"s:{number}", channel="service", chat_id=str(number),
            invocation_id=f"i-{number}", trace_id=f"t-{number}", request_id=f"r-{number}",
            session_id=f"s-{number}", execution_owner="worker-test", attempt=2,
        ))
        try:
            entered.append(number)
            if len(entered) == 2:
                both.set()
            await both.wait()
            message = logging.LogRecord("runtime", logging.INFO, __file__, 1,
                                        "Tool password=%s", (SECRET,), None)
            return json.loads(formatter.format(message))
        finally:
            reset_execution_context(token)

    outputs = await asyncio.gather(log_for(1), log_for(2))
    assert [output["invocationId"] for output in outputs] == ["i-1", "i-2"]
    assert [output["requestId"] for output in outputs] == ["r-1", "r-2"]
    assert outputs[0]["attempt"] == 2
    assert SECRET not in json.dumps(outputs)


def test_service_stdout_is_json_for_standard_logging_and_loguru():
    code = """
import logging
from loguru import logger
from membot.service.logging import configure_json_logging
configure_json_logging('worker')
logging.getLogger('membot.events').info('password=do-not-log')
logger.info('api_key=sk-doNotLog1234567890123456')
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    assert len(lines) == 2 and all(line["component"] == "worker" for line in lines)
    assert "do-not-log" not in result.stdout and "sk-doNotLog" not in result.stdout
    assert not result.stderr


def test_api_import_does_not_load_an_execution_agent():
    subprocess.run([sys.executable, "-c", "import sys; import membot.service.api; "
                    "assert 'membot.agent.loop' not in sys.modules"], check=True)


def test_service_config_defaults_and_diagnostic_version(monkeypatch):
    for key in os.environ:
        if key.startswith("MEMBOT_") or key in {"DATABASE_URL", "REDIS_URL"}:
            monkeypatch.delenv(key)
    config = ServiceConfig.from_env()
    assert isinstance(config.database_url, str) and config.worker_prefetch == 8
    first = config.diagnostic_snapshot()
    changed = ServiceConfig(model="other").diagnostic_snapshot()
    assert first["configVersion"] != changed["configVersion"]
    assert config.database_url not in json.dumps(first)


class _TwoRoundProvider(LLMProvider):
    def __init__(self, command: str):
        super().__init__()
        self.calls = 0
        self.command = command

    def get_default_model(self):
        return "recording-fake"

    async def chat(self, messages, **kwargs):
        self.calls += 1
        if self.calls == 1:
            return LLMResponse(content="<think>PRIVATE_REASONING</think>visible",
                               reasoning_content="PRIVATE_REASONING",
                               tool_calls=[ToolCallRequest("call-exec", "exec", {"command": self.command})])
        return LLMResponse(content="recorded final")


@pytest.fixture
async def recording(tmp_path: Path):
    from membot.agent.tools.base import Tool

    sentinel = tmp_path / "external-write-must-not-happen"
    command = f"touch {sentinel}"

    class FixtureExec(Tool):
        name = "exec"
        description = "An original fake tool; no external subprocess."
        parameters = {"type": "object", "properties": {"command": {"type": "string"}}}

        async def execute(self, command):
            return "recorded output"

    task = {"invocationId": "unit-inv", "traceId": "unit-trace", "requestId": "unit-request",
            "sessionId": "unit-session", "status": "SUCCEEDED",
            "submittedAt": "2026-01-01T00:00:00+00:00", "startedAt": "2026-01-01T00:00:01+00:00",
            "finishedAt": "2026-01-01T00:00:02+00:00"}
    events = []

    async def capture(category, payload):
        sequence = len(events) + 1
        events.append(event_record({
            "event_id": sequence, "event_seq": sequence,
            "invocation_id": task["invocationId"], "trace_id": task["traceId"],
            "request_id": task["requestId"], "session_id": task["sessionId"],
            "event_type": category, "step": payload.get("step"), "attempt": 1,
            "worker_id": "unit-worker", "duration_ms": payload.get("duration_ms"),
            "span_id": payload.get("span_id"), "parent_span_id": payload.get("parent_span_id"),
            "tool_call_id": payload.get("tool_call_id"), "error_code": payload.get("error_code"),
            "payload": DiagnosticPolicy().capture(copy.deepcopy(payload)),
            "created_at": datetime.now(timezone.utc),
            "expires_at": datetime.now(timezone.utc) + timedelta(days=7),
        }))

    initial = [{"role": "system", "content": "fixture system"}, {"role": "user", "content": "fixture task"}]
    await capture("accepted", {"step": "committed", "input": {"message": "fixture task"}})
    await capture("CONFIG", {"step": "snapshot", "config": {"model": "recording-fake", "maxIterations": 4, "configVersion": "fixture-v1"}})
    await capture("CONTEXT", {"step": "snapshot", "history": [], "messages": copy.deepcopy(initial)})
    agent = AgentLoop(bus=MessageBus(), provider=_TwoRoundProvider(command), workspace=tmp_path,
                      enable_consolidation=False, enable_subagents=False, enable_cron=False)
    agent.tools.register(FixtureExec())
    try:
        result = await agent._run_agent_loop(initial, event_callback=capture)
        await capture("result", {"step": "end", "status": "SUCCEEDED"})
        await capture("FINAL", {"step": "end", "content": result.final_content})
        return {"invocation": task, "events": events}, sentinel
    finally:
        await agent.shutdown()


@pytest.mark.asyncio
async def test_reproduction_uses_fixture_tools_and_never_runs_external_writes(recording, monkeypatch):
    data, sentinel = recording

    async def prohibited(*args, **kwargs):
        pytest.fail("replay attempted a real shell or write tool")

    monkeypatch.setattr("membot.agent.tools.shell.ExecTool.execute", prohibited)
    monkeypatch.setattr("membot.agent.tools.filesystem.WriteFileTool.execute", prohibited)
    output = await reproduce_recording(data)
    assert output["matches_recorded_status"] and output["final"] == "recorded final"
    assert output["external_operations"] is False and not sentinel.exists()


@pytest.mark.asyncio
async def test_replay_refuses_missing_truncated_expired_or_tampered_records(recording):
    data, _ = recording
    for kind in ("missing", "truncated", "expired", "tampered"):
        changed = copy.deepcopy(data)
        llm = next(event for event in changed["events"] if event["event_type"] == "LLM" and event["step"] == "end")
        if kind == "missing":
            changed["events"].remove(llm)
        elif kind == "truncated":
            llm["payload"]["truncated"] = True
        elif kind == "expired":
            llm["expires_at"] = "2020-01-01T00:00:00+00:00"
        else:
            first = next(event for event in changed["events"] if event["event_type"] == "LLM" and event["step"] == "start")
            first["payload"]["messages"][0]["content"] = "changed context"
        with pytest.raises(ReplayUnavailableError):
            await reproduce_recording(changed)


def test_failure_candidate_has_no_automatic_golden_answer():
    data = {"invocation": {"invocationId": "failed", "traceId": "trace", "requestId": "req",
                           "sessionId": "session", "status": "FAILED", "errorCode": "PROVIDER_ERROR",
                           "result": {"final_content": "WRONG_ANSWER"}},
            "events": [{"event_type": "accepted", "step": "committed", "payload": {"input": {"message": f"task user@example.com 13800138000 password={SECRET}"}}},
                       {"event_type": "CONTEXT", "step": "snapshot", "payload": {"history": []}},
                       {"event_type": "CONFIG", "step": "snapshot", "payload": {"config": {"configVersion": "version"}}}]}
    exported = candidate_case(data)
    assert exported["expected"] == {"confirmed": False, "status": None, "answer": None,
                                    "tool_constraints": {"allowed_tools": None, "forbidden_tools": None}}
    assert "WRONG_ANSWER" not in json.dumps(exported)
    assert "user@example.com" not in json.dumps(exported) and "13800138000" not in json.dumps(exported)
    assert SECRET not in json.dumps(exported)
    confirmed = confirm_expected(exported, status="SUCCEEDED", allowed_tools=["read_file"], forbidden_tools=["exec"])
    assert confirmed["expected"]["confirmed"] and exported["review_required"]


@pytest.mark.asyncio
async def test_timeline_and_cli_default_are_read_only(recording, tmp_path):
    data, sentinel = recording
    path = tmp_path / "recording.json"
    path.write_text(json.dumps(data))
    output = subprocess.run([sys.executable, "scripts/trace_probe.py", "--from-file", str(path),
                             "--assert-correlation", "--assert-redaction"], capture_output=True, text=True, check=True)
    assert "LLM.start" in output.stdout and "TOOL.end" in output.stdout and "FINAL.end" in output.stdout
    assert not sentinel.exists() and path.read_text() == json.dumps(data)
    summary = timing_summary(data)
    assert summary["llm_ms"] >= 0 and summary["tool_ms"] >= 0 and not summary["incomplete_spans"]
    assert "original request=unit-request" in render_timeline(data)
