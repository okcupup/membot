from __future__ import annotations

import asyncio
import os
import shlex
import sys
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer
from membot.agent.loop import AgentLoop
from membot.agent.tools.shell import ExecTool
from membot.bus.queue import MessageBus
from membot.providers.base import LLMProvider
from membot.service.api import ADMISSION, begin_api_drain, create_api_app, wait_api_drain
from membot.service.config import ServiceConfig


@pytest.mark.parametrize("values", [
    {"worker_concurrency": 0}, {"worker_concurrency": 3, "worker_prefetch": 2},
    {"max_iterations": 101}, {"max_payload_bytes": 1_048_577},
    {"execution_timeout_seconds": float("nan")}, {"db_timeout_seconds": None},
    {"worker_stale_seconds": 2}, {"database_url": "sqlite://local"},
    {"provider": "fake"}, {"provider": "custom"},
])
def test_invalid_configuration_fails_instead_of_silently_clamping(values):
    with pytest.raises(ValueError):
        ServiceConfig(**values)


def test_environment_deadlines_and_heartbeat(monkeypatch):
    monkeypatch.setenv("MEMBOT_EXECUTION_TIMEOUT_SECONDS", "none")
    monkeypatch.setenv("MEMBOT_WORKER_HEARTBEAT_SECONDS", "2")
    monkeypatch.setenv("MEMBOT_WORKER_STALE_SECONDS", "6")
    assert ServiceConfig.from_env().execution_timeout_seconds is None
    assert ServiceConfig.from_env().worker_heartbeat_seconds == 2
    monkeypatch.setenv("MEMBOT_EXECUTION_TIMEOUT_SECONDS", "0")
    with pytest.raises(ValueError):
        ServiceConfig.from_env()


class HealthRepository:
    def __init__(self):
        self.error = False
        self.capacity = True
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.committed = False

    async def admission_health(self, owner, limit):
        if self.error:
            raise ConnectionError("postgres unavailable")
        return {"postgres": "available", "can_accept": self.capacity, "unfinished": 0, "capacity": limit}

    async def service_diagnostics(self, owner, **kwargs):
        return {"queued": 3, "running": 0, "unpublishedOutbox": 3,
                "worker": {"alive": False, "state": "ABSENT"}}

    async def create_session(self, **kwargs):
        self.entered.set()
        await self.release.wait()
        self.committed = True
        return type("Session", (), kwargs)()


@pytest.mark.asyncio
async def test_live_ready_and_doctor_report_different_scopes():
    repo = HealthRepository()
    try:
        async with TestClient(TestServer(create_api_app(repo, ServiceConfig(instance_id="api-test")))) as client:
            ready = await client.get("/health/ready")
            assert ready.status == 200  # Worker absent doesn't break durable Outbox admission
            doctor = await client.get("/health/doctor")
            assert (await doctor.json())["execution"]["worker"]["alive"] is False
            repo.error = True
            assert (await client.get("/health/ready")).status == 503
            live = await client.get("/health/live")
            assert live.status == 200 and live.headers["X-Instance-ID"] == "api-test"
            repo.error = False
            repo.capacity = False
            assert (await client.get("/health/ready")).status == 503
    except PermissionError as exc:
        pytest.skip(f"API socket unavailable: {exc}")


@pytest.mark.asyncio
async def test_api_drain_rejects_new_mutations_and_completes_admitted_transaction():
    repo = HealthRepository()
    app = create_api_app(repo)
    try:
        async with TestClient(TestServer(app)) as client:
            accepted = asyncio.create_task(client.post("/v1/sessions", json={"sessionId": "first"}))
            await asyncio.wait_for(repo.entered.wait(), 2)
            assert len(app[ADMISSION].mutations) == 1
            begin_api_drain(app)
            assert (await client.get("/health/ready")).status == 503
            assert (await client.get("/health/live")).status == 200
            assert (await client.post("/v1/sessions", json={"sessionId": "second"})).status == 503
            drain = asyncio.create_task(wait_api_drain(app, 2))
            await asyncio.sleep(0)
            assert not drain.done()
            repo.release.set()
            assert (await accepted).status == 201
            await drain
            assert repo.committed and not app[ADMISSION].mutations
    except PermissionError as exc:
        pytest.skip(f"API socket unavailable: {exc}")


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups")
async def test_cancel_shell_kills_descendants(tmp_path):
    marker = tmp_path / "child.pid"
    child = f"import os,time; from pathlib import Path; Path({str(marker)!r}).write_text(str(os.getpid())); time.sleep(30)"
    parent = f"import subprocess; subprocess.Popen([{sys.executable!r}, '-c', {child!r}]).wait()"
    task = asyncio.create_task(ExecTool(working_dir=str(tmp_path)).execute(
        f"{shlex.quote(sys.executable)} -c {shlex.quote(parent)}"))

    async def entered():
        while not marker.exists():
            await asyncio.sleep(0.01)
    try:
        await asyncio.wait_for(entered(), 5)
        pid = int(marker.read_text())
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 6)
        process = Path(f"/proc/{pid}/stat")
        assert not process.exists() or process.read_text().split()[2] == "Z"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_shutdown_cancels_shared_mcp_initialization_and_closes_clients(tmp_path, monkeypatch):
    entered, released, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def connect(servers, tools, stack):
        stack.callback(released.set)
        entered.set()
        await asyncio.Event().wait()

    class Provider(LLMProvider):
        async def chat(self, **kwargs):
            raise AssertionError("no LLM call expected")

        def get_default_model(self):
            return "fixture"

        async def aclose(self):
            closed.set()

    monkeypatch.setattr("membot.agent.tools.mcp.connect_mcp_servers", connect)
    loop = AgentLoop(MessageBus(), Provider(), tmp_path, mcp_servers={"fixture": {}})
    init = asyncio.create_task(loop._connect_mcp())
    await asyncio.wait_for(entered.wait(), 2)
    await asyncio.wait_for(loop.shutdown(), 2)
    await asyncio.gather(init, return_exceptions=True)
    assert released.is_set() and closed.is_set()
    assert loop._mcp_init_task is None and loop._mcp_stack is None
