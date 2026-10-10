"""Real HTTP admission, PostgreSQL Outbox, Redis Streams and one asyncio Worker."""

from __future__ import annotations

import asyncio
import os
import tempfile
import time
import uuid
from dataclasses import replace
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from membot.agent.persistence.redis_transport import RedisStreamTransport
from membot.agent.persistence.repository import PostgresRepository
from membot.service.api import create_api_app
from membot.service.config import ServiceConfig
from membot.service.worker import AsyncWorker

from .fixtures import FixtureProvider, fixture_registry, prepare_workspace
from .schema import TERMINAL

# The remaining native-bus/message/cancellation contracts run in the kernel
# suite; unsupported scenarios are explicitly listed as excluded by the CLI.
QUEUE_CASE_IDS = {
    "basic-echo", "tool-read", "tool-multi-step", "context-three-serial",
    "context-parallel", "context-hot-fairness", "error-tool", "error-provider",
    "error-missing-final", "error-iteration-limit", "error-llm-timeout", "error-tool-timeout",
}


class _CaseWorker(AsyncWorker):
    def __init__(self, *args, case, workspace, **kwargs):
        self.case = case
        self.case_workspace = workspace
        super().__init__(*args, workspace=workspace, **kwargs)

    async def _build_agent(self):
        agent = await super()._build_agent()
        agent.tools = fixture_registry(self.case, self.case_workspace, agent.bus)
        return agent


class QueueRuntime:
    """Own isolated test APIs/streams, using real production handlers and stores.

    Exactly one Worker runs at a time. Each Case/repetition gets a new owner,
    Session scope and disposable Tool workspace; rows remain available for
    diagnostic export in the explicitly selected evaluation database.
    """

    def __init__(self, database_url=None, redis_url=None):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        self.redis_url = redis_url or os.getenv("REDIS_URL")
        if not self.database_url or not self.redis_url:
            raise ValueError("queue evaluation requires DATABASE_URL and REDIS_URL")
        self.repository = self.query_repository = self.redis = self.transport = None
        self.worker = self.worker_task = None
        self.environment = {}

    async def start(self):
        from redis.asyncio import Redis
        try:
            self.repository = await PostgresRepository.connect(self.database_url, min_size=1, max_size=8)
            await self.repository.migrate()
            self.query_repository = await PostgresRepository.connect(self.database_url, min_size=1, max_size=4)
            self.redis = Redis.from_url(self.redis_url, decode_responses=True,
                                        socket_connect_timeout=3, socket_timeout=3)
            await self.redis.ping()
            self.environment = {"postgres_version": await self.repository.pool.fetchval("SHOW server_version"),
                                "redis_version": (await self.redis.info("server"))["redis_version"]}
            suffix = uuid.uuid4().hex
            self.transport = RedisStreamTransport(
                self.redis, stream_key=f"membot:m6:{suffix}", group=f"membot-m6:{suffix}",
                consumer=f"consumer:{suffix}", max_depth=64, prefetch=8,
            )
        except BaseException:
            await self.close()
            raise

    async def execute(self, case, *, repeat=0):
        if case.id not in QUEUE_CASE_IDS:
            raise ValueError(f"Case {case.id} is not enabled for queue integration")
        with tempfile.TemporaryDirectory(prefix="membot-queue-eval-") as temporary:
            workspace = Path(temporary)
            prepare_workspace(case, workspace)
            provider = FixtureProvider(case)
            owner = f"m6-eval-{uuid.uuid4().hex}"
            config = ServiceConfig(
                database_url=self.database_url, redis_url=self.redis_url, owner_id=owner,
                workspace=str(workspace), model="m6-fixture-v1", provider="fake", allow_fake_provider=True,
                worker_concurrency=2, worker_prefetch=8, max_iterations=case.budget.max_iterations,
                queue_timeout_seconds=case.budget.queue_seconds,
                execution_timeout_seconds=case.budget.execution_seconds,
                llm_timeout_seconds=case.budget.llm_seconds,
                tool_timeout_seconds=case.budget.tool_seconds,
                outbox_poll_seconds=0.01, queue_poll_seconds=0.01, worker_pending_idle_ms=0,
                instance_id="m6-api1",
            )
            clients = [TestClient(TestServer(create_api_app(repo, replace(config, instance_id=f"m6-api{i}"))))
                       for i, repo in enumerate((self.repository, self.query_repository), 1)]
            rows, contracts, sessions, starts, bodies = [], [], {}, {}, {}
            admission = {"requests": 0, "accepted_responses": 0, "rejected_responses": 0, "duplicate_responses": 0}

            async def submit(turn):
                session_id = sessions.setdefault(turn.session, f"{owner}-{turn.session}")
                request_id, trace_id = str(uuid.uuid4()), str(uuid.uuid4())
                provider.request_plans[request_id] = turn.provider
                body = {"sessionId": session_id, "sessionKey": f"eval:{turn.session}", "message": turn.input,
                        "channel": turn.channel, "chatId": turn.chat_id or turn.session,
                        "maxIterations": case.budget.max_iterations}
                key = str(uuid.uuid4())
                started = time.monotonic()
                response = await clients[0].post("/v1/invocations", json=body, headers={
                    "X-Request-ID": request_id, "X-Trace-ID": trace_id, "Idempotency-Key": key})
                data = await response.json()
                admission["requests"] += 1
                accepted = response.status == 202
                admission["accepted_responses" if accepted else "rejected_responses"] += 1
                row = {"turn": turn.id, "accepted": accepted, "admission_status": response.status,
                       "admission_ms": (time.monotonic() - started) * 1000,
                       "invocationId": data.get("invocationId"), "requestId": request_id,
                       "traceId": trace_id, "sessionId": session_id, "events": [],
                       "status": data.get("status", "REJECTED"), "error_code": None if accepted else f"HTTP_{response.status}",
                       "final": None, "delivery": None}
                rows.append(row)
                contracts.append({"name": f"202_location:{turn.id}", "passed": accepted and
                                  response.headers.get("Location") == f"/v1/invocations/{row['invocationId']}"})
                if accepted:
                    starts[row["invocationId"]] = started
                    bodies[turn.id] = body, key
                return row

            async def terminal(row):
                if not row["accepted"]:
                    return
                while True:
                    response = await clients[1].get(f"/v1/invocations/{row['invocationId']}",
                                                   headers={"X-Request-ID": "eval-query-request"})
                    if response.status != 200:
                        raise RuntimeError(f"cross-API query failed HTTP {response.status}")
                    data = await response.json()
                    if data["status"] in TERMINAL:
                        break
                    await asyncio.sleep(0.01)
                row["e2e_ms"] = (time.monotonic() - starts[row["invocationId"]]) * 1000
                current = await self.query_repository.get_invocation(row["invocationId"])
                row.update(status=current.status.value, error_code=current.error_code,
                           final=(current.result or {}).get("final_content"), session_seq=current.session_seq,
                           queue_wait_ms=((current.started_at or current.finished_at) - current.submitted_at).total_seconds() * 1000,
                           execution_ms=(current.finished_at - current.started_at).total_seconds() * 1000
                           if current.started_at else None,
                           durable_e2e_ms=(current.finished_at - current.submitted_at).total_seconds() * 1000)
                contracts.append({"name": f"cross_api_ids:{row['turn']}", "passed":
                    response.headers.get("X-Instance-ID") == "m6-api2"
                    and response.headers.get("X-Request-ID") == "eval-query-request"
                    and all(data[key] == row[key] for key in ("invocationId", "requestId", "traceId", "sessionId"))})

            async def settled():
                while self.worker._tasks or await self.transport.depth():
                    await asyncio.sleep(0.01)

            async def schedule():
                if case.scenario == "single":
                    for turn in case.turns:
                        row = await submit(turn)
                        await terminal(row)
                else:
                    for turn in case.turns:
                        await submit(turn)
                    if all(row["accepted"] for row in rows):
                        expected = 1 if case.scenario == "serial" else 2
                        await provider.entered[expected].wait()
                        contracts.append({"name": "overlap_and_semaphore", "passed":
                                          provider.active == provider.max_active == expected})
                        if case.scenario == "hot":
                            contracts.append({"name": "cold_session_advances", "passed":
                                provider.starts[:2] == [case.turns[0].input, case.turns[-1].input]})
                    provider.release.set()
                    await asyncio.gather(*(terminal(row) for row in rows))
                await settled()
                if case.scenario == "serial":
                    contracts.append({"name": "session_acceptance_order", "passed":
                        provider.starts == [turn.input for turn in case.turns]
                        and [row.get("session_seq") for row in rows] == list(range(1, len(rows) + 1))})
                first = next((row for row in rows if row["accepted"]), None)
                if first:
                    before_calls = dict(provider.calls)
                    body, key = bodies[first["turn"]]
                    duplicate = await clients[1].post("/v1/invocations", json=body, headers={"Idempotency-Key": key})
                    duplicate_data = await duplicate.json()
                    admission["requests"] += 1
                    admission["accepted_responses" if duplicate.status == 202 else "rejected_responses"] += 1
                    admission["duplicate_responses"] += int(duplicate.status == 202)
                    contracts.append({"name": "http_idempotency", "passed": duplicate.status == 202
                                      and duplicate_data.get("invocationId") == first["invocationId"]})
                    await self.transport.publish({"invocationId": first["invocationId"], "sessionId": first["sessionId"]})
                    await settled()
                    contracts.append({"name": "duplicate_delivery_no_execution", "passed": dict(provider.calls) == before_calls})
                for row in rows:
                    if not row["accepted"]:
                        continue
                    response = await clients[1].get(f"/v1/invocations/{row['invocationId']}/events", params={"limit": 1000})
                    if response.status != 200:
                        raise RuntimeError("events query failed")
                    data = await response.json()
                    row["events"] = data["events"]
                    row["recording_limited"] = data.get("nextAfter") is not None
                    contracts.append({"name": f"queue_trace:{row['turn']}", "passed":
                        any(e["event_type"] == "OUTBOX" and e["step"] == "end" for e in row["events"])
                        and any(e["event_type"] == "QUEUE" and e["step"] == "ack" for e in row["events"])})

            try:
                for client in clients:
                    await client.start_server()
                self.worker = _CaseWorker(self.repository, self.transport, config, case=case,
                                          workspace=workspace, provider=provider)
                await self.worker.start()
                self.worker_task = asyncio.create_task(self.worker.run())
                await asyncio.wait_for(schedule(), case.budget.case_seconds)
            except Exception as exc:
                contracts.append({"name": "queue_harness_complete", "passed": False, "reason": str(exc)})
            finally:
                provider.release.set()
                if self.worker:
                    await self.worker.stop(worker_lost=False)
                if self.worker_task:
                    self.worker_task.cancel()
                    await asyncio.gather(self.worker_task, return_exceptions=True)
                self.worker = self.worker_task = None
                for client in clients:
                    await client.close()
            return {"case_id": case.id, "case_hash": case.case_hash, "repeat": repeat,
                    "mode": "queue", "invocations": rows, "contracts": contracts,
                    "admission": admission, "outbound": []}

    async def close(self):
        if self.worker:
            await self.worker.stop(worker_lost=False)
        if self.worker_task:
            self.worker_task.cancel()
            await asyncio.gather(self.worker_task, return_exceptions=True)
        if self.transport:
            try:
                await self.redis.delete(self.transport.stream_key)
            finally:
                await self.transport.close()
        elif self.redis:
            await self.redis.aclose()
        if self.query_repository:
            await self.query_repository.close()
        if self.repository:
            await self.repository.close()
