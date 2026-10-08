from __future__ import annotations

import json

import pytest
from aiohttp.test_utils import TestClient, TestServer
from membot.service.api import create_api_app
from membot.service.config import ServiceConfig


class FakePool:
    async def fetchval(self, query, *args):
        return 1


class FakeInvocation:
    def __init__(self, *, invocation_id="inv-1", status="QUEUED", trace_id="trace-1"):
        from datetime import datetime, timezone

        self.invocation_id = invocation_id
        self.session_id = "session-1"
        self.session_key = "service:session-1"
        self.session_seq = 1
        self.status = type("Status", (), {"value": status})()
        self.request_id = "request-1"
        self.trace_id = trace_id
        self.submitted_at = datetime.now(timezone.utc)
        self.started_at = None
        self.finished_at = None
        self.result = None
        self.error_code = None
        self.error_message = None


class FakeRepository:
    def __init__(self):
        self.pool = FakePool()
        self.invocation = FakeInvocation()
        self.accepted: list[dict] = []

    async def create_session(self, **kwargs):
        return type("Snapshot", (), {
            "session_id": kwargs["session_id"], "session_key": kwargs["session_key"],
        })()

    async def accept_invocation(self, **kwargs):
        self.accepted.append(kwargs)
        return self.invocation, False

    async def get_invocation(self, invocation_id):
        return self.invocation if invocation_id == self.invocation.invocation_id else None

    async def events(self, invocation_id):
        return [{"event_type": "accepted", "invocation_id": invocation_id}]


@pytest.fixture
async def client():
    repo = FakeRepository()
    config = ServiceConfig(max_payload_bytes=1024, max_iterations=4)
    app = create_api_app(repo, config)
    try:
        async with TestClient(TestServer(app)) as value:
            yield value, repo
    except PermissionError as exc:
        pytest.skip(f"socket access unavailable in this environment: {exc}")


@pytest.mark.asyncio
async def test_invocation_is_accepted_as_202_and_query_keeps_original_trace(client):
    value, repo = client
    response = await value.post(
        "/v1/invocations",
        data=json.dumps({"sessionId": "session-1", "message": "hello"}),
        headers={"Content-Type": "application/json", "X-Trace-ID": "trace-submit", "Idempotency-Key": "k"},
    )
    assert response.status == 202
    body = await response.json()
    assert body["invocationId"] == "inv-1"
    assert body["status"] == "QUEUED"
    assert body["traceId"] == "trace-1" or body["traceId"] == "trace-submit"
    assert response.headers["Location"].endswith("inv-1")
    query = await value.get("/v1/invocations/inv-1", headers={"X-Request-ID": "query-request"})
    assert query.status == 200
    query_body = await query.json()
    assert query_body["traceId"] == body["traceId"]
    assert query.headers["X-Request-ID"] == "query-request"
    assert repo.accepted[0]["idempotency_key"] == "k"


@pytest.mark.asyncio
async def test_events_and_validation(client):
    value, _ = client
    too_large = await value.post(
        "/v1/invocations", data="x" * 2048, headers={"Content-Type": "application/json"},
    )
    assert too_large.status == 413
    missing = await value.post(
        "/v1/invocations", json={"sessionId": "session-1"},
    )
    assert missing.status == 400
    events = await value.get("/v1/invocations/inv-1/events")
    assert events.status == 200
    assert (await events.json())["events"][0]["event_type"] == "accepted"
