"""Stateless aiohttp API for durable invocation admission and queries."""

from __future__ import annotations

import json
import uuid
from typing import Any

from aiohttp import web

from membot.agent.persistence.repository import (
    CapacityError,
    IdempotencyConflictError,
    Invocation,
    PostgresRepository,
)
from membot.service.config import ServiceConfig


def _request_id(request: web.Request) -> str:
    return request.headers.get("X-Request-ID") or request.headers.get("X-Request-Id") or str(uuid.uuid4())


def _trace_id(request: web.Request) -> str:
    return request.headers.get("X-Trace-ID") or request.headers.get("X-Trace-Id") or str(uuid.uuid4())


def _owner_id(request: web.Request, config: ServiceConfig) -> str:
    requested = request.headers.get("X-Owner-ID") or request.headers.get("X-Owner-Id")
    if requested and requested != config.owner_id:
        raise web.HTTPForbidden(text="owner is not served by this Worker")
    return config.owner_id


def _json_response(payload: dict[str, Any], *, status: int = 200, request_id: str | None = None) -> web.Response:
    headers = {"Content-Type": "application/json"}
    if request_id:
        headers["X-Request-ID"] = request_id
    return web.json_response(payload, status=status, headers=headers)


def invocation_payload(invocation: Invocation) -> dict[str, Any]:
    payload = {
        "invocationId": invocation.invocation_id,
        "sessionId": invocation.session_id,
        "sessionKey": invocation.session_key,
        "sessionSeq": invocation.session_seq,
        "status": invocation.status.value,
        "requestId": invocation.request_id,
        "traceId": invocation.trace_id,
        "submittedAt": invocation.submitted_at.isoformat(),
        "startedAt": invocation.started_at.isoformat() if invocation.started_at else None,
        "finishedAt": invocation.finished_at.isoformat() if invocation.finished_at else None,
    }
    if invocation.result is not None:
        payload["result"] = invocation.result
    if invocation.error_code:
        payload["errorCode"] = invocation.error_code
    if invocation.error_message:
        payload["errorMessage"] = invocation.error_message
    return payload


async def _read_json(request: web.Request, max_bytes: int) -> dict[str, Any]:
    if request.content_length is not None and request.content_length > max_bytes:
        raise web.HTTPRequestEntityTooLarge(max_size=max_bytes, actual_size=request.content_length)
    body = await request.read()
    if len(body) > max_bytes:
        raise web.HTTPRequestEntityTooLarge(max_size=max_bytes, actual_size=len(body))
    if not body:
        return {}
    try:
        value = json.loads(body)
    except json.JSONDecodeError as exc:
        raise web.HTTPBadRequest(text="request body must be JSON") from exc
    if not isinstance(value, dict):
        raise web.HTTPBadRequest(text="request body must be an object")
    return value


def create_api_app(
    repository: PostgresRepository,
    config: ServiceConfig | None = None,
    *,
    own_repository: bool = False,
) -> web.Application:
    """Create an API app.  No AgentLoop is constructed in this process."""
    config = config or ServiceConfig.from_env()
    app = web.Application(client_max_size=config.max_payload_bytes)
    app["repository"] = repository
    app["config"] = config
    app["own_repository"] = own_repository

    async def live(_: web.Request) -> web.Response:
        return _json_response({"status": "live"})

    async def ready(request: web.Request) -> web.Response:
        try:
            await repository.pool.fetchval("SELECT 1")
        except Exception as exc:
            return _json_response({"status": "not_ready", "error": str(exc)[:200]}, status=503, request_id=_request_id(request))
        return _json_response({"status": "ready"}, request_id=_request_id(request))

    async def create_session(request: web.Request) -> web.Response:
        request_id = _request_id(request)
        try:
            body = await _read_json(request, config.max_payload_bytes)
            owner = _owner_id(request, config)
            session_id = str(body.get("sessionId") or uuid.uuid4())
            session_key = str(body.get("sessionKey") or f"service:{session_id}").strip()
            if not session_key or len(session_key) > 512:
                raise web.HTTPBadRequest(text="sessionKey is required and must be <= 512 characters")
            snapshot = await repository.create_session(
                owner_id=owner, session_id=session_id, session_key=session_key,
            )
        except web.HTTPException:
            raise
        except ValueError as exc:
            return _json_response({"error": str(exc)}, status=409, request_id=request_id)
        except Exception as exc:
            return _json_response({"error": "database unavailable", "detail": str(exc)[:200]}, status=503, request_id=request_id)
        response = _json_response(
            {"sessionId": snapshot.session_id, "sessionKey": snapshot.session_key, "ownerId": owner},
            status=201, request_id=request_id,
        )
        response.headers["Location"] = f"/v1/sessions/{snapshot.session_id}"
        return response

    async def create_invocation(request: web.Request) -> web.Response:
        request_id = _request_id(request)
        trace_id = _trace_id(request)
        try:
            body = await _read_json(request, config.max_payload_bytes)
            owner = _owner_id(request, config)
            path_session = request.match_info.get("session_id")
            session_id = str(body.get("sessionId") or path_session or "")
            session_key = str(body.get("sessionKey") or "").strip()
            if not session_id and not session_key:
                raise web.HTTPBadRequest(text="sessionId or sessionKey is required")
            if not session_id:
                session_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"membot:{owner}:{session_key}"))
            if not session_key:
                session_key = f"service:{session_id}"
            content = body.get("message", body.get("content"))
            if not isinstance(content, str) or not content.strip():
                raise web.HTTPBadRequest(text="message must be a non-empty string")
            iterations = int(body.get("maxIterations", config.max_iterations))
            if iterations < 1 or iterations > config.max_iterations:
                raise web.HTTPBadRequest(text=f"maxIterations must be between 1 and {config.max_iterations}")
            payload = {
                "message": content,
                "sessionId": session_id,
                "sessionKey": session_key,
                "channel": str(body.get("channel") or "service"),
                "chatId": str(body.get("chatId") or session_id),
                "media": body.get("media") or [],
                "maxIterations": iterations,
            }
            idempotency_key = request.headers.get("Idempotency-Key") or body.get("idempotencyKey")
            invocation, duplicate = await repository.accept_invocation(
                owner_id=owner, session_id=session_id, session_key=session_key,
                request_id=request_id, trace_id=trace_id, payload=payload,
                idempotency_key=str(idempotency_key) if idempotency_key else None,
                queue_timeout_seconds=config.queue_timeout_seconds,
                execution_timeout_seconds=config.execution_timeout_seconds,
                max_unfinished=config.max_unfinished,
            )
        except web.HTTPException:
            raise
        except IdempotencyConflictError as exc:
            return _json_response({"error": "idempotency_conflict", "detail": str(exc)}, status=409, request_id=request_id)
        except CapacityError as exc:
            return _json_response({"error": "capacity_exhausted", "detail": str(exc)}, status=429, request_id=request_id)
        except (ConnectionError, OSError) as exc:
            return _json_response({"error": "database_unavailable", "detail": str(exc)[:200]}, status=503, request_id=request_id)
        except Exception as exc:
            return _json_response({"error": "database_unavailable", "detail": str(exc)[:200]}, status=503, request_id=request_id)
        response = _json_response(invocation_payload(invocation), status=202, request_id=request_id)
        response.headers["Location"] = f"/v1/invocations/{invocation.invocation_id}"
        return response

    async def get_invocation(request: web.Request) -> web.Response:
        request_id = _request_id(request)
        try:
            invocation = await repository.get_invocation(request.match_info["invocation_id"])
        except Exception as exc:
            return _json_response({"error": "database_unavailable", "detail": str(exc)[:200]}, status=503, request_id=request_id)
        if invocation is None:
            return _json_response({"error": "not_found"}, status=404, request_id=request_id)
        return _json_response(invocation_payload(invocation), request_id=request_id)

    async def get_events(request: web.Request) -> web.Response:
        request_id = _request_id(request)
        try:
            invocation = await repository.get_invocation(request.match_info["invocation_id"])
            if invocation is None:
                return _json_response({"error": "not_found"}, status=404, request_id=request_id)
            events = await repository.events(invocation.invocation_id)
        except Exception as exc:
            return _json_response({"error": "database_unavailable", "detail": str(exc)[:200]}, status=503, request_id=request_id)
        return _json_response({"invocationId": invocation.invocation_id, "traceId": invocation.trace_id, "events": events}, request_id=request_id)

    app.router.add_get("/live", live)
    app.router.add_get("/ready", ready)
    app.router.add_post("/v1/sessions", create_session)
    app.router.add_post("/v1/invocations", create_invocation)
    app.router.add_post("/v1/sessions/{session_id}/invocations", create_invocation)
    app.router.add_get("/v1/invocations/{invocation_id}", get_invocation)
    app.router.add_get("/v1/invocations/{invocation_id}/events", get_events)

    async def cleanup(_: web.Application) -> None:
        if app["own_repository"]:
            await repository.close()

    app.on_cleanup.append(cleanup)
    return app
