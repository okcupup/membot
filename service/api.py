"""Stateless aiohttp API for durable invocation admission and queries."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from aiohttp import web

from membot.agent.persistence.repository import (
    CapacityError,
    IdempotencyConflictError,
    Invocation,
    PostgresRepository,
)
from membot.agent.redaction import redact_data
from membot.service.config import ServiceConfig
from membot.service.models import invocation_payload

logger = logging.getLogger(__name__)
REPOSITORY = web.AppKey("repository", PostgresRepository)
CONFIG = web.AppKey("config", ServiceConfig)
OWN_REPOSITORY = web.AppKey("own_repository", bool)


@dataclass
class AdmissionState:
    draining: bool = False
    mutations: set[asyncio.Task] = field(default_factory=set)


ADMISSION = web.AppKey("admission", AdmissionState)


def begin_api_drain(app: web.Application) -> None:
    app[ADMISSION].draining = True


async def wait_api_drain(app: web.Application, timeout: float) -> None:
    active = set(app[ADMISSION].mutations)
    if active:
        await asyncio.wait(active, timeout=timeout)
_RequestKey = getattr(web, "RequestKey", web.AppKey)
REQUEST_ID = _RequestKey("request_id", str)
TRACE_ID = _RequestKey("trace_id", str)
INVOCATION = _RequestKey("invocation", Invocation)


def _request_id(request: web.Request) -> str:
    return request.get(REQUEST_ID) or request.headers.get("X-Request-ID") or str(uuid.uuid4())


def _trace_id(request: web.Request) -> str:
    return request.get(TRACE_ID) or request.headers.get("X-Trace-ID") or str(uuid.uuid4())


def _owner_id(request: web.Request, config: ServiceConfig) -> str:
    requested = request.headers.get("X-Owner-ID") or request.headers.get("X-Owner-Id")
    if requested and requested != config.owner_id:
        raise web.HTTPForbidden(text="owner is not served by this Worker")
    return config.owner_id


def _json_response(payload: dict[str, Any], *, status: int = 200, request_id: str | None = None) -> web.Response:
    headers = {}
    if request_id:
        headers["X-Request-ID"] = request_id
    return web.json_response(redact_data(payload) if status >= 400 else payload, status=status, headers=headers)


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
    admission = AdmissionState()
    @web.middleware
    async def request_logging(request: web.Request, handler):
        request[REQUEST_ID] = _request_id(request)
        request[TRACE_ID] = _trace_id(request)
        started = time.monotonic()
        mutation = request.method in {"POST", "PUT", "PATCH", "DELETE"}
        if mutation and admission.draining:
            response = _json_response({"error": "draining"}, status=503)
        else:
            task = asyncio.current_task()
            if mutation:
                admission.mutations.add(task)
            try:
                response = await handler(request)
            except web.HTTPException as exc:
                response = _json_response({"error": exc.reason, "detail": exc.text}, status=exc.status,
                                          request_id=request[REQUEST_ID])
            finally:
                admission.mutations.discard(task)
        response.headers["X-Request-ID"] = request[REQUEST_ID]
        response.headers["X-Instance-ID"] = config.instance_id
        task = request.get(INVOCATION)
        correlation = {
            "requestId": task.request_id if task else request[REQUEST_ID],
            "traceId": task.trace_id if task else request[TRACE_ID],
            "queryRequestId": request[REQUEST_ID] if task else None,
            "invocationId": task.invocation_id if task else None,
            "sessionId": task.session_id if task else None,
            "event_type": "HTTP_API", "step": "response",
            "duration_ms": (time.monotonic() - started) * 1000,
            "status": response.status, "method": request.method,
            "route": request.match_info.route.resource.canonical if request.match_info.route.resource else None,
            "error_code": f"HTTP_{response.status}" if response.status >= 400 else None,
            "instanceId": config.instance_id,
        }
        logger.info("HTTP response", extra={"correlation": correlation})
        return response

    app = web.Application(client_max_size=config.max_payload_bytes, middlewares=[request_logging])
    app[REPOSITORY] = repository
    app[CONFIG] = config
    app[OWN_REPOSITORY] = own_repository
    app[ADMISSION] = admission

    async def live(_: web.Request) -> web.Response:
        return _json_response({"status": "live", "instanceId": config.instance_id})

    async def ready(request: web.Request) -> web.Response:
        if admission.draining:
            return _json_response({"status": "not_ready", "reason": "draining"}, status=503)
        try:
            details = await asyncio.wait_for(repository.admission_health(config.owner_id, config.max_unfinished),
                                             config.db_timeout_seconds)
        except Exception as exc:
            return _json_response({"status": "not_ready", "error": redact_data(str(exc))[:200]}, status=503, request_id=_request_id(request))
        return _json_response({"status": "ready" if details["can_accept"] else "not_ready",
                               "admission": details, "instanceId": config.instance_id},
                              status=200 if details["can_accept"] else 503, request_id=_request_id(request))

    async def doctor(request: web.Request) -> web.Response:
        try:
            details = await asyncio.wait_for(repository.service_diagnostics(
                config.owner_id, stale_seconds=config.worker_stale_seconds), config.db_timeout_seconds)
        except Exception as exc:
            return _json_response({"error": "database_unavailable", "detail": str(exc)[:200]}, status=503)
        return _json_response({"instanceId": config.instance_id, "draining": admission.draining,
                               "execution": details, "scope": "Worker heartbeat does not verify model availability"})

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
            try:
                iterations = int(body.get("maxIterations", config.max_iterations))
            except (ValueError, TypeError):
                raise web.HTTPBadRequest(text="maxIterations must be an integer")
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
                diagnostic_config=config.diagnostic_snapshot(),
            )
            request[INVOCATION] = invocation
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
        if invocation.owner_id != _owner_id(request, config):
            return _json_response({"error": "not_found"}, status=404, request_id=request_id)
        request[INVOCATION] = invocation
        return _json_response(invocation_payload(invocation), request_id=request_id)

    async def get_events(request: web.Request) -> web.Response:
        request_id = _request_id(request)
        try:
            after = int(request.query.get("after", 0))
            limit = int(request.query.get("limit", 500))
            if after < 0 or not 1 <= limit <= 1000:
                raise ValueError("cursor out of range")
        except ValueError:
            raise web.HTTPBadRequest(text="after must be >= 0 and limit between 1 and 1000")
        try:
            invocation = await repository.get_invocation(request.match_info["invocation_id"])
            if invocation is None:
                return _json_response({"error": "not_found"}, status=404, request_id=request_id)
            if invocation.owner_id != _owner_id(request, config):
                return _json_response({"error": "not_found"}, status=404, request_id=request_id)
            request[INVOCATION] = invocation
            events = await repository.events(invocation.invocation_id, after=after, limit=limit)
        except web.HTTPException:
            raise
        except Exception as exc:
            return _json_response({"error": "database_unavailable", "detail": str(exc)[:200]}, status=503, request_id=request_id)
        return _json_response({
            "invocationId": invocation.invocation_id, "traceId": invocation.trace_id,
            "requestId": invocation.request_id, "sessionId": invocation.session_id,
            "events": events, "nextAfter": events[-1]["sequence"] if len(events) == limit else None,
            "retentionDays": config.diagnostic_retention_days,
        }, request_id=request_id)

    app.router.add_get("/live", live)
    app.router.add_get("/ready", ready)
    app.router.add_get("/health/live", live)
    app.router.add_get("/health/ready", ready)
    app.router.add_get("/health/doctor", doctor)
    app.router.add_post("/v1/sessions", create_session)
    app.router.add_post("/v1/invocations", create_invocation)
    app.router.add_post("/v1/sessions/{session_id}/invocations", create_invocation)
    app.router.add_get("/v1/invocations/{invocation_id}", get_invocation)
    app.router.add_get("/v1/invocations/{invocation_id}/events", get_events)

    async def cleanup(_: web.Application) -> None:
        if app[OWN_REPOSITORY]:
            await repository.close()

    app.on_cleanup.append(cleanup)
    return app
