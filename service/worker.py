"""Single-process Redis Streams Worker for durable AgentLoop invocations."""

from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from pathlib import Path
from typing import Any, Awaitable, Callable

from membot.agent.conversation_memory.engine import ConversationMemoryEngine
from membot.agent.diagnostics import config_version
from membot.agent.execution import ExecutionContext
from membot.agent.execution_result import ExecutionOutcome
from membot.agent.loop import AgentLoop
from membot.agent.persistence.redis_transport import OutboxRelay, RedisStreamTransport
from membot.agent.persistence.repository import InvocationStatus, PostgresRepository
from membot.bus.queue import MessageBus
from membot.service.config import ServiceConfig

logger = logging.getLogger(__name__)


class WorkerAlreadyRunningError(RuntimeError):
    """Another execution Worker owns the PostgreSQL singleton lock."""


class WorkerStoppedError(RuntimeError):
    """The Worker lost ownership and has stopped consuming work."""


class _AlreadyTerminalError(RuntimeError):
    """A duplicate Redis delivery has already reached a terminal DB state."""


class AsyncWorker:
    """Coordinate Outbox publication, Redis delivery, and AgentLoop execution.

    The Worker owns one AgentLoop and one execution process.  Redis delivery is
    at-least-once; PostgreSQL state and the execution owner fence determine
    whether an entry is claimable.  The stream is never ACKed before the
    terminal DB transaction succeeds.
    """

    ADVISORY_LOCK_KEY = 843199322540219

    def __init__(
        self,
        repository: PostgresRepository,
        transport: RedisStreamTransport,
        config: ServiceConfig | None = None,
        *,
        provider: Any | None = None,
        provider_factory: Callable[[], Any] | Callable[[], Awaitable[Any]] | None = None,
        workspace: Path | None = None,
    ):
        self.repository = repository
        self.transport = transport
        self.config = config or ServiceConfig.from_env()
        self.provider = provider
        self.provider_factory = provider_factory
        self.workspace = workspace or Path(self.config.workspace)
        self.execution_owner = f"{self.config.worker_id}:{uuid.uuid4().hex}"
        self.agent: AgentLoop | None = None
        self._lock_connection: Any | None = None
        self._running = False
        self._draining = False
        self._stop_lock = asyncio.Lock()
        self._lost = False
        self._stop_event = asyncio.Event()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._inflight: dict[str, asyncio.Task[Any]] = {}
        self._claimed: set[str] = set()
        self._needs_reconcile = True
        self._next_reconcile_at = 0.0
        self._next_retention_at = 0.0
        self._next_pending_at = 0.0

    async def _acquire_singleton_lock(self) -> None:
        pool = self.repository.pool
        connection = await pool.acquire()
        try:
            acquired = await connection.fetchval(
                "SELECT pg_try_advisory_lock($1)", self.ADVISORY_LOCK_KEY,
            )
        except Exception:
            await pool.release(connection)
            raise
        if not acquired:
            await pool.release(connection)
            raise WorkerAlreadyRunningError("another Membot Worker already owns the execution lock")
        self._lock_connection = connection

    async def _release_singleton_lock(self) -> None:
        connection = self._lock_connection
        self._lock_connection = None
        if connection is None:
            return
        try:
            await connection.execute("SELECT pg_advisory_unlock($1)", self.ADVISORY_LOCK_KEY)
        except Exception:
            logger.warning("Worker ownership connection was already closed")
        finally:
            await self.repository.pool.release(connection)

    async def _build_provider(self) -> Any:
        if self.provider is not None:
            return self.provider
        if self.provider_factory is not None:
            value = self.provider_factory()
            return await value if inspect.isawaitable(value) else value
        from membot.service.runtime_provider import build_provider
        return build_provider(self.config)

    async def _admit(self, context: ExecutionContext) -> bool:
        """Claim only after AgentLoop has acquired its Worker semaphore."""
        if self._lost or not self._running:
            raise WorkerStoppedError("Worker no longer owns execution")
        if self._draining:
            raise asyncio.CancelledError("Worker is draining")
        try:
            await self.repository.claim_invocation(
                context.invocation_id or "", self.execution_owner,
                lease_seconds=self.config.worker_lease_seconds,
            )
            self._claimed.add(context.invocation_id or "")
            if self._draining:
                raise asyncio.CancelledError("Worker drained during claim")
            effective_config = self.config.diagnostic_snapshot()
            if self.agent is not None:
                effective_config["providerType"] = type(self.agent.provider).__name__
                effective_config["toolsetVersion"] = config_version({"tools": self.agent.tools.get_definitions()})
            effective_config["maxIterations"] = context.max_iterations or self.config.max_iterations
            if context.execution_timeout_override:
                effective_config["executionTimeoutSeconds"] = context.execution_timeout_seconds
            effective_config["configVersion"] = config_version({
                key: value for key, value in effective_config.items() if key != "configVersion"
            })
            await self._event(context.invocation_id or "", "CONFIG", {
                "step": "snapshot", "config": effective_config,
            })
            return True
        except RuntimeError as exc:
            # A predecessor may still be running.  AgentLoop puts this Session
            # head back without consuming the semaphore and retries it.
            if "predecessor" in str(exc).lower():
                return False
            if "queue deadline" in str(exc).lower():
                await self.repository.finish_interrupted(
                    context.invocation_id or "", ExecutionOutcome.TIMEOUT,
                    error_message="invocation queue deadline expired",
                )
                raise _AlreadyTerminalError(context.invocation_id or "") from exc
            current = await self.repository.get_invocation(context.invocation_id or "")
            if current is None or current.status in {
                InvocationStatus.SUCCEEDED, InvocationStatus.FAILED, InvocationStatus.TIMEOUT,
            }:
                raise _AlreadyTerminalError(context.invocation_id or "") from exc
            raise

    async def _ready(self, context: ExecutionContext) -> bool:
        """Avoid taking the execution semaphore while a predecessor waits."""
        if self._lost or not self._running:
            raise WorkerStoppedError("Worker no longer owns execution")
        return await self.repository.session_predecessor_ready(context.invocation_id or "")

    async def _event(self, invocation_id: str, event_type: str, payload: dict[str, Any]) -> None:
        await self.repository.append_event(
            invocation_id, event_type, {**payload, "worker": self.execution_owner},
            execution_owner=self.execution_owner,
        )

    async def _ack(self, invocation_id: str, entry_id: str) -> None:
        span = uuid.uuid4().hex
        meta = {"worker": self.execution_owner, "entry_id": entry_id,
                "span_id": span, "parent_span_id": "invocation"}
        await self.repository.append_event(invocation_id, "QUEUE", {**meta, "step": "ack_start"})
        started = asyncio.get_running_loop().time()
        count = await self.transport.ack(entry_id)
        await self.repository.append_event(invocation_id, "QUEUE", {
            **meta, "step": "ack", "acknowledged": count,
            "duration_ms": (asyncio.get_running_loop().time() - started) * 1000,
        })

    async def _build_agent(self) -> AgentLoop:
        provider = await self._build_provider()
        return AgentLoop(
            bus=MessageBus(
                inbound_maxsize=self.config.worker_prefetch,
                outbound_maxsize=self.config.worker_prefetch,
            ),
            provider=provider,
            workspace=self.workspace,
            model=self.config.model,
            max_iterations=self.config.max_iterations,
            memory_engine=ConversationMemoryEngine.for_postgres(
                self.repository, owner_id=self.config.owner_id,
            ),
            max_concurrent_invocations=self.config.worker_concurrency,
            max_pending_invocations=self.config.worker_prefetch,
            queue_timeout=self.config.queue_timeout_seconds,
            execution_timeout=self.config.execution_timeout_seconds,
            llm_timeout=self.config.llm_timeout_seconds,
            tool_timeout=self.config.tool_timeout_seconds,
            admission_callback=self._admit,
            readiness_callback=self._ready,
            strict_events=True,
            enable_consolidation=False,
            enable_subagents=False,
            enable_cron=False,
            restrict_to_workspace=True,
        )

    async def _run_entry(self, entry_id: str, envelope: dict[str, Any], *, recovered: bool = False) -> None:
        invocation_id = str(envelope.get("invocationId") or "")
        if not invocation_id:
            await self.transport.ack(entry_id)
            return
        existing = self._inflight.get(invocation_id)
        if existing is not None and existing is not asyncio.current_task():
            try:
                await asyncio.shield(existing)
            except Exception:
                pass
            current = await self.repository.get_invocation(invocation_id)
            if current and current.status in {
                InvocationStatus.SUCCEEDED, InvocationStatus.FAILED, InvocationStatus.TIMEOUT,
            }:
                await self._ack(invocation_id, entry_id)
            return
        self._inflight[invocation_id] = asyncio.current_task()
        try:
            current = await self.repository.get_invocation(invocation_id)
            if current is None:
                await self.transport.ack(entry_id)
                return
            await self.repository.append_event(invocation_id, "QUEUE", {
                "step": "consume", "worker": self.execution_owner,
                "entry_id": entry_id, "envelope": envelope,
                "delivery_state": current.status.value,
                "pending_recovery": recovered, "consumer": self.transport.consumer,
                "group": self.transport.group,
            })
            if current.status in {
                InvocationStatus.SUCCEEDED, InvocationStatus.FAILED, InvocationStatus.TIMEOUT,
            }:
                await self._ack(invocation_id, entry_id)
                return
            if current.status is not InvocationStatus.QUEUED:
                return
            if self._draining:
                return
            if current.owner_id != self.config.owner_id:
                logger.error("invocation %s belongs to unsupported owner %s", invocation_id, current.owner_id)
                await self.repository.finish_interrupted(
                    invocation_id, ExecutionOutcome.INTERNAL_ERROR,
                    error_code="OWNER_UNSUPPORTED",
                    error_message="Worker owner scope does not match invocation",
                )
                await self._ack(invocation_id, entry_id)
                return
            # This entry may have waited behind local prefetch while the
            # consumer loop was processing another task.  Re-read the row
            # after any scheduling delay and fence it before AgentLoop sees it.
            current = await self.repository.get_invocation(invocation_id)
            if current is None:
                await self.transport.ack(entry_id)
                return
            if current.status in {
                InvocationStatus.SUCCEEDED, InvocationStatus.FAILED, InvocationStatus.TIMEOUT,
            }:
                await self._ack(invocation_id, entry_id)
                return
            if current.status is not InvocationStatus.QUEUED:
                return
            # Redis delivery order is not authoritative.  Wait outside the
            # AgentLoop until PostgreSQL says this Session sequence is ready;
            # otherwise an out-of-order entry could sit at the local queue head
            # and prevent its predecessor from ever being submitted.
            while self._running:
                if self._draining:
                    return
                # expire_queued() is the sole queue-deadline transition; it
                # runs in maintenance and avoids racing a claim transaction.
                current = await self.repository.get_invocation(invocation_id)
                if current is None:
                    await self.transport.ack(entry_id)
                    return
                if current.status in {
                    InvocationStatus.SUCCEEDED, InvocationStatus.FAILED, InvocationStatus.TIMEOUT,
                }:
                    await self._ack(invocation_id, entry_id)
                    return
                if current.status is not InvocationStatus.QUEUED:
                    return
                if await self._ready(ExecutionContext(
                    session_key=current.session_key,
                    channel=str(current.payload.get("channel") or "service"),
                    chat_id=str(current.payload.get("chatId") or current.session_id),
                    invocation_id=current.invocation_id,
                    owner_id=current.owner_id,
                    session_id=current.session_id,
                    session_seq=current.session_seq,
                    request_id=current.request_id,
                    trace_id=current.trace_id,
                    execution_owner=self.execution_owner,
                )):
                    break
                await asyncio.sleep(self.config.queue_poll_seconds)
            if self.agent is None:
                raise WorkerStoppedError("AgentLoop is not initialized")
            payload = current.payload
            context = {
                "ownerId": current.owner_id,
                "sessionId": current.session_id,
                "sessionSeq": current.session_seq,
                "requestId": current.request_id,
                "traceId": current.trace_id,
                "invocationId": current.invocation_id,
                "executionOwner": self.execution_owner,
                "attempt": current.attempt_count + 1,
                "executionTimeoutSeconds": current.execution_timeout_seconds,
            }

            async def progress(_: str, **kwargs: Any) -> None:
                # Service diagnostics record visible provider/tool output;
                # there is no channel gateway consuming the CLI progress bus.
                pass

            async def entry_event(event_type: str, event_payload: dict[str, Any]) -> None:
                await self._event(invocation_id, event_type, event_payload)

            await self.agent.process_direct(
                str(payload.get("message") or payload.get("content") or ""),
                session_key=current.session_key,
                channel=str(payload.get("channel") or "service"),
                chat_id=str(payload.get("chatId") or current.session_id),
                metadata=context,
                event_callback=entry_event,
                max_iterations=int(payload.get("maxIterations", self.config.max_iterations)),
                media=list(payload.get("media") or []),
                on_progress=progress,
            )
            terminal = await self.repository.get_invocation(invocation_id)
            if terminal and terminal.status in {
                InvocationStatus.SUCCEEDED, InvocationStatus.FAILED, InvocationStatus.TIMEOUT,
            }:
                await self._ack(invocation_id, entry_id)
        except _AlreadyTerminalError:
            await self._ack(invocation_id, entry_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Worker execution failed", extra={"correlation": {"invocationId": invocation_id}})
            current = await self.repository.get_invocation(invocation_id)
            if current and current.status in {
                InvocationStatus.SUCCEEDED, InvocationStatus.FAILED, InvocationStatus.TIMEOUT,
            }:
                await self._ack(invocation_id, entry_id)
        finally:
            self._inflight.pop(invocation_id, None)
            self._claimed.discard(invocation_id)

    async def _publish_once(self) -> None:
        relay = OutboxRelay(
            self.repository, self.transport,
            batch_size=self.config.outbox_batch_size,
            owner_id=self.config.owner_id, worker_id=self.execution_owner,
        )
        await relay.publish_once()

    async def _reconcile(self) -> None:
        """Republish durable QUEUED rows after Redis loss or Worker restart."""
        # Pending/new stream entries already represent the durable rows.  A
        # zero-length stream is the useful signal after Redis data loss.
        # The Outbox relay may have published entries which are still pending
        # in the consumer group.  Reconciliation is only needed when the
        # stream is genuinely empty and no pending entry remains.
        if await self.transport.depth() > 0:
            return
        for envelope in await self.repository.queued_envelopes(
            limit=self.config.worker_prefetch, owner_id=self.config.owner_id,
        ):
            try:
                entry_id = await self.transport.publish(envelope)
                await self.repository.append_event(envelope["invocationId"], "QUEUE", {
                    "step": "reconcile", "worker": self.execution_owner, "entry_id": entry_id,
                })
            except Exception:
                logger.exception("could not reconcile invocation %s", envelope.get("invocationId"))
                break

    async def _maintenance(self) -> None:
        while self._running:
            try:
                await self._publish_once()
                await self.repository.expire_queued(limit=self.config.worker_prefetch, worker_id=self.execution_owner)
                for invocation_id in tuple(self._claimed):
                    renewed = await self.repository.renew_lease(
                        invocation_id, self.execution_owner,
                        lease_seconds=self.config.worker_lease_seconds,
                    )
                    if not renewed:
                        current = await self.repository.get_invocation(invocation_id)
                        if current and current.status is InvocationStatus.RUNNING:
                            self._lost = True
                            self._running = False
                            for task in tuple(self._tasks):
                                task.cancel()
                            break
                now = asyncio.get_running_loop().time()
                if now >= self._next_retention_at:
                    await self.repository.purge_expired_events()
                    self._next_retention_at = now + 60.0
                if self._needs_reconcile or now >= self._next_reconcile_at:
                    await self._reconcile()
                    self._needs_reconcile = False
                    self._next_reconcile_at = now + 5.0
            except asyncio.CancelledError:
                raise
            except Exception:
                self._needs_reconcile = True
                logger.exception("Worker maintenance iteration failed")
            await asyncio.sleep(self.config.outbox_poll_seconds)

    async def _consume_once(self) -> None:
        if self._draining:
            return
        available = self.config.worker_prefetch - len(self._tasks)
        if available <= 0:
            await asyncio.sleep(self.config.queue_poll_seconds)
            return
        now = asyncio.get_running_loop().time()
        recovered = now >= self._next_pending_at
        entries = []
        if recovered:
            entries = await self.transport.recover_pending(
                min_idle_ms=self.config.worker_pending_idle_ms,
                count=available,
            )
            self._next_pending_at = now + 1.0
        if not entries:
            recovered = False
            entries = await self.transport.read(timeout=self.config.queue_poll_seconds, count=available)
        for entry_id, envelope in entries:
            if self._draining:
                break  # remains pending, replacement Worker reclaims it
            task = asyncio.create_task(self._run_entry(entry_id, envelope, recovered=recovered))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        if self._tasks:
            await asyncio.sleep(0)

    async def start(self) -> None:
        if self._running:
            return
        await self._acquire_singleton_lock()
        try:
            await self.transport.ensure_group()
            # A previous process can only leave uncertain RUNNING work.  It is
            # fenced as failed; QUEUED rows remain ordered and are republished.
            await self.repository.fail_all_running_worker_lost(owner_id=self.config.owner_id)
            self.agent = await self._build_agent()
            self._running = True
            self._draining = False
            self._lost = False
            self._stop_event.clear()
            self._needs_reconcile = True
            self._next_reconcile_at = 0.0
            self._next_pending_at = 0.0
            # Publish the committed Outbox before the first consume cycle.
            await self._publish_once()
            await self.repository.worker_heartbeat(self.config.owner_id, self.execution_owner, "RUNNING", start=True)
        except Exception:
            await self._release_singleton_lock()
            raise

    async def run(self) -> None:
        await self.start()
        maintenance = asyncio.create_task(self._maintenance())
        ownership = asyncio.create_task(self._watch_ownership())
        try:
            while self._running and not self._draining:
                await self._consume_once()
        except BaseException:
            if not self._draining:
                self._lost = True
            raise
        finally:
            try:
                grace = self.config.worker_drain_seconds if self._draining and not self._lost else 0
                await asyncio.wait_for(self.stop(worker_lost=self._lost, grace=grace),
                                       grace + self.config.cleanup_seconds)
            finally:
                maintenance.cancel()
                ownership.cancel()
                await asyncio.gather(maintenance, ownership, return_exceptions=True)

    async def _watch_ownership(self) -> None:
        """Stop consumption when the PostgreSQL advisory-lock connection dies."""
        while self._running and self._lock_connection is not None:
            try:
                await self._lock_connection.fetchval("SELECT 1")
                await self.repository.worker_heartbeat(
                    self.config.owner_id, self.execution_owner, "DRAINING" if self._draining else "RUNNING",
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Worker execution ownership connection was lost")
                self._lost = True
                self._running = False
                for task in tuple(self._tasks):
                    task.cancel()
                return
            await asyncio.sleep(self.config.worker_heartbeat_seconds)

    run_forever = run

    def request_drain(self) -> None:
        self._draining = True
        logger.info("Worker draining", extra={"correlation": {"worker": self.execution_owner,
                                                               "event_type": "WORKER_DRAIN"}})

    async def stop(self, *, worker_lost: bool = True, grace: float = 0) -> None:
        async with self._stop_lock:
            await self._stop(worker_lost=worker_lost, grace=grace)

    async def _stop(self, *, worker_lost: bool, grace: float) -> None:
        if not self._running and self._lock_connection is None:
            return
        self._draining = True
        self._lost = worker_lost
        if grace and not worker_lost:
            # Unclaimed work must stay QUEUED and not start after SIGTERM.
            for identifier, task in tuple(self._inflight.items()):
                if identifier not in self._claimed:
                    task.cancel()
            if self._tasks:
                await asyncio.wait(tuple(self._tasks), timeout=grace)
        self._running = False
        for task in tuple(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.agent is not None:
            await self.agent.shutdown()
            self.agent = None
        if worker_lost:
            # AgentLoop records cancellation while unwinding.  Reclassify only
            # this Worker's own uncertain claims as WORKER_LOST.
            await self.repository.reclassify_worker_cancellations(self.execution_owner)
        elif grace:
            await self.repository.reclassify_worker_cancellations(
                self.execution_owner, error_code="WORKER_DRAIN_TIMEOUT",
            )
        try:
            await self.repository.worker_heartbeat(self.config.owner_id, self.execution_owner, "STOPPED")
        except Exception:
            logger.exception("could not persist stopped Worker heartbeat")
        await self._release_singleton_lock()
        self._stop_event.set()
