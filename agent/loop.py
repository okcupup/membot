"""Agent loop: the core processing engine."""

from __future__ import annotations

import asyncio
import copy
import json
import re
import time
from collections import deque
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from loguru import logger

from membot.agent.context import ContextBuilder
from membot.agent.conversation_memory.engine import ConversationMemoryEngine
from membot.agent.execution import (
    ExecutionContext,
    reset_execution_context,
    set_execution_context,
)
from membot.agent.execution_result import ExecutionOutcome, ExecutionResult
from membot.agent.memory import MemoryStore
from membot.agent.subagent import SubagentManager
from membot.agent.tools.cron import CronTool
from membot.agent.tools.filesystem import EditFileTool, ListDirTool, ReadFileTool, WriteFileTool
from membot.agent.tools.message import MessageTool
from membot.agent.tools.registry import ToolRegistry
from membot.agent.tools.shell import ExecTool
from membot.agent.tools.spawn import SpawnTool
from membot.agent.tools.web import WebFetchTool, WebSearchTool
from membot.bus.events import InboundMessage, OutboundMessage
from membot.bus.queue import MessageBus
from membot.providers.base import LLMProvider
from membot.session.manager import Session, SessionManager

if TYPE_CHECKING:
    from membot.config.schema import ChannelsConfig, ExecToolConfig
    from membot.cron.service import CronService


class InvocationQueueFullError(RuntimeError):
    """Raised when the bounded Worker scheduler cannot accept more work."""


class InvocationQueueTimeoutError(TimeoutError):
    """Raised when an invocation waits too long before Worker admission."""


class InvocationExecutionTimeoutError(TimeoutError):
    """Raised when an admitted invocation exceeds its execution budget."""


@dataclass(slots=True)
class _ScheduledInvocation:
    """One accepted invocation waiting in, or executing from, a Session queue."""

    message: InboundMessage
    context: ExecutionContext
    future: asyncio.Future[OutboundMessage | None]
    accepted_at: float
    on_progress: Callable[..., Awaitable[None]] | None = None
    execution_task: asyncio.Task | None = None
    cancelled: bool = False


@dataclass(slots=True)
class _SessionState:
    """Mutable scheduler state owned by exactly one normalized Session key."""

    queue: deque[_ScheduledInvocation] = field(default_factory=deque)
    running: _ScheduledInvocation | None = None
    drain_task: asyncio.Task | None = None


class AgentLoop:
    """
    The agent loop is the core processing engine.

    It:
    1. Receives messages from the bus
    2. Builds context with history, memory, skills
    3. Calls the LLM
    4. Executes tool calls
    5. Sends responses back
    """

    _TOOL_RESULT_MAX_CHARS = 500

    def __init__(
        self,
        bus: MessageBus,
        provider: LLMProvider,
        workspace: Path,
        model: str | None = None,
        max_iterations: int = 40,
        temperature: float = 0.1,
        max_tokens: int = 4096,
        memory_window: int = 100,
        reasoning_effort: str | None = None,
        brave_api_key: str | None = None,
        exec_config: ExecToolConfig | None = None,
        cron_service: CronService | None = None,
        restrict_to_workspace: bool = False,
        session_manager: SessionManager | None = None,
        mcp_servers: dict | None = None,
        channels_config: ChannelsConfig | None = None,
        max_concurrent_invocations: int = 4,
        max_pending_invocations: int = 256,
        queue_timeout: float | None = None,
        execution_timeout: float | None = None,
        max_subagent_tasks: int = 16,
        max_concurrent_subagents: int = 4,
        enable_consolidation: bool = True,
        enable_subagents: bool = True,
        enable_cron: bool = True,
        max_concurrency: int | None = None,
        max_queue_size: int | None = None,
        memory_engine: ConversationMemoryEngine | None = None,
    ):
        from membot.config.schema import ExecToolConfig
        self.bus = bus
        self.channels_config = channels_config
        self.provider = provider
        self.workspace = workspace
        self.model = model or provider.get_default_model()
        self.max_iterations = max_iterations
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.memory_window = memory_window
        self.reasoning_effort = reasoning_effort
        self.brave_api_key = brave_api_key
        self.exec_config = exec_config or ExecToolConfig()
        self.cron_service = cron_service
        self.restrict_to_workspace = restrict_to_workspace
        self.enable_consolidation = enable_consolidation
        self.enable_subagents = enable_subagents
        self.enable_cron = enable_cron

        self.context = ContextBuilder(workspace)
        if memory_engine is None:
            self.sessions = session_manager or SessionManager(workspace)
            self.memory_engine = ConversationMemoryEngine.for_workspace(
                workspace, session_manager=self.sessions,
            )
        else:
            self.sessions = session_manager
            self.memory_engine = memory_engine
        self._durable_memory = bool(getattr(self.memory_engine.store, "durable", False))
        self.tools = ToolRegistry()
        self.subagents = SubagentManager(
            provider=provider,
            workspace=workspace,
            bus=bus,
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            reasoning_effort=reasoning_effort,
            brave_api_key=brave_api_key,
            exec_config=self.exec_config,
            restrict_to_workspace=restrict_to_workspace,
            max_tasks=max_subagent_tasks,
            max_concurrent=max_concurrent_subagents,
        )

        self._running = False
        self._mcp_servers = mcp_servers or {}
        self._mcp_stack: AsyncExitStack | None = None
        self._mcp_connected = False
        self._mcp_init_lock = asyncio.Lock()
        self._mcp_init_task: asyncio.Task | None = None

        self.max_concurrent_invocations = max(
            1, max_concurrency if max_concurrency is not None else max_concurrent_invocations
        )
        self.max_pending_invocations = max(
            1, max_queue_size if max_queue_size is not None else max_pending_invocations
        )
        self.queue_timeout = queue_timeout if queue_timeout and queue_timeout > 0 else None
        self.execution_timeout = (
            execution_timeout if execution_timeout and execution_timeout > 0 else None
        )
        self._execution_semaphore = asyncio.Semaphore(self.max_concurrent_invocations)
        self._scheduler_lock = asyncio.Lock()
        self._long_term_memory_lock = asyncio.Lock()
        self._session_states: dict[str, _SessionState] = {}
        self._pending_invocations = 0
        self._active_tasks: dict[str, set[asyncio.Task]] = {}
        self._accepting = True
        self._register_default_tools()

    def _register_default_tools(self) -> None:
        """Register the default set of tools."""
        allowed_dir = self.workspace if self.restrict_to_workspace else None
        for cls in (ReadFileTool, WriteFileTool, EditFileTool, ListDirTool):
            self.tools.register(cls(workspace=self.workspace, allowed_dir=allowed_dir))
        self.tools.register(ExecTool(
            working_dir=str(self.workspace),
            timeout=self.exec_config.timeout,
            restrict_to_workspace=self.restrict_to_workspace,
            path_append=self.exec_config.path_append,
        ))
        self.tools.register(WebSearchTool(api_key=self.brave_api_key))
        self.tools.register(WebFetchTool())
        self.tools.register(MessageTool(send_callback=self.bus.publish_outbound))
        if self.enable_subagents:
            self.tools.register(SpawnTool(manager=self.subagents))
        if self.cron_service and self.enable_cron:
            self.tools.register(CronTool(self.cron_service))

    async def _connect_mcp(self) -> None:
        """Connect to configured MCP servers once, sharing the in-flight task."""
        if self._mcp_connected or not self._mcp_servers:
            return

        async with self._mcp_init_lock:
            if self._mcp_connected:
                return
            task = self._mcp_init_task
            if task is None or task.done():
                task = asyncio.create_task(self._initialize_mcp())
                self._mcp_init_task = task

        try:
            # Shield the shared initializer: cancellation of one invocation must
            # not cancel initialization for all other invocations.
            await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Failed to connect MCP servers (will retry next message): {}", exc)
            async with self._mcp_init_lock:
                if self._mcp_init_task is task:
                    self._mcp_init_task = None

    async def _initialize_mcp(self) -> None:
        """Perform one MCP initialization attempt."""
        from membot.agent.tools.mcp import connect_mcp_servers

        stack = AsyncExitStack()
        try:
            await stack.__aenter__()
            await connect_mcp_servers(self._mcp_servers, self.tools, stack)
        except Exception:
            try:
                await stack.aclose()
            except Exception:
                pass
            raise
        self._mcp_stack = stack
        self._mcp_connected = True

    @staticmethod
    def _normalize_key(value: str) -> str:
        """Normalize a user/session key without changing its scope semantics."""
        value = str(value or "").strip()
        if ":" not in value:
            return value
        channel, chat_id = value.split(":", 1)
        return f"{channel.strip()}:{chat_id.strip()}"

    def _resolve_execution_context(
        self,
        msg: InboundMessage,
        session_key: str | None = None,
    ) -> ExecutionContext:
        """Resolve the real target before touching Session or tool state."""
        if msg.channel == "system":
            channel, chat_id = (
                msg.chat_id.split(":", 1)
                if ":" in msg.chat_id
                else ("cli", msg.chat_id)
            )
            channel = channel.strip() or "cli"
            chat_id = chat_id.strip() or "direct"
            key = self._normalize_key(f"{channel}:{chat_id}")
        else:
            channel = str(msg.channel or "cli").strip()
            chat_id = str(msg.chat_id or "direct").strip()
            requested = session_key if session_key is not None else msg.session_key_override
            key = self._normalize_key(requested or f"{channel}:{chat_id}")

        metadata = msg.metadata or {}
        return ExecutionContext(
            session_key=key,
            channel=channel,
            chat_id=chat_id,
            message_id=metadata.get("message_id") or metadata.get("messageId"),
            request_id=metadata.get("request_id") or metadata.get("requestId"),
            trace_id=metadata.get("trace_id") or metadata.get("traceId"),
            invocation_id=metadata.get("invocation_id") or metadata.get("invocationId"),
            owner_id=metadata.get("owner_id") or metadata.get("ownerId"),
            session_id=metadata.get("session_id") or metadata.get("sessionId"),
            session_seq=metadata.get("session_seq") or metadata.get("sessionSeq"),
            execution_owner=metadata.get("execution_owner") or metadata.get("executionOwner"),
        )

    def _build_invocation_tools(self, context: ExecutionContext) -> ToolRegistry:
        """Clone registered tools and bind routing state to this invocation."""
        tools = ToolRegistry()
        for name, template in self.tools.items():
            if isinstance(template, MessageTool):
                tool = template.clone_for_execution()
                tool.set_context(context.channel, context.chat_id, context.message_id)
            elif isinstance(template, SpawnTool):
                tool = template.clone_for_execution()
                tool.set_context(context.channel, context.chat_id)
                tool.set_session_key(context.session_key)
                tool.set_correlation(
                    context.request_id,
                    context.trace_id,
                    context.invocation_id,
                )
            elif isinstance(template, CronTool):
                tool = template.clone_for_execution()
                tool.set_context(context.channel, context.chat_id)
            else:
                clone = getattr(template, "clone_for_execution", None)
                tool = clone() if clone else copy.copy(template)
            tools.register(tool)
        return tools

    def _set_tool_context(
        self,
        channel: str,
        chat_id: str,
        message_id: str | None = None,
        *,
        tools: ToolRegistry | None = None,
        session_key: str | None = None,
    ) -> None:
        """Bind compatibility callers to a supplied, invocation-owned registry."""
        registry = tools or self.tools
        for name in ("message", "spawn", "cron"):
            if tool := registry.get(name):
                if name == "message" and hasattr(tool, "set_context"):
                    tool.set_context(channel, chat_id, message_id)
                elif name == "spawn" and hasattr(tool, "set_context"):
                    tool.set_context(channel, chat_id)
                    if session_key and hasattr(tool, "set_session_key"):
                        tool.set_session_key(session_key)
                elif name == "cron" and hasattr(tool, "set_context"):
                    tool.set_context(channel, chat_id)

    @staticmethod
    def _strip_think(text: str | None) -> str | None:
        """Remove <think>…</think> blocks that some models embed in content."""
        if not text:
            return None
        return re.sub(r"<think>[\s\S]*?</think>", "", text).strip() or None

    @staticmethod
    def _tool_hint(tool_calls: list) -> str:
        """Format tool calls as concise hint, e.g. 'web_search("query")'."""
        def _fmt(tc):
            args = (tc.arguments[0] if isinstance(tc.arguments, list) else tc.arguments) or {}
            val = next(iter(args.values()), None) if isinstance(args, dict) else None
            if not isinstance(val, str):
                return tc.name
            return f'{tc.name}("{val[:40]}…")' if len(val) > 40 else f'{tc.name}("{val}")'
        return ", ".join(_fmt(tc) for tc in tool_calls)

    async def _run_agent_loop(
        self,
        initial_messages: list[dict],
        on_progress: Callable[..., Awaitable[None]] | None = None,
        tools: ToolRegistry | None = None,
    ) -> ExecutionResult:
        """Run the agent iteration loop and classify its technical outcome."""
        messages = initial_messages
        registry = tools or self.tools
        iteration = 0
        final_content = None
        tools_used: list[str] = []

        while iteration < self.max_iterations:
            iteration += 1

            try:
                response = await self.provider.chat(
                    messages=messages,
                    tools=registry.get_definitions(),
                    model=self.model,
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                    reasoning_effort=self.reasoning_effort,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("LLM provider failed")
                return ExecutionResult.failure(
                    ExecutionOutcome.PROVIDER_ERROR,
                    final_content="Sorry, I encountered an error calling the AI model.",
                    messages=messages,
                    error_code="PROVIDER_ERROR",
                    error_message=str(exc),
                )

            if response.has_tool_calls:
                if on_progress:
                    clean = self._strip_think(response.content)
                    if clean:
                        await on_progress(clean)
                    await on_progress(self._tool_hint(response.tool_calls), tool_hint=True)

                tool_call_dicts = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.name,
                            "arguments": json.dumps(tc.arguments, ensure_ascii=False)
                        }
                    }
                    for tc in response.tool_calls
                ]
                messages = self.context.add_assistant_message(
                    messages, response.content, tool_call_dicts,
                    reasoning_content=response.reasoning_content,
                    thinking_blocks=response.thinking_blocks,
                )

                for tool_call in response.tool_calls:
                    tools_used.append(tool_call.name)
                    args_str = json.dumps(tool_call.arguments, ensure_ascii=False)
                    logger.info("Tool call: {}({})", tool_call.name, args_str[:200])
                    try:
                        result = await registry.execute(tool_call.name, tool_call.arguments)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.exception("Tool failed: {}", tool_call.name)
                        return ExecutionResult.failure(
                            ExecutionOutcome.TOOL_ERROR,
                            messages=messages,
                            tools_used=tools_used,
                            error_code="TOOL_ERROR",
                            error_message=str(exc),
                        )
                    messages = self.context.add_tool_result(
                        messages, tool_call.id, tool_call.name, result
                    )
                    if isinstance(result, str) and result.lstrip().startswith("Error"):
                        return ExecutionResult.failure(
                            ExecutionOutcome.TOOL_ERROR,
                            messages=messages,
                            tools_used=tools_used,
                            error_code="TOOL_ERROR",
                            error_message=result,
                        )
            else:
                clean = self._strip_think(response.content)
                # Don't persist error responses to session history — they can
                # poison the context and cause permanent 400 loops (#1303).
                if response.finish_reason == "error":
                    logger.error("LLM returned error: {}", (clean or "")[:200])
                    return ExecutionResult.failure(
                        ExecutionOutcome.PROVIDER_ERROR,
                        final_content=clean or "Sorry, I encountered an error calling the AI model.",
                        messages=messages,
                        error_code="PROVIDER_ERROR",
                        error_message=clean,
                    )
                if clean is None:
                    return ExecutionResult.failure(
                        ExecutionOutcome.PROVIDER_ERROR,
                        messages=messages,
                        error_code="MISSING_FINAL",
                        error_message="provider returned no final content",
                    )
                messages = self.context.add_assistant_message(
                    messages, clean, reasoning_content=response.reasoning_content,
                    thinking_blocks=response.thinking_blocks,
                )
                final_content = clean
                break

        if final_content is None and iteration >= self.max_iterations:
            logger.warning("Max iterations ({}) reached", self.max_iterations)
            return ExecutionResult.failure(
                ExecutionOutcome.ITERATION_LIMIT,
                messages=messages,
                tools_used=tools_used,
                error_code="ITERATION_LIMIT",
                error_message=f"maximum iterations reached: {self.max_iterations}",
            )

        return ExecutionResult(
            outcome=ExecutionOutcome.FINAL,
            final_content=final_content,
            messages=messages,
            tools_used=tools_used,
        )

    async def run(self) -> None:
        """Consume the native bus and submit each message to the shared scheduler."""
        self._running = True
        await self._connect_mcp()
        logger.info("Agent loop started")

        while self._running:
            try:
                msg = await asyncio.wait_for(self.bus.consume_inbound(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if msg.content.strip().lower() == "/stop":
                await self._handle_stop(msg)
            else:
                task = asyncio.create_task(self._dispatch(msg))
                key = self._resolve_execution_context(msg).session_key
                self._active_tasks.setdefault(key, set()).add(task)

                def _cleanup(done: asyncio.Task, session_key: str = key) -> None:
                    tasks = self._active_tasks.get(session_key)
                    if not tasks:
                        return
                    tasks.discard(done)
                    if not tasks:
                        self._active_tasks.pop(session_key, None)

                task.add_done_callback(_cleanup)

    async def _handle_stop(self, msg: InboundMessage) -> None:
        """Cancel all active tasks and subagents for the session."""
        key = self._resolve_execution_context(msg).session_key
        tasks = list(self._active_tasks.pop(key, set()))
        cancelled = sum(1 for t in tasks if not t.done() and t.cancel())
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        sub_cancelled = await self.subagents.cancel_by_session(key)
        total = cancelled + sub_cancelled
        content = f"⏹ Stopped {total} task(s)." if total else "No active task to stop."
        await self.bus.publish_outbound(OutboundMessage(
            channel=key.split(":", 1)[0] if ":" in key else msg.channel,
            chat_id=key.split(":", 1)[1] if ":" in key else msg.chat_id,
            content=content,
            metadata=msg.metadata or {},
        ))

    async def _dispatch(self, msg: InboundMessage) -> None:
        """Submit a bus message; execution is serialized by its effective Session."""
        context = self._resolve_execution_context(msg)
        try:
            await self._execute_entry(msg, context=context, publish=True)
        except asyncio.CancelledError:
            logger.info("Task cancelled for session {}", context.session_key)
            raise
        except Exception:
            logger.exception("Error processing message for session {}", context.session_key)
            await self.bus.publish_outbound(OutboundMessage(
                channel=context.channel,
                chat_id=context.chat_id,
                content="Sorry, I encountered an error.",
                metadata=msg.metadata or {},
            ))

    async def _execute_entry(
        self,
        msg: InboundMessage,
        *,
        context: ExecutionContext | None = None,
        session_key: str | None = None,
        on_progress: Callable[..., Awaitable[None]] | None = None,
        publish: bool,
    ) -> OutboundMessage | None:
        """Single execution entrance used by bus, direct CLI, and future services."""
        context = context or self._resolve_execution_context(msg, session_key)
        response = await self._submit_invocation(msg, context, on_progress)
        if publish:
            if response is not None:
                await self.bus.publish_outbound(response)
            elif msg.channel == "cli":
                await self.bus.publish_outbound(OutboundMessage(
                    channel=context.channel,
                    chat_id=context.chat_id,
                    content="",
                    metadata=msg.metadata or {},
                ))
        return response

    async def _submit_invocation(
        self,
        msg: InboundMessage,
        context: ExecutionContext,
        on_progress: Callable[..., Awaitable[None]] | None,
    ) -> OutboundMessage | None:
        """Enqueue work without consuming the Worker semaphore while waiting."""
        async with self._scheduler_lock:
            if not self._accepting:
                raise RuntimeError("Agent loop is shutting down")
            if self._pending_invocations >= self.max_pending_invocations:
                raise InvocationQueueFullError(
                    f"invocation queue is full ({self.max_pending_invocations})"
                )
            future: asyncio.Future[OutboundMessage | None] = asyncio.get_running_loop().create_future()
            item = _ScheduledInvocation(
                message=msg,
                context=context,
                future=future,
                accepted_at=time.monotonic(),
                on_progress=on_progress,
            )
            state = self._session_states.setdefault(context.session_key, _SessionState())
            state.queue.append(item)
            self._pending_invocations += 1
            if state.drain_task is None:
                state.drain_task = asyncio.create_task(self._drain_session(context.session_key))

        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            await self._cancel_invocation(item)
            raise

    async def _cancel_invocation(self, item: _ScheduledInvocation) -> None:
        """Remove a queued item or cancel its isolated execution task."""
        execution_task: asyncio.Task | None = None
        async with self._scheduler_lock:
            if item.future.done() or item.cancelled:
                return
            item.cancelled = True
            for key, state in tuple(self._session_states.items()):
                if item in state.queue:
                    state.queue.remove(item)
                    self._pending_invocations -= 1
                    if not state.queue and state.running is None and state.drain_task is None:
                        self._session_states.pop(key, None)
                    item.future.cancel()
                    return
                if state.running is item:
                    execution_task = item.execution_task
                    break
        if execution_task and not execution_task.done():
            execution_task.cancel()
            await asyncio.gather(execution_task, return_exceptions=True)
        if not item.future.done():
            item.future.cancel()

    async def _drain_session(self, session_key: str) -> None:
        """Run only the Session head, then release state when it becomes empty."""
        try:
            while True:
                async with self._scheduler_lock:
                    state = self._session_states.get(session_key)
                    if state is None:
                        return
                    while state.queue and state.queue[0].cancelled:
                        state.queue.popleft()
                    if not state.queue:
                        state.drain_task = None
                        if state.running is None:
                            self._session_states.pop(session_key, None)
                        return
                    item = state.queue.popleft()
                    state.running = item

                item.execution_task = asyncio.create_task(self._run_scheduled(item))
                try:
                    result = await item.execution_task
                except asyncio.CancelledError:
                    if not item.future.done():
                        item.future.cancel()
                    # A shutdown cancellation targets the drain task itself.
                    # Do not swallow it and start the next queued invocation.
                    if asyncio.current_task() and asyncio.current_task().cancelling():
                        raise
                except Exception as exc:
                    if not item.future.done():
                        item.future.set_exception(exc)
                else:
                    if not item.future.done():
                        item.future.set_result(result)
                finally:
                    async with self._scheduler_lock:
                        state = self._session_states.get(session_key)
                        if state is not None:
                            state.running = None
                        self._pending_invocations -= 1
        finally:
            # A normal drain exits through the branch above. This fallback keeps
            # cancellation/shutdown from retaining an empty Session state.
            async with self._scheduler_lock:
                state = self._session_states.get(session_key)
                if state is not None and state.drain_task is asyncio.current_task():
                    state.drain_task = None
                    for queued in state.queue:
                        queued.cancelled = True
                        if not queued.future.done():
                            queued.future.cancel()
                    state.queue.clear()
                    if state.running is not None and not state.running.future.done():
                        state.running.future.cancel()
                    self._session_states.pop(session_key, None)

    async def _run_scheduled(self, item: _ScheduledInvocation) -> OutboundMessage | None:
        """Admit one Session head, apply deadlines, and always release capacity."""
        remaining_queue = None
        if self.queue_timeout is not None:
            remaining_queue = self.queue_timeout - (time.monotonic() - item.accepted_at)
            if remaining_queue <= 0:
                raise InvocationQueueTimeoutError("invocation expired while waiting for Worker admission")

        acquired = False
        try:
            if remaining_queue is None:
                await self._execution_semaphore.acquire()
            else:
                try:
                    await asyncio.wait_for(self._execution_semaphore.acquire(), remaining_queue)
                except asyncio.TimeoutError as exc:
                    raise InvocationQueueTimeoutError(
                        "invocation expired while waiting for Worker admission"
                    ) from exc
            acquired = True

            token = set_execution_context(item.context)
            try:
                await self._connect_mcp()
                process = self._process_message(
                    item.message,
                    session_key=item.context.session_key,
                    on_progress=item.on_progress,
                    execution_context=item.context,
                )
                if self.execution_timeout is None:
                    return await process
                try:
                    return await asyncio.wait_for(process, self.execution_timeout)
                except asyncio.TimeoutError as exc:
                    raise InvocationExecutionTimeoutError(
                        "invocation exceeded the execution timeout"
                    ) from exc
            finally:
                reset_execution_context(token)
        except asyncio.CancelledError:
            await self._finish_durable_interruption(item.context, ExecutionOutcome.CANCELLED, "invocation cancelled")
            raise
        except InvocationQueueTimeoutError as exc:
            await self._finish_durable_interruption(item.context, ExecutionOutcome.TIMEOUT, str(exc))
            raise
        except InvocationExecutionTimeoutError as exc:
            await self._finish_durable_interruption(item.context, ExecutionOutcome.TIMEOUT, str(exc))
            raise
        except Exception as exc:
            await self._finish_durable_interruption(item.context, ExecutionOutcome.INTERNAL_ERROR, str(exc))
            raise
        finally:
            if acquired:
                self._execution_semaphore.release()

    async def _finish_durable_interruption(
        self,
        context: ExecutionContext,
        outcome: ExecutionOutcome,
        message: str,
    ) -> None:
        """Best-effort terminal persistence for cancellation and Worker errors."""
        if not self._durable_memory or not context.invocation_id:
            return
        finish = getattr(self.memory_engine.store, "finish_interrupted", None)
        if finish is None:
            return
        try:
            await finish(
                context.invocation_id,
                outcome,
                error_message=message,
                execution_owner=context.execution_owner,
            )
        except Exception:
            logger.exception("Unable to persist durable interruption for {}", context.invocation_id)

    async def close_mcp(self) -> None:
        """Close MCP connections."""
        if self._mcp_init_task and not self._mcp_init_task.done():
            try:
                await asyncio.shield(self._mcp_init_task)
            except (asyncio.CancelledError, Exception):
                pass
        if self._mcp_stack:
            try:
                await self._mcp_stack.aclose()
            except (RuntimeError, BaseExceptionGroup):
                pass  # MCP SDK cancel scope cleanup is noisy but harmless
            self._mcp_stack = None
        self._mcp_connected = False
        self._mcp_init_task = None

    def stop(self) -> None:
        """Stop the agent loop."""
        self._running = False
        logger.info("Agent loop stopping")

    async def shutdown(self) -> None:
        """Cancel in-flight work, clear scheduler state, and close resources."""
        self.stop()
        async with self._scheduler_lock:
            self._accepting = False
        active = {
            task
            for tasks in self._active_tasks.values()
            for task in tasks
            if not task.done()
        }
        for task in active:
            task.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)

        async with self._scheduler_lock:
            drains = {
                state.drain_task
                for state in self._session_states.values()
                if state.drain_task is not None and not state.drain_task.done()
            }
        for task in drains:
            task.cancel()
        if drains:
            await asyncio.gather(*drains, return_exceptions=True)

        async with self._scheduler_lock:
            for state in self._session_states.values():
                for item in state.queue:
                    item.cancelled = True
                    if not item.future.done():
                        item.future.cancel()
                if state.running and not state.running.future.done():
                    state.running.future.cancel()
            self._session_states.clear()
            self._active_tasks.clear()
            self._pending_invocations = 0

        await self.subagents.cancel_all()
        await self.close_mcp()

    async def _process_message(
        self,
        msg: InboundMessage,
        session_key: str | None = None,
        on_progress: Callable[[str], Awaitable[None]] | None = None,
        execution_context: ExecutionContext | None = None,
    ) -> OutboundMessage | None:
        """Process one message with an invocation-local context and tool set."""
        context = execution_context or self._resolve_execution_context(msg, session_key)
        token = set_execution_context(context)
        try:
            return await self._process_message_with_context(msg, context, on_progress)
        finally:
            reset_execution_context(token)

    async def _process_message_with_context(
        self,
        msg: InboundMessage,
        context: ExecutionContext,
        on_progress: Callable[[str], Awaitable[None]] | None,
    ) -> OutboundMessage | None:
        """Run the complete read/build/LLM-tool/save range for one Session head."""
        tools = self._build_invocation_tools(context)

        async def _commit(result: ExecutionResult, all_messages: list[dict], skip: int) -> None:
            if self._durable_memory:
                if not context.invocation_id:
                    raise RuntimeError("durable execution requires invocation_id in ExecutionContext")
                await self.memory_engine.finish_invocation(
                    context.invocation_id,
                    context.session_key,
                    result,
                    all_messages,
                    skip,
                    execution_owner=context.execution_owner,
                )
            elif result.technical_success:
                await self.memory_engine.save_turn(context.session_key, all_messages, skip)

        if msg.channel == "system":
            logger.info("Processing system message from {}", msg.sender_id)
            key = context.session_key
            history = await self.memory_engine.get_history(key, self.memory_window)
            messages = self.context.build_messages(
                history=history, current_message=msg.content,
                channel=context.channel, chat_id=context.chat_id,
            )
            result = await self._run_agent_loop(messages, tools=tools)
            await _commit(result, result.messages, 1 + len(history))
            return OutboundMessage(
                channel=context.channel,
                chat_id=context.chat_id,
                content=result.final_content or "Background task failed.",
                metadata=msg.metadata or {},
            )

        preview = msg.content[:80] + "..." if len(msg.content) > 80 else msg.content
        logger.info("Processing message from {}:{}: {}", msg.channel, msg.sender_id, preview)

        key = context.session_key
        session = None if self._durable_memory else self.memory_engine.store.get_or_create_session(key)

        # Slash commands
        cmd = msg.content.strip().lower()
        if cmd == "/new":
            if self._durable_memory:
                if not context.invocation_id:
                    raise RuntimeError("durable /new requires invocation_id")
                await self.memory_engine.archive_and_finish_new(
                    context.invocation_id,
                    execution_owner=context.execution_owner,
                )
                return OutboundMessage(
                    channel=context.channel, chat_id=context.chat_id,
                    content="New session started.", metadata=msg.metadata or {},
                )
            try:
                snapshot = session.messages[session.last_consolidated:]
                if snapshot:
                    temp = Session(key=session.key)
                    temp.messages = list(snapshot)
                    if not await self._consolidate_memory(temp, archive_all=True):
                        return OutboundMessage(
                            channel=context.channel, chat_id=context.chat_id,
                            content="Memory archival failed, session not cleared. Please try again.",
                            metadata=msg.metadata or {},
                        )
            except Exception:
                logger.exception("/new archival failed for {}", session.key)
                return OutboundMessage(
                    channel=context.channel, chat_id=context.chat_id,
                    content="Memory archival failed, session not cleared. Please try again.",
                    metadata=msg.metadata or {},
                )

            await self.memory_engine.clear(session.key)
            return OutboundMessage(
                channel=context.channel, chat_id=context.chat_id,
                content="New session started.", metadata=msg.metadata or {},
            )
        if cmd == "/help":
            if self._durable_memory:
                result = ExecutionResult(
                    outcome=ExecutionOutcome.FINAL,
                    final_content="🐈 membot commands:\n/new — Start a new conversation\n/stop — Stop the current task\n/help — Show available commands",
                    messages=[],
                )
                await _commit(result, [], 0)
            return OutboundMessage(
                channel=context.channel, chat_id=context.chat_id,
                content="🐈 membot commands:\n/new — Start a new conversation\n/stop — Stop the current task\n/help — Show available commands",
                metadata=msg.metadata or {},
            )

        unconsolidated = len(session.messages) - session.last_consolidated if session else 0
        if self.enable_consolidation and unconsolidated >= self.memory_window:
            # Consolidation is deliberately inline in M1. It shares the same
            # Session consistency range and cannot race the next turn.
            await self._consolidate_memory(session)

        if message_tool := tools.get("message"):
            if isinstance(message_tool, MessageTool):
                message_tool.start_turn()

        history = await self.memory_engine.get_history(key, self.memory_window)
        initial_messages = self.context.build_messages(
            history=history,
            current_message=msg.content,
            media=msg.media if msg.media else None,
            channel=context.channel, chat_id=context.chat_id,
        )

        async def _bus_progress(content: str, *, tool_hint: bool = False) -> None:
            meta = dict(msg.metadata or {})
            meta["_progress"] = True
            meta["_tool_hint"] = tool_hint
            await self.bus.publish_outbound(OutboundMessage(
                channel=context.channel, chat_id=context.chat_id, content=content, metadata=meta,
            ))

        result = await self._run_agent_loop(
            initial_messages, on_progress=on_progress or _bus_progress,
            tools=tools,
        )

        await _commit(result, result.messages, 1 + len(history))
        final_content = result.final_content
        if final_content is None:
            if result.outcome is ExecutionOutcome.ITERATION_LIMIT:
                final_content = (
                    f"I reached the maximum number of tool call iterations ({self.max_iterations}) "
                    "without completing the task. You can try breaking the task into smaller steps."
                )
            elif result.outcome is not ExecutionOutcome.PROVIDER_ERROR:
                final_content = "Sorry, I encountered an error calling the AI model."

        if (mt := tools.get("message")) and isinstance(mt, MessageTool) and mt._sent_in_turn:
            return None

        preview = final_content[:120] + "..." if len(final_content) > 120 else final_content
        logger.info("Response to {}:{}: {}", msg.channel, msg.sender_id, preview)
        return OutboundMessage(
            channel=context.channel, chat_id=context.chat_id, content=final_content,
            metadata=msg.metadata or {},
        )

    def _save_turn(self, session: Session, messages: list[dict], skip: int) -> None:
        """Save new-turn messages into session, truncating large tool results."""
        from datetime import datetime
        for m in messages[skip:]:
            entry = dict(m)
            role, content = entry.get("role"), entry.get("content")
            if role == "assistant" and not content and not entry.get("tool_calls"):
                continue  # skip empty assistant messages — they poison session context
            if role == "tool" and isinstance(content, str) and len(content) > self._TOOL_RESULT_MAX_CHARS:
                entry["content"] = content[:self._TOOL_RESULT_MAX_CHARS] + "\n... (truncated)"
            elif role == "user":
                if isinstance(content, str) and content.startswith(ContextBuilder._RUNTIME_CONTEXT_TAG):
                    continue
                if isinstance(content, list):
                    entry["content"] = [
                        {"type": "text", "text": "[image]"} if (
                            c.get("type") == "image_url"
                            and c.get("image_url", {}).get("url", "").startswith("data:image/")
                        ) else c for c in content
                    ]
            entry.setdefault("timestamp", datetime.now().isoformat())
            session.messages.append(entry)
        session.updated_at = datetime.now()

    async def _consolidate_memory(self, session, archive_all: bool = False) -> bool:
        """Delegate to MemoryStore.consolidate(). Returns True on success."""
        async with self._long_term_memory_lock:
            return await MemoryStore(self.workspace).consolidate(
                session, self.provider, self.model,
                archive_all=archive_all, memory_window=self.memory_window,
            )

    async def process_direct(
        self,
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        on_progress: Callable[[str], Awaitable[None]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Process direct/cron work through the same Session scheduler as the bus."""
        msg = InboundMessage(
            channel=channel,
            sender_id="user",
            chat_id=chat_id,
            content=content,
            metadata=metadata or {},
        )
        context = self._resolve_execution_context(msg, session_key)
        response = await self._execute_entry(
            msg,
            context=context,
            on_progress=on_progress,
            publish=False,
        )
        return response.content if response else ""
