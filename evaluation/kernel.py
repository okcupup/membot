"""Instrument the actual Session scheduler, memory engine and Tool loop."""

from __future__ import annotations

import asyncio
import copy
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from membot.agent.diagnostics import DiagnosticPolicy
from membot.agent.execution import get_execution_context
from membot.agent.loop import AgentLoop
from membot.bus.events import InboundMessage
from membot.bus.queue import MessageBus

from .fixtures import FixtureProvider, RecordedFixtureRegistry, fixture_registry, prepare_workspace
from .schema import Case


class ObservedAgent(AgentLoop):
    """Observation hook only; all execution and persistence use production code."""

    def _build_invocation_tools(self, context):
        tools = super()._build_invocation_tools(context)
        if isinstance(self.tools, RecordedFixtureRegistry):
            recorded = RecordedFixtureRegistry()
            for _, tool in tools.items():
                recorded.register(tool)
            return recorded
        return tools

    async def _run_agent_loop(self, *args, **kwargs):
        result = await super()._run_agent_loop(*args, **kwargs)
        self.execution_results[get_execution_context().invocation_id] = result
        return result


async def _pending(agent, count):
    while agent._pending_invocations < count:
        await asyncio.sleep(0)


async def run_kernel(case: Case, *, repeat: int = 0, provider=None, fault=None, runtime_budget=None) -> dict:
    budget = runtime_budget or case.budget
    with TemporaryDirectory(prefix="membot-eval-") as temporary:
        workspace = Path(temporary)
        prepare_workspace(case, workspace)
        bus = MessageBus(inbound_maxsize=64, outbound_maxsize=128)
        provider = provider or FixtureProvider(case, fault=fault)
        agent = ObservedAgent(
            bus=bus, provider=provider, workspace=workspace, model=provider.get_default_model(),
            max_iterations=budget.max_iterations, temperature=0.0, max_tokens=1024,
            max_concurrent_invocations=1 if case.scenario == "queue_timeout" else 2,
            max_pending_invocations=32,
            queue_timeout=budget.queue_seconds,
            execution_timeout=budget.execution_seconds,
            llm_timeout=budget.llm_seconds, tool_timeout=budget.tool_seconds,
            enable_consolidation=False, enable_subagents=False, enable_cron=False,
            restrict_to_workspace=True,
        )
        agent.execution_results = {}
        agent.tools = fixture_registry(case, workspace, bus, fault=fault)
        if case.history:
            await agent.memory_engine.save_turn(f"eval:{case.turns[0].session}", case.history, 0)
        observations = {}
        starts = {}
        contracts = []
        tasks = []

        async def admitted(context):
            starts[context.invocation_id] = time.monotonic()
            return True

        agent.admission_callback = admitted

        async def execute(turn):
            invocation = str(uuid.uuid4())
            request, trace = str(uuid.uuid4()), str(uuid.uuid4())
            row = {"turn": turn.id, "invocationId": invocation, "requestId": request,
                   "traceId": trace, "sessionId": turn.session, "events": [{
                       "sequence": 1, "event_type": "ACCEPTED", "step": "observed",
                       "payload": {"source": "in_process_observer"},
                       "invocationId": invocation, "requestId": request, "traceId": trace,
                       "sessionId": turn.session}], "accepted": True,
                   "status": "QUEUED", "error_code": None, "final": None, "delivery": None}
            observations[turn.id] = row
            accepted_at = time.monotonic()

            async def event(category, payload):
                row["events"].append({"sequence": len(row["events"]) + 1,
                    "invocationId": invocation, "requestId": request, "traceId": trace,
                    "sessionId": turn.session, "time": datetime.now(timezone.utc).isoformat(),
                    "event_type": category, "step": payload.get("step"),
                    "span_id": payload.get("span_id"), "tool_call_id": payload.get("tool_call_id"),
                    "error_code": payload.get("error_code"), "duration_ms": payload.get("duration_ms"),
                    "payload": DiagnosticPolicy().capture(copy.deepcopy(payload))})

            metadata = {"invocationId": invocation, "requestId": request, "traceId": trace,
                        "message_id": f"m-{turn.id}", "sessionId": turn.session}
            if isinstance(provider, FixtureProvider):
                provider.request_plans[request] = turn.provider
            try:
                if case.scenario == "callback":
                    agent.event_callback = event
                    await agent._dispatch(InboundMessage(channel="system", sender_id="callback",
                        chat_id=f"eval:{turn.session}", content=turn.input, metadata=metadata))
                else:
                    row["delivery"] = await agent.process_direct(turn.input,
                        session_key=f"eval:{turn.session}", channel=turn.channel,
                        chat_id=turn.chat_id or turn.session, metadata=metadata, event_callback=event)
                result = agent.execution_results.get(invocation)
                if result is None:
                    raise RuntimeError("execution did not produce a structured result")
                row.update(status="SUCCEEDED" if result.technical_success else
                           "TIMEOUT" if result.outcome.value == "TIMEOUT" else "FAILED",
                           error_code=result.error_code, final=result.final_content)
            except asyncio.CancelledError:
                row.update(status="FAILED", error_code="CANCELLED")
            except Exception as exc:
                name = type(exc).__name__
                code = "QUEUE_TIMEOUT" if name == "InvocationQueueTimeoutError" else \
                       "EXECUTION_TIMEOUT" if name == "InvocationExecutionTimeoutError" else "HARNESS_ERROR"
                row.update(status="TIMEOUT" if "TIMEOUT" in code else "FAILED", error_code=code)
                if code == "HARNESS_ERROR":
                    row["harness_error"] = str(exc)
            finally:
                finished_at = time.monotonic()
                running_at = starts.get(invocation)
                row["e2e_ms"] = (finished_at - accepted_at) * 1000
                row["queue_wait_ms"] = ((running_at or finished_at) - accepted_at) * 1000
                row["execution_ms"] = (finished_at - running_at) * 1000 if running_at else None
            return row

        def launch(turn):
            task = asyncio.create_task(execute(turn))
            tasks.append(task)
            return task

        async def schedule():
            if case.scenario in {"single", "callback"}:
                for turn in case.turns:
                    await launch(turn)
            elif case.scenario in {"cancel", "queue_timeout"}:
                first = launch(case.turns[0])
                await provider.entered[1].wait()
                if case.scenario == "cancel":
                    first.cancel()
                    await first
                    contracts.append({"name": "provider_cancelled", "passed": provider.cancelled.is_set()})
                    await launch(case.turns[1])
                elif case.scenario == "queue_timeout":
                    second = launch(case.turns[1])
                    await second  # execute() records the expected queue deadline
                    first.cancel()
                    await asyncio.gather(first, return_exceptions=True)
                    provider.release.set()
                    await launch(case.turns[2])
            else:
                for turn in case.turns:
                    launch(turn)
                await _pending(agent, len(case.turns))
                expected = 1 if case.scenario == "serial" else 2
                await provider.entered[expected].wait()
                contracts.append({"name": "overlap_and_semaphore", "passed": provider.active == expected
                                  and provider.max_active == expected})
                if case.scenario == "hot":
                    contracts.append({"name": "cold_session_advances", "passed":
                                      provider.starts[:2] == [case.turns[0].input, case.turns[-1].input]})
                provider.release.set()
                await asyncio.gather(*tasks)

        try:
            await asyncio.wait_for(schedule(), budget.case_seconds)
        except Exception as exc:
            contracts.append({"name": "harness_complete", "passed": False, "reason": str(exc)})
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            contracts.append({"name": "resources_released", "passed": agent._pending_invocations == 0
                              and not agent._session_states})
            await agent.shutdown()
        outbound = []
        while not bus.outbound.empty():
            message = bus.outbound.get_nowait()
            if not message.metadata.get("_progress"):
                outbound.append({"channel": message.channel, "chat_id": message.chat_id,
                                 "content": message.content})
        if case.scenario == "serial":
            contracts.append({"name": "acceptance_order", "passed": provider.starts == [t.input for t in case.turns]})
        if fault == "wrong_status":
            for row in observations.values():
                if row["status"] == "FAILED":
                    row["status"] = "SUCCEEDED"
        return {"case_id": case.id, "case_hash": case.case_hash, "repeat": repeat,
                "mode": "deterministic", "invocations": list(observations.values()),
                "contracts": contracts, "outbound": outbound}
