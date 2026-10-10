"""Safe deterministic adapters injected into the production AgentLoop.

No shell, network, MCP or real external writes are registered in either mode.
"""

from __future__ import annotations

import asyncio
import copy
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from membot.agent.execution import get_execution_context
from membot.agent.tools.base import Tool
from membot.agent.tools.message import MessageTool
from membot.agent.tools.registry import ToolRegistry
from membot.providers.base import LLMProvider, LLMResponse, ToolCallRequest

from .schema import Case

TOOL_SPECS = {
    "calculate": ("Compute addition or multiplication.", {"operation": {"type": "string", "enum": ["add", "multiply"]}, "a": {"type": "number"}, "b": {"type": "number"}}),
    "read_note": ("Read a note in the isolated fixture workspace.", {"path": {"type": "string"}}),
    "write_note": ("Write a note in the isolated fixture workspace.", {"path": {"type": "string"}, "content": {"type": "string"}}),
    "lookup": ("Look up a supplied inventory record.", {"key": {"type": "string"}}),
    "notify": ("Record a notification in the fixture ledger.", {"recipient": {"type": "string"}, "text": {"type": "string"}}),
    "record_context": ("Return the current invocation routing context.", {}),
    "fail_tool": ("Controlled Tool error fixture.", {"action": {"type": "string"}}),
}


class FixtureProvider(LLMProvider):
    def __init__(self, case: Case, *, fault: str | None = None):
        super().__init__(api_key="eval-fixture")
        self.case = case
        self.fault = fault
        self.plans = {turn.input: turn.provider for turn in case.turns}
        self.request_plans: dict[str, list[dict[str, Any]]] = {}
        self.calls: dict[str, int] = defaultdict(int)
        self.active = 0
        self.max_active = 0
        self.starts: list[str] = []
        self.entered: dict[int, asyncio.Event] = defaultdict(asyncio.Event)
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.gate = case.scenario in {"serial", "parallel", "limit", "hot", "context", "message", "queue_timeout"}

    def get_default_model(self):
        return "m6-fixture-v1"

    async def chat(self, messages, **kwargs):
        del kwargs
        context = get_execution_context()
        if context is None:
            raise RuntimeError("fixture executed outside Invocation context")
        key = context.invocation_id
        current = next(message["content"] for message in reversed(messages)
                       if message.get("role") == "user")
        if not isinstance(current, str) or current not in self.plans:
            raise RuntimeError("no fixture plan for current input")
        index = self.calls[key]
        self.calls[key] += 1
        plan = self.request_plans.get(context.request_id, self.plans[current])
        spec = copy.deepcopy(plan[min(index, len(plan) - 1)])
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.starts.append(current)
        self.entered[len(self.starts)].set()
        try:
            if self.gate and index == 0:
                await self.release.wait()
            if spec.get("hang"):
                await asyncio.Event().wait()
            if spec.get("raise"):
                raise RuntimeError(spec["raise"])
            if spec.get("from_tool"):
                spec["content"] = next(message["content"] for message in reversed(messages)
                                       if message.get("role") == "tool")
            if "from_history" in spec:
                marker = spec["from_history"]
                previous = [str(message.get("content", "")) for message in messages[:-1]
                            if message.get("role") == "user"]
                spec["content"] = next((text for text in previous if marker in text), "MISSING_HISTORY")
            if self.fault == "context_leak" and "from_history" in spec:
                spec["content"] = "other-session-private-value"
            if self.fault == "missing_tool" and spec.get("tool_calls"):
                spec = {"content": "Done. I completed every requested action."}
            await asyncio.sleep(0)
            return LLMResponse(content=spec.get("content"),
                               finish_reason=spec.get("finish_reason", "stop"),
                               tool_calls=[ToolCallRequest(**call) for call in spec.get("tool_calls", [])])
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        finally:
            self.active -= 1


class FixtureTool(Tool):
    def __init__(self, name: str, workspace: Path, fixtures: dict[str, Any], *, fault=None):
        self._name = name
        self.workspace = workspace
        self.fixtures = fixtures
        self.fault = fault
        self.recorded_index = 0

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        return TOOL_SPECS.get(self.name, ("Recorded fixture; no external operations.", {}))[0]

    @property
    def parameters(self):
        recorded = self.fixtures.get("tool_schemas", {}).get(self.name)
        if recorded:
            return recorded
        props = TOOL_SPECS.get(self.name, ("", {}))[1]
        return {"type": "object", "properties": props, "required": list(props), "additionalProperties": False}

    def path(self, name: str) -> Path:
        target = (self.workspace / name).resolve()
        if not target.is_relative_to(self.workspace.resolve()):
            raise ValueError("fixture path escapes isolated workspace")
        return target

    async def execute(self, **params):
        context = get_execution_context()
        if self.fault == "tool_error":
            raise RuntimeError("injected unexpected Tool error")
        recorded = self.fixtures.get("recorded_tools", {}).get(self.name)
        if recorded is not None:
            if self.recorded_index >= len(recorded):
                raise RuntimeError("recorded tool responses exhausted")
            item = recorded[self.recorded_index]
            self.recorded_index += 1
            if params != item["arguments"]:
                raise RuntimeError("recorded tool arguments mismatch")
            if item.get("hang"):
                await asyncio.Event().wait()
            return item["result"]
        if self.name == "calculate":
            return str(params["a"] + params["b"] if params["operation"] == "add" else params["a"] * params["b"])
        if self.name == "read_note":
            return self.path(params["path"]).read_text()
        if self.name == "write_note":
            target = self.path(params["path"])
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(params["content"])
            return "saved"
        if self.name == "lookup":
            return json.dumps(self.fixtures.get("records", {})[params["key"]], ensure_ascii=False, sort_keys=True)
        if self.name == "notify":
            # A per-invocation clone owns this in-memory ledger.
            return json.dumps({"delivered": True, **params}, ensure_ascii=False, sort_keys=True)
        if self.name == "record_context":
            return json.dumps({"session": context.session_key, "channel": context.channel,
                               "chat_id": context.chat_id, "message_id": context.message_id}, sort_keys=True)
        if self.name == "fail_tool":
            if params["action"] == "hang":
                await asyncio.Event().wait()
            raise RuntimeError("fixture Tool failure")
        raise RuntimeError("unsupported safe fixture Tool")


def fixture_registry(case: Case, workspace: Path, bus, *, fault=None) -> ToolRegistry:
    class RecordedFixtureRegistry(ToolRegistry):
        async def execute(self, name, params):
            # Diagnostic events store the exact visible result after the normal
            # registry error wrapper. Do not append its hint a second time.
            if case.fixtures.get("recorded_tools"):
                tool = self.get(name)
                if tool is None:
                    raise RuntimeError(f"recorded adapter for Tool {name} is missing")
                errors = tool.validate_params(params)
                if errors:
                    raise RuntimeError("recorded Tool argument validation failed: " + "; ".join(errors))
                return await tool.execute(**params)
            return await super().execute(name, params)

    registry = RecordedFixtureRegistry()
    for name in case.tools.allowed_tools:
        if name == "message" and name not in case.fixtures.get("recorded_tools", {}):
            registry.register(MessageTool(send_callback=bus.publish_outbound if bus else None))
        else:
            registry.register(FixtureTool(name, workspace, case.fixtures, fault=fault))
    return registry


def prepare_workspace(case: Case, workspace: Path) -> None:
    (workspace / "AGENTS.md").write_text((Path(__file__).parent / "prompt.md").read_text())
    for name, content in case.fixtures.get("files", {}).items():
        target = FixtureTool("read_note", workspace, {}).path(name)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
