"""Real-provider evaluation with shared bounded request/token/cost reservations."""

from __future__ import annotations

import asyncio
import json
import math
import os
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import Field, model_validator

from membot.providers.base import LLMProvider
from membot.providers.custom_provider import CustomProvider

from . import judge
from .kernel import run_kernel
from .schema import StrictModel


class RealConfig(StrictModel):
    agent_model: str
    judge_model: str
    base_url: str
    judge_base_url: str | None = None
    model_version_confirmed: bool = False
    max_requests: int = Field(ge=1, le=1000)
    max_reserved_tokens: int = Field(ge=1, le=2_000_000)
    max_cost_usd: float = Field(gt=0, le=100)
    max_output_tokens: int = Field(ge=1, le=4096)
    agent_input_usd_per_million: float = Field(gt=0)
    agent_output_usd_per_million: float = Field(gt=0)
    judge_input_usd_per_million: float = Field(gt=0)
    judge_output_usd_per_million: float = Field(gt=0)
    labels_file: str = "evaluation/judge_labels.json"
    judge_timeout_seconds: float = Field(default=30.0, gt=0, le=120)

    @model_validator(mode="after")
    def pinned(self):
        if not self.model_version_confirmed or any("SET_" in model or not model for model in (self.agent_model, self.judge_model)):
            raise ValueError("explicitly pin/confirm Agent and Judge model versions")
        for url in (self.base_url, self.judge_base_url or self.base_url):
            if urlsplit(url).scheme not in {"https", "http"} or urlsplit(url).username or urlsplit(url).password:
                raise ValueError("provider URL must be HTTP(S) without embedded credentials")
        if not all(math.isfinite(value) for value in (self.max_cost_usd, self.agent_input_usd_per_million,
            self.agent_output_usd_per_million, self.judge_input_usd_per_million, self.judge_output_usd_per_million)):
            raise ValueError("budget/prices must be finite")
        return self

    def public_snapshot(self):
        return self.model_dump() | {"temperature": 0.0, "judge_prompt_hash": judge.JUDGE_PROMPT_HASH}


class BudgetExceededError(RuntimeError):
    pass


class Ledger:
    def __init__(self, config):
        self.config = config
        self.requests = 0
        self.reserved_tokens = 0
        self.reserved_cost_usd = 0.0
        self.actual_tokens = 0
        self.lock = asyncio.Lock()

    async def reserve(self, messages, tools, output, role):
        # UTF-8 bytes conservatively bound normal BPE tokens. This is a spending
        # reservation, not an assertion about the provider's tokenizer/bill.
        inputs = len(json.dumps({"messages": messages, "tools": tools}, ensure_ascii=False).encode()) + 512
        output = min(output, self.config.max_output_tokens)
        input_price = getattr(self.config, f"{role}_input_usd_per_million")
        output_price = getattr(self.config, f"{role}_output_usd_per_million")
        cost = (inputs * input_price + output * output_price) / 1_000_000
        async with self.lock:
            if self.requests + 1 > self.config.max_requests or \
               self.reserved_tokens + inputs + output > self.config.max_reserved_tokens or \
               self.reserved_cost_usd + cost > self.config.max_cost_usd:
                raise BudgetExceededError("real evaluation request/token/cost reservation exhausted")
            self.requests += 1
            self.reserved_tokens += inputs + output
            self.reserved_cost_usd += cost
        return output

    def snapshot(self):
        return {"requests": self.requests, "reserved_tokens": self.reserved_tokens,
                "reserved_cost_usd": self.reserved_cost_usd, "actual_tokens": self.actual_tokens,
                "policy": "no retries; conservative reservations retained for uncertain/failed calls"}


class MeteredProvider(LLMProvider):
    def __init__(self, provider, ledger, role):
        super().__init__()
        self.provider, self.ledger, self.role = provider, ledger, role

    def get_default_model(self):
        return self.provider.get_default_model()

    async def chat(self, messages, tools=None, max_tokens=1024, **kwargs):
        bound = await self.ledger.reserve(messages, tools, max_tokens, self.role)
        response = await self.provider.chat(messages=messages, tools=tools, max_tokens=bound, **kwargs)
        if response.finish_reason != "error":
            usage = response.usage
            if not all(isinstance(usage.get(key), int) and not isinstance(usage.get(key), bool)
                       and usage[key] >= 0 for key in ("prompt_tokens", "completion_tokens")):
                raise RuntimeError("provider usage missing; evaluation evidence incomplete")
            self.ledger.actual_tokens += usage["prompt_tokens"] + usage["completion_tokens"]
        return response

    async def aclose(self):
        # The suite runtime owns the shared clients. An isolated Case can't close
        # the shared Judge/Agent client for the next Case.
        pass


class RealRuntime:
    def __init__(self, config, *, agent=None, judge_provider=None):
        self.config = config
        self.ledger = Ledger(config)
        if agent is None:
            agent_key = os.getenv("MEMBOT_EVAL_API_KEY", "")
            judge_key = os.getenv("MEMBOT_JUDGE_API_KEY", "") or agent_key
            if not agent_key or not judge_key:
                raise ValueError("missing MEMBOT_EVAL_API_KEY / MEMBOT_JUDGE_API_KEY; real evaluation not run")
            agent = CustomProvider(agent_key, config.base_url, config.agent_model)
            judge_provider = CustomProvider(judge_key, config.judge_base_url or config.base_url, config.judge_model)
            # SDK retries would bypass the shared request/cost reservation.
            agent._client = agent._client.with_options(max_retries=0)
            judge_provider._client = judge_provider._client.with_options(max_retries=0)
        if judge_provider is None:
            raise ValueError("an explicit Judge provider is required")
        self.clients = [agent, judge_provider]
        self.agent = MeteredProvider(agent, self.ledger, "agent")
        self.judge_provider = MeteredProvider(judge_provider, self.ledger, "judge")
        self.calibration = None

    async def preflight(self):
        labels = json.loads(Path(self.config.labels_file).read_text())
        self.calibration = await judge.calibrate(self.judge_provider, labels,
                                               timeout=self.config.judge_timeout_seconds)
        if not self.calibration["passed"]:
            raise ValueError("Judge calibration failed; raw calibration verdicts retained")

    async def execute(self, case, *, repeat=0):
        if not case.real_model or case.scenario != "single":
            raise ValueError("Case is not enabled for real-model evaluation")
        result = await run_kernel(case, repeat=repeat, provider=self.agent, runtime_budget=case.real_budget)
        result["mode"] = "real"
        return result

    async def judge(self, case, result):
        if not result["deterministic_passed"]:
            result["judge"] = {"passed": False, "score": 0.0, "reason": "deterministic gate failed",
                               "evidence": [], "error": "DETERMINISTIC_GATE_FAILED"}
            return
        if not self.calibration or not self.calibration["passed"]:
            result["judge"] = {"passed": False, "score": 0.0, "reason": "Judge not calibrated",
                               "evidence": [], "error": "JUDGE_NOT_CALIBRATED"}
            return
        events = [event for row in result["invocations"] for event in row["events"]]
        verdict = await judge.evaluate(self.judge_provider, rubric=case.judge_rubric, input=case.input,
            final=result["invocations"][-1]["final"], events=events, timeout=self.config.judge_timeout_seconds)
        result["judge"] = verdict
        result["passed"] = verdict["passed"] and verdict["error"] is None

    async def close(self):
        await asyncio.gather(*(client.aclose() for client in self.clients))
