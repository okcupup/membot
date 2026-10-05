"""Deterministic providers used by local regression baselines."""

from __future__ import annotations

import asyncio
from typing import Any

from membot.providers.base import LLMProvider, LLMResponse


class FakeProvider(LLMProvider):
    """A delayed, non-network provider with an inspectable call timeline."""

    def __init__(self, delay: float = 0.02):
        super().__init__(api_key="m0-fake")
        self.delay = delay
        self.events: list[dict[str, Any]] = []
        self.active = 0
        self.max_active = 0
        self._next_call = 0

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
        max_tokens: int = 4096,
        temperature: float = 0.7,
        reasoning_effort: str | None = None,
    ) -> LLMResponse:
        del tools, model, max_tokens, temperature, reasoning_effort
        self._next_call += 1
        call_id = self._next_call
        prompt = messages[-1].get("content", "") if messages else ""
        if not isinstance(prompt, str):
            prompt = str(prompt)

        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.events.append({"event": "llm_start", "call": call_id, "active": self.active})
        try:
            await asyncio.sleep(self.delay)
        finally:
            self.active -= 1
            self.events.append({"event": "llm_end", "call": call_id, "active": self.active})

        return LLMResponse(content=f"fake response: {prompt}")

    def get_default_model(self) -> str:
        return "m0-fake"
