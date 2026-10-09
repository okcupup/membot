"""Explicit service provider configuration; CLI config behavior is unchanged."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from membot.service.config import ServiceConfig


def build_provider(config: ServiceConfig):
    if config.provider == "fake":
        from membot.providers.base import LLMProvider, LLMResponse

        class DeploymentFakeProvider(LLMProvider):
            """Opt-in deterministic smoke fixture, never a real-model claim."""

            def get_default_model(self):
                return "deployment-fake"

            async def chat(self, messages, **kwargs):
                message = next(item["content"] for item in reversed(messages) if item["role"] == "user")
                if message.startswith("m5:sleep:"):
                    await asyncio.sleep(float(message.split(":", 2)[2]))
                if message == "m5:provider-error":
                    raise RuntimeError("deployment fixture provider failure")
                previous = sum(item["role"] == "assistant" for item in messages)
                return LLMResponse(content=f"deployment-fake history_turns={previous}")

        return DeploymentFakeProvider()
    key_file = os.getenv("MEMBOT_LLM_API_KEY_FILE")
    key = Path(key_file).read_text().strip() if key_file else os.getenv("MEMBOT_LLM_API_KEY", "")
    if not key:
        raise ValueError("Worker needs MEMBOT_LLM_API_KEY_FILE or MEMBOT_LLM_API_KEY")
    if config.provider == "custom":
        from membot.providers.custom_provider import CustomProvider
        return CustomProvider(api_key=key, api_base=config.provider_base_url, default_model=config.model)
    from membot.providers.litellm_provider import LiteLLMProvider
    return LiteLLMProvider(api_key=key, api_base=config.provider_base_url, default_model=config.model)
