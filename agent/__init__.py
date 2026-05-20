"""Agent core module."""

from typing import Any

__all__ = ["AgentLoop", "ContextBuilder", "MemoryStore", "SkillsLoader"]


def __getattr__(name: str) -> Any:
    if name == "AgentLoop":
        from nanobot.agent.loop import AgentLoop
        return AgentLoop
    if name == "ContextBuilder":
        from nanobot.agent.context import ContextBuilder
        return ContextBuilder
    if name == "MemoryStore":
        from nanobot.agent.memory import MemoryStore
        return MemoryStore
    if name == "SkillsLoader":
        from nanobot.agent.skills import SkillsLoader
        return SkillsLoader
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
