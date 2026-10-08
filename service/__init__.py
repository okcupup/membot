"""Membot's small asynchronous HTTP API and Redis Worker."""

from typing import Any

__all__ = ["AsyncWorker", "ServiceConfig", "create_api_app"]


def __getattr__(name: str) -> Any:
    # Importing API/config/read-only diagnostics never loads Worker/AgentLoop
    # or provider/tool clients as a side effect of package initialization.
    if name == "AsyncWorker":
        from membot.service.worker import AsyncWorker
        return AsyncWorker
    if name == "ServiceConfig":
        from membot.service.config import ServiceConfig
        return ServiceConfig
    if name == "create_api_app":
        from membot.service.api import create_api_app
        return create_api_app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
