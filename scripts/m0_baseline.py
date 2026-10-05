#!/usr/bin/env python3
"""Record the M0 execution baseline without contacting an LLM service."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

# Keep the script runnable from any working directory in the source checkout.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from membot.agent.loop import AgentLoop  # noqa: E402
from membot.bus.events import InboundMessage  # noqa: E402
from membot.bus.queue import MessageBus  # noqa: E402

from tests.fakes import FakeProvider  # noqa: E402


def _new_loop(workspace: Path, provider: FakeProvider, bus: MessageBus) -> AgentLoop:
    return AgentLoop(
        bus=bus,
        provider=provider,
        workspace=workspace,
        model="m0-fake",
        max_iterations=2,
        memory_window=20,
    )


async def _stop_task(task: asyncio.Task[Any], loop: AgentLoop) -> None:
    loop.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await loop.close_mcp()


async def _bus_baseline() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="membot-m0-bus-") as directory:
        workspace = Path(directory)
        bus = MessageBus()
        provider = FakeProvider()
        loop = _new_loop(workspace, provider, bus)
        runner = asyncio.create_task(loop.run())
        submitted = [
            ("alpha", "alpha-1"),
            ("alpha", "alpha-2"),
            ("beta", "beta-1"),
            ("beta", "beta-2"),
        ]
        for chat_id, content in submitted:
            await bus.publish_inbound(InboundMessage(
                channel="baseline",
                sender_id="fake-user",
                chat_id=chat_id,
                content=content,
            ))

        responses = []
        try:
            while len(responses) < len(submitted):
                message = await asyncio.wait_for(bus.consume_outbound(), timeout=5)
                if not message.metadata.get("_progress"):
                    responses.append({"chat_id": message.chat_id, "content": message.content})
        finally:
            await _stop_task(runner, loop)

        return {
            "route": "native_message_bus",
            "submitted": [
                {"session": f"baseline:{chat_id}", "content": content}
                for chat_id, content in submitted
            ],
            "responses": responses,
            "provider_max_active": provider.max_active,
            "provider_events": provider.events,
            "observed_global_dispatch_serial": provider.max_active == 1,
        }


async def _direct_baseline() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="membot-m0-direct-") as directory:
        workspace = Path(directory)
        bus = MessageBus()
        provider = FakeProvider()
        loop = _new_loop(workspace, provider, bus)
        try:
            results = await asyncio.gather(
                loop.process_direct("direct-1", session_key="baseline:direct"),
                loop.process_direct("direct-2", session_key="baseline:direct"),
            )
        finally:
            await loop.close_mcp()

        return {
            "route": "process_direct",
            "submitted": ["direct-1", "direct-2"],
            "responses": results,
            "bus_outbound_size": bus.outbound_size,
            "provider_max_active": provider.max_active,
            "provider_events": provider.events,
            "observed_dispatch_lock_bypassed": provider.max_active > 1,
        }


async def collect_baseline() -> dict[str, Any]:
    return {
        "bus": await _bus_baseline(),
        "direct": await _direct_baseline(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Write JSON to this path as well as stdout")
    args = parser.parse_args()
    result = asyncio.run(collect_baseline())
    encoded = json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True)
    print(encoded)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
