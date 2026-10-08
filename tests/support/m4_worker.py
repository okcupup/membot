"""One-invocation subprocess harness using the production AsyncWorker.

Only Provider/Tool responses are fixtures. PostgreSQL, Outbox, Redis, admission,
event persistence, ownership and JSON logging use production implementations.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from membot.agent.persistence.redis_transport import RedisStreamTransport  # noqa: E402
from membot.agent.persistence.repository import PostgresRepository  # noqa: E402
from membot.service.config import ServiceConfig  # noqa: E402
from membot.service.logging import configure_json_logging  # noqa: E402
from membot.service.worker import AsyncWorker  # noqa: E402
from test_diagnostics_integration import DiagnosticProvider, DiagnosticTool  # noqa: E402


async def run() -> None:
    config = ServiceConfig.from_env()
    configure_json_logging("worker", policy=config.diagnostic_policy)
    repository = await PostgresRepository.connect(config.database_url, diagnostics=config.diagnostic_policy)
    transport = await RedisStreamTransport.connect(config.redis_url, stream_key=os.environ["M4_TEST_STREAM"],
                                                    group=os.environ["M4_TEST_GROUP"], prefetch=config.worker_prefetch)
    sentinel = Path(os.environ["M4_TEST_SENTINEL"])
    worker = AsyncWorker(repository, transport, config, provider=DiagnosticProvider(sentinel))
    await worker.start()
    for name in ("write_file", "diagnostic_tool"):
        worker.agent.tools.register(DiagnosticTool(name, sentinel, asyncio.Event()))
    task = asyncio.create_task(worker.run())
    try:
        async def completed():
            while True:
                events = await repository.events(os.environ["M4_TEST_INVOCATION"])
                if any(event["event_type"] == "QUEUE" and event["step"] == "ack" for event in events):
                    return
                await asyncio.sleep(0.02)
        await asyncio.wait_for(completed(), 10.0)
    finally:
        await worker.stop(worker_lost=False)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await transport.close()
        await repository.close()


if __name__ == "__main__":
    asyncio.run(run())
