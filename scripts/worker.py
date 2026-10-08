"""Run the single Membot asyncio execution Worker."""

from __future__ import annotations

import asyncio

from membot.agent.persistence.redis_transport import RedisStreamTransport
from membot.agent.persistence.repository import PostgresRepository
from membot.service.config import ServiceConfig
from membot.service.worker import AsyncWorker


async def _run() -> None:
    config = ServiceConfig.from_env()
    repository = await PostgresRepository.connect(config.database_url)
    await repository.migrate()
    transport = await RedisStreamTransport.connect(
        config.redis_url,
        max_depth=config.redis_stream_max_depth,
        prefetch=config.worker_prefetch,
    )
    worker = AsyncWorker(repository, transport, config)
    try:
        await worker.run()
    finally:
        await transport.close()
        await repository.close()


if __name__ == "__main__":
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
