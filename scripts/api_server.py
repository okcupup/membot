"""Run one stateless Membot API process."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace

from aiohttp import web
from membot.agent.persistence.repository import PostgresRepository
from membot.service.api import create_api_app
from membot.service.config import ServiceConfig


async def _serve(config: ServiceConfig) -> None:
    repository = await PostgresRepository.connect(config.database_url)
    await repository.migrate()
    app = create_api_app(repository, config, own_repository=True)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.api_host, config.api_port)
    await site.start()
    print(f"membot API listening on http://{config.api_host}:{config.api_port}", flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    args = parser.parse_args()
    config = ServiceConfig.from_env()
    if args.host:
        config = replace(config, api_host=args.host)
    if args.port:
        config = replace(config, api_port=args.port)
    asyncio.run(_serve(config))


if __name__ == "__main__":
    main()
