"""Installed single-process entry points with bounded signal-driven shutdown."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from dataclasses import replace

from membot.agent.persistence.repository import PostgresRepository
from membot.service.config import ServiceConfig
from membot.service.logging import configure_json_logging

logger = logging.getLogger(__name__)


def signals(callback):
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, callback)


async def connect(config):
    return await PostgresRepository.connect(config.database_url, diagnostics=config.diagnostic_policy,
                                            command_timeout=config.db_timeout_seconds)


async def serve_api(config: ServiceConfig) -> None:
    from aiohttp import web

    from membot.service.api import begin_api_drain, create_api_app, wait_api_drain

    repository = await connect(config)
    runner = None
    try:
        await repository.assert_schema()
        app = create_api_app(repository, config)
        runner = web.AppRunner(app, access_log=None, shutdown_timeout=config.api_drain_seconds,
                               handler_cancellation=False)
        await runner.setup()
        site = web.TCPSite(runner, config.api_host, config.api_port)
        stopped = asyncio.Event()

        def drain():
            begin_api_drain(app)
            stopped.set()
            logger.info("API draining", extra={"correlation": {"instanceId": config.instance_id,
                                                                 "event_type": "API_DRAIN"}})

        signals(drain)
        await site.start()
        logger.info("API listening", extra={"correlation": {"instanceId": config.instance_id,
                                                             "event_type": "API_START", "port": config.api_port}})
        await stopped.wait()
        await wait_api_drain(app, config.api_drain_seconds)
    finally:
        try:
            if runner:
                await asyncio.wait_for(runner.cleanup(), config.cleanup_seconds)
        finally:
            await asyncio.wait_for(repository.close(), config.cleanup_seconds)


async def serve_worker(config: ServiceConfig) -> None:
    from membot.agent.persistence.redis_transport import RedisStreamTransport
    from membot.service.worker import AsyncWorker

    repository = await connect(config)
    transport = None
    try:
        await repository.assert_schema()
        transport = await RedisStreamTransport.connect(
            config.redis_url, max_depth=config.redis_stream_max_depth, prefetch=config.worker_prefetch,
            stream_key=config.redis_stream_key, group=config.redis_group,
        )
        worker = AsyncWorker(repository, transport, config)
        stopped = asyncio.Event()

        def drain():
            stopped.set()
            worker.request_drain()

        signals(drain)
        await worker.start()
        if stopped.is_set():
            worker.request_drain()
        try:
            await worker.run()
        finally:
            await asyncio.wait_for(worker.stop(worker_lost=worker._lost), config.cleanup_seconds)
    finally:
        if transport:
            await transport.close()
        await asyncio.wait_for(repository.close(), config.cleanup_seconds)


async def migrate(config: ServiceConfig) -> None:
    repository = await connect(config)
    try:
        await repository.migrate()
        logger.info("Migrations applied", extra={"correlation": {"event_type": "MIGRATED"}})
    finally:
        await repository.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=["api", "worker", "migrate", "check-config"])
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    args = parser.parse_args()
    try:
        config = ServiceConfig.from_env()
        if args.host:
            config = replace(config, api_host=args.host)
        if args.port:
            config = replace(config, api_port=args.port)
        configure_json_logging(args.role, policy=config.diagnostic_policy)
        if args.role == "check-config":
            logger.info("Service configuration valid")
        else:
            asyncio.run({"api": serve_api, "worker": serve_worker, "migrate": migrate}[args.role](config))
    except Exception as exc:
        from membot.agent.redaction import redact_data
        parser.exit(1, f"service startup/shutdown failed: {redact_data(str(exc))}\n")


if __name__ == "__main__":
    main()
