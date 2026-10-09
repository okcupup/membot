"""Healthcheck CLI; admission, Worker heartbeat, Redis and backlog are distinct."""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import sys
import urllib.request

from membot.service.config import ServiceConfig


async def doctor(config):
    from redis.asyncio import Redis

    from membot.agent.persistence.repository import PostgresRepository

    repository = await PostgresRepository.connect(config.database_url, command_timeout=config.db_timeout_seconds)
    redis = Redis.from_url(config.redis_url, socket_timeout=config.db_timeout_seconds)
    try:
        admission = await repository.admission_health(config.owner_id, config.max_unfinished)
        details = await repository.service_diagnostics(config.owner_id, stale_seconds=config.worker_stale_seconds)
        try:
            await redis.ping()
            queue = {"available": True, "streamDepth": await redis.xlen(config.redis_stream_key)}
        except Exception:
            queue = {"available": False}
        return {"admission": admission, "execution": details, "redis": queue,
                "model": "not_probed", "scope": "Admission readiness is not Agent completion readiness"}
    finally:
        await redis.aclose()
        await repository.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=["api", "worker", "doctor"])
    parser.add_argument("--url", default="http://127.0.0.1:8080/health/ready")
    args = parser.parse_args()
    try:
        if args.role == "api":
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(args.url, timeout=3) as response:
                data = json.load(response)
        else:
            config = ServiceConfig.from_env()
            data = asyncio.run(doctor(config))
            if args.role == "worker":
                worker = data["execution"]["worker"]
                own_process = (worker.get("executionOwner") or "").startswith(f"worker:{socket.gethostname()}:")
                if not worker["alive"] or not own_process or not data["redis"]["available"]:
                    raise RuntimeError("Worker heartbeat or Redis unavailable")
        print(json.dumps(data))
    except Exception as exc:
        from membot.agent.redaction import redact_data
        print(json.dumps({"healthy": False, "error": redact_data(str(exc))}))
        sys.exit(1)


if __name__ == "__main__":
    main()
