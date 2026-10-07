"""Apply Membot PostgreSQL migrations."""

from __future__ import annotations

import argparse
import asyncio
import os

from membot.agent.persistence.repository import PostgresRepository


async def main(dsn: str) -> None:
    repository = await PostgresRepository.connect(dsn)
    try:
        await repository.migrate()
    finally:
        await repository.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL", "postgresql://membot:membot@127.0.0.1:55432/membot"))
    args = parser.parse_args()
    asyncio.run(main(args.dsn))
