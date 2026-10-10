import os

import pytest
from membot.evaluation.grading import grade
from membot.evaluation.queue import QUEUE_CASE_IDS, QueueRuntime
from membot.evaluation.schema import load_cases

asyncpg = pytest.importorskip("asyncpg")
redis_async = pytest.importorskip("redis.asyncio")
RedisError = pytest.importorskip("redis.exceptions").RedisError


@pytest.mark.asyncio
async def test_selected_cases_use_real_postgres_redis_worker_path():
    database_url = os.getenv("DATABASE_URL")
    redis_url = os.getenv("REDIS_URL")
    if not database_url or not redis_url:
        pytest.skip("DATABASE_URL and REDIS_URL are required for M6 queue integration")
    runtime = QueueRuntime(database_url, redis_url)
    try:
        await runtime.start()
    except (OSError, asyncpg.PostgresError, RedisError) as exc:
        pytest.skip(f"PostgreSQL/Redis integration unavailable: {exc}")
    try:
        cases = {case.id: case for case in load_cases()}
        for case_id in sorted(QUEUE_CASE_IDS):
            observation = await runtime.execute(cases[case_id])
            result = grade(cases[case_id], observation)
            assert result["passed"], [check for check in result["checks"] if not check["passed"]]
    finally:
        await runtime.close()
