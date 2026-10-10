import os

import pytest
from membot.evaluation.candidates import register_candidate
from membot.evaluation.grading import grade
from membot.evaluation.pipeline import run_suite
from membot.evaluation.queue import QUEUE_CASE_IDS, QueueRuntime
from membot.evaluation.schema import load_cases
from membot.service.diagnostics import candidate_case, confirm_expected, read_recording

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
            assert observation["admission"]["duplicate_responses"] == 1
            if case_id in {"error-tool", "error-provider"}:
                invocation_id = observation["invocations"][0]["invocationId"]
                recording = await read_recording(runtime.query_repository, invocation_id)
                candidate = candidate_case(recording)
                contract = cases[case_id].tools
                reviewed = confirm_expected(candidate, status="FAILED", allowed_tools=contract.allowed_tools,
                                            forbidden_tools=contract.forbidden_tools)
                reviewed.update(reviewer="integration-test-only-review", review_reason="Reproduce retained failure contract.",
                                required_tools=contract.required_tools,
                                expected_calls=[call.model_dump() for call in contract.expected_calls],
                                error_code=observation["invocations"][0]["error_code"])
                imported = register_candidate(candidate, recording, reviewed, case_id=f"candidate-{case_id}")
                replay = await run_suite([imported])
                assert replay["passed"]
    finally:
        await runtime.close()
