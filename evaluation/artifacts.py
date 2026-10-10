"""Portable provenance and private atomic reports, with bounded reads."""

from __future__ import annotations

import json
import os
import platform
import subprocess
import tempfile
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from membot.agent.redaction import redact_data

from .schema import digest

ROOT = Path(__file__).resolve().parent.parent
MAX_ARTIFACT_BYTES = 32 * 1024 * 1024


def read_json(path: Path):
    if path.stat().st_size > MAX_ARTIFACT_BYTES:
        raise ValueError("artifact exceeds 32 MiB")
    return json.loads(path.read_text())


def write_json(path: Path, value):
    encoded = json.dumps(redact_data(value), ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    if len(encoded.encode()) > MAX_ARTIFACT_BYTES:
        raise ValueError("report exceeds 32 MiB; reduce repetitions or split the suite")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=".eval-", delete=False) as file:
        os.chmod(file.name, 0o600)
        file.write(encoded)
        temporary = file.name
    os.replace(temporary, path)


def provenance(cases, mode, repetitions, *, real_config=None):
    from .fixtures import fixture_registry

    def git(*args):
        result = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=False)
        return result.stdout.strip() if result.returncode == 0 else "unavailable"
    # Hash effective files as well as HEAD: a working-tree run must not masquerade
    # as a released commit. User development rules and learning docs aren't runtime.
    effective = {}
    for directory in ("agent", "bus", "providers", "session", "config", "utils", "cron",
                      "heartbeat", "service", "evaluation", "templates", "skills"):
        for file in sorted((ROOT / directory).rglob("*")):
            if file.is_file() and file.suffix in {".py", ".json", ".md", ".sql"} and "__pycache__" not in file.parts:
                if directory == "evaluation" and any(part in file.relative_to(ROOT / directory).parts
                                                     for part in ("reports", "baselines", "candidates")):
                    continue
                if directory == "evaluation" and file.parent == ROOT / directory and file.suffix == ".json" \
                   and file.name not in {"real.example.json", "judge_labels.json"}:
                    continue
                effective[str(file.relative_to(ROOT))] = digest(file.read_text())
    prompts = {name: value for name, value in effective.items() if name.startswith("templates/")
               or name.startswith("skills/") or name in {"agent/context.py", "evaluation/prompt.md"}}
    schemas = {}
    for case in cases:
        schemas[case.id] = fixture_registry(case, Path("/isolated-eval"), None).get_definitions()
    dependencies = {}
    for name in ("membot-ai", "pydantic", "litellm", "openai", "asyncpg", "redis", "aiohttp"):
        try:
            dependencies[name] = version(name)
        except PackageNotFoundError:
            dependencies[name] = "not_installed"
    result = {"schema_version": 1, "mode": mode,
              "case_hashes": {case.id: case.case_hash for case in cases},
              "case_versions": {case.id: case.version for case in cases},
              "code_commit": git("rev-parse", "HEAD"), "effective_code_hash": digest(effective),
              "working_tree_patch_hash": digest(git("diff", "HEAD", "--", "agent", "providers", "service", "evaluation")),
              "prompt_hash": digest(prompts), "tool_schema_hash": digest(schemas),
              "model": "m6-fixture-v1", "parameters": {"temperature": 0.0, "max_tokens": 1024},
              "rubric_hash": digest({case.id: [case.rubric_version, case.judge_rubric] for case in cases}),
              "environment": {"python": platform.python_version(), "system": platform.system(),
                              "machine": platform.machine(), "dependencies": dependencies},
              "repetitions": repetitions, "tool_schemas": schemas}
    if real_config:
        from .judge import JUDGE_PROMPT_HASH
        result.update(model=real_config.agent_model, judge_model=real_config.judge_model,
                      parameters=real_config.public_snapshot(), judge_prompt_hash=JUDGE_PROMPT_HASH)
    return result
