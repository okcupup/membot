"""Build, inspect and install the service wheel in a fresh non-editable venv."""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import venv
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    with tempfile.TemporaryDirectory(prefix="membot-wheel-") as name:
        directory = Path(name)
        env = {key: value for key, value in os.environ.items() if key not in {"PYTHONPATH", "VIRTUAL_ENV"}}
        env.update(membot_SKIP_WEBUI_BUILD="1", LITELLM_LOCAL_MODEL_COST_MAP="True")
        subprocess.run([sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", name, str(ROOT)], env=env, check=True)
        wheel = next(directory.glob("*.whl"))
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
            for required in ("membot/agent/loop.py", "membot/cli/commands.py", "membot/service/entrypoints.py",
                             "membot/agent/persistence/migrations/0003_worker_health.sql", "membot/templates/AGENTS.md"):
                assert required in names, required
            assert not any("__pycache__" in path or path.endswith(".pyc") for path in names)
        venv.create(directory / "clean", with_pip=True)
        python = directory / "clean" / "bin" / "python"
        subprocess.run([str(python), "-m", "pip", "install", "-r", str(ROOT / "deploy/requirements.lock")],
                       cwd=directory, env=env, check=True)
        subprocess.run([str(python), "-m", "pip", "install", "--no-deps", str(wheel)], cwd=directory, env=env, check=True)
        subprocess.run([str(python), "-m", "pip", "check"], cwd=directory, env=env, check=True)
        subprocess.run([str(python), "-c", "import membot.agent.loop, membot.cli.commands, membot.service.entrypoints; "
            "from importlib.resources import files; assert (files('membot.agent.persistence')/'migrations'/'0003_worker_health.sql').is_file(); "
            "print('clean installed wheel:', membot.agent.loop.__file__)"], cwd=directory, env=env, check=True)
        subprocess.run([str(directory / "clean" / "bin" / "membot"), "--help"], cwd=directory, env=env, check=True)
        subprocess.run([str(directory / "clean" / "bin" / "membot-service"), "check-config"], cwd=directory, env=env, check=True)


if __name__ == "__main__":
    main()
