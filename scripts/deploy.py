"""Single-host Compose operations. No shell eval; no destructive live restore.

Use the Makefile or run with the project Python 3.11. JSON reports contain
observations, including failed samples; Fake Provider drills are opt-in.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import secrets
import select
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy"
TERMINAL = {"SUCCEEDED", "FAILED", "TIMEOUT"}


def load_env(path: Path) -> dict[str, str]:
    from dotenv import dotenv_values
    return {key: value or "" for key, value in dotenv_values(path, interpolate=False).items()}


class Deployment:
    def __init__(self, path: Path):
        self.path = path.resolve()
        self.values = {**load_env(DEPLOY / "images.env"), **load_env(self.path)}
        self.env = {**os.environ, **self.values}
        docker = os.getenv("DOCKER_BIN", "docker")
        compose = os.getenv("COMPOSE_BIN")
        self.command = [compose] if compose else [docker, "compose"]
        self.command += ["--env-file", str(DEPLOY / "images.env"), "--env-file", str(self.path), "-f", str(DEPLOY / "docker-compose.yml")]
        host = self.values.get("MEMBOT_PUBLIC_HOST", "localhost")
        port = self.values.get("MEMBOT_HTTPS_PORT", "8443")
        self.url = f"https://{host}:{port}"
        ca = self.values.get("MEMBOT_CLIENT_CA")
        self.ssl = ssl.create_default_context(cafile=self.file(ca) if ca else None)
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                                              urllib.request.HTTPSHandler(context=self.ssl))

    @staticmethod
    def file(value: str) -> str:
        path = Path(value).expanduser()
        return str(path.resolve() if path.is_absolute() else (DEPLOY / path).resolve())

    def compose(self, *args, **kwargs):
        return subprocess.run(self.command + list(args), env=self.env, check=True, **kwargs)

    def output(self, *args) -> str:
        return self.compose(*args, capture_output=True, text=True).stdout.strip()

    def report(self, name: str, data: dict):
        directory = DEPLOY / "reports"
        directory.mkdir(exist_ok=True)
        path = directory / f"{name}-{time.time_ns()}.json"
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
        print(json.dumps({"report": str(path), **data}, ensure_ascii=False))

    def validate(self):
        password = self.values.get("POSTGRES_PASSWORD", "")
        if not re.fullmatch(r"[A-Za-z0-9_-]{20,128}", password) or password.startswith("replace_"):
            raise ValueError("POSTGRES_PASSWORD must be a generated URL-safe value (20..128 characters)")
        host = self.values.get("MEMBOT_PUBLIC_HOST", "")
        if not re.fullmatch(r"[A-Za-z0-9.-]+", host):
            raise ValueError("MEMBOT_PUBLIC_HOST must be a hostname (no scheme/port)")
        for name in ("MEMBOT_HTTP_PORT", "MEMBOT_HTTPS_PORT"):
            if not 1 <= int(self.values.get(name, "0")) <= 65535:
                raise ValueError(f"{name} must be a valid port")
        certs = Path(self.file(self.values.get("MEMBOT_TLS_DIR", "")))
        if not (certs / "fullchain.pem").is_file() or not (certs / "privkey.pem").is_file():
            raise ValueError("TLS directory requires fullchain.pem and privkey.pem")
        pair = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        pair.load_cert_chain(certs / "fullchain.pem", certs / "privkey.pem")
        key = Path(self.file(self.values.get("MEMBOT_LLM_API_KEY_FILE", "")))
        if not key.is_file() or not key.read_text().strip():
            raise ValueError("MEMBOT_LLM_API_KEY_FILE must name a nonempty file")
        config_env = {**self.env, "DATABASE_URL": f"postgresql://membot:{password}@postgres:5432/membot",
                      "REDIS_URL": "redis://redis:6379/0"}
        # Test the same validation as installed container entry points.
        subprocess.run([sys.executable, "-m", "membot.service.entrypoints", "check-config"],
                       env=config_env, cwd=ROOT, check=True)
        self.compose("config", "--quiet")

    def request(self, path: str, body: dict | None = None, key: str | None = None, *, retries=0):
        if retries and body is not None and not key:
            raise ValueError("POST retries require a stable Idempotency-Key")
        headers = {"X-Request-ID": f"deploy-{uuid.uuid4()}", "Connection": "close"}
        if key:
            headers["Idempotency-Key"] = key
        data = json.dumps(body).encode() if body is not None else None
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.url + path, data=data, headers=headers)
        started = time.monotonic()
        for attempt in range(retries + 1):
            try:
                with self.http.open(request, timeout=20) as response:
                    raw = response.read()
                    result = {"status": response.status, "instanceId": response.headers.get("X-Instance-ID"),
                              "body": json.loads(raw), "location": response.headers.get("Location")}
            except urllib.error.HTTPError as exc:
                raw = exc.read()
                result = {"status": exc.code, "instanceId": exc.headers.get("X-Instance-ID"),
                          "body": raw.decode(errors="replace")[:300]}
            except (OSError, urllib.error.URLError) as exc:
                result = {"status": 0, "error": type(exc).__name__}
            result["elapsedSeconds"] = time.monotonic() - started
            if result["status"] not in {0, 429, 502, 503, 504} or attempt == retries:
                return result
            time.sleep(min(0.25 * 2**attempt, 2))

    def require(self, response: dict, status: int):
        if response["status"] != status:
            raise RuntimeError(f"expected HTTP {status}: {response}")
        return response["body"]

    def accept(self, session: str, message="m5:hello", key=None):
        key = key or str(uuid.uuid4())
        body = {"sessionId": session, "message": message, "maxIterations": 4}
        accepted = self.request("/v1/invocations", body, key, retries=6)
        invocation = self.require(accepted, 202)
        if accepted.get("location") != f"/v1/invocations/{invocation['invocationId']}":
            raise AssertionError("missing/wrong Location")
        return invocation, body, key, accepted

    def wait(self, invocation_id: str, states=TERMINAL, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = self.request(f"/v1/invocations/{invocation_id}", retries=2)
            if response["status"] == 200 and response["body"]["status"] in states:
                return response["body"]
            time.sleep(0.2)
        raise TimeoutError(f"Invocation {invocation_id} did not reach {states}")

    def health(self):
        for service in ("api1", "api2"):
            self.compose("exec", "-T", service, "membot-health", "api")
        self.compose("exec", "-T", "worker", "membot-health", "worker")
        self.require(self.request("/health/ready", retries=4), 200)

    def smoke(self):
        self.health()
        session = f"m5-smoke-{uuid.uuid4()}"
        created = self.require(self.request("/v1/sessions", {"sessionId": session}), 201)
        invocation, body, key, response = self.accept(session)
        duplicate = self.require(self.request("/v1/invocations", body, key, retries=4), 202)
        assert invocation["invocationId"] == duplicate["invocationId"]
        assert self.request("/v1/invocations", {**body, "message": "different"}, key)["status"] == 409
        terminal = self.wait(invocation["invocationId"])
        assert terminal["status"] == "SUCCEEDED", terminal
        events = self.require(self.request(f"/v1/invocations/{invocation['invocationId']}/events"), 200)
        assert {"accepted", "running", "LLM", "HISTORY", "FINAL"} <= {e["event_type"] for e in events["events"]}
        self.report("smoke", {"https": self.url, "certificateVerified": True, "session": created,
                              "submission": response, "terminal": terminal, "eventCount": len(events["events"])})

    def fake_only(self):
        if self.values.get("MEMBOT_PROVIDER") != "fake" or self.values.get("MEMBOT_ALLOW_FAKE_PROVIDER") != "true":
            raise ValueError("controlled drills require explicit fake provider; use a dedicated test deployment")

    def api_drill(self):
        self.fake_only()
        before = [self.request("/health/live") for _ in range(12)]
        assert {r.get("instanceId") for r in before if r["status"] == 200} == {"api1", "api2"}, before
        invocation, body, key, _ = self.accept(f"api-drill-{uuid.uuid4()}", "m5:sleep:3")
        self.wait(invocation["invocationId"], {"RUNNING"})
        old_id = self.output("ps", "-q", "api1")
        old_ip = subprocess.check_output([os.getenv("DOCKER_BIN", "docker"), "inspect", "--format",
                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", old_id], env=self.env, text=True).strip()
        network = subprocess.check_output([os.getenv("DOCKER_BIN", "docker"), "inspect", "--format",
                    "{{range $name,$network := .NetworkSettings.Networks}}{{$name}}{{end}}", old_id], env=self.env, text=True).strip()
        nginx_id = self.output("ps", "-q", "nginx")
        started = time.monotonic()
        self.compose("kill", "--signal", "SIGKILL", "api1")
        outage = []
        reservation = "membot-dns-drill-" + uuid.uuid4().hex
        docker = os.getenv("DOCKER_BIN", "docker")
        try:
            for _ in range(30):
                sample = self.request("/health/live")
                sample["atSeconds"] = time.monotonic() - started
                outage.append(sample)
                time.sleep(0.1)
            accepted = self.require(self.request("/v1/invocations", body, key, retries=6), 202)
            assert accepted["invocationId"] == invocation["invocationId"]
            assert self.wait(invocation["invocationId"])["status"] == "SUCCEEDED"
            assert any(r.get("instanceId") == "api2" for r in outage if r["status"] == 200)
        finally:
            # Recreate verifies dynamic DNS rather than just restarting same IP.
            self.compose("rm", "-f", "api1")
            try:
                subprocess.run([docker, "run", "-d", "--name", reservation, "--network", network, "--ip", old_ip,
                    "--memory", "32m", "--pids-limit", "16", "--cap-drop", "ALL", "--read-only", "--entrypoint", "python",
                    self.values.get("PYTHON_IMAGE", "python:3.11.11-slim-bookworm"),
                    "-c", "import time; time.sleep(90)"], env=self.env, check=True, stdout=subprocess.DEVNULL)
            finally:
                try:
                    self.compose("up", "-d", "--no-deps", "api1")
                finally:
                    subprocess.run([docker, "rm", "-f", reservation], env=self.env, check=False, stdout=subprocess.DEVNULL)
        restored = []
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            restored.append(self.request("/health/live"))
            if {r.get("instanceId") for r in restored if r["status"] == 200} == {"api1", "api2"}:
                break
            time.sleep(0.2)
        assert {r.get("instanceId") for r in restored if r["status"] == 200} == {"api1", "api2"}, restored
        new_id = self.output("ps", "-q", "api1")
        new_ip = subprocess.check_output([os.getenv("DOCKER_BIN", "docker"), "inspect", "--format",
                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", new_id], env=self.env, text=True).strip()
        assert old_ip != new_ip, "DNS drill must exercise a changed IP"
        assert self.output("ps", "-q", "nginx") == nginx_id
        failures = [r for r in outage if r["status"] != 200]
        self.report("api-drill", {"oldIP": old_ip, "newIP": new_ip, "changedAddress": old_ip != new_ip,
            "nginxUnchanged": True, "before": before, "during": outage, "restored": restored,
            "errors": len(failures), "lastObservedErrorAtSeconds": failures[-1]["atSeconds"] if failures else None,
            "invocationId": invocation["invocationId"], "idempotentDuplicate": True})

    def worker_drill(self):
        self.fake_only()
        invocation, _, _, _ = self.accept(f"worker-drill-{uuid.uuid4()}", "m5:sleep:60")
        self.wait(invocation["invocationId"], {"RUNNING"})
        self.compose("kill", "--signal", "SIGKILL", "worker")
        queued, _, _, _ = self.accept(f"queued-after-kill-{uuid.uuid4()}")
        api_ready = self.require(self.request("/health/ready"), 200)
        self.compose("up", "-d", "worker")
        lost = self.wait(invocation["invocationId"])
        assert lost["status"] == "FAILED" and lost["errorCode"] == "WORKER_LOST", lost
        continued = self.wait(queued["invocationId"])
        assert continued["status"] == "SUCCEEDED", continued
        graceful, _, _, _ = self.accept(f"graceful-{uuid.uuid4()}", "m5:sleep:2")
        self.wait(graceful["invocationId"], {"RUNNING"})
        self.compose("stop", "worker")
        completed = self.wait(graceful["invocationId"])
        assert completed["status"] == "SUCCEEDED", completed
        self.compose("up", "-d", "worker")
        slow, _, _, _ = self.accept(f"drain-deadline-{uuid.uuid4()}", "m5:sleep:60")
        self.wait(slow["invocationId"], {"RUNNING"})
        successor, _, _, _ = self.accept(slow["sessionId"])
        self.compose("stop", "worker")
        drain_timeout = self.wait(slow["invocationId"])
        assert drain_timeout["status"] == "FAILED" and drain_timeout["errorCode"] == "WORKER_DRAIN_TIMEOUT", drain_timeout
        waiting = self.require(self.request(f"/v1/invocations/{successor['invocationId']}"), 200)
        assert waiting["status"] == "QUEUED", waiting
        self.compose("up", "-d", "worker")
        assert self.wait(successor["invocationId"])["status"] == "SUCCEEDED"
        self.report("worker-drill", {"lost": lost, "queuedRecovery": continued,
                    "apiReadyWhileWorkerDown": api_ready, "graceful": completed,
                    "drainTimeout": drain_timeout, "successorRemainedQueued": waiting})

    def restart_check(self):
        self.fake_only()
        session = f"restart-{uuid.uuid4()}"
        first, _, _, _ = self.accept(session)
        original = self.wait(first["invocationId"])
        assert original["status"] == "SUCCEEDED"
        self.compose("down")  # never -v; persistence is the acceptance assertion
        self.compose("up", "-d", "--wait", "--wait-timeout", "120")
        persisted = self.require(self.request(f"/v1/invocations/{first['invocationId']}", retries=6), 200)
        assert original == persisted, (original, persisted)
        second, _, _, _ = self.accept(session)
        continued = self.wait(second["invocationId"])
        assert "history_turns=1" in continued["result"]["final_content"], continued
        self.report("restart-check", {"persisted": persisted, "continuedWithHistory": continued})

    def rate_check(self):
        def sample(_):
            return self.request("/v1/invocations/does-not-exist")["status"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            results = list(executor.map(sample, range(200)))
        assert 429 in results, results
        assert self.request("/health/live")["status"] == 200
        self.report("rate-check", {"statuses": {str(code): results.count(code) for code in set(results)},
                                  "healthExempt": True})

    @staticmethod
    def fingerprint_sql():
        tables = {"sessions": "owner_id,session_id", "session_messages": "owner_id,session_id,message_seq",
                  "invocations": "invocation_id", "session_archives": "owner_id,session_id,archive_seq",
                  "invocation_events": "event_id", "outbox": "outbox_id"}
        pairs = []
        for table, order in tables.items():
            pairs.append(f"'{table}',(SELECT json_build_object('count',count(*),'hash',"
                         f"md5(coalesce(string_agg(to_jsonb(t)::text,'' ORDER BY {order}),''))) FROM {table} t)")
        return "SELECT json_build_object(" + ",".join(pairs) + ");"

    def backup(self):
        directory = DEPLOY / "backups"
        directory.mkdir(exist_ok=True, mode=0o700)
        path = directory / f"membot-{time.time_ns()}.dump"
        # An exported snapshot keeps the manifest and pg_dump consistent even
        # while the Worker commits new turns. Only the backup transaction lives
        # across this await; no Invocation row locks/LLM calls are involved.
        holder = subprocess.Popen(self.command + ["exec", "-T", "postgres", "psql", "-X", "-qAt",
                     "-v", "ON_ERROR_STOP=1", "-U", "membot", "-d", "membot"], env=self.env,
                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

        def query(sql):
            holder.stdin.write(sql + "\n")
            holder.stdin.flush()
            if not select.select([holder.stdout], [], [], 15)[0]:
                raise TimeoutError("backup snapshot command timed out")
            answer = holder.stdout.readline().strip()
            if not answer:
                raise RuntimeError("backup snapshot connection lost")
            return answer
        try:
            snapshot = query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SELECT pg_export_snapshot();")
            manifest = json.loads(query(self.fingerprint_sql()))
            with path.open("xb") as output:
                path.chmod(0o600)
                self.compose("exec", "-T", "postgres", "pg_dump", "-U", "membot", "-d", "membot",
                             "--format=custom", "--no-owner", f"--snapshot={snapshot}", stdout=output)
            metadata = {"fingerprint": manifest, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "snapshot": snapshot, "file": str(path)}
            path.with_suffix(".json").write_text(json.dumps(metadata, indent=2) + "\n")
            path.with_suffix(".json").chmod(0o600)
            self.report("backup", metadata)
        finally:
            holder.stdin.close()
            try:
                holder.wait(timeout=5)
            except subprocess.TimeoutExpired:
                holder.kill()
                holder.wait(timeout=5)

    def restore_check(self, path: Path):
        path = path.resolve()
        metadata = json.loads(path.with_suffix(".json").read_text())
        assert hashlib.sha256(path.read_bytes()).hexdigest() == metadata["sha256"], "backup checksum mismatch"
        name = "membot_restore_" + uuid.uuid4().hex
        self.compose("exec", "-T", "postgres", "createdb", "-U", "membot", name)
        try:
            with path.open("rb") as source:
                self.compose("exec", "-T", "postgres", "pg_restore", "-U", "membot", "-d", name,
                             "--no-owner", "--exit-on-error", stdin=source)
            restored = json.loads(self.output("exec", "-T", "postgres", "psql", "-U", "membot",
                           "-d", name, "-XqAt", "-c", self.fingerprint_sql()))
            assert restored == metadata["fingerprint"], (restored, metadata["fingerprint"])
            self.report("restore-check", {"isolatedDatabase": name, "fingerprintMatches": True,
                                          "fingerprint": restored, "backup": str(path)})
        finally:
            # Drops only the random DB created by this invocation, never membot.
            self.compose("exec", "-T", "postgres", "dropdb", "-U", "membot", name)


def init_local(path: Path):
    if path.exists():
        raise ValueError(f"refusing to overwrite {path}")
    certs, keys = DEPLOY / "certs", DEPLOY / "secrets"
    certs.mkdir(exist_ok=True, mode=0o700)
    keys.mkdir(exist_ok=True, mode=0o700)
    if (certs / "privkey.pem").exists() or (keys / "llm_api_key").exists():
        raise ValueError("local fixture files already exist; use a dedicated checkout/deployment")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "7",
        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        "-keyout", str(certs / "privkey.pem"), "-out", str(certs / "fullchain.pem")],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    (certs / "privkey.pem").chmod(0o600)
    (keys / "llm_api_key").write_text("local-fake-provider-only\n")
    # Readable by the nonroot Worker through a Compose secret bind mount.
    (keys / "llm_api_key").chmod(0o644)
    values = load_env(ROOT / ".env.example")
    values.update(POSTGRES_PASSWORD=secrets.token_urlsafe(32), MEMBOT_PUBLIC_HOST="localhost",
                  MEMBOT_PROVIDER="fake", MEMBOT_ALLOW_FAKE_PROVIDER="true",
                  MEMBOT_CLIENT_CA=str(certs / "fullchain.pem"), MEMBOT_TLS_DIR=str(certs),
                  MEMBOT_LLM_API_KEY_FILE=str(keys / "llm_api_key"), COMPOSE_PROJECT_NAME="membot-m5-local")
    path.write_text("# Local self-signed TLS + explicit deterministic fixture, not public HTTPS proof.\n" +
                    "\n".join(f"{key}={value}" for key, value in values.items()) + "\n")
    path.chmod(0o600)
    print(f"Created {path}; make deploy-up ENV_FILE={path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("action", choices=["init-local", "config", "up", "down", "health", "doctor", "smoke",
                         "api-drill", "worker-drill", "restart-check", "rate-check", "backup", "restore-check"])
    parser.add_argument("--backup", type=Path)
    args = parser.parse_args()
    if args.action == "init-local":
        init_local(args.env_file)
        return
    if not args.env_file.is_file():
        parser.error("environment file missing; copy .env.example or explicitly run init-local")
    deploy = Deployment(args.env_file)
    if args.action == "config":
        deploy.validate()
    elif args.action == "up":
        deploy.validate()
        if deploy.values.get("MEMBOT_BUILD_OFFLINE") == "1":
            deploy.compose("build")
        else:
            deploy.compose("build", "--pull")
        deploy.compose("up", "-d", "--wait", "--wait-timeout", "180")
        deploy.health()
    elif args.action == "down":
        deploy.compose("down")
    elif args.action == "doctor":
        print(deploy.output("ps", "--all", "--format", "json"))
        deploy.compose("exec", "-T", "api1", "membot-health", "doctor")
    elif args.action == "restore-check":
        if args.backup is None:
            parser.error("restore-check requires --backup PATH.dump")
        deploy.restore_check(args.backup)
    else:
        getattr(deploy, args.action.replace("-", "_"))()


if __name__ == "__main__":
    main()
