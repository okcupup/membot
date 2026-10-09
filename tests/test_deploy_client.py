from __future__ import annotations

import importlib.util
import io
import json
import ssl
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("deploy_script", Path(__file__).parents[1] / "scripts/deploy.py")
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


def client(tmp_path, monkeypatch):
    monkeypatch.setattr(ssl, "create_default_context", lambda **kwargs: ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT))
    monkeypatch.setattr(deploy.time, "sleep", lambda value: None)
    env = tmp_path / ".env"
    env.write_text("MEMBOT_PUBLIC_HOST=localhost\nMEMBOT_HTTPS_PORT=8443\n")
    return deploy.Deployment(env)


def test_client_retries_post_with_identical_key_and_bytes(tmp_path, monkeypatch):
    value = client(tmp_path, monkeypatch)
    sent = []

    class Reply(io.BytesIO):
        status = 202
        headers = {"X-Instance-ID": "api2"}

    class HTTP:
        def open(self, request, **kwargs):
            sent.append((request.data, request.get_header("Idempotency-key")))
            if len(sent) == 1:
                raise OSError("response lost after commit")
            return Reply(json.dumps({"invocationId": "original"}).encode())

    value.http = HTTP()
    result = value.request("/v1/invocations", {"message": "hello"}, "stable-key", retries=1)
    assert result["status"] == 202 and result["instanceId"] == "api2"
    assert len(sent) == 2 and sent[0] == sent[1] and sent[0][1] == "stable-key"


def test_client_refuses_post_retries_without_key(tmp_path, monkeypatch):
    value = client(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="Idempotency-Key"):
        value.request("/v1/sessions", {"sessionId": "session"}, retries=1)


def test_environment_is_read_as_data_and_not_executed(tmp_path):
    marker = tmp_path / "should-not-exist"
    env = tmp_path / ".env"
    env.write_text(f"EXAMPLE=$(touch {marker})\nBRACED=${{EXAMPLE}}\n")
    loaded = deploy.load_env(env)
    assert loaded["EXAMPLE"].startswith("$(touch ") and loaded["BRACED"] == "${EXAMPLE}"
    assert not marker.exists()
