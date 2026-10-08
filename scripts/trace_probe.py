"""Inspect an Invocation, export a failure candidate, or replay recorded outputs.

The default only reads API/DB/file records. --reproduce uses fixture responses
and a disposable directory, never normal providers or external-write tools.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from membot.agent.redaction import redact_data
from membot.service.diagnostics import (
    MAX_BUNDLE_BYTES,
    candidate_case,
    confirm_expected,
    read_recording,
    render_timeline,
    reproduce_recording,
)


async def _from_api(base_url: str, invocation_id: str) -> dict:
    import httpx

    # Correlation headers are explicit per HTTP query. These GETs do not submit
    # any Invocation and cannot overwrite the original submission requestId.
    async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=10.0, trust_env=False) as client:
        response = await client.get(f"/v1/invocations/{invocation_id}")
        response.raise_for_status()
        task = response.json()
        events, cursor, size, limited = [], 0, 0, False
        while len(events) < 2048:
            response = await client.get(f"/v1/invocations/{invocation_id}/events", params={"after": cursor, "limit": 100})
            response.raise_for_status()
            data = response.json()
            for event in data["events"]:
                size += len(json.dumps(event).encode())
                if size > MAX_BUNDLE_BYTES - 65_536:
                    limited = True
                    break
                events.append(event)
            if limited or data.get("nextAfter") is None:
                break
            cursor = data["nextAfter"]
        else:
            limited = True
        sequences = [event["sequence"] for event in events]
        return {"schema_version": 1, "kind": "invocation_recording", "invocation": task,
                "events": events, "recording_limited": limited,
                "retention_gaps": not events or sequences != list(range(1, sequences[-1] + 1))}


async def _load(args: argparse.Namespace) -> dict:
    if args.from_file:
        path = Path(args.from_file)
        if path.stat().st_size > MAX_BUNDLE_BYTES:
            raise ValueError("recording exceeds the 8 MiB read limit")
        return redact_data(json.loads(path.read_text(encoding="utf-8")))
    if args.api_url:
        return redact_data(await _from_api(args.api_url, args.invocation_id))
    from membot.agent.persistence.repository import PostgresRepository

    repository = await PostgresRepository.connect(args.database_url)
    try:
        # A viewer never runs migrations or modifies service state.
        return await read_recording(repository, args.invocation_id)
    finally:
        await repository.close()


def _write(path: str, value: dict) -> None:
    encoded = json.dumps(redact_data(value), ensure_ascii=False, indent=2).encode("utf-8")
    if len(encoded) > MAX_BUNDLE_BYTES:
        raise ValueError("export exceeds the 8 MiB artifact limit")
    destination = Path(path)
    # The file is private before any bytes are written. Replace atomically;
    # avoid a transient world-readable export or following an output symlink.
    with NamedTemporaryFile(dir=destination.parent, delete=False) as temporary:
        temporary_path = Path(temporary.name)
        try:
            temporary.write(encoded + b"\n")
            temporary.flush()
            os.replace(temporary_path, destination)
        finally:
            temporary_path.unlink(missing_ok=True)


async def run(args: argparse.Namespace) -> None:
    recording = await _load(args)
    if args.assert_correlation:
        task = recording["invocation"]
        for event in recording["events"]:
            for key in ("invocationId", "traceId", "requestId", "sessionId"):
                if event[key] != task[key]:
                    raise ValueError(f"event correlation mismatch: {key}")
    if args.assert_redaction and redact_data(recording) != recording:
        raise ValueError("unredacted diagnostic payload")
    if args.export_recording:
        _write(args.export_recording, recording)
    if args.export_candidate:
        case = candidate_case(recording)
        if args.confirm_expected:
            case = confirm_expected(case, status=args.expected_status,
                                    allowed_tools=args.allow_tool or [], forbidden_tools=args.forbid_tool or [],
                                    answer=args.expected_answer)
        _write(args.export_candidate, case)
    if args.reproduce:
        print(json.dumps(await reproduce_recording(recording), ensure_ascii=False, indent=2))
    elif args.json:
        print(json.dumps(recording, ensure_ascii=False, indent=2))
    else:
        print(render_timeline(recording, show_payload=args.show_payload))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("invocation_id", nargs="?")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--from-file", help="read an exported recording, without DB/API access")
    source.add_argument("--api-url", help="read only the Invocation and events GET endpoints")
    source.add_argument("--database-url", default=os.getenv("DATABASE_URL", "postgresql://membot:membot@127.0.0.1:55432/membot"))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--show-payload", action="store_true")
    parser.add_argument("--export-recording", metavar="FILE")
    parser.add_argument("--export-candidate", metavar="FILE")
    parser.add_argument("--reproduce", action="store_true")
    parser.add_argument("--assert-correlation", action="store_true")
    parser.add_argument("--assert-redaction", action="store_true")
    parser.add_argument("--confirm-expected", action="store_true", help="explicit human review of expected fields")
    parser.add_argument("--expected-status", choices=["SUCCEEDED", "FAILED", "TIMEOUT"])
    parser.add_argument("--expected-answer")
    parser.add_argument("--allow-tool", action="append")
    parser.add_argument("--forbid-tool", action="append")
    args = parser.parse_args()
    if not args.from_file and not args.invocation_id:
        parser.error("provide invocation_id or --from-file")
    if args.confirm_expected and (not args.export_candidate or not args.expected_status or
                                 args.allow_tool is None and args.forbid_tool is None):
        parser.error("confirmation needs --export-candidate, --expected-status and explicit tool constraints")
    try:
        asyncio.run(run(args))
    except Exception as exc:
        parser.exit(1, f"diagnostic operation failed: {redact_data(str(exc))}\n")


if __name__ == "__main__":
    main()
