from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from nanobot.agent.conversation_memory.models import MemoryRecord
from nanobot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND
from nanobot.agent.conversation_memory.stores.jsonl import JsonlMessageStore


_TEST_TMP = Path(__file__).resolve().parents[1] / ".test_tmp"


class JsonlMessageStoreTest(unittest.TestCase):
    @classmethod
    def tearDownClass(cls) -> None:
        if _TEST_TMP.exists():
            shutil.rmtree(_TEST_TMP)

    def test_append_recent_list_and_clear(self) -> None:
        _TEST_TMP.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=_TEST_TMP) as tmp:
            store = JsonlMessageStore(Path(tmp))
            session_key = "cli:test"

            store.append_messages(
                session_key,
                [
                    {"role": "assistant", "content": "orphan"},
                    {"role": "user", "content": "hello"},
                    {"role": "assistant", "content": "hi"},
                ],
            )

            session = store.load(session_key)
            self.assertIsNotNone(session)
            assert session is not None
            self.assertEqual(len(session.messages), 3)

            recent = store.get_recent(session_key, limit=3)
            self.assertEqual(
                recent,
                [
                    {"role": "user", "content": "hello"},
                    {"role": "assistant", "content": "hi"},
                ],
            )

            sessions = store.list_sessions()
            self.assertEqual(sessions[0]["key"], session_key)

            asyncio.run(store.clear(session_key))
            cleared = store.get_or_create_session(session_key)
            self.assertEqual(cleared.messages, [])

    def test_save_records_preserves_current_jsonl_shape(self) -> None:
        _TEST_TMP.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=_TEST_TMP) as tmp:
            store = JsonlMessageStore(Path(tmp))
            session_key = "cli:records"

            asyncio.run(store.save_records(
                session_key,
                [
                    MemoryRecord(
                        kind=RAW_MESSAGE_KIND,
                        session_key=session_key,
                        payload={
                            "role": "assistant",
                            "content": None,
                            "timestamp": "2026-05-20T00:00:00",
                            "tool_calls": [{"id": "call_1"}],
                        },
                    ),
                    MemoryRecord(
                        kind="summary",
                        session_key=session_key,
                        payload={"summary": "ignored"},
                    ),
                ],
            ))

            path = Path(tmp) / "sessions" / "cli_records.jsonl"
            lines = path.read_text(encoding="utf-8").splitlines()
            metadata = json.loads(lines[0])
            message = json.loads(lines[1])

            self.assertEqual(metadata["_type"], "metadata")
            self.assertEqual(metadata["key"], session_key)
            self.assertEqual(message["role"], "assistant")
            self.assertEqual(message["tool_calls"], [{"id": "call_1"}])
            self.assertEqual(len(lines), 2)


if __name__ == "__main__":
    unittest.main()
