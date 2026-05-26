from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from membot.agent.conversation_memory.models import MemoryRecord
from membot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND
from membot.agent.conversation_memory.stores.jsonl import JsonlMessageStore


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

    def test_save_records_ignores_non_turn_raw_payload(self) -> None:
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
            self.assertFalse(path.exists())

    def test_save_records_expands_turn_payload(self) -> None:
        _TEST_TMP.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=_TEST_TMP) as tmp:
            store = JsonlMessageStore(Path(tmp))
            session_key = "cli:turn-record"
            turn_id = "turn:cli_turn-record:000001"

            asyncio.run(store.save_records(
                session_key,
                [
                    MemoryRecord(
                        kind=RAW_MESSAGE_KIND,
                        session_key=session_key,
                        source_kind="session_turn",
                        source_id=turn_id,
                        payload={
                            "turn_id": turn_id,
                            "messages": [
                                {"role": "user", "content": "hello", "timestamp": "2026-05-26T10:00:00"},
                                {"role": "assistant", "content": "hi", "timestamp": "2026-05-26T10:00:01"},
                            ],
                        },
                    ),
                ],
            ))

            session = store.get_or_create_session(session_key)
            self.assertEqual(len(session.messages), 2)
            self.assertEqual(session.messages[0]["turn_id"], turn_id)
            self.assertEqual(store.get_recent(session_key, 2), [
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "hi"},
            ])


if __name__ == "__main__":
    unittest.main()

