from __future__ import annotations

import asyncio
import shutil
import tempfile
import unittest
from pathlib import Path

from membot.agent.conversation_memory.engine import ConversationMemoryEngine


_TEST_TMP = Path(__file__).resolve().parents[1] / ".test_tmp_engine"


class ConversationMemoryEngineTest(unittest.TestCase):
    @classmethod
    def tearDownClass(cls) -> None:
        if _TEST_TMP.exists():
            shutil.rmtree(_TEST_TMP)

    def test_recent_history_matches_session_get_history(self) -> None:
        _TEST_TMP.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=_TEST_TMP) as tmp:
            engine = ConversationMemoryEngine.for_workspace(Path(tmp))
            session_key = "cli:pipeline"
            messages = [
                {"role": "system", "content": "system prompt"},
                {"role": "user", "content": "[Runtime Context\nCurrent Time: now"},
                {"role": "user", "content": "first"},
                {"role": "assistant", "content": "one"},
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {"name": "read_file", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_1",
                    "name": "read_file",
                    "content": "tool result",
                },
                {"role": "assistant", "content": "two"},
                {"role": "user", "content": "second"},
                {"role": "assistant", "content": "done"},
            ]

            asyncio.run(engine.save_turn(session_key, messages, skip=1))

            session = engine.store.get_or_create_session(session_key)
            for limit in range(1, 8):
                self.assertEqual(
                    asyncio.run(engine.get_history(session_key, limit)),
                    session.get_history(max_messages=limit),
                )

    def test_save_turn_sanitizes_like_current_session_persistence(self) -> None:
        _TEST_TMP.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=_TEST_TMP) as tmp:
            engine = ConversationMemoryEngine.for_workspace(Path(tmp))
            session_key = "cli:sanitize"
            messages = [
                {"role": "system", "content": "system prompt"},
                {"role": "user", "content": "[Runtime Context\nCurrent Time: now"},
                {"role": "assistant", "content": ""},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": "data:image/png;base64,abc"},
                        },
                        {"type": "text", "text": "look"},
                    ],
                },
                {"role": "tool", "tool_call_id": "call_1", "name": "tool", "content": "x" * 501},
            ]

            asyncio.run(engine.save_turn(session_key, messages, skip=1))

            session = engine.store.get_or_create_session(session_key)
            self.assertEqual(len(session.messages), 2)
            self.assertEqual(session.messages[0]["content"][0], {"type": "text", "text": "[image]"})
            self.assertTrue(session.messages[1]["content"].endswith("\n... (truncated)"))

    def test_save_turn_preserves_extra_assistant_fields(self) -> None:
        _TEST_TMP.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=_TEST_TMP) as tmp:
            engine = ConversationMemoryEngine.for_workspace(Path(tmp))
            session_key = "cli:assistant-fields"

            asyncio.run(engine.save_turn(
                session_key,
                [
                    {"role": "system", "content": "system prompt"},
                    {
                        "role": "assistant",
                        "content": "answer",
                        "reasoning_content": "private reasoning",
                        "thinking_blocks": [{"type": "thinking", "text": "notes"}],
                    },
                ],
                skip=1,
            ))

            session = engine.store.get_or_create_session(session_key)
            self.assertEqual(session.messages[0]["reasoning_content"], "private reasoning")
            self.assertEqual(session.messages[0]["thinking_blocks"], [{"type": "thinking", "text": "notes"}])


if __name__ == "__main__":
    unittest.main()
