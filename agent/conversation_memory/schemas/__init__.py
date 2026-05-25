"""Payload schema metadata for memory record kinds."""

from membot.agent.conversation_memory.schemas.fact import FACT_KIND
from membot.agent.conversation_memory.schemas.preference import PREFERENCE_KIND
from membot.agent.conversation_memory.schemas.raw_message import RAW_MESSAGE_KIND
from membot.agent.conversation_memory.schemas.summary import SUMMARY_KIND
from membot.agent.conversation_memory.schemas.task import TASK_KIND
from membot.agent.conversation_memory.schemas.graph import GRAPH_KIND

__all__ = [
    "FACT_KIND",
    "PREFERENCE_KIND",
    "RAW_MESSAGE_KIND",
    "SUMMARY_KIND",
    "TASK_KIND",
    "GRAPH_KIND",
]
