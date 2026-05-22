"""LLM-backed summary extraction for conversation message batches."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

from loguru import logger

from membot.agent.conversation_memory.extractors.base import MemoryExtractor
from membot.agent.conversation_memory.models import MemoryRecord, MemorySource
from membot.agent.conversation_memory.schemas.summary import SUMMARY_KIND

# if TYPE_CHECKING:
from membot.providers.base import LLMProvider


_SAVE_SUMMARY_EXTRACTION_TOOL = [
    {
        "type": "function",
        "function": {
            "name": "save_summary_extraction",
            "description": "Save structured summary memory extracted from conversation messages.",
            "parameters": {
                "type": "object",
                "properties": {
                    "summary": {
                        "type": "string",
                        "description": "Faithful concise summary of memory-relevant conversation content.",
                    },
                    "keywords": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Short search keywords.",
                    },
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Short memory classification tags.",
                    },
                    "topics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Conversation topics worth retrieving later.",
                    },
                    "preferences": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "preference": {"type": "string"},
                                "evidence": {"type": "string"},
                                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                            },
                            "required": ["preference"],
                        },
                        "description": "User preference candidates supported by conversation evidence.",
                    },
                    "confidence": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 1,
                        "description": "Confidence in the extraction.",
                    },
                },
                "required": ["summary"],
            },
        },
    }
]


class SummarizationExtractor(MemoryExtractor):
    """Extract a structured summary record from a conversation message batch."""

    TOOL_NAME = "save_summary_extraction"

    def __init__(
        self,
        provider: LLMProvider,
        *,
        model: str | None = None,
        max_tokens: int = 2048,
        temperature: float = 0.0,
        extractor_version: str = "summarization-v1",
    ):
        self.provider = provider
        self.model = model or provider.get_default_model()
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.extractor_version = extractor_version

    async def extract(self, source: MemorySource) -> Sequence[MemoryRecord]:
        messages = _get_message_batch(source)
        if not messages:
            return ()

        try:
            response = await self.provider.chat(
                messages=[
                    {"role": "system", "content": _build_system_prompt()},
                    {"role": "user", "content": _build_user_prompt(messages)},
                ],
                tools=_SAVE_SUMMARY_EXTRACTION_TOOL,
                model=self.model,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )
        except Exception:
            logger.exception("Summary extraction provider call failed")
            return ()

        arguments = _get_summary_arguments(response)
        if arguments is None:
            return ()

        payload = _build_summary_payload(arguments, messages, source, self.extractor_version)
        if payload is None:
            return ()

        return (
            MemoryRecord(
                kind=SUMMARY_KIND,
                payload=payload,
                scope=source.scope,
                session_key=source.session_key,
                source_kind=source.kind,
                source_id=source.source_id,
                metadata=dict(source.metadata),
            ),
        )


def _get_message_batch(source: MemorySource) -> list[dict[str, Any]]:
    payload = source.payload
    messages = payload.get("messages") if isinstance(payload, Mapping) else None
    if not isinstance(messages, list):
        return []

    return [dict(message) for message in messages if isinstance(message, Mapping)]


def _get_summary_arguments(response: Any) -> dict[str, Any] | None:
    if not getattr(response, "has_tool_calls", False):
        logger.warning("Summary extraction skipped: LLM did not call {}", SummarizationExtractor.TOOL_NAME)
        return None

    tool_calls = getattr(response, "tool_calls", ())
    for tool_call in tool_calls:
        if getattr(tool_call, "name", None) != SummarizationExtractor.TOOL_NAME:
            continue
        arguments = getattr(tool_call, "arguments", None)
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                logger.warning("Summary extraction skipped: invalid JSON tool arguments")
                return None
        if isinstance(arguments, dict):
            return arguments

        logger.warning(
            "Summary extraction skipped: unexpected tool arguments type {}",
            type(arguments).__name__,
        )
        return None

    logger.warning("Summary extraction skipped: summary tool call was not returned")
    return None


def _build_summary_payload(
    arguments: dict[str, Any],
    messages: Sequence[dict[str, Any]],
    source: MemorySource,
    extractor_version: str,
) -> dict[str, Any] | None:
    summary = arguments.get("summary")
    if not isinstance(summary, str) or not summary.strip():
        logger.warning("Summary extraction skipped: summary is missing or empty")
        return None

    payload: dict[str, Any] = {
        "summary": summary.strip(),
        "covered_message_ids": _get_message_ids(messages),
        "time_range": _get_time_range(messages, source),
        "keywords": _get_string_list(arguments.get("keywords")),
        "tags": _get_string_list(arguments.get("tags")),
        "topics": _get_string_list(arguments.get("topics")),
        "preferences": _get_preferences(arguments.get("preferences")),
        "extractor_version": extractor_version,
    }
    confidence = _get_confidence(arguments.get("confidence"))
    if confidence is not None:
        payload["confidence"] = confidence
    return payload


def _build_system_prompt() -> str:
    return (
        "You extract structured conversation summary memory. "
        "Call the save_summary_extraction tool exactly once. "
        "Only extract information useful for later memory retrieval. "
        "Keep the summary faithful to the supplied messages and do not invent details. "
        "Keep keywords, tags, and topics concise. "
        "Extract preferences only when the conversation gives evidence. "
        "Do not treat tool protocols, system noise, or runtime context as user preferences."
    )


def _build_user_prompt(messages: Sequence[dict[str, Any]]) -> str:
    rendered = json.dumps(list(messages), ensure_ascii=False, default=str)
    return (
        "Extract summary memory from this conversation message batch.\n\n"
        "Conversation messages:\n"
        f"{rendered}"
    )


def _get_message_ids(messages: Sequence[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    for message in messages:
        candidate = _first_identifier(
            message.get("id"),
            message.get("message_id"),
            _metadata_identifier(message.get("metadata")),
        )
        if candidate and candidate not in ids:
            ids.append(candidate)
    return ids


def _metadata_identifier(metadata: Any) -> Any:
    if not isinstance(metadata, Mapping):
        return None
    return metadata.get("id") or metadata.get("message_id")


def _first_identifier(*candidates: Any) -> str | None:
    for candidate in candidates:
        if isinstance(candidate, (str, int)):
            value = str(candidate).strip()
            if value:
                return value
    return None


def _get_time_range(messages: Sequence[dict[str, Any]], source: MemorySource) -> dict[str, str | None]:
    timestamps = [
        value for message in messages
        if (value := _timestamp_string(message.get("timestamp"))) is not None
    ]
    if timestamps:
        return {"start": timestamps[0], "end": timestamps[-1]}

    fallback = source.timestamp.isoformat() if source.timestamp else None
    return {"start": fallback, "end": fallback}


def _timestamp_string(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _get_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []

    result: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        item = item.strip()
        if item and item not in result:
            result.append(item)
    return result


def _get_preferences(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    preferences: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue

        preference = item.get("preference")
        if not isinstance(preference, str) or not preference.strip():
            continue

        entry: dict[str, Any] = {"preference": preference.strip()}
        evidence = item.get("evidence")
        if isinstance(evidence, str) and evidence.strip():
            entry["evidence"] = evidence.strip()
        confidence = _get_confidence(item.get("confidence"))
        if confidence is not None:
            entry["confidence"] = confidence
        preferences.append(entry)
    return preferences


def _get_confidence(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    confidence = float(value)
    return confidence if 0.0 <= confidence <= 1.0 else None
