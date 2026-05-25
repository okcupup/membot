"""LLM-backed knowledge graph extraction for conversation message batches."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

from loguru import logger

from membot.agent.conversation_memory.extractors.base import MemoryExtractor
from membot.agent.conversation_memory.models import MemoryRecord, MemorySource
from membot.agent.conversation_memory.schemas.graph import GRAPH_KIND

if TYPE_CHECKING:
    from membot.providers.base import LLMProvider


_SAVE_GRAPH_EXTRACTION_TOOL = [
    {
        "type": "function",
        "function": {
            "name": "save_graph_extraction",
            "description": "Save entities, relations, and triples extracted from conversation messages.",
            "parameters": {
                "type": "object",
                "properties": {
                    "entities": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "name": {"type": "string"},
                                "type": {"type": "string"},
                                "aliases": {"type": "array", "items": {"type": "string"}},
                                "description": {"type": "string"},
                                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                                "observed_at": {"type": ["string", "null"]},
                                "valid_from": {"type": ["string", "null"]},
                                "valid_until": {"type": ["string", "null"]},
                                "expires_at": {"type": ["string", "null"]},
                                "source_message_ids": {"type": "array", "items": {"type": "string"}},
                            },
                            "required": ["name"],
                        },
                    },
                    "relations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "subject": {"type": "string"},
                                "predicate": {"type": "string"},
                                "object": {"type": "string"},
                                "description": {"type": "string"},
                                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                                "observed_at": {"type": ["string", "null"]},
                                "valid_from": {"type": ["string", "null"]},
                                "valid_until": {"type": ["string", "null"]},
                                "expires_at": {"type": ["string", "null"]},
                                "source_message_ids": {"type": "array", "items": {"type": "string"}},
                            },
                            "required": ["subject", "predicate", "object"],
                        },
                    },
                    "triples": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "subject": {"type": "string"},
                                "predicate": {"type": "string"},
                                "object": {"type": "string"},
                                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                                "observed_at": {"type": ["string", "null"]},
                                "valid_from": {"type": ["string", "null"]},
                                "valid_until": {"type": ["string", "null"]},
                                "expires_at": {"type": ["string", "null"]},
                                "source_message_ids": {"type": "array", "items": {"type": "string"}},
                            },
                            "required": ["subject", "predicate", "object"],
                        },
                    },
                },
                "required": ["entities", "relations", "triples"],
            },
        },
    }
]


class GraphExtractionExtractor(MemoryExtractor):
    """Extract a knowledge graph record from a conversation message batch."""

    TOOL_NAME = "save_graph_extraction"

    def __init__(
        self,
        provider: LLMProvider,
        *,
        model: str | None = None,
        max_tokens: int = 4096,
        temperature: float = 0.0,
        extractor_version: str = "graph-v1",
    ):
        self.provider = provider
        self.model = model
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
                tools=_SAVE_GRAPH_EXTRACTION_TOOL,
                model=self.model,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )
        except Exception:
            logger.exception("Graph extraction provider call failed")
            return ()

        arguments = _get_graph_arguments(response)
        if arguments is None:
            return ()

        payload = _build_graph_payload(arguments, messages, source, self.extractor_version)
        if payload is None:
            return ()

        return (
            MemoryRecord(
                kind=GRAPH_KIND,
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


def _get_graph_arguments(response: Any) -> dict[str, Any] | None:
    if not getattr(response, "has_tool_calls", False):
        logger.warning("Graph extraction skipped: LLM did not call {}", GraphExtractionExtractor.TOOL_NAME)
        return None

    tool_calls = getattr(response, "tool_calls", ())
    for tool_call in tool_calls:
        if getattr(tool_call, "name", None) != GraphExtractionExtractor.TOOL_NAME:
            continue
        arguments = getattr(tool_call, "arguments", None)
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                logger.warning("Graph extraction skipped: invalid JSON tool arguments")
                return None
        if isinstance(arguments, dict):
            return arguments

        logger.warning(
            "Graph extraction skipped: unexpected tool arguments type {}",
            type(arguments).__name__,
        )
        return None

    logger.warning("Graph extraction skipped: graph tool call was not returned")
    return None


def _build_graph_payload(
    arguments: dict[str, Any],
    messages: Sequence[dict[str, Any]],
    source: MemorySource,
    extractor_version: str,
) -> dict[str, Any] | None:
    entities = _normalize_entities(arguments.get("entities"))
    relations = _normalize_relations(arguments.get("relations"))
    triples = _normalize_triples(arguments.get("triples"))
    if not entities and not relations and not triples:
        return None

    return {
        "entities": entities,
        "relations": relations,
        "triples": triples,
        "time_range": _get_time_range(messages, source),
        "covered_message_ids": _get_message_ids(messages),
        "extractor_version": extractor_version,
    }


def _build_system_prompt() -> str:
    return (
        "You extract knowledge graph memory from conversation messages. "
        "Call the save_graph_extraction tool exactly once. "
        "Only extract information valuable for long-term memory, later retrieval, or graph reasoning. "
        "Do not turn ordinary greetings into entities or relations. "
        "Do not treat runtime context, tool protocols, or system noise as user facts. "
        "Extract preferences, relations, valid time ranges, and expiration times only when the conversation gives evidence. "
        "Return null for uncertain time fields; do not guess. "
        "Use stable, concise entity names. "
        "Use concise predicates from the preferred set when possible: uses, prefers, works_on, depends_on, mentions, replaces, conflicts_with, related_to, other. "
        "Triples must be faithful to the input and must not add unsupported facts."
    )


def _build_user_prompt(messages: Sequence[dict[str, Any]]) -> str:
    rendered = json.dumps(list(messages), ensure_ascii=False, default=str)
    return (
        "Extract graph memory from this conversation message batch.\n\n"
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


def _get_confidence(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    confidence = float(value)
    return confidence if 0.0 <= confidence <= 1.0 else None


def _get_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _get_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []

    result: list[str] = []
    for item in value:
        item = _get_optional_string(item)
        if item and item not in result:
            result.append(item)
    return result


def _normalize_entities(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    entities: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for item in value:
        if not isinstance(item, Mapping):
            continue

        name = _get_optional_string(item.get("name"))
        if not name:
            continue

        entity: dict[str, Any] = {"name": name}
        _set_optional(entity, "id", item.get("id"))
        _set_optional(entity, "type", item.get("type"))
        aliases = _get_string_list(item.get("aliases"))
        if aliases:
            entity["aliases"] = aliases
        _set_optional(entity, "description", item.get("description"))
        _set_confidence(entity, item.get("confidence"))
        _set_time_fields(entity, item)
        source_ids = _get_string_list(item.get("source_message_ids"))
        if source_ids:
            entity["source_message_ids"] = source_ids

        key = (entity.get("id"), entity["name"].lower(), entity.get("type"))
        if key in seen:
            continue
        seen.add(key)
        entities.append(entity)
    return entities


def _normalize_relations(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    relations: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in value:
        if not isinstance(item, Mapping):
            continue

        subject = _get_optional_string(item.get("subject"))
        predicate = _get_optional_string(item.get("predicate"))
        obj = _get_optional_string(item.get("object"))
        if not subject or not predicate or not obj:
            continue

        relation: dict[str, Any] = {
            "subject": subject,
            "predicate": predicate,
            "object": obj,
        }
        _set_optional(relation, "id", item.get("id"))
        _set_optional(relation, "description", item.get("description"))
        _set_confidence(relation, item.get("confidence"))
        _set_time_fields(relation, item)
        source_ids = _get_string_list(item.get("source_message_ids"))
        if source_ids:
            relation["source_message_ids"] = source_ids

        key = (subject.lower(), predicate.lower(), obj.lower())
        if key in seen:
            continue
        seen.add(key)
        relations.append(relation)
    return relations


def _normalize_triples(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    triples: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in value:
        if not isinstance(item, Mapping):
            continue

        subject = _get_optional_string(item.get("subject"))
        predicate = _get_optional_string(item.get("predicate"))
        obj = _get_optional_string(item.get("object"))
        if not subject or not predicate or not obj:
            continue

        triple: dict[str, Any] = {
            "subject": subject,
            "predicate": predicate,
            "object": obj,
        }
        _set_confidence(triple, item.get("confidence"))
        _set_time_fields(triple, item)
        source_ids = _get_string_list(item.get("source_message_ids"))
        if source_ids:
            triple["source_message_ids"] = source_ids

        key = (subject.lower(), predicate.lower(), obj.lower())
        if key in seen:
            continue
        seen.add(key)
        triples.append(triple)
    return triples


def _set_optional(target: dict[str, Any], key: str, value: Any) -> None:
    value = _get_optional_string(value)
    if value is not None:
        target[key] = value


def _set_confidence(target: dict[str, Any], value: Any) -> None:
    confidence = _get_confidence(value)
    if confidence is not None:
        target["confidence"] = confidence


def _set_time_fields(target: dict[str, Any], item: Mapping[str, Any]) -> None:
    for field in ("observed_at", "valid_from", "valid_until", "expires_at"):
        if field not in item:
            continue
        target[field] = _get_optional_string(item.get(field))
