# Conversation Memory Architecture

This document describes the current conversation memory lifecycle and the
extension points for future memory types.

## Baseline Lifecycle

The first implementation preserves the previous session-history behavior while
moving the responsibilities behind explicit memory pipeline interfaces.

The baseline pipeline is:

1. Extraction: `RawMessageExtractor`
   - Location: `agent/conversation_memory/extractors/raw_message.py`
   - Converts each sanitized, newly-added message from the current turn into a
     `raw_message` `MemoryRecord`.

2. Management: `SimpleMessageManager`
   - Location: `agent/conversation_memory/managers/simple.py`
   - Keeps record order and filters out empty payloads.
   - Does not summarize, merge, deduplicate, promote, rank, or retrieve.

3. Storage: `JsonlMessageStore`
   - Location: `agent/conversation_memory/stores/jsonl.py`
   - Wraps the existing `SessionManager`.
   - Keeps the JSONL format unchanged: first line is metadata, following lines
     are session message dictionaries.

4. Retrieval: `RecentMessageRetriever`
   - Location: `agent/conversation_memory/retrievers/recent.py`
   - Reads the current session messages, takes the most recent N messages, and
     preserves the old `Session.get_history()` alignment rule: if the truncated
     window starts with a non-user message, drop messages until the first user
     message. This avoids orphaned assistant/tool-result blocks.

The orchestrator is `ConversationMemoryEngine` in
`agent/conversation_memory/engine.py`.

## AgentLoop Integration

`AgentLoop` owns both the existing `SessionManager` and the new
`ConversationMemoryEngine`.

In `AgentLoop.__init__`, the engine is created with the same `SessionManager`
instance:

```python
self.sessions = session_manager or SessionManager(workspace)
self.memory_engine = ConversationMemoryEngine.for_workspace(
    workspace,
    session_manager=self.sessions,
)
```

This keeps compatibility with code that still reads `self.sessions`, while the
main conversation history path goes through the engine.

Current delegated calls in `agent/loop.py`:

- Session lookup:
  `self.memory_engine.store.get_or_create_session(key)`
- History retrieval:
  `await self.memory_engine.get_history(key, self.memory_window)`
- Turn persistence:
  `await self.memory_engine.save_turn(key, all_msgs, 1 + len(history))`
- `/new` clearing:
  `await self.memory_engine.clear(session.key)`

The prompt shape passed to the model is unchanged. `ContextBuilder.build_messages`
still receives the same OpenAI-style history list, then adds system prompt,
runtime context, and the current user message.

## Raw Message Payload

`raw_message` is the baseline payload kind.

The `MemoryRecord` envelope is defined in `agent/conversation_memory/models.py`.
For `raw_message`, the payload is a session-style message dictionary. It must
include:

```python
{
    "role": str,
    "content": Any,
    "timestamp": str,
}
```

It may also include fields used by tool-call history:

```python
{
    "tool_calls": list[dict],
    "tool_call_id": str,
    "name": str,
}
```

The extractor intentionally preserves additional sanitized message fields, such
as `reasoning_content` and `thinking_blocks`, because the previous `_save_turn`
logic wrote those fields to JSONL when present.

Before extraction, `sanitize_messages_for_storage()` in
`agent/conversation_memory/sanitizer.py` mirrors the old save behavior:

- skips runtime-context user messages,
- skips empty assistant messages without tool calls,
- truncates long tool results,
- replaces base64 image payloads with `[image]`,
- adds a `timestamp` when missing.

## Envelope vs Payload

The architecture standardizes the lifecycle interfaces, not every payload
schema.

The common envelope is `MemoryRecord`:

```python
MemoryRecord(
    kind="raw_message",
    payload={...},
    scope=MemoryScope.SESSION,
    session_key="cli:direct",
    ...
)
```

Different extractors are expected to produce different payload shapes. The
`kind` identifies how to interpret `payload`.

Examples:

```python
# raw_message payload
{
    "role": "user",
    "content": "hello",
    "timestamp": "2026-05-20T12:00:00",
}

# summary payload
{
    "summary": "The user asked about memory refactoring.",
    "covered_message_ids": ["..."],
    "time_range": {"start": "...", "end": "..."},
}

# fact payload
{
    "subject": "user",
    "predicate": "prefers",
    "object": "minimal invasive refactors",
    "confidence": 0.8,
}
```

Schemas for known kinds live under `agent/conversation_memory/schemas/`.

## Future Extensions

### SummaryExtractor

A `SummaryExtractor` can be added under `agent/conversation_memory/extractors/`.
It should emit `MemoryRecord(kind="summary", payload=...)`.

Expected responsibilities:

- select a range of raw messages,
- call or receive a summarizer,
- produce a compact summary payload,
- record which message ids or time range it covers.

The manager layer can later decide when summaries should be created, retained,
or promoted.

### FactExtractor

A `FactExtractor` can emit durable facts from conversation text:

```python
{
    "subject": "...",
    "predicate": "...",
    "object": "...",
    "confidence": 0.0,
}
```

Future managers can deduplicate facts, resolve conflicts, or promote high
confidence facts into long-term memory.

### VectorStore

A vector store should implement the store/retrieval side without changing the
`MemoryRecord` envelope.

Likely shape:

- store the record envelope and payload metadata,
- embed selected text from the payload,
- keep `kind`, `session_key`, `scope`, and timestamps as filters,
- return `RetrievedMemory` with similarity scores.

This can live as `VectorMemoryStore` or similar under
`agent/conversation_memory/stores/`.

### HybridRetriever

A `HybridRetriever` can combine multiple retrieval strategies:

- recent raw messages from `RecentMessageRetriever`,
- keyword matches,
- vector similarity results,
- summaries or facts,
- optional reranking.

It should return `RetrievedMemory` objects, leaving prompt-specific conversion
to formatters such as `OpenAIMessageFormatter`.

## Design Rule

Keep the lifecycle stable:

```text
extract -> manage -> store
retrieve -> format
```

Keep payloads flexible:

```text
MemoryRecord is shared.
Payload schemas are kind-specific.
```

This is the key boundary that allows the current JSONL session history to remain
compatible while adding summaries, facts, preferences, tasks, Redis, vectors, and
hybrid retrieval later.
