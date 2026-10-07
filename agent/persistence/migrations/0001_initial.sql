CREATE TABLE sessions (
    owner_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    session_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_session_seq BIGINT NOT NULL DEFAULT 1 CHECK (next_session_seq > 0),
    next_message_seq BIGINT NOT NULL DEFAULT 1 CHECK (next_message_seq > 0),
    PRIMARY KEY (owner_id, session_id),
    UNIQUE (owner_id, session_key)
);

CREATE TABLE invocations (
    invocation_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    session_key TEXT NOT NULL,
    session_seq BIGINT NOT NULL CHECK (session_seq > 0),
    request_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    idempotency_key TEXT,
    payload_hash CHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'QUEUED'
        CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMEOUT')),
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    queue_timeout_seconds DOUBLE PRECISION CHECK (queue_timeout_seconds IS NULL OR queue_timeout_seconds > 0),
    execution_timeout_seconds DOUBLE PRECISION CHECK (execution_timeout_seconds IS NULL OR execution_timeout_seconds > 0),
    execution_owner TEXT,
    execution_lease_until TIMESTAMPTZ,
    result JSONB,
    error_code TEXT,
    error_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    UNIQUE (owner_id, session_id, session_seq),
    FOREIGN KEY (owner_id, session_id) REFERENCES sessions(owner_id, session_id)
);
CREATE UNIQUE INDEX invocations_owner_idempotency_key
    ON invocations(owner_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX invocations_session_order
    ON invocations(owner_id, session_id, session_seq);
CREATE INDEX invocations_status_submitted
    ON invocations(status, submitted_at);
CREATE INDEX invocations_expired_lease
    ON invocations(execution_lease_until)
    WHERE status = 'RUNNING';

CREATE TABLE session_messages (
    owner_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_seq BIGINT NOT NULL CHECK (message_seq > 0),
    invocation_id TEXT NOT NULL REFERENCES invocations(invocation_id),
    session_seq BIGINT NOT NULL,
    turn_index INTEGER NOT NULL CHECK (turn_index >= 0),
    role TEXT NOT NULL,
    message JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_id, session_id, message_seq),
    UNIQUE (owner_id, session_id, session_seq, turn_index),
    FOREIGN KEY (owner_id, session_id) REFERENCES sessions(owner_id, session_id)
);
CREATE INDEX session_messages_recent
    ON session_messages(owner_id, session_id, message_seq DESC);

CREATE TABLE invocation_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    invocation_id TEXT NOT NULL REFERENCES invocations(invocation_id),
    owner_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX invocation_events_order
    ON invocation_events(invocation_id, event_id);

CREATE TABLE outbox (
    outbox_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    invocation_id TEXT NOT NULL UNIQUE REFERENCES invocations(invocation_id),
    envelope JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT
);
CREATE INDEX outbox_pending
    ON outbox(available_at, outbox_id)
    WHERE published_at IS NULL;

CREATE TABLE session_archives (
    owner_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    archive_seq BIGINT NOT NULL,
    invocation_id TEXT NOT NULL UNIQUE REFERENCES invocations(invocation_id),
    messages JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_id, session_id, archive_seq),
    FOREIGN KEY (owner_id, session_id) REFERENCES sessions(owner_id, session_id)
);
