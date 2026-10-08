ALTER TABLE invocations ADD COLUMN next_event_seq BIGINT NOT NULL DEFAULT 1;
ALTER TABLE invocation_events
    ADD COLUMN event_seq BIGINT,
    ADD COLUMN worker_id TEXT,
    ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    ADD COLUMN step TEXT NOT NULL DEFAULT 'recorded',
    ADD COLUMN span_id TEXT,
    ADD COLUMN parent_span_id TEXT,
    ADD COLUMN tool_call_id TEXT,
    ADD COLUMN duration_ms DOUBLE PRECISION CHECK (duration_ms IS NULL OR duration_ms >= 0),
    ADD COLUMN error_code TEXT,
    ADD COLUMN expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '7 days');
WITH ranked AS (
    SELECT event_id, row_number() OVER (PARTITION BY invocation_id ORDER BY event_id) AS seq
    FROM invocation_events
)
UPDATE invocation_events e SET event_seq = ranked.seq FROM ranked WHERE e.event_id = ranked.event_id;
UPDATE invocation_events SET expires_at = created_at + interval '7 days';
UPDATE invocations i SET next_event_seq = COALESCE((
    SELECT max(event_seq) + 1 FROM invocation_events e WHERE e.invocation_id = i.invocation_id
), 1);
ALTER TABLE invocation_events ALTER COLUMN event_seq SET NOT NULL;
ALTER TABLE invocation_events ADD CONSTRAINT invocation_events_sequence UNIQUE (invocation_id, event_seq);
ALTER TABLE invocation_events ALTER COLUMN created_at SET DEFAULT clock_timestamp();
CREATE INDEX invocation_events_expiry ON invocation_events(expires_at);
