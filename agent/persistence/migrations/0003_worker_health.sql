CREATE TABLE worker_runtime (
    owner_id TEXT PRIMARY KEY,
    execution_owner TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('RUNNING', 'DRAINING', 'STOPPED')),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
