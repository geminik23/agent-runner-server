CREATE TABLE service_sessions (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    profile_revision TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    PRIMARY KEY (owner_id, client_id, session_id),
    FOREIGN KEY (owner_id, client_id, session_id)
        REFERENCES sessions (owner_id, client_id, session_id)
) WITHOUT ROWID;

CREATE TABLE service_jobs (
    queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    credential_generation TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    state_realm TEXT NOT NULL,
    deadline_ms INTEGER NOT NULL,
    claim_token TEXT,
    stop_cause TEXT CHECK (stop_cause IS NULL OR stop_cause IN ('cancelled', 'timed_out', 'interrupted')),
    UNIQUE (owner_id, client_id, run_id),
    FOREIGN KEY (owner_id, client_id, run_id, attempt_id)
        REFERENCES attempts (owner_id, client_id, run_id, attempt_id)
);

CREATE INDEX service_jobs_queue ON service_jobs (queue_id, claim_token);
