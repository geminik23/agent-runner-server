CREATE TABLE namespaces (
    owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
    client_id TEXT NOT NULL CHECK (length(client_id) > 0),
    PRIMARY KEY (owner_id, client_id)
) WITHOUT ROWID;

CREATE TABLE skill_versions (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    skill_id TEXT NOT NULL CHECK (length(skill_id) > 0),
    version TEXT NOT NULL CHECK (length(version) > 0),
    markdown TEXT NOT NULL CHECK (typeof(markdown) = 'text'),
    PRIMARY KEY (owner_id, client_id, skill_id, version),
    FOREIGN KEY (owner_id, client_id) REFERENCES namespaces (owner_id, client_id)
) WITHOUT ROWID;

CREATE TRIGGER skill_versions_immutable
BEFORE UPDATE ON skill_versions
BEGIN
    SELECT RAISE(ABORT, 'skill versions are immutable');
END;

CREATE TRIGGER skill_versions_no_replacement
BEFORE INSERT ON skill_versions
WHEN EXISTS (
    SELECT 1 FROM skill_versions
    WHERE owner_id = NEW.owner_id AND client_id = NEW.client_id
      AND skill_id = NEW.skill_id AND version = NEW.version
      AND CAST(markdown AS BLOB) <> CAST(NEW.markdown AS BLOB)
)
BEGIN
    SELECT RAISE(ABORT, 'skill versions are immutable');
END;

CREATE TRIGGER skill_versions_no_delete
BEFORE DELETE ON skill_versions
BEGIN
    SELECT RAISE(ABORT, 'skill versions are retained');
END;

CREATE TABLE sessions (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    session_id TEXT NOT NULL CHECK (length(session_id) > 0),
    runtime TEXT NOT NULL CHECK (length(runtime) > 0),
    state_realm TEXT NOT NULL CHECK (length(state_realm) > 0),
    native_id TEXT CHECK (native_id IS NULL OR length(native_id) > 0),
    reserved_run_id TEXT,
    cleanup TEXT CHECK (cleanup IS NULL OR cleanup IN ('settled', 'uncertain')),
    disposition TEXT CHECK (disposition IS NULL OR disposition IN ('reusable', 'uncertain', 'not_started')),
    PRIMARY KEY (owner_id, client_id, session_id),
    UNIQUE (runtime, state_realm, native_id),
    FOREIGN KEY (owner_id, client_id) REFERENCES namespaces (owner_id, client_id),
    FOREIGN KEY (owner_id, client_id, reserved_run_id)
        REFERENCES runs (owner_id, client_id, run_id) DEFERRABLE INITIALLY DEFERRED
) WITHOUT ROWID;

CREATE TABLE runs (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL CHECK (length(run_id) > 0),
    session_id TEXT,
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) > 0),
    normalized_request BLOB NOT NULL,
    manifest BLOB NOT NULL,
    runtime TEXT NOT NULL CHECK (length(runtime) > 0),
    synthetic INTEGER NOT NULL CHECK (synthetic IN (0, 1)),
    scenario TEXT,
    state TEXT NOT NULL CHECK (state IN ('queued', 'preparing', 'running', 'validating', 'succeeded', 'failed', 'cancelled', 'timed_out', 'interrupted')),
    PRIMARY KEY (owner_id, client_id, run_id),
    UNIQUE (owner_id, client_id, idempotency_key),
    CHECK (synthetic = 0 OR (scenario IS NOT NULL AND length(scenario) > 0)),
    CHECK (runtime <> 'dummy' OR synthetic = 1),
    FOREIGN KEY (owner_id, client_id) REFERENCES namespaces (owner_id, client_id),
    FOREIGN KEY (owner_id, client_id, session_id) REFERENCES sessions (owner_id, client_id, session_id)
) WITHOUT ROWID;

CREATE TABLE attempts (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL CHECK (length(attempt_id) > 0),
    state TEXT NOT NULL CHECK (state IN ('queued', 'preparing', 'running', 'validating', 'succeeded', 'failed', 'cancelled', 'timed_out', 'interrupted')),
    spawn_intent INTEGER NOT NULL DEFAULT 0 CHECK (spawn_intent IN (0, 1)),
    cleanup TEXT CHECK (cleanup IS NULL OR cleanup IN ('settled', 'uncertain')),
    disposition TEXT CHECK (disposition IS NULL OR disposition IN ('not_applicable', 'reusable', 'uncertain')),
    PRIMARY KEY (owner_id, client_id, run_id, attempt_id),
    FOREIGN KEY (owner_id, client_id, run_id) REFERENCES runs (owner_id, client_id, run_id)
) WITHOUT ROWID;

CREATE TABLE run_skills (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    skill_id TEXT NOT NULL,
    version TEXT NOT NULL,
    PRIMARY KEY (owner_id, client_id, run_id, skill_id),
    FOREIGN KEY (owner_id, client_id, run_id) REFERENCES runs (owner_id, client_id, run_id),
    FOREIGN KEY (owner_id, client_id, skill_id, version) REFERENCES skill_versions (owner_id, client_id, skill_id, version)
) WITHOUT ROWID;

CREATE TABLE hook_bindings (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    binding_id TEXT NOT NULL CHECK (length(binding_id) > 0),
    hook_revision TEXT NOT NULL CHECK (length(hook_revision) > 0),
    destination_revision TEXT NOT NULL CHECK (length(destination_revision) > 0),
    projection BLOB NOT NULL,
    PRIMARY KEY (owner_id, client_id, run_id, binding_id),
    FOREIGN KEY (owner_id, client_id, run_id) REFERENCES runs (owner_id, client_id, run_id)
) WITHOUT ROWID;

CREATE TABLE events (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    event_id TEXT NOT NULL CHECK (length(event_id) > 0),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    kind TEXT NOT NULL CHECK (length(kind) > 0),
    payload BLOB NOT NULL,
    PRIMARY KEY (owner_id, client_id, event_id),
    UNIQUE (owner_id, client_id, run_id, event_id),
    UNIQUE (owner_id, client_id, run_id, sequence),
    FOREIGN KEY (owner_id, client_id, run_id) REFERENCES runs (owner_id, client_id, run_id)
) WITHOUT ROWID;

CREATE UNIQUE INDEX one_terminal_event_per_run
ON events (owner_id, client_id, run_id) WHERE kind = 'run.finished';

CREATE TABLE results (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    revision TEXT NOT NULL CHECK (length(revision) > 0),
    payload BLOB NOT NULL,
    PRIMARY KEY (owner_id, client_id, run_id),
    FOREIGN KEY (owner_id, client_id, run_id) REFERENCES runs (owner_id, client_id, run_id)
) WITHOUT ROWID;

CREATE TABLE delivery_intents (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    delivery_id TEXT NOT NULL CHECK (length(delivery_id) > 0),
    run_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'leased', 'retry_pending', 'succeeded', 'failed', 'uncertain', 'skipped')),
    PRIMARY KEY (owner_id, client_id, delivery_id),
    UNIQUE (owner_id, client_id, event_id, binding_id),
    FOREIGN KEY (owner_id, client_id, run_id, event_id) REFERENCES events (owner_id, client_id, run_id, event_id),
    FOREIGN KEY (owner_id, client_id, run_id, binding_id) REFERENCES hook_bindings (owner_id, client_id, run_id, binding_id)
) WITHOUT ROWID;

CREATE TABLE inbox_items (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    inbox_id TEXT NOT NULL CHECK (length(inbox_id) > 0),
    run_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (owner_id, client_id, inbox_id),
    UNIQUE (owner_id, client_id, run_id, event_id),
    FOREIGN KEY (owner_id, client_id, run_id, event_id) REFERENCES events (owner_id, client_id, run_id, event_id)
) WITHOUT ROWID;
