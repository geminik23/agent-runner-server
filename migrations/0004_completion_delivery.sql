CREATE TABLE completion_hook_versions (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    hook_id TEXT NOT NULL CHECK (length(hook_id) > 0),
    version TEXT NOT NULL CHECK (length(version) > 0),
    destination_id TEXT NOT NULL CHECK (length(destination_id) > 0),
    destination_revision TEXT NOT NULL CHECK (length(destination_revision) > 0),
    destination_snapshot BLOB NOT NULL,
    events TEXT NOT NULL,
    projection TEXT NOT NULL CHECK (projection = 'metadata'),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    PRIMARY KEY (owner_id, client_id, hook_id, version),
    FOREIGN KEY (owner_id, client_id) REFERENCES namespaces (owner_id, client_id)
) WITHOUT ROWID;

CREATE TRIGGER completion_hook_version_immutable
BEFORE UPDATE OF destination_id, destination_revision, destination_snapshot, events, projection ON completion_hook_versions
BEGIN
    SELECT RAISE(ABORT, 'hook versions are immutable');
END;

CREATE TRIGGER completion_hook_version_no_replacement
BEFORE INSERT ON completion_hook_versions
WHEN EXISTS (
    SELECT 1 FROM completion_hook_versions
    WHERE owner_id = NEW.owner_id AND client_id = NEW.client_id
      AND hook_id = NEW.hook_id AND version = NEW.version
      AND (destination_id <> NEW.destination_id OR destination_revision <> NEW.destination_revision
        OR destination_snapshot <> NEW.destination_snapshot OR events <> NEW.events
        OR projection <> NEW.projection)
)
BEGIN
    SELECT RAISE(ABORT, 'hook versions are immutable');
END;

CREATE TRIGGER completion_hook_version_no_delete
BEFORE DELETE ON completion_hook_versions
BEGIN
    SELECT RAISE(ABORT, 'hook versions are retained');
END;

CREATE TABLE completion_hook_defaults (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    hook_id TEXT NOT NULL,
    version TEXT NOT NULL,
    PRIMARY KEY (owner_id, client_id, position),
    UNIQUE (owner_id, client_id, hook_id),
    FOREIGN KEY (owner_id, client_id, hook_id, version)
        REFERENCES completion_hook_versions (owner_id, client_id, hook_id, version)
) WITHOUT ROWID;

CREATE TABLE completion_bindings (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    hook_id TEXT NOT NULL,
    hook_version TEXT NOT NULL,
    destination_id TEXT NOT NULL,
    destination_revision TEXT NOT NULL,
    destination_snapshot BLOB NOT NULL,
    connector TEXT NOT NULL CHECK (connector IN ('inbox', 'webhook_test', 'notion_mock')),
    events TEXT NOT NULL,
    projection TEXT NOT NULL CHECK (projection = 'metadata'),
    max_payload_bytes INTEGER NOT NULL CHECK (max_payload_bytes > 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    delivery_ttl_ms INTEGER NOT NULL CHECK (delivery_ttl_ms > 0),
    retention_ms INTEGER NOT NULL CHECK (retention_ms > 0),
    charged INTEGER NOT NULL DEFAULT 1 CHECK (charged IN (0, 1)),
    PRIMARY KEY (owner_id, client_id, run_id, binding_id),
    FOREIGN KEY (owner_id, client_id, run_id, binding_id)
        REFERENCES hook_bindings (owner_id, client_id, run_id, binding_id),
    FOREIGN KEY (owner_id, client_id, hook_id, hook_version)
        REFERENCES completion_hook_versions (owner_id, client_id, hook_id, version)
) WITHOUT ROWID;

CREATE TABLE completion_delivery_jobs (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    lease_owner TEXT,
    lease_generation INTEGER NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
    lease_until_ms INTEGER,
    dispatch_started INTEGER NOT NULL DEFAULT 0 CHECK (dispatch_started IN (0, 1)),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_ms INTEGER NOT NULL,
    expires_ms INTEGER NOT NULL,
    settled_ms INTEGER,
    reconciled_ms INTEGER,
    reconciled_by TEXT,
    reconciled_generation TEXT,
    reconcile_outcome TEXT CHECK (reconcile_outcome IS NULL OR reconcile_outcome IN ('confirmed_delivered', 'confirmed_not_delivered')),
    payload_purged INTEGER NOT NULL DEFAULT 0 CHECK (payload_purged IN (0, 1)),
    last_reason TEXT,
    PRIMARY KEY (owner_id, client_id, delivery_id),
    FOREIGN KEY (owner_id, client_id, delivery_id)
        REFERENCES delivery_intents (owner_id, client_id, delivery_id)
) WITHOUT ROWID;

CREATE INDEX completion_delivery_ready ON completion_delivery_jobs (next_attempt_ms, expires_ms);

CREATE TABLE completion_delivery_attempts (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    lease_generation INTEGER NOT NULL,
    started_ms INTEGER NOT NULL,
    observed_ms INTEGER,
    observation TEXT,
    PRIMARY KEY (owner_id, client_id, delivery_id, lease_generation),
    FOREIGN KEY (owner_id, client_id, delivery_id)
        REFERENCES completion_delivery_jobs (owner_id, client_id, delivery_id)
) WITHOUT ROWID;

CREATE TABLE completion_delivery_observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    lease_generation INTEGER NOT NULL,
    observed_ms INTEGER NOT NULL,
    observation TEXT NOT NULL,
    FOREIGN KEY (owner_id, client_id, delivery_id, lease_generation)
        REFERENCES completion_delivery_attempts (owner_id, client_id, delivery_id, lease_generation)
);

CREATE TABLE completion_inbox (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    inbox_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    expires_ms INTEGER NOT NULL,
    acknowledged_ms INTEGER,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'available', 'skipped')),
    reason TEXT,
    payload_purged INTEGER NOT NULL DEFAULT 0 CHECK (payload_purged IN (0, 1)),
    UNIQUE (owner_id, client_id, inbox_id),
    FOREIGN KEY (owner_id, client_id, inbox_id)
        REFERENCES inbox_items (owner_id, client_id, inbox_id),
    FOREIGN KEY (owner_id, client_id, run_id, binding_id)
        REFERENCES completion_bindings (owner_id, client_id, run_id, binding_id)
);

CREATE INDEX completion_inbox_page ON completion_inbox (owner_id, client_id, sequence);
