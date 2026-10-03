ALTER TABLE service_jobs ADD COLUMN independent_new_sessions INTEGER NOT NULL DEFAULT 0 CHECK (independent_new_sessions IN (0, 1));

CREATE TABLE session_workspaces (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    plan BLOB NOT NULL,
    managed_root TEXT,
    materialization_intent INTEGER NOT NULL DEFAULT 0 CHECK (materialization_intent IN (0, 1)),
    directory BLOB,
    PRIMARY KEY (owner_id, client_id, session_id),
    FOREIGN KEY (owner_id, client_id, session_id) REFERENCES sessions (owner_id, client_id, session_id)
) WITHOUT ROWID;

CREATE INDEX managed_workspace_capacity ON session_workspaces (managed_root);

CREATE TABLE run_workspaces (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    resource_path TEXT NOT NULL CHECK (length(resource_path) > 0),
    access TEXT NOT NULL CHECK (access IN ('shared_read', 'exclusive')),
    held INTEGER NOT NULL DEFAULT 0 CHECK (held IN (0, 1)),
    uncertain INTEGER NOT NULL DEFAULT 0 CHECK (uncertain IN (0, 1)),
    PRIMARY KEY (owner_id, client_id, run_id),
    FOREIGN KEY (owner_id, client_id, run_id) REFERENCES runs (owner_id, client_id, run_id)
) WITHOUT ROWID;

CREATE INDEX active_workspace_reservations ON run_workspaces (held, resource_path);

CREATE TABLE workspace_ancestors (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    device TEXT NOT NULL,
    inode TEXT NOT NULL,
    is_target INTEGER NOT NULL CHECK (is_target IN (0, 1)),
    PRIMARY KEY (owner_id, client_id, run_id, device, inode),
    FOREIGN KEY (owner_id, client_id, run_id) REFERENCES run_workspaces (owner_id, client_id, run_id)
) WITHOUT ROWID;

CREATE INDEX workspace_identity_lookup ON workspace_ancestors (device, inode, is_target);

CREATE TABLE workspace_settlements (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (owner_id, client_id, run_id),
    FOREIGN KEY (owner_id, client_id, run_id) REFERENCES run_workspaces (owner_id, client_id, run_id)
) WITHOUT ROWID;

CREATE TABLE workspace_journal_epochs (
    owner_identity TEXT PRIMARY KEY,
    epoch INTEGER NOT NULL CHECK (epoch > 0)
) WITHOUT ROWID;
