CREATE TABLE run_retention (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    retention_ms INTEGER NOT NULL CHECK (retention_ms > 0),
    expires_ms INTEGER,
    expired INTEGER NOT NULL DEFAULT 0 CHECK (expired IN (0, 1)),
    purged INTEGER NOT NULL DEFAULT 0 CHECK (purged IN (0, 1)),
    PRIMARY KEY (owner_id, client_id, run_id),
    FOREIGN KEY (owner_id, client_id, run_id) REFERENCES runs (owner_id, client_id, run_id),
    CHECK (expired = 0 OR expires_ms IS NOT NULL),
    CHECK (purged = 0 OR expired = 1)
) WITHOUT ROWID;

CREATE INDEX run_retention_due ON run_retention (expired, expires_ms);
