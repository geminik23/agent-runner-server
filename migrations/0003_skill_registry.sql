CREATE TABLE skills (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    skill_id TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    PRIMARY KEY (owner_id, client_id, skill_id),
    FOREIGN KEY (owner_id, client_id) REFERENCES namespaces (owner_id, client_id)
) WITHOUT ROWID;

CREATE TABLE skill_policies (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    skill_id TEXT NOT NULL,
    version TEXT NOT NULL,
    required_capabilities TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    PRIMARY KEY (owner_id, client_id, skill_id, version),
    FOREIGN KEY (owner_id, client_id, skill_id, version)
        REFERENCES skill_versions (owner_id, client_id, skill_id, version)
) WITHOUT ROWID;

CREATE TRIGGER skill_policy_capabilities_immutable
BEFORE UPDATE OF required_capabilities ON skill_policies
BEGIN
    SELECT RAISE(ABORT, 'skill capabilities are immutable');
END;

INSERT INTO skills (owner_id, client_id, skill_id)
SELECT DISTINCT owner_id, client_id, skill_id FROM skill_versions;

INSERT INTO skill_policies (owner_id, client_id, skill_id, version, required_capabilities)
SELECT owner_id, client_id, skill_id, version, '[]' FROM skill_versions;

CREATE TABLE skill_defaults (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    skill_id TEXT NOT NULL,
    version TEXT NOT NULL,
    PRIMARY KEY (owner_id, client_id, position),
    UNIQUE (owner_id, client_id, skill_id),
    FOREIGN KEY (owner_id, client_id, skill_id, version)
        REFERENCES skill_versions (owner_id, client_id, skill_id, version)
) WITHOUT ROWID;

ALTER TABLE run_skills ADD COLUMN position INTEGER NOT NULL DEFAULT 0 CHECK (position >= 0);

CREATE TABLE session_skills (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    skill_id TEXT NOT NULL,
    version TEXT NOT NULL,
    PRIMARY KEY (owner_id, client_id, session_id, position),
    UNIQUE (owner_id, client_id, session_id, skill_id),
    FOREIGN KEY (owner_id, client_id, session_id)
        REFERENCES sessions (owner_id, client_id, session_id),
    FOREIGN KEY (owner_id, client_id, skill_id, version)
        REFERENCES skill_versions (owner_id, client_id, skill_id, version)
) WITHOUT ROWID;

CREATE TABLE attempt_prompt_snapshots (
    owner_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    compiler_revision TEXT NOT NULL,
    context BLOB NOT NULL,
    prompt BLOB NOT NULL,
    PRIMARY KEY (owner_id, client_id, run_id, attempt_id),
    FOREIGN KEY (owner_id, client_id, run_id, attempt_id)
        REFERENCES attempts (owner_id, client_id, run_id, attempt_id)
) WITHOUT ROWID;

CREATE TRIGGER attempt_prompt_snapshots_immutable
BEFORE UPDATE ON attempt_prompt_snapshots
BEGIN
    SELECT RAISE(ABORT, 'prompt snapshots are immutable');
END;
