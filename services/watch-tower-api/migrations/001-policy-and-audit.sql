CREATE SCHEMA IF NOT EXISTS watch_tower;

CREATE TABLE IF NOT EXISTS watch_tower.schema_migration (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS watch_tower.identity_link (
    issuer text NOT NULL,
    client_id text NOT NULL,
    external_subject text NOT NULL,
    subject_id text NOT NULL,
    fitness_owner_id text NOT NULL,
    linked_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz,
    PRIMARY KEY (issuer, client_id, external_subject)
);
CREATE INDEX IF NOT EXISTS identity_link_subject_idx
    ON watch_tower.identity_link (subject_id) WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS watch_tower.consent_grant (
    grant_id text PRIMARY KEY,
    subject_id text NOT NULL,
    client_id text NOT NULL,
    scope text NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    granted_by_identity text NOT NULL,
    granted_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    revoked_at timestamptz,
    UNIQUE (subject_id, client_id, scope, version)
);
CREATE INDEX IF NOT EXISTS consent_grant_active_idx
    ON watch_tower.consent_grant (subject_id, client_id, scope, version DESC)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS watch_tower.mcp_audit (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    client_id text NOT NULL,
    operation text NOT NULL,
    tool_name text NOT NULL,
    outcome text NOT NULL DEFAULT 'started'
        CHECK (outcome IN ('started', 'succeeded', 'failed')),
    actor_id text NOT NULL DEFAULT 'unknown',
    subject_id text,
    authorization_allowed boolean NOT NULL DEFAULT false,
    consent_version bigint
);
ALTER TABLE watch_tower.mcp_audit
    ADD COLUMN IF NOT EXISTS actor_id text NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS subject_id text,
    ADD COLUMN IF NOT EXISTS authorization_allowed boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS consent_version bigint;
CREATE INDEX IF NOT EXISTS mcp_audit_occurred_at_idx
    ON watch_tower.mcp_audit (occurred_at DESC);
CREATE INDEX IF NOT EXISTS mcp_audit_client_occurred_at_idx
    ON watch_tower.mcp_audit (client_id, occurred_at DESC);
