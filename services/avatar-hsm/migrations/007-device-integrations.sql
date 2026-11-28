CREATE TABLE IF NOT EXISTS avatar_hsm.device_connection (
    subject_id text NOT NULL REFERENCES avatar_hsm.subject,
    provider text NOT NULL,
    generation bigint NOT NULL DEFAULT 1,
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'synced', 'revoked')),
    last_synced_at timestamptz,
    grants text[] NOT NULL DEFAULT '{}',
    lease_hash bytea,
    lease_expires_at timestamptz,
    PRIMARY KEY (subject_id, provider)
);

CREATE TABLE IF NOT EXISTS avatar_hsm.wearable_record (
    subject_id text NOT NULL,
    provider text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('steps', 'heart_rate', 'sleep', 'workout')),
    source text NOT NULL,
    source_id text NOT NULL,
    started_at timestamptz NOT NULL,
    ended_at timestamptz NOT NULL CHECK (ended_at >= started_at),
    modified_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (subject_id, provider, kind, source, source_id),
    FOREIGN KEY (subject_id, provider) REFERENCES avatar_hsm.device_connection
);
CREATE INDEX IF NOT EXISTS hsm_wearable_record_window ON avatar_hsm.wearable_record
    (subject_id, kind, started_at, provider, source, source_id);

CREATE TABLE IF NOT EXISTS avatar_hsm.wearable_daily (
    subject_id text NOT NULL,
    provider text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('steps', 'heart_rate')),
    source text NOT NULL,
    local_day text NOT NULL,
    timezone text NOT NULL,
    calculated_by text NOT NULL CHECK (calculated_by IN ('provider', 'hsm')),
    stats jsonb NOT NULL,
    PRIMARY KEY (subject_id, provider, kind, source, local_day, timezone, calculated_by),
    FOREIGN KEY (subject_id, provider) REFERENCES avatar_hsm.device_connection
);
