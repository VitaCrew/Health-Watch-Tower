CREATE TABLE IF NOT EXISTS avatar_hsm.state_event (
    subject_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    event_id text NOT NULL,
    event_type text NOT NULL,
    event_version integer NOT NULL DEFAULT 1 CHECK (event_version > 0),
    actor_id text NOT NULL,
    occurred_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    source_ref text,
    correlation_id text,
    causation_id text,
    consent_version text,
    rule_version text,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (subject_id, sequence),
    UNIQUE (subject_id, event_id)
);
CREATE TABLE IF NOT EXISTS avatar_hsm.state_current (
    subject_id text PRIMARY KEY,
    sequence bigint NOT NULL DEFAULT 0 CHECK (sequence >= 0),
    state jsonb NOT NULL DEFAULT '{}'::jsonb,
    state_sha256 text NOT NULL DEFAULT '',
    reducer_version integer NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS avatar_hsm.state_snapshot (
    subject_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    state jsonb NOT NULL,
    state_sha256 text NOT NULL,
    reducer_version integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_id, sequence)
);
CREATE TABLE IF NOT EXISTS avatar_hsm.state_outbox (
    subject_id text NOT NULL,
    sequence bigint NOT NULL,
    event_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_owner text,
    lease_until timestamptz,
    delivered_at timestamptz,
    PRIMARY KEY (subject_id, sequence),
    UNIQUE (subject_id, event_id),
    FOREIGN KEY (subject_id, sequence)
        REFERENCES avatar_hsm.state_event(subject_id, sequence) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS state_outbox_pending_idx
    ON avatar_hsm.state_outbox (created_at) WHERE delivered_at IS NULL;
