CREATE TABLE IF NOT EXISTS avatar_hsm.watch_tower_record_outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subject_id text NOT NULL,
    provider text NOT NULL,
    request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    observed_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_owner text,
    lease_until timestamptz,
    delivered_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (subject_id, provider, request_sha256)
);

CREATE INDEX IF NOT EXISTS watch_tower_record_outbox_pending_idx
    ON avatar_hsm.watch_tower_record_outbox (id)
    WHERE delivered_at IS NULL;
