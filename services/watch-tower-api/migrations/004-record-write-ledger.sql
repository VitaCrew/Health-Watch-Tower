CREATE TABLE IF NOT EXISTS watch_tower.record_receipt (
    subject_id text NOT NULL,
    adapter_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    source_id text NOT NULL,
    record_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    actor_id text NOT NULL,
    observed_at timestamptz NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_id, adapter_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS record_receipt_subject_seen_idx
    ON watch_tower.record_receipt (subject_id, last_seen_at DESC);
