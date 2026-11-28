CREATE TABLE IF NOT EXISTS avatar_hsm.snapshot_write_receipt (
    subject_id text NOT NULL,
    provider text NOT NULL,
    lease_hash bytea NOT NULL,
    request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    result jsonb NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_id, provider, lease_hash),
    FOREIGN KEY (subject_id, provider)
        REFERENCES avatar_hsm.device_connection(subject_id, provider)
        ON DELETE CASCADE
);
