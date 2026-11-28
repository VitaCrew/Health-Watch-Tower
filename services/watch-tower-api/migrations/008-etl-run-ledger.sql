-- Payload-free execution lineage for device ingestion and replay.
CREATE TABLE IF NOT EXISTS watch_tower.etl_run (
    subject_id text NOT NULL,
    run_id uuid NOT NULL,
    source_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('succeeded')),
    receipt_count bigint NOT NULL DEFAULT 1 CHECK (receipt_count > 0),
    latest_request_sha256 text NOT NULL CHECK (latest_request_sha256 ~ '^[0-9a-f]{64}$'),
    first_receipt_at timestamptz NOT NULL DEFAULT now(),
    last_receipt_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_id, run_id)
);

CREATE INDEX IF NOT EXISTS etl_run_subject_recent_idx
    ON watch_tower.etl_run (subject_id, last_receipt_at DESC);
