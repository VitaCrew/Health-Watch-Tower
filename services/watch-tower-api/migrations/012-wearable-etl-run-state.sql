-- Payload-free lifecycle visibility for Watch Tower provider collection runs.
CREATE TABLE IF NOT EXISTS watch_tower.wearable_etl_run (
    subject_id text NOT NULL,
    run_id uuid NOT NULL,
    provider text NOT NULL,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    failure_code text CHECK (failure_code IS NULL OR failure_code IN (
        'provider_rate_limited', 'collection_or_state_owner_failed'
    )),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_id, run_id),
    CHECK ((status = 'running' AND finished_at IS NULL AND failure_code IS NULL)
        OR (status = 'succeeded' AND finished_at IS NOT NULL AND failure_code IS NULL)
        OR (status = 'failed' AND finished_at IS NOT NULL AND failure_code IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS wearable_etl_run_subject_recent_idx
    ON watch_tower.wearable_etl_run (subject_id, started_at DESC);
