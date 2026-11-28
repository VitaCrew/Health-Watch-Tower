ALTER TABLE avatar_hsm.wearable_oauth_connection ADD COLUMN IF NOT EXISTS provider_account_hash bytea;
CREATE INDEX IF NOT EXISTS wearable_oauth_provider_account ON avatar_hsm.wearable_oauth_connection(provider, provider_account_hash);
ALTER TABLE avatar_hsm.wearable_sync_schedule ADD COLUMN IF NOT EXISTS wake_requested boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS avatar_hsm.wearable_webhook_receipt (
    provider text NOT NULL,
    event_hash bytea NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, event_hash)
);
CREATE INDEX IF NOT EXISTS wearable_webhook_time ON avatar_hsm.wearable_webhook_receipt(received_at);

CREATE TABLE IF NOT EXISTS avatar_hsm.wearable_fit_job (
    subject_id text NOT NULL,
    provider text NOT NULL,
    activity_id text NOT NULL,
    exercise_type text,
    next_attempt timestamptz NOT NULL DEFAULT now(),
    job_id uuid,
    leased_until timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','stored','unavailable')),
    PRIMARY KEY (subject_id, provider, activity_id),
    FOREIGN KEY (subject_id, provider) REFERENCES avatar_hsm.device_connection ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS wearable_fit_due ON avatar_hsm.wearable_fit_job(next_attempt) WHERE status='pending';

INSERT INTO avatar_hsm.wearable_fit_job(subject_id,provider,activity_id,exercise_type)
SELECT r.subject_id,r.provider,r.source_id,r.payload->>'exercise_type'
FROM avatar_hsm.wearable_record r JOIN avatar_hsm.device_connection d USING(subject_id,provider)
WHERE r.kind='workout' AND r.provider IN ('polar','coros','garmin_personal')
  AND d.state='synced' AND r.source_id ~ '^[A-Za-z0-9_-]{1,128}$'
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS avatar_hsm.wearable_fit (
    subject_id text NOT NULL,
    provider text NOT NULL,
    activity_id text NOT NULL,
    data bytea NOT NULL CHECK (octet_length(data) BETWEEN 14 AND 12582912),
    sha256 bytea NOT NULL CHECK (octet_length(sha256)=32),
    captured_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_id, provider, activity_id),
    FOREIGN KEY (subject_id, provider, activity_id) REFERENCES avatar_hsm.wearable_fit_job ON DELETE CASCADE
);
