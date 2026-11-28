CREATE TABLE avatar_hsm.garmin_pull_job (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_id text NOT NULL,
    provider text NOT NULL DEFAULT 'garmin' CHECK (provider='garmin'),
    feed text NOT NULL,
    permission smallint NOT NULL CHECK (permission IN (1,2)),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    sealed_source bytea,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','stored','unavailable')),
    attempts smallint NOT NULL DEFAULT 0,
    received_at timestamptz NOT NULL DEFAULT now(),
    next_attempt timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz,
    lease uuid,
    UNIQUE (subject_id,feed,request_hash),
    FOREIGN KEY (subject_id,provider) REFERENCES avatar_hsm.wearable_oauth_connection ON DELETE CASCADE
);
CREATE INDEX garmin_pull_ready ON avatar_hsm.garmin_pull_job(next_attempt) WHERE status='pending';
