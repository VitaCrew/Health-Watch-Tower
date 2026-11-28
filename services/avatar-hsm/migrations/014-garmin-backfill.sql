CREATE TABLE avatar_hsm.garmin_backfill (
    subject_id text NOT NULL,
    provider text NOT NULL DEFAULT 'garmin' CHECK (provider='garmin'),
    start_at timestamptz NOT NULL,
    end_at timestamptz NOT NULL CHECK (start_at < end_at),
    feed text NOT NULL,
    permission smallint NOT NULL CHECK (permission IN (1,2)),
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','accepted','duplicate','failed','permission_removed')),
    provider_status integer,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz,
    job_id uuid,
    requested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_id,start_at,end_at,feed),
    FOREIGN KEY (subject_id,provider) REFERENCES avatar_hsm.wearable_oauth_connection ON DELETE CASCADE
);
CREATE INDEX garmin_backfill_ready ON avatar_hsm.garmin_backfill(next_attempt) WHERE status='queued';
