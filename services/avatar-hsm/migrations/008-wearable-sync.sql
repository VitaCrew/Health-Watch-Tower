ALTER TABLE avatar_hsm.subject DROP CONSTRAINT IF EXISTS subject_id_check;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
        WHERE conrelid='avatar_hsm.wearable_record'::regclass AND conname='wearable_record_kind_v2_check') THEN
        ALTER TABLE avatar_hsm.wearable_record DROP CONSTRAINT IF EXISTS wearable_record_kind_check;
        ALTER TABLE avatar_hsm.wearable_record ADD CONSTRAINT wearable_record_kind_v2_check
            CHECK (kind IN ('steps','heart_rate','sleep','workout','metric'));
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS avatar_hsm.wearable_profile (
    subject_id text PRIMARY KEY REFERENCES avatar_hsm.subject,
    read_hash bytea NOT NULL UNIQUE,
    control_hash bytea NOT NULL UNIQUE,
    CHECK (read_hash <> control_hash)
);

CREATE TABLE IF NOT EXISTS avatar_hsm.wearable_oauth_state (
    state_hash bytea PRIMARY KEY,
    subject_id text NOT NULL REFERENCES avatar_hsm.wearable_profile,
    provider text NOT NULL,
    verifier text NOT NULL,
    claimed_at timestamptz,
    expires_at timestamptz NOT NULL,
    UNIQUE (subject_id, provider)
);

CREATE TABLE IF NOT EXISTS avatar_hsm.wearable_oauth_connection (
    subject_id text NOT NULL REFERENCES avatar_hsm.wearable_profile,
    provider text NOT NULL,
    generation uuid NOT NULL DEFAULT gen_random_uuid(),
    sealed_tokens bytea NOT NULL,
    PRIMARY KEY (subject_id, provider),
    FOREIGN KEY (subject_id, provider) REFERENCES avatar_hsm.device_connection ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS avatar_hsm.wearable_sync_schedule (
    subject_id text NOT NULL,
    provider text NOT NULL,
    next_sync timestamptz NOT NULL DEFAULT now(),
    backfill_to timestamptz NOT NULL DEFAULT now(),
    backfill_from timestamptz NOT NULL,
    job_id uuid,
    job_until timestamptz,
    failures integer NOT NULL DEFAULT 0,
    last_error text,
    PRIMARY KEY (subject_id, provider),
    FOREIGN KEY (subject_id, provider) REFERENCES avatar_hsm.device_connection ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS wearable_sync_due ON avatar_hsm.wearable_sync_schedule (next_sync);
