ALTER TABLE avatar_hsm.wearable_oauth_connection
    ADD COLUMN google_location_granted boolean NOT NULL DEFAULT false;

CREATE TABLE avatar_hsm.google_workout_tcx (
    subject_id text NOT NULL,
    provider text NOT NULL DEFAULT 'google_health' CHECK (provider = 'google_health'),
    kind text NOT NULL DEFAULT 'workout' CHECK (kind = 'workout'),
    source text NOT NULL,
    source_id text NOT NULL,
    fingerprint bytea NOT NULL,
    data bytea,
    byte_count integer GENERATED ALWAYS AS (octet_length(data)) STORED,
    sha256 bytea GENERATED ALWAYS AS (sha256(data)) STORED,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt timestamptz NOT NULL DEFAULT now(),
    job_id uuid,
    leased_until timestamptz,
    PRIMARY KEY (subject_id, source, source_id),
    FOREIGN KEY (subject_id, provider, kind, source, source_id)
        REFERENCES avatar_hsm.wearable_record ON DELETE CASCADE
);

CREATE INDEX google_workout_tcx_due ON avatar_hsm.google_workout_tcx (next_attempt)
    WHERE data IS NULL AND attempts < 6;
