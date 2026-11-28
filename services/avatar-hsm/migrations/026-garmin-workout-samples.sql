CREATE TABLE avatar_hsm.garmin_workout_samples (
    subject_id text NOT NULL,
    provider text NOT NULL DEFAULT 'garmin' CHECK (provider = 'garmin'),
    kind text NOT NULL DEFAULT 'metric' CHECK (kind = 'metric'),
    source text NOT NULL,
    source_id text NOT NULL,
    modified_at timestamptz NOT NULL,
    points jsonb NOT NULL,
    PRIMARY KEY (subject_id, source, source_id),
    FOREIGN KEY (subject_id, provider, kind, source, source_id)
        REFERENCES avatar_hsm.wearable_record(subject_id, provider, kind, source, source_id)
        ON DELETE CASCADE
);

CREATE INDEX wearable_record_garmin_activity_details
ON avatar_hsm.wearable_record (subject_id, source, source_id)
WHERE provider = 'garmin' AND kind = 'metric' AND payload->>'metric' = 'activityDetails';
