CREATE TABLE avatar_hsm.apple_export_workout_route (
    subject_id text NOT NULL,
    provider text NOT NULL DEFAULT 'apple_health_export' CHECK (provider = 'apple_health_export'),
    kind text NOT NULL DEFAULT 'workout' CHECK (kind = 'workout'),
    source text NOT NULL,
    source_id text NOT NULL,
    points jsonb NOT NULL,
    PRIMARY KEY (subject_id,source,source_id),
    FOREIGN KEY (subject_id,provider,kind,source,source_id)
        REFERENCES avatar_hsm.wearable_record(subject_id,provider,kind,source,source_id) ON DELETE CASCADE
);
