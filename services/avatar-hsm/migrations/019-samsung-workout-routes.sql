CREATE TABLE avatar_hsm.samsung_workout_route (
    subject_id text NOT NULL,
    provider text NOT NULL DEFAULT 'samsung_health' CHECK (provider = 'samsung_health'),
    kind text NOT NULL DEFAULT 'workout' CHECK (kind = 'workout'),
    source text NOT NULL,
    source_id text NOT NULL,
    modified_at timestamptz NOT NULL,
    source_point_count integer NOT NULL,
    points jsonb NOT NULL CHECK (jsonb_array_length(points) BETWEEN 1 AND 2000
        AND source_point_count >= jsonb_array_length(points)),
    PRIMARY KEY (subject_id, provider, kind, source, source_id),
    FOREIGN KEY (subject_id, provider, kind, source, source_id)
        REFERENCES avatar_hsm.wearable_record ON DELETE CASCADE
);
