ALTER TABLE fitness.workout_sessions
    DROP CONSTRAINT IF EXISTS workout_sessions_source_check;

ALTER TABLE fitness.workout_sessions
    ADD CONSTRAINT workout_sessions_source_check
    CHECK (source IN ('manual', 'workflow', 'strong', 'hevy'));

INSERT INTO fitness.schema_migration(version) VALUES (3) ON CONFLICT DO NOTHING;
