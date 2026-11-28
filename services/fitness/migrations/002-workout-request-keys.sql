ALTER TABLE fitness.workout_sessions
    ADD COLUMN IF NOT EXISTS request_key text;

UPDATE fitness.workout_sessions
SET request_key = 'workflow:' || workflow_run_id
WHERE request_key IS NULL AND workflow_run_id IS NOT NULL;

ALTER TABLE fitness.workout_sessions
    ALTER COLUMN request_key SET NOT NULL,
    ALTER COLUMN workflow_run_id DROP NOT NULL;

ALTER TABLE fitness.workout_sessions
    DROP CONSTRAINT IF EXISTS workout_sessions_owner_id_workflow_run_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS workout_sessions_owner_request
    ON fitness.workout_sessions (owner_id, request_key);

CREATE UNIQUE INDEX IF NOT EXISTS workout_sessions_owner_workflow
    ON fitness.workout_sessions (owner_id, workflow_run_id)
    WHERE workflow_run_id IS NOT NULL;

INSERT INTO fitness.schema_migration(version) VALUES (2) ON CONFLICT DO NOTHING;
