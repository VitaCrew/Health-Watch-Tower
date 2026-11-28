-- Keep Google TCX queue scheduling and leases with Watch Tower. TCX bytes and
-- personal route/telemetry stay in avatar_hsm.google_workout_tcx.
CREATE TABLE IF NOT EXISTS watch_tower.google_workout_tcx_queue (
    subject_id text NOT NULL,
    source text NOT NULL,
    source_id text NOT NULL,
    fingerprint bytea NOT NULL,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt timestamptz NOT NULL DEFAULT now(),
    job_id uuid,
    leased_until timestamptz,
    PRIMARY KEY (subject_id, source, source_id)
);

CREATE INDEX IF NOT EXISTS google_workout_tcx_queue_due
    ON watch_tower.google_workout_tcx_queue (next_attempt);

-- HSM migrations can run before this Watch Tower migration, or on a separate
-- HSM start before Watch Tower is available. Reconcile legacy queue metadata
-- every time Watch Tower connects, preserving retries and active leases.
DO $$
BEGIN
    IF to_regclass('avatar_hsm.google_workout_tcx') IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='avatar_hsm' AND table_name='google_workout_tcx'
             AND column_name='attempts'
       ) THEN
        INSERT INTO watch_tower.google_workout_tcx_queue
            (subject_id,source,source_id,fingerprint,attempts,next_attempt,job_id,leased_until)
        SELECT subject_id,source,source_id,fingerprint,attempts,next_attempt,job_id,leased_until
        FROM avatar_hsm.google_workout_tcx
        ON CONFLICT(subject_id,source,source_id) DO UPDATE
        SET fingerprint=EXCLUDED.fingerprint,attempts=EXCLUDED.attempts,
            next_attempt=EXCLUDED.next_attempt,job_id=EXCLUDED.job_id,
            leased_until=EXCLUDED.leased_until
        WHERE watch_tower.google_workout_tcx_queue.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint;

        ALTER TABLE avatar_hsm.google_workout_tcx
            DROP COLUMN IF EXISTS attempts,
            DROP COLUMN IF EXISTS next_attempt,
            DROP COLUMN IF EXISTS job_id,
            DROP COLUMN IF EXISTS leased_until,
            DROP COLUMN IF EXISTS fingerprint;
    END IF;
END $$;
