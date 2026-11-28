-- Watch Tower claims and retries remote Withings subscription cleanup.
DO $$
BEGIN
    IF to_regclass('watch_tower.withings_notify_cleanup') IS NOT NULL THEN
        ALTER TABLE watch_tower.withings_notify_cleanup
            ADD COLUMN IF NOT EXISTS job_id uuid,
            ADD COLUMN IF NOT EXISTS leased_until timestamptz,
            ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0);
        CREATE INDEX IF NOT EXISTS withings_notify_cleanup_claimable
            ON watch_tower.withings_notify_cleanup (next_attempt, expires_at);
    END IF;
END $$;
