ALTER TABLE avatar_hsm.wearable_sync_schedule
ADD COLUMN next_live_sync timestamptz NOT NULL DEFAULT (now() + interval '1 hour');
