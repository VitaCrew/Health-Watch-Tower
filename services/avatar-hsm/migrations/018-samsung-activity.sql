ALTER TABLE avatar_hsm.wearable_daily
    DROP CONSTRAINT wearable_daily_kind_check,
    ADD CONSTRAINT wearable_daily_kind_check CHECK (kind IN ('steps', 'heart_rate', 'activity'));

ALTER TABLE avatar_hsm.device_connection
    ADD COLUMN samsung_activity_granted boolean NOT NULL DEFAULT false;
