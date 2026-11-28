ALTER TABLE avatar_hsm.wearable_daily ADD COLUMN modified_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE avatar_hsm.wearable_oauth_connection ADD COLUMN garmin_daily_projected boolean NOT NULL DEFAULT false;
