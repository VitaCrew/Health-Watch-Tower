ALTER TABLE avatar_hsm.wearable_oauth_connection ADD COLUMN garmin_permissions smallint NOT NULL DEFAULT 0
    CHECK (garmin_permissions BETWEEN 0 AND 3);
ALTER TABLE avatar_hsm.wearable_oauth_connection ADD COLUMN garmin_permissions_observed_at timestamptz;

UPDATE avatar_hsm.device_connection SET grants='{}' WHERE provider='garmin';
UPDATE avatar_hsm.wearable_fit_job SET sealed_source=NULL, status='unavailable', job_id=NULL, leased_until=NULL
WHERE provider='garmin' AND status='pending';
