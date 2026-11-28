CREATE UNIQUE INDEX wearable_garmin_account_owner ON avatar_hsm.wearable_oauth_connection(provider_account_hash)
WHERE provider='garmin' AND provider_account_hash IS NOT NULL;

ALTER TABLE avatar_hsm.wearable_fit_job ADD COLUMN sealed_source bytea;
