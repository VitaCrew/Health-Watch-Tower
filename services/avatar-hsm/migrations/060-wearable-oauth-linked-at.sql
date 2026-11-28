ALTER TABLE avatar_hsm.wearable_oauth_connection
    ADD COLUMN linked_at timestamptz;

ALTER TABLE avatar_hsm.wearable_oauth_connection
    ALTER COLUMN linked_at SET DEFAULT clock_timestamp();
