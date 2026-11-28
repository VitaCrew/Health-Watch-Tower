ALTER TABLE avatar_hsm.wearable_oauth_connection
    ADD COLUMN withings_notify_config bytea,
    ADD COLUMN withings_notify_state text NOT NULL DEFAULT 'pending'
        CHECK (withings_notify_state IN ('pending', 'active', 'retrying', 'no_scope')),
    ADD COLUMN withings_notify_next timestamptz NOT NULL DEFAULT now();

CREATE INDEX wearable_withings_notify_due
    ON avatar_hsm.wearable_oauth_connection(withings_notify_next)
    WHERE provider='withings' AND provider_account_hash IS NOT NULL;
