CREATE TABLE IF NOT EXISTS avatar_hsm.guardian_subscription (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    subscription jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS avatar_hsm.guardian_alert (
    event_id text PRIMARY KEY REFERENCES avatar_hsm.health_outbox,
    notification_state text NOT NULL DEFAULT 'queued'
        CHECK (notification_state IN ('queued','sending','submitted','failed','expired')),
    created_at timestamptz NOT NULL DEFAULT now(),
    confirmed_at timestamptz
);
