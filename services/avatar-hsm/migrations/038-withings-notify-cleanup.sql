CREATE TABLE avatar_hsm.withings_notify_cleanup (
    subject_id text NOT NULL REFERENCES avatar_hsm.subject,
    account_hash bytea NOT NULL,
    generation uuid NOT NULL,
    sealed_cleanup bytea NOT NULL,
    expires_at timestamptz NOT NULL,
    next_attempt timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_id, account_hash)
);
CREATE INDEX withings_notify_cleanup_due ON avatar_hsm.withings_notify_cleanup (next_attempt);
