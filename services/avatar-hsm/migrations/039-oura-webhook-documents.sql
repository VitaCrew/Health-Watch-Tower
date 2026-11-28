CREATE TABLE avatar_hsm.oura_webhook_document (
    subject_id text NOT NULL,
    provider text NOT NULL DEFAULT 'oura' CHECK (provider = 'oura'),
    data_type text NOT NULL,
    object_id text NOT NULL,
    generation uuid NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt timestamptz NOT NULL DEFAULT now(),
    job_id uuid,
    leased_until timestamptz,
    PRIMARY KEY (subject_id, data_type, object_id),
    FOREIGN KEY (subject_id, provider)
        REFERENCES avatar_hsm.wearable_oauth_connection ON DELETE CASCADE
);

CREATE INDEX oura_webhook_document_due ON avatar_hsm.oura_webhook_document (next_attempt)
    WHERE attempts < 6;
