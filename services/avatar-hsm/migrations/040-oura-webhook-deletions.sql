ALTER TABLE avatar_hsm.oura_webhook_document
    ADD COLUMN delete_requested boolean NOT NULL DEFAULT false;

CREATE TABLE avatar_hsm.oura_deleted_document (
    subject_id text NOT NULL,
    provider text NOT NULL DEFAULT 'oura' CHECK (provider = 'oura'),
    kind text NOT NULL,
    source_id text NOT NULL,
    PRIMARY KEY (subject_id, kind, source_id),
    FOREIGN KEY (subject_id, provider)
        REFERENCES avatar_hsm.wearable_oauth_connection ON DELETE CASCADE
);
