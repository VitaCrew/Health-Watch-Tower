ALTER TABLE avatar_hsm.oura_webhook_document
    ADD COLUMN event_at timestamptz,
    ADD COLUMN completed_at timestamptz;

DROP INDEX avatar_hsm.oura_webhook_document_due;
CREATE INDEX oura_webhook_document_due ON avatar_hsm.oura_webhook_document (next_attempt)
    WHERE completed_at IS NULL AND attempts < 6;
