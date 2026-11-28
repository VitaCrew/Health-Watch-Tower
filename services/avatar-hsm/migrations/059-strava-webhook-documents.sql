ALTER TABLE avatar_hsm.wearable_webhook_document
    DROP CONSTRAINT wearable_webhook_document_provider_check,
    ADD CHECK (provider IN ('oura','whoop','strava'));

ALTER TABLE avatar_hsm.wearable_deleted_document
    DROP CONSTRAINT wearable_deleted_document_provider_check,
    ADD CHECK (provider IN ('oura','whoop','strava'));
