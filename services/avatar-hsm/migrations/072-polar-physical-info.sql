ALTER TABLE avatar_hsm.wearable_webhook_document
    DROP CONSTRAINT wearable_webhook_document_provider_check,
    ADD CHECK (provider IN ('oura','whoop','strava','intervals_icu','polar'));
