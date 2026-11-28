ALTER TABLE avatar_hsm.oura_webhook_document RENAME TO wearable_webhook_document;
ALTER TABLE avatar_hsm.wearable_webhook_document
    DROP CONSTRAINT oura_webhook_document_provider_check,
    DROP CONSTRAINT oura_webhook_document_pkey,
    ALTER COLUMN provider DROP DEFAULT,
    ADD CHECK (provider IN ('oura','whoop')),
    ADD PRIMARY KEY (subject_id,provider,data_type,object_id);

ALTER TABLE avatar_hsm.oura_deleted_document RENAME TO wearable_deleted_document;
ALTER TABLE avatar_hsm.wearable_deleted_document
    DROP CONSTRAINT oura_deleted_document_provider_check,
    DROP CONSTRAINT oura_deleted_document_pkey,
    ALTER COLUMN provider DROP DEFAULT,
    ADD CHECK (provider IN ('oura','whoop')),
    ADD PRIMARY KEY (subject_id,provider,kind,source_id);
