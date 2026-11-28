-- Move durable ETL work queues and webhook deduplication into Watch Tower.
-- ALTER TABLE preserves queue rows, indexes, and foreign-key identity in place.
CREATE SCHEMA IF NOT EXISTS watch_tower;

DO $$
DECLARE
    queue_name text;
    legacy_table regclass;
    owner_table regclass;
BEGIN
    FOREACH queue_name IN ARRAY ARRAY[
        'wearable_fit_job',
        'wearable_webhook_document',
        'wearable_webhook_receipt',
        'garmin_backfill',
        'garmin_pull_job',
        'withings_notify_cleanup'
    ] LOOP
        legacy_table := to_regclass('avatar_hsm.' || quote_ident(queue_name));
        owner_table := to_regclass('watch_tower.' || quote_ident(queue_name));
        IF legacy_table IS NOT NULL AND owner_table IS NOT NULL THEN
            RAISE EXCEPTION 'both HSM and Watch Tower ETL queue tables exist: %', queue_name;
        END IF;
        IF legacy_table IS NOT NULL THEN
            EXECUTE format('ALTER TABLE avatar_hsm.%I SET SCHEMA watch_tower', queue_name);
        END IF;
    END LOOP;
END $$;
