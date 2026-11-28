-- Provider sync scheduling and late-data replay queues belong to Watch Tower.
-- Move the existing rows in place so installations keep their retry history.
CREATE SCHEMA IF NOT EXISTS watch_tower;

DO $$
BEGIN
    IF to_regclass('avatar_hsm.wearable_sync_schedule') IS NOT NULL
       AND to_regclass('watch_tower.wearable_sync_schedule') IS NOT NULL THEN
        RAISE EXCEPTION 'both HSM and Watch Tower wearable sync schedules exist';
    END IF;
    IF to_regclass('avatar_hsm.wearable_target_day') IS NOT NULL
       AND to_regclass('watch_tower.wearable_target_day') IS NOT NULL THEN
        RAISE EXCEPTION 'both HSM and Watch Tower wearable target queues exist';
    END IF;

    IF to_regclass('avatar_hsm.wearable_sync_schedule') IS NOT NULL THEN
        ALTER TABLE avatar_hsm.wearable_sync_schedule SET SCHEMA watch_tower;
    END IF;
    IF to_regclass('avatar_hsm.wearable_target_day') IS NOT NULL THEN
        ALTER TABLE avatar_hsm.wearable_target_day SET SCHEMA watch_tower;
    END IF;
END $$;
