-- OpenHuman owns the hourly Oura cadence. HSM only consumes explicitly
-- requested due work and retains webhook/backfill retries in its work table.
ALTER TABLE avatar_hsm.wearable_sync_schedule
    ADD COLUMN last_openhuman_slot timestamptz;

UPDATE avatar_hsm.wearable_sync_schedule
SET next_live_sync = now() + interval '50 years'
WHERE provider = 'oura';
