ALTER TABLE avatar_hsm.watch_tower_record_outbox
    ADD COLUMN IF NOT EXISTS run_id uuid;
