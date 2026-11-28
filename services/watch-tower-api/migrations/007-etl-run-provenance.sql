ALTER TABLE watch_tower.record_receipt
    ADD COLUMN IF NOT EXISTS last_run_id uuid;
