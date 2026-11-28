ALTER TABLE watch_tower.etl_run
    DROP CONSTRAINT IF EXISTS etl_run_status_check,
    ADD CONSTRAINT etl_run_status_check CHECK (status IN ('succeeded', 'failed')),
    DROP CONSTRAINT IF EXISTS etl_run_receipt_count_check,
    ADD CONSTRAINT etl_run_receipt_count_check CHECK (receipt_count >= 0),
    ADD COLUMN IF NOT EXISTS failure_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_failure_code text;

ALTER TABLE watch_tower.etl_run
    ALTER COLUMN receipt_count SET DEFAULT 0;

ALTER TABLE watch_tower.etl_run
    DROP CONSTRAINT IF EXISTS etl_run_failure_count_check,
    DROP CONSTRAINT IF EXISTS etl_run_failure_code_check,
    ADD CONSTRAINT etl_run_failure_count_check CHECK (failure_count >= 0),
    ADD CONSTRAINT etl_run_failure_code_check
        CHECK (last_failure_code IS NULL OR last_failure_code = 'record_dispatch_failed');
