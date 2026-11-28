ALTER TABLE watch_tower.followup_request
    DROP CONSTRAINT IF EXISTS followup_request_dispatch_state_check;

ALTER TABLE watch_tower.followup_request
    ADD CONSTRAINT followup_request_dispatch_state_check
    CHECK (dispatch_state IN ('pending', 'dispatching', 'dispatched', 'finished', 'cancelled'));

ALTER TABLE watch_tower.followup_request
    ADD COLUMN IF NOT EXISTS workflow_finished_at timestamptz;
