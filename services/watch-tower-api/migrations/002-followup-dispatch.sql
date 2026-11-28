ALTER TABLE watch_tower.identity_link
    ADD COLUMN IF NOT EXISTS openhuman_user_id text;

CREATE TABLE IF NOT EXISTS watch_tower.followup_request (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    subject_id text NOT NULL,
    adapter_id text NOT NULL,
    record_id text NOT NULL,
    reason_code text NOT NULL,
    role text NOT NULL CHECK (role IN ('caregiver', 'clinician', 'trainer', 'nutritionist')),
    urgency text NOT NULL CHECK (urgency IN ('routine', 'soon', 'urgent')),
    assignees jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(assignees) = 'array'),
    dispatch_state text NOT NULL DEFAULT 'pending'
        CHECK (dispatch_state IN ('pending', 'dispatching', 'dispatched', 'cancelled')),
    lease_owner text,
    lease_until timestamptz,
    assigned_actor text,
    workflow_id text,
    workflow_run_id text,
    task_key text,
    created_at timestamptz NOT NULL DEFAULT now(),
    dispatched_at timestamptz,
    UNIQUE (subject_id, adapter_id, record_id, reason_code, role, urgency)
);
CREATE INDEX IF NOT EXISTS followup_pending_idx
    ON watch_tower.followup_request (created_at, id)
    WHERE dispatch_state IN ('pending', 'dispatching');
