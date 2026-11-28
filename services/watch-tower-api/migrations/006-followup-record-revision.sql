ALTER TABLE watch_tower.followup_request
    ADD COLUMN IF NOT EXISTS record_revision bigint NOT NULL DEFAULT 1
    CHECK (record_revision > 0);

DO $$
DECLARE
    old_constraint text;
BEGIN
    FOR old_constraint IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'watch_tower.followup_request'::regclass
          AND contype = 'u'
          AND pg_get_constraintdef(oid) =
              'UNIQUE (subject_id, adapter_id, record_id, reason_code, role, urgency)'
    LOOP
        EXECUTE format(
            'ALTER TABLE watch_tower.followup_request DROP CONSTRAINT %I',
            old_constraint
        );
    END LOOP;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS followup_record_revision_unique
    ON watch_tower.followup_request
       (subject_id, adapter_id, record_id, record_revision, reason_code, role, urgency);
