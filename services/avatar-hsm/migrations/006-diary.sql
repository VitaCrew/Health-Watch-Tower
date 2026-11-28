CREATE TABLE IF NOT EXISTS avatar_hsm.diary_entry (
    subject_id text NOT NULL REFERENCES avatar_hsm.subject,
    id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('meal', 'water', 'strength')),
    details jsonb NOT NULL CHECK (jsonb_typeof(details) = 'object'),
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_id, id)
);
CREATE INDEX IF NOT EXISTS hsm_diary_time ON avatar_hsm.diary_entry
    (subject_id, recorded_at DESC, id);

ALTER TABLE avatar_hsm.diary_entry ADD COLUMN IF NOT EXISTS revision bigint NOT NULL DEFAULT 1;
ALTER TABLE avatar_hsm.diary_entry ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
