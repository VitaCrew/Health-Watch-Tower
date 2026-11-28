ALTER TABLE avatar_hsm.samsung_sleep_revision RENAME TO samsung_session_revision;
ALTER TABLE avatar_hsm.samsung_session_revision
    ADD COLUMN kind text NOT NULL DEFAULT 'sleep' CHECK (kind IN ('sleep', 'workout')),
    DROP CONSTRAINT samsung_sleep_revision_pkey;
ALTER TABLE avatar_hsm.samsung_session_revision
    ALTER COLUMN kind DROP DEFAULT,
    ADD PRIMARY KEY (subject_id, kind, uid);
