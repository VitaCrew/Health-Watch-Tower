CREATE TABLE avatar_hsm.samsung_sleep_revision (
    subject_id text NOT NULL,
    provider text NOT NULL DEFAULT 'samsung_health' CHECK (provider = 'samsung_health'),
    uid text NOT NULL,
    changed_at timestamptz NOT NULL,
    deleted boolean NOT NULL,
    PRIMARY KEY (subject_id, uid),
    FOREIGN KEY (subject_id, provider) REFERENCES avatar_hsm.device_connection ON DELETE CASCADE
);
