CREATE TABLE avatar_hsm.samsung_goals (
    subject_id text PRIMARY KEY,
    provider text NOT NULL DEFAULT 'samsung_health' CHECK (provider = 'samsung_health'),
    observed_at timestamptz NOT NULL,
    timezone text NOT NULL,
    targets jsonb NOT NULL CHECK (jsonb_typeof(targets) = 'array'),
    FOREIGN KEY (subject_id, provider) REFERENCES avatar_hsm.device_connection ON DELETE CASCADE
);
