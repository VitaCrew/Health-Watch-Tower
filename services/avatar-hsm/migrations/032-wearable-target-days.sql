CREATE TABLE avatar_hsm.wearable_target_day (
    subject_id text NOT NULL,
    provider text NOT NULL,
    day date NOT NULL,
    revision uuid NOT NULL DEFAULT gen_random_uuid(),
    PRIMARY KEY (subject_id, provider, day),
    FOREIGN KEY (subject_id, provider) REFERENCES avatar_hsm.wearable_sync_schedule ON DELETE CASCADE
);
