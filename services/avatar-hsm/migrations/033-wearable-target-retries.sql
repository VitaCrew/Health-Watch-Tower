ALTER TABLE avatar_hsm.wearable_target_day
    ADD COLUMN next_attempt timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN failures integer NOT NULL DEFAULT 0;
