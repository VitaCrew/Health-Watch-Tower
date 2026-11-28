ALTER TABLE avatar_hsm.wearable_fit
    ADD COLUMN IF NOT EXISTS splits_parsed boolean NOT NULL DEFAULT false;
