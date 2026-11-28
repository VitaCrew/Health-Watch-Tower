ALTER TABLE avatar_hsm.wearable_fit
    ADD COLUMN IF NOT EXISTS strength_parsed boolean NOT NULL DEFAULT false;
