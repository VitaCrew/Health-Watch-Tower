ALTER TABLE avatar_hsm.wearable_fit ADD COLUMN detail_state text NOT NULL DEFAULT 'pending'
    CHECK (detail_state IN ('pending','ready','unavailable'));
ALTER TABLE avatar_hsm.wearable_fit ADD COLUMN detail jsonb;
CREATE INDEX wearable_fit_detail_ready ON avatar_hsm.wearable_fit(captured_at) WHERE detail_state='pending';
