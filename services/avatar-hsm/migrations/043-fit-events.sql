ALTER TABLE avatar_hsm.wearable_fit
ADD COLUMN events_parsed boolean NOT NULL DEFAULT false;

DROP INDEX avatar_hsm.wearable_fit_detail_ready;
CREATE INDEX wearable_fit_detail_queue ON avatar_hsm.wearable_fit(captured_at)
WHERE NOT events_parsed;
