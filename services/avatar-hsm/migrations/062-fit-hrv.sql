ALTER TABLE avatar_hsm.wearable_fit
ADD COLUMN hrv_parsed boolean NOT NULL DEFAULT false;

DROP INDEX avatar_hsm.wearable_fit_detail_queue;
CREATE INDEX wearable_fit_detail_queue ON avatar_hsm.wearable_fit(captured_at)
WHERE NOT developer_fields_parsed OR NOT devices_parsed OR NOT hrv_parsed;
