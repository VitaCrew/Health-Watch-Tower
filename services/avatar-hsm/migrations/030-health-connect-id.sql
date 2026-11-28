CREATE INDEX wearable_record_health_connect_source_id
ON avatar_hsm.wearable_record (subject_id, source_id)
WHERE provider = 'health_connect';
