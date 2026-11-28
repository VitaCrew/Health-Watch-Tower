CREATE INDEX wearable_record_healthkit_source_id
ON avatar_hsm.wearable_record (subject_id, lower(source_id))
WHERE provider = 'healthkit';
