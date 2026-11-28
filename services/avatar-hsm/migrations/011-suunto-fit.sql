INSERT INTO avatar_hsm.wearable_fit_job(subject_id,provider,activity_id,exercise_type)
SELECT r.subject_id,r.provider,r.source_id,r.payload->>'exercise_type'
FROM avatar_hsm.wearable_record r JOIN avatar_hsm.device_connection d USING(subject_id,provider)
WHERE r.kind='workout' AND r.provider='suunto'
  AND d.state='synced' AND r.source_id ~ '^[0-9A-Fa-f]{24}$'
ON CONFLICT DO NOTHING;
