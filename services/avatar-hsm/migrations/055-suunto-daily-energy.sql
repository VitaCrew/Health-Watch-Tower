CREATE VIEW avatar_hsm.wearable_suunto_daily_energy AS
SELECT DISTINCT ON (subject_id, source, local_day)
       subject_id, provider, 'activity'::text AS kind, source, local_day,
       COALESCE(detail->>'timezone','') AS timezone, 'provider'::text AS calculated_by,
       jsonb_build_object('active_energy_kcal', detail->'value') AS stats,
       modified_at
FROM avatar_hsm.wearable_record
CROSS JOIN LATERAL (SELECT payload->'provider_data' AS detail) data
CROSS JOIN LATERAL (SELECT detail->>'date' AS local_day) recorded
WHERE provider='suunto' AND kind='metric' AND payload->>'metric'='daily_energyconsumption'
  AND local_day ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  AND detail->>'unit'='kcal'
  AND CASE WHEN jsonb_typeof(detail->'value')='number' THEN (detail->'value')::numeric >= 0 ELSE false END
ORDER BY subject_id, source, local_day, modified_at DESC, started_at DESC, source_id DESC;
