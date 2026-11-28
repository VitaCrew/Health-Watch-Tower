CREATE VIEW avatar_hsm.wearable_daily_activity_projection AS
SELECT subject_id, provider, kind, source, local_day, timezone, calculated_by, stats, modified_at
FROM avatar_hsm.wearable_daily_energy
UNION ALL
SELECT subject_id, provider, kind, source, local_day, timezone, calculated_by, stats, modified_at
FROM avatar_hsm.wearable_suunto_daily_energy
UNION ALL
SELECT subject_id, provider, 'activity'::text, source, local_day, COALESCE(detail->>'timezone',''),
       'provider'::text, measurements.stats ||
       CASE WHEN energy.total >= energy.active
            THEN jsonb_build_object('passive_energy_kcal', energy.total - energy.active)
            ELSE '{}'::jsonb END,
       modified_at
FROM (
    SELECT DISTINCT ON (subject_id, source, local_day)
           subject_id, provider, source, local_day, detail, modified_at
    FROM avatar_hsm.wearable_record
    CROSS JOIN LATERAL (SELECT payload->'provider_data' AS detail) data
    CROSS JOIN LATERAL (SELECT detail->>'date' AS local_day) dated
    WHERE provider='withings' AND kind='metric' AND payload->>'metric'='daily_activity'
      AND local_day ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    ORDER BY subject_id, source, local_day, modified_at DESC, started_at DESC, source_id DESC
) latest
CROSS JOIN LATERAL (
    SELECT jsonb_object_agg(key, detail->field) AS stats
    FROM (VALUES ('distance','distance_meters'),
                 ('elevation','floors_climbed'),
                 ('active','active_seconds'),
                 ('soft','light_intensity_seconds'),
                 ('moderate','moderate_intensity_seconds'),
                 ('intense','vigorous_intensity_seconds'),
                 ('calories','active_energy_kcal'),
                 ('totalcalories','total_energy_kcal')) AS fields(field, key)
    WHERE CASE WHEN jsonb_typeof(detail->field)='number' THEN (detail->field)::numeric >= 0 ELSE false END
) measurements
CROSS JOIN LATERAL (
    SELECT (measurements.stats->>'total_energy_kcal')::numeric AS total,
           (measurements.stats->>'active_energy_kcal')::numeric AS active
) energy
WHERE measurements.stats IS NOT NULL;
