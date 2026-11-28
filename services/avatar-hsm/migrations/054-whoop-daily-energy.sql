ALTER VIEW avatar_hsm.wearable_daily_energy RENAME TO wearable_oura_polar_daily_energy;

CREATE VIEW avatar_hsm.wearable_daily_energy AS
SELECT subject_id, provider, kind, source, local_day, timezone, calculated_by, stats, modified_at
FROM avatar_hsm.wearable_oura_polar_daily_energy
UNION ALL
SELECT subject_id, provider, 'activity'::text, source, detail->>'local_day',
       CASE WHEN COUNT(DISTINCT detail->>'timezone') = 1
                 AND COUNT(detail->>'timezone') = COUNT(*)
            THEN MIN(detail->>'timezone') ELSE '' END,
       'hsm'::text,
       jsonb_build_object('total_energy_kcal', SUM(energy), 'cycle_count', COUNT(*),
                          'day_basis', 'cycle_midpoint'),
       MAX(modified_at)
FROM avatar_hsm.wearable_record
CROSS JOIN LATERAL (SELECT payload->'provider_data' AS detail) data
CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(detail->'value')='number' THEN (detail->'value')::numeric END AS energy
) sample
WHERE provider='whoop' AND kind='metric' AND payload->>'metric'='total_energy'
  AND detail->>'unit'='kcal' AND energy>0
  AND detail->>'local_day' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
GROUP BY subject_id, provider, source, detail->>'local_day';
