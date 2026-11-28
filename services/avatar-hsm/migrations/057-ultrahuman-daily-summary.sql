CREATE VIEW avatar_hsm.wearable_ultrahuman_daily AS
WITH snapshots AS (
    SELECT subject_id, provider, source, source_id, modified_at,
           to_char(started_at AT TIME ZONE 'UTC','YYYY-MM-DD') AS local_day,
           payload->>'metric' AS metric,
           CASE WHEN payload->>'metric'='steps' THEN payload#>'{provider_data,total}'
                ELSE payload#>'{provider_data,value}' END AS measurement
    FROM avatar_hsm.wearable_record
    WHERE provider='ultrahuman' AND kind='metric'
      AND payload->>'metric' IN ('steps','active_minutes','calories_burned')
), latest AS (
    SELECT DISTINCT ON (subject_id, source, local_day, metric)
           subject_id, provider, source, local_day, metric, modified_at,
           CASE WHEN jsonb_typeof(measurement)='number' THEN measurement::numeric END AS value
    FROM snapshots
    ORDER BY subject_id, source, local_day, metric, modified_at DESC, source_id DESC
)
SELECT subject_id, provider, 'steps'::text AS kind, source, local_day, ''::text AS timezone,
       'provider'::text AS calculated_by, jsonb_build_object('total',value,'unit','steps') AS stats, modified_at
FROM latest WHERE metric='steps' AND value BETWEEN 0 AND 1000000 AND value=trunc(value)
UNION ALL
SELECT subject_id, provider, 'activity'::text, source, local_day, '', 'provider',
       jsonb_object_agg(
           CASE metric WHEN 'active_minutes' THEN 'exercise_seconds' ELSE 'total_energy_kcal' END,
           CASE metric WHEN 'active_minutes' THEN value * 60 ELSE value END
       ), MAX(modified_at)
FROM latest
WHERE (metric='active_minutes' AND value BETWEEN 0 AND 1440)
   OR (metric='calories_burned' AND value>=0)
GROUP BY subject_id, provider, source, local_day;
