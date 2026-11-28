CREATE OR REPLACE VIEW avatar_hsm.wearable_oura_polar_daily_energy AS
WITH summaries AS (
    SELECT subject_id, provider, source, source_id, modified_at, detail,
           CASE WHEN provider = 'oura' THEN detail->>'day'
                ELSE COALESCE(detail->>'date', left(detail->>'end_time', 10)) END AS local_day
    FROM avatar_hsm.wearable_record
    CROSS JOIN LATERAL (SELECT payload->'provider_data' AS detail) raw
    WHERE provider IN ('oura', 'polar') AND kind = 'metric' AND payload->>'metric' = 'daily_activity'
), latest AS (
    SELECT DISTINCT ON (subject_id, provider, source, local_day)
           subject_id, provider, source, local_day, modified_at, detail
    FROM summaries
    WHERE local_day ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    ORDER BY subject_id, provider, source, local_day, modified_at DESC, source_id DESC
)
SELECT subject_id, provider, 'activity'::text AS kind, source, local_day, ''::text AS timezone,
       'provider'::text AS calculated_by,
       metrics.stats ||
       CASE WHEN (metrics.stats->>'total_energy_kcal')::numeric >= (metrics.stats->>'active_energy_kcal')::numeric
            THEN jsonb_build_object('basal_energy_kcal',
                 (metrics.stats->>'total_energy_kcal')::numeric - (metrics.stats->>'active_energy_kcal')::numeric)
            ELSE '{}'::jsonb END AS stats,
       modified_at
FROM latest
CROSS JOIN LATERAL (
    SELECT jsonb_object_agg(key, measurement) AS stats
    FROM (VALUES
        (CASE WHEN provider='oura' THEN 'total_calories' ELSE 'calories' END, 'total_energy_kcal', false),
        ('active_calories', 'active_energy_kcal', false),
        ('distance_from_steps', 'step_distance_meters', false),
        ('inactivity_alert_count', 'inactivity_alert_count', false),
        ('daily_activity', 'activity_goal_percent', false),
        ('active_duration', 'active_seconds', true),
        ('inactive_duration', 'inactive_seconds', true)
    ) AS fields(field,key,is_duration)
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN is_duration AND detail->>field ~ '^(P1D|PT(?=[0-9])([0-9]{1,2}H)?([0-9]{1,2}M)?([0-9]{1,2}(\.[0-9]{1,6})?S)?)$'
                THEN EXTRACT(EPOCH FROM (detail->>field)::interval)
            WHEN NOT is_duration AND jsonb_typeof(detail->field)='number'
                THEN (detail->field)::numeric END AS measurement
    ) parsed
    WHERE (provider='polar' OR key IN ('total_energy_kcal','active_energy_kcal'))
      AND measurement >= 0 AND (NOT is_duration OR measurement <= 86400)
      AND (key <> 'inactivity_alert_count' OR measurement=trunc(measurement))
) metrics
WHERE metrics.stats IS NOT NULL;
