CREATE VIEW avatar_hsm.wearable_daily_energy AS
WITH summaries AS (
    SELECT subject_id, provider, source, source_id, modified_at,
           CASE WHEN provider = 'oura' THEN payload#>>'{provider_data,day}'
                ELSE COALESCE(payload#>>'{provider_data,date}',
                              left(payload#>>'{provider_data,end_time}', 10)) END AS local_day,
           CASE WHEN provider = 'oura' THEN payload#>'{provider_data,total_calories}'
                ELSE payload#>'{provider_data,calories}' END AS raw_total,
           payload#>'{provider_data,active_calories}' AS raw_active
    FROM avatar_hsm.wearable_record
    WHERE provider IN ('oura', 'polar') AND kind = 'metric' AND payload->>'metric' = 'daily_activity'
), latest AS (
    SELECT DISTINCT ON (subject_id, provider, source, local_day)
           subject_id, provider, source, local_day, modified_at,
           CASE WHEN jsonb_typeof(raw_total) = 'number' THEN raw_total::numeric END AS total,
           CASE WHEN jsonb_typeof(raw_active) = 'number' THEN raw_active::numeric END AS active
    FROM summaries
    WHERE local_day ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    ORDER BY subject_id, provider, source, local_day, modified_at DESC, source_id DESC
)
SELECT subject_id, provider, 'activity'::text AS kind, source, local_day, ''::text AS timezone,
       'provider'::text AS calculated_by,
       jsonb_strip_nulls(jsonb_build_object(
           'total_energy_kcal', CASE WHEN total >= 0 THEN total END,
           'active_energy_kcal', CASE WHEN active >= 0 THEN active END,
           'basal_energy_kcal', CASE WHEN total >= active AND active >= 0 THEN total - active END
       )) AS stats,
       modified_at
FROM latest
WHERE total >= 0 OR active >= 0;
