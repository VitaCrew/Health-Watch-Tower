CREATE VIEW avatar_hsm.wearable_coros_day_source AS
SELECT DISTINCT ON (subject_id, source, started_at)
       subject_id, provider, source, started_at, modified_at, payload->'provider_data' AS detail
FROM avatar_hsm.wearable_record
WHERE provider='coros' AND kind='metric' AND payload->>'metric'='daily_summary'
ORDER BY subject_id, source, started_at, modified_at DESC, source_id DESC;

CREATE VIEW avatar_hsm.wearable_coros_daily_energy AS
SELECT subject_id, provider, 'activity'::text AS kind, source,
       to_char(started_at AT TIME ZONE 'UTC','YYYY-MM-DD') AS local_day,
       ''::text AS timezone, 'provider'::text AS calculated_by,
       jsonb_build_object('total_energy_kcal', detail->'calorie') AS stats, modified_at
FROM avatar_hsm.wearable_coros_day_source
WHERE CASE WHEN jsonb_typeof(detail->'calorie')='number'
           THEN (detail->'calorie')::numeric >= 0 ELSE false END;

CREATE VIEW avatar_hsm.wearable_coros_daily_metrics AS
SELECT subject_id, provider, 'metric'::text AS kind, source,
       metric.name || ':' || to_char(started_at AT TIME ZONE 'UTC','YYYY-MM-DD') AS source_id,
       started_at, started_at AS ended_at, modified_at,
       jsonb_build_object('metric',metric.name,'time_precision','provider_calendar_date',
           'provider_data',jsonb_strip_nulls(jsonb_build_object(
               'date',to_char(started_at AT TIME ZONE 'UTC','YYYY-MM-DD'),
               'value', CASE WHEN metric.unit='s' THEN value.number * 60 ELSE value.number END,
               'unit',metric.unit))) AS payload
FROM avatar_hsm.wearable_coros_day_source
CROSS JOIN (VALUES
    ('sleepDuration','coros_daily_sleep_duration','s',0,1440),
    ('ppgHrv','coros_overnight_hrv',NULL,1,NULL)
) AS metric(field,name,unit,minimum,maximum)
CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(detail->metric.field)='number'
                THEN (detail->metric.field)::numeric END AS number
) value
WHERE value.number=trunc(value.number) AND value.number>=metric.minimum
  AND (metric.maximum IS NULL OR value.number<=metric.maximum);
