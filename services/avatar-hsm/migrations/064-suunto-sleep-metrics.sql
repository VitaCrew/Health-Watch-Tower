CREATE VIEW avatar_hsm.wearable_suunto_sleep_metrics AS
SELECT r.subject_id, r.provider, 'metric'::text AS kind, r.source,
       metric.name || ':' || r.source_id AS source_id,
       r.started_at, r.ended_at, r.modified_at,
       jsonb_build_object(
           'metric', metric.name,
           'provider_data', jsonb_build_object(
               'value', CASE WHEN metric.unit = '%' AND value.number <= 1
                             THEN value.number * 100 ELSE value.number END,
               'unit', metric.unit, 'statistic', metric.statistic,
               'sleep_source_id', r.source_id,
               'nap', r.payload->'provider_data'->'IsNap',
               'interval_start', r.payload->'provider_data'->'BedtimeStart',
               'interval_end', r.payload->'provider_data'->'BedtimeEnd'
           )
       ) AS payload
FROM avatar_hsm.wearable_record r
CROSS JOIN (VALUES
    ('HRMin', 'suunto_sleep_min_heart_rate', 'bpm', 'min', 350),
    ('HRAvg', 'suunto_sleep_average_heart_rate', 'bpm', 'average', 350),
    ('AvgHRV', 'suunto_sleep_hrv_rmssd', 'ms', 'average', NULL),
    ('MaxSpo2', 'suunto_sleep_max_spo2', '%', 'max', 100)
) AS metric(field, name, unit, statistic, maximum)
CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(r.payload->'provider_data'->metric.field) = 'number'
                THEN (r.payload->'provider_data'->>metric.field)::numeric END AS number
) AS value
WHERE r.provider = 'suunto' AND r.kind = 'sleep'
  AND value.number > 0
  AND (metric.maximum IS NULL OR value.number <= metric.maximum)
  AND (metric.unit <> 'bpm' OR value.number >= 1);
