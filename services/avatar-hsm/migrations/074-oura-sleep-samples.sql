ALTER FUNCTION avatar_hsm.polar_activity_zone_time(text, boolean)
RENAME TO wearable_provider_timestamp;

CREATE VIEW avatar_hsm.wearable_oura_sleep_samples AS
SELECT r.subject_id, r.provider, 'metric'::text AS kind, r.source,
       series.name || ':' || r.source_id || ':' || (point.ordinal - 1) AS source_id,
       sample.at AS started_at, sample.at AS ended_at, r.modified_at,
       jsonb_build_object('metric',series.name,
           'provider_data',jsonb_build_object(
               'value',reading.value,'unit',series.unit,'sleep_id',r.source_id,
               'sample_index',point.ordinal - 1,'interval_seconds',clock.interval_seconds,
               'series_start',series.detail->>'timestamp')) AS payload,
       r.started_at AS sleep_started_at, r.ended_at AS sleep_ended_at
FROM avatar_hsm.wearable_record r
CROSS JOIN LATERAL (VALUES
    ('sleep_heart_rate','bpm',r.payload#>'{provider_data,heart_rate}',1,350),
    ('hrv_rmssd','ms',r.payload#>'{provider_data,hrv}',0,NULL)
) series(name,unit,detail,minimum,maximum)
CROSS JOIN LATERAL (
    SELECT CASE WHEN series.detail->>'timestamp' ~
        '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2})$'
        THEN avatar_hsm.wearable_provider_timestamp(series.detail->>'timestamp',true) END AS at,
        CASE WHEN jsonb_typeof(series.detail->'interval')='number'
        THEN (series.detail->'interval')::numeric END AS interval_seconds
) clock
CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(series.detail->'items')='array'
         THEN series.detail->'items' ELSE '[]'::jsonb END
) WITH ORDINALITY point(item,ordinal)
CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(point.item)='number' THEN point.item::numeric END AS value
) reading
CROSS JOIN LATERAL (
    SELECT CASE WHEN clock.interval_seconds BETWEEN 0.000001 AND 86400
        THEN clock.at + make_interval(secs => ((point.ordinal - 1) * clock.interval_seconds)::double precision)
        END AS at
) sample
WHERE r.provider='oura' AND r.kind='sleep'
  AND sample.at >= r.started_at AND sample.at < r.ended_at
  AND reading.value>=series.minimum AND (series.maximum IS NULL OR reading.value<=series.maximum);
