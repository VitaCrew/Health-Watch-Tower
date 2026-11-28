CREATE VIEW avatar_hsm.wearable_oura_activity_met AS
SELECT r.subject_id,r.provider,r.kind,r.source,
       'activity_met:' || r.source_id || ':' || (point.ordinal - 1) AS source_id,
       sample.at AS started_at,sample.at AS ended_at,r.modified_at,
       jsonb_build_object('metric','activity_met',
           'provider_data',jsonb_build_object(
               'date',raw.detail->'day','value',point.value,'unit','MET',
               'document_id',raw.detail->'id','sample_index',point.ordinal - 1,
               'interval_seconds',clock.interval_seconds,
               'series_start',raw.detail#>'{met,timestamp}')) AS payload
FROM avatar_hsm.wearable_record r
CROSS JOIN LATERAL (SELECT r.payload->'provider_data' AS detail) raw
CROSS JOIN LATERAL (
    SELECT CASE WHEN raw.detail#>>'{met,timestamp}' ~
        '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2})$'
        THEN avatar_hsm.wearable_provider_timestamp(raw.detail#>>'{met,timestamp}',true) END AS at,
        CASE WHEN jsonb_typeof(raw.detail#>'{met,interval}')='number'
        THEN (raw.detail#>'{met,interval}')::numeric END AS interval_seconds
) clock
CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(raw.detail#>'{met,items}')='array'
         THEN raw.detail#>'{met,items}' ELSE '[]'::jsonb END
) WITH ORDINALITY point(value,ordinal)
CROSS JOIN LATERAL (
    SELECT CASE WHEN clock.interval_seconds BETWEEN 0.000001 AND 86400
        THEN clock.at + make_interval(secs => ((point.ordinal - 1) * clock.interval_seconds)::double precision)
        END AS at
) sample
WHERE r.provider='oura' AND r.kind='metric' AND r.payload->>'metric'='daily_activity'
  AND sample.at IS NOT NULL
  AND CASE WHEN jsonb_typeof(point.value)='number' THEN point.value::numeric>=0 ELSE false END;
