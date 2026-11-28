CREATE FUNCTION avatar_hsm.polar_activity_zone_time(stamp text, is_instant boolean)
RETURNS timestamptz LANGUAGE plpgsql STABLE STRICT AS $$
BEGIN
    IF is_instant THEN
        RETURN stamp::timestamptz;
    END IF;
    RETURN stamp::timestamp AT TIME ZONE 'UTC';
EXCEPTION
    WHEN invalid_datetime_format OR datetime_field_overflow OR invalid_time_zone_displacement_value THEN
        RETURN NULL;
END;
$$;

CREATE VIEW avatar_hsm.wearable_polar_activity_zones AS
WITH latest AS (
    SELECT DISTINCT ON (r.subject_id, r.source, data.detail->>'date')
           r.subject_id, r.provider, r.source, r.modified_at, data.detail
    FROM avatar_hsm.wearable_record r
    CROSS JOIN LATERAL (SELECT r.payload->'provider_data' AS detail) data
    WHERE r.provider='polar' AND r.kind='metric' AND r.payload->>'metric'='activity_samples'
      AND data.detail->>'date' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    ORDER BY r.subject_id, r.source, data.detail->>'date', r.modified_at DESC, r.source_id DESC
), points AS (
    SELECT latest.*, sample, ordinal, sample->>'timestamp' AS stamp
    FROM latest
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(detail#>'{activity_zones,samples}')='array'
             THEN detail#>'{activity_zones,samples}' ELSE '[]'::jsonb END
    ) WITH ORDINALITY AS zone(sample, ordinal)
    WHERE sample->>'zone' IN ('SEDENTARY','LIGHT','SLEEP','MODERATE','VIGOROUS')
)
SELECT DISTINCT ON (subject_id, source, at, precision)
       subject_id, provider, 'metric'::text AS kind, source,
       'polar_activity_zone:' || precision || ':' ||
           to_char(at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US') AS source_id,
       at AS started_at, at AS ended_at, modified_at,
       jsonb_build_object(
           'metric','polar_activity_zone','time_precision',precision,
           'provider_data',jsonb_build_object('zone',sample->>'zone',
               'timestamp',stamp,'provider_day',detail->>'date')
       ) AS payload
FROM points
CROSS JOIN LATERAL (
    SELECT CASE WHEN stamp ~ '(Z|[+-][0-9]{2}:[0-9]{2})$' THEN 'instant' ELSE 'provider_civil_time' END AS precision
) clock
CROSS JOIN LATERAL (
    SELECT CASE
        WHEN stamp !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]{1,9})?)?(Z|[+-][0-9]{2}:[0-9]{2})?$' THEN NULL
        ELSE avatar_hsm.polar_activity_zone_time(stamp, precision='instant')
    END AS at
) parsed
WHERE at IS NOT NULL
ORDER BY subject_id, source, at, precision, modified_at DESC, ordinal DESC;
