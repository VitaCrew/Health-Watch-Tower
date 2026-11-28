CREATE INDEX wearable_health_connect_intensity_window
ON avatar_hsm.wearable_record(subject_id, started_at, ended_at)
WHERE provider='health_connect' AND kind='metric' AND payload->>'metric'='activity_intensity';

CREATE FUNCTION avatar_hsm.health_connect_daily_intensity(
    owner_id text, first_day date, last_day date, origin text
) RETURNS TABLE (
    subject_id text, provider text, kind text, source text, local_day text,
    timezone text, calculated_by text, stats jsonb, modified_at timestamptz
) LANGUAGE SQL STABLE AS $$
WITH days AS (
    SELECT s.id, s.timezone, first_day + day.number AS date,
           (first_day + day.number)::timestamp AT TIME ZONE s.timezone AS starts,
           (first_day + day.number + 1)::timestamp AT TIME ZONE s.timezone AS ends
    FROM avatar_hsm.subject s
    CROSS JOIN generate_series(0,last_day-first_day) AS day(number)
    WHERE s.id=owner_id
), coverage AS (
    SELECT d.id, r.source, d.date, d.timezone, max(r.modified_at) AS modified_at,
           range_agg(piece.span) AS covered,
           range_agg(piece.span) FILTER (WHERE r.payload#>'{provider_data,intensity_code}'='1'::jsonb) AS vigorous
    FROM days d
    JOIN avatar_hsm.wearable_record r
      ON r.subject_id=d.id AND r.started_at<d.ends AND r.ended_at>d.starts
    CROSS JOIN LATERAL (
        SELECT tstzrange(GREATEST(r.started_at,d.starts), LEAST(r.ended_at,d.ends), '[)') AS span
    ) piece
    WHERE r.provider='health_connect' AND r.kind='metric'
      AND r.payload->>'metric'='activity_intensity'
      AND r.payload#>'{provider_data,intensity_code}' IN ('0'::jsonb,'1'::jsonb)
      AND r.started_at<r.ended_at AND d.starts<d.ends
      AND (origin IS NULL OR r.source=origin)
    GROUP BY d.id, r.source, d.date, d.timezone
), totals AS (
    SELECT c.*,
           (SELECT sum(extract(epoch FROM upper(span)-lower(span))) FROM unnest(c.covered) AS span) AS duration,
           COALESCE((SELECT sum(extract(epoch FROM upper(span)-lower(span))) FROM unnest(c.vigorous) AS span),0) AS vigorous_duration
    FROM coverage c
)
SELECT t.id, 'health_connect'::text, 'activity'::text, t.source, t.date::text, t.timezone, 'hsm'::text,
       jsonb_build_object('moderate_seconds', t.duration-t.vigorous_duration,
                          'vigorous_seconds', t.vigorous_duration,
                          'activity_intensity_seconds', t.duration,
                          'weighted_intensity_minutes', round((t.duration+t.vigorous_duration)/60, 2)),
       t.modified_at
FROM totals t
$$;
