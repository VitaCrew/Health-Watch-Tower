WITH legacy AS (
    SELECT subject_id, source, source_id, modified_at,
           COALESCE(payload->'provider_data'->'summary',payload->'provider_data')
               ->'lastUpdatedTimeInSeconds' AS revision
    FROM avatar_hsm.wearable_record
    WHERE provider = 'garmin' AND kind = 'metric' AND payload->>'metric' = 'mct'
), checked AS (
    SELECT subject_id, source, source_id, modified_at,
           CASE WHEN jsonb_typeof(revision) = 'number' AND revision::text ~ '^[0-9]{1,12}$'
                THEN (revision::text)::bigint END AS epoch
    FROM legacy
)
UPDATE avatar_hsm.wearable_record r
SET modified_at = to_timestamp(checked.epoch)
FROM checked
WHERE r.subject_id = checked.subject_id AND r.source = checked.source AND r.source_id = checked.source_id
  AND r.provider = 'garmin' AND r.kind = 'metric' AND r.payload->>'metric' = 'mct'
  AND checked.epoch BETWEEN 1 AND EXTRACT(EPOCH FROM checked.modified_at + INTERVAL '5 minutes')
  AND r.modified_at <> to_timestamp(checked.epoch);

UPDATE avatar_hsm.wearable_oauth_connection c
SET garmin_daily_projected = false
WHERE c.provider = 'garmin' AND c.garmin_daily_projected
  AND (c.garmin_permissions & 2) <> 0
  AND EXISTS (
      SELECT 1 FROM avatar_hsm.wearable_record r
      WHERE r.subject_id = c.subject_id AND r.provider = c.provider
        AND r.kind = 'metric' AND r.payload->>'metric' = 'mct'
  );
