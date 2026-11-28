CREATE INDEX IF NOT EXISTS wearable_record_garmin_summary
ON avatar_hsm.wearable_record (subject_id, source, (payload->>'garmin_summary_id'), (payload->>'metric'))
WHERE provider = 'garmin' AND kind = 'metric' AND payload ? 'garmin_summary_id';

UPDATE avatar_hsm.wearable_oauth_connection c
SET garmin_daily_projected = false
WHERE c.provider = 'garmin' AND c.garmin_daily_projected
  AND (c.garmin_permissions & 2) <> 0
  AND EXISTS (
      SELECT 1 FROM avatar_hsm.wearable_record r
      WHERE r.subject_id = c.subject_id AND r.provider = c.provider
        AND r.kind = 'metric' AND r.payload->>'metric' IN ('stressDetails','allDayRespiration','pulseox')
  );
