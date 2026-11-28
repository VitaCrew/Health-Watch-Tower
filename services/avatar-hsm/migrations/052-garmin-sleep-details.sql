CREATE INDEX IF NOT EXISTS garmin_nap_parent
    ON avatar_hsm.wearable_record (subject_id, source, (payload->>'garmin_parent_sleep_id'))
    WHERE provider='garmin' AND kind='sleep' AND payload ? 'garmin_parent_sleep_id';

UPDATE avatar_hsm.wearable_oauth_connection c SET garmin_daily_projected=false
WHERE c.provider='garmin' AND c.garmin_daily_projected AND (c.garmin_permissions & 2)<>0
  AND EXISTS (SELECT 1 FROM avatar_hsm.wearable_record r
              WHERE r.subject_id=c.subject_id AND r.provider=c.provider AND r.kind='sleep');
