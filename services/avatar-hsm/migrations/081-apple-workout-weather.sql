CREATE VIEW avatar_hsm.wearable_apple_workout_weather AS
SELECT w.subject_id, w.provider, 'metric'::text AS kind, w.source,
       w.source_id || ':' || weather.metric AS source_id,
       w.started_at, w.ended_at, w.modified_at,
       jsonb_build_object(
           'metric', weather.metric,
           'provider_data', jsonb_build_object('value', weather.value, 'unit', weather.unit,
                                               'workout_id', w.source_id)
       ) AS payload,
       weather.metric AS metric_name
FROM avatar_hsm.wearable_record w
CROSS JOIN LATERAL (VALUES
    ('weather_temperature', w.payload->'weather_temperature_c', '°C'),
    ('weather_humidity', w.payload->'weather_humidity_percent', '%')
) AS weather(metric, value, unit)
WHERE w.provider IN ('healthkit', 'apple_health_export') AND w.kind='workout'
  AND jsonb_typeof(weather.value)='number';
