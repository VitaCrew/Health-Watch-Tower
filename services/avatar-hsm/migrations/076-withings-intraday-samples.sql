CREATE VIEW avatar_hsm.wearable_withings_intraday_samples AS
SELECT r.subject_id, r.provider, r.kind, r.source,
       metric.name || ':' || r.source_id AS source_id,
       r.started_at, r.ended_at, r.modified_at,
       jsonb_build_object('metric',metric.name,
           'provider_data',jsonb_build_object(
               'value',sample.number,'unit',metric.unit,
               'measurement_context','intraday_activity',
               'provider_field',metric.field,'raw_record_id',r.source_id)) AS payload,
       metric.name AS metric_name
FROM avatar_hsm.wearable_record r
CROSS JOIN (VALUES
    ('steps','step_count','count',0,NULL),
    ('elevation','floors_climbed','count',0,NULL),
    ('calories','active_energy','kcal',0,NULL),
    ('distance','activity_distance','m',0,NULL),
    ('stroke','swimming_stroke_count','count',0,NULL),
    ('pool_lap','swimming_lap_count','count',0,NULL),
    ('duration','activity_duration','s',0,NULL),
    ('spo2_auto','oxygen_saturation','%',0,100),
    ('rmssd','hrv_rmssd','ms',0,NULL),
    ('sdnn1','hrv_sdnn','ms',0,NULL),
    ('hrv_quality','hrv_quality','score',0,NULL),
    ('core_body_temperature','core_body_temperature','celsius',NULL,NULL),
    ('rr','respiratory_rate','breaths/min',0,NULL),
    ('chest_movement_rate','chest_movement_rate','events/min',0,NULL)
) metric(field,name,unit,minimum,maximum)
CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(r.payload->'provider_data'->metric.field)='number'
                THEN (r.payload->'provider_data'->metric.field)::numeric END AS number
) sample
WHERE r.provider='withings' AND r.kind='metric' AND r.payload->>'metric'='intraday_activity'
  AND sample.number IS NOT NULL
  AND (metric.minimum IS NULL OR sample.number>=metric.minimum)
  AND (metric.maximum IS NULL OR sample.number<=metric.maximum);
