CREATE VIEW avatar_hsm.wearable_oura_daily_indicators AS
SELECT r.subject_id,r.provider,r.kind,r.source,
       metric.name || ':' || r.source_id AS source_id,
       local_day.at AS started_at,local_day.at AS ended_at,r.modified_at,
       jsonb_build_object('metric',metric.name,'time_precision','provider_calendar_date',
           'provider_data',jsonb_strip_nulls(jsonb_build_object(
               'date',detail.value->'day','value',reading.number,'unit',metric.unit,
               'document_id',detail.value->'id','provider_timestamp',detail.value->'timestamp',
               'provider_field',metric.field,'measurement_context',metric.document))) AS payload,
       metric.name AS metric_name
FROM avatar_hsm.wearable_record r
JOIN (VALUES
    ('daily_readiness','score','readiness_score','score',0,100,true),
    ('daily_readiness','temperature_deviation','skin_temperature_deviation','celsius',NULL,NULL,false),
    ('daily_readiness','temperature_trend_deviation','skin_temperature_trend_deviation','celsius',NULL,NULL,false),
    ('daily_stress','stress_high','high_stress_duration','s',0,NULL,true),
    ('daily_stress','recovery_high','high_recovery_duration','s',0,NULL,true),
    ('daily_cardiovascular_age','vascular_age','vascular_age','years',18,100,true),
    ('daily_cardiovascular_age','pulse_wave_velocity','pulse_wave_velocity','m/s',0,NULL,false)
) metric(document,field,name,unit,minimum,maximum,whole_number) ON r.payload->>'metric'=metric.document
CROSS JOIN LATERAL (SELECT r.payload->'provider_data' AS value) detail
CROSS JOIN LATERAL (
    SELECT CASE WHEN detail.value->>'day' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN avatar_hsm.wearable_provider_timestamp(detail.value->>'day',false) END AS at
) local_day
CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(detail.value->metric.field)='number'
                THEN (detail.value->metric.field)::numeric END AS number
) reading
WHERE r.provider='oura' AND r.kind='metric' AND local_day.at IS NOT NULL
  AND reading.number IS NOT NULL
  AND (metric.minimum IS NULL OR reading.number>=metric.minimum)
  AND (metric.maximum IS NULL OR reading.number<=metric.maximum)
  AND (NOT metric.whole_number OR reading.number=trunc(reading.number));
