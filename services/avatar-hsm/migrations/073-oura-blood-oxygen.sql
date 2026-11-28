CREATE VIEW avatar_hsm.wearable_oura_blood_oxygen AS
SELECT r.subject_id, r.provider, r.kind, r.source,
       metric.name || ':' || r.source_id AS source_id,
       r.started_at, r.ended_at, r.modified_at,
       jsonb_build_object(
           'metric',metric.name,'time_precision','provider_calendar_date',
           'provider_data',jsonb_build_object(
               'date',detail.value->'day','value',sample.number,
               'unit',metric.unit,'document_id',detail.value->'id')) AS payload
FROM avatar_hsm.wearable_record r
CROSS JOIN LATERAL (SELECT r.payload->'provider_data' AS value) detail
CROSS JOIN LATERAL (VALUES
    ('oxygen_saturation','%',detail.value#>'{spo2_percentage,average}',100),
    ('breathing_disturbance_index','index',detail.value->'breathing_disturbance_index',NULL)
) metric(name,unit,value,maximum)
CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(metric.value)='number' THEN metric.value::numeric END AS number
) sample
WHERE r.provider='oura' AND r.kind='metric' AND r.payload->>'metric'='daily_spo2'
  AND detail.value->>'day' = to_char(r.started_at AT TIME ZONE 'UTC','YYYY-MM-DD')
  AND sample.number>=0 AND (metric.maximum IS NULL OR sample.number<=metric.maximum);
