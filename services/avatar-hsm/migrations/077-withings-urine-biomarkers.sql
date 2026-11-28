CREATE VIEW avatar_hsm.wearable_withings_urine_biomarkers AS
SELECT DISTINCT ON (r.subject_id, r.source, r.source_id, metric.name)
       r.subject_id, r.provider, r.kind, r.source,
       r.source_id || ':' || metric.name AS source_id,
       r.started_at, r.ended_at, r.modified_at,
       jsonb_build_object('metric',metric.name,'provider_data',jsonb_build_object(
           'unit',metric.unit,'value',reading.mantissa * power(10::numeric,reading.exponent),
           'measurement',point.value,'raw_record_id',r.source_id)) AS payload,
       metric.name AS metric_name
FROM avatar_hsm.wearable_record r
CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(r.payload#>'{provider_data,measures}')='array'
         THEN r.payload#>'{provider_data,measures}' ELSE '[]'::jsonb END
) WITH ORDINALITY point(value,ordinal)
JOIN (VALUES
    ('147','urinary_ph','pH'),
    ('148','urine_specific_gravity','ratio'),
    ('151','urinary_nitrites','µmol/L'),
    ('204','urinary_ketones','mmol/L'),
    ('205','urinary_vitamin_c','mmol/L'),
    ('248','urinary_calcium','mmol/L'),
    ('249','urinary_creatinine','mmol/L'),
    ('251','urinary_calcium_creatinine_ratio','mmol/mmol')
) metric(code,name,unit) ON point.value->>'type'=metric.code
CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(point.value->'value')='number'
                      AND point.value->>'value' ~ '^[0-9]{1,19}$'
                THEN (point.value->'value')::numeric END AS mantissa,
           CASE WHEN jsonb_typeof(point.value->'unit')='number'
                      AND point.value->>'unit' ~ '^-?([0-9]|1[0-2])$'
                THEN (point.value->>'unit')::integer END AS exponent
) reading
WHERE r.provider='withings' AND r.kind='metric' AND r.payload->>'metric'='measurements'
  AND jsonb_typeof(point.value->'type')='number'
  AND reading.mantissa IS NOT NULL AND reading.exponent IS NOT NULL
ORDER BY r.subject_id,r.source,r.source_id,metric.name,point.ordinal;
