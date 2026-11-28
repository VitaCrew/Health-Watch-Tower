CREATE VIEW avatar_hsm.wearable_withings_sleep_samples AS
SELECT DISTINCT ON (r.subject_id, r.source, r.source_id, metric.name, sample.at, model.id)
       r.subject_id, r.provider, 'metric'::text AS kind, r.source,
       metric.name || ':' || r.source_id || ':' || model.id || ':' || point.stamp AS source_id,
       sample.at AS started_at, sample.at AS ended_at, r.modified_at,
       jsonb_build_object('metric',metric.name,
           'provider_data',jsonb_strip_nulls(jsonb_build_object(
               'value',reading.value,'unit',metric.unit,'sleep_id',r.source_id,
               'model_id',s.segment->'model_id','provider_field',metric.field))) AS payload,
       r.started_at AS sleep_started_at, r.ended_at AS sleep_ended_at
FROM avatar_hsm.wearable_record r
CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(r.payload->'provider_sleep_segments')='array'
         THEN r.payload->'provider_sleep_segments' ELSE '[]'::jsonb END
) WITH ORDINALITY s(segment,ordinal)
CROSS JOIN (VALUES
    ('hr','sleep_heart_rate','bpm',1,350),
    ('rr','respiratory_rate','breaths/min',0,NULL),
    ('rmssd','hrv_rmssd','ms',0,NULL),
    ('sdnn_1','hrv_sdnn','ms',0,NULL),
    ('snoring','snoring_duration','s',0,NULL),
    ('hrv_quality','hrv_quality','score',0,NULL),
    ('mvt_score','bed_movement_score','score',0,255),
    ('chest_movement_rate','chest_movement_rate','events/min',0,NULL),
    ('withings_index','withings_breathing_index','events/hour',0,NULL),
    ('breathing_sounds','breathing_sound_duration','s',0,NULL)
) metric(field,name,unit,minimum,maximum)
CROSS JOIN LATERAL jsonb_each(
    CASE WHEN jsonb_typeof(s.segment->metric.field)='object'
         THEN s.segment->metric.field ELSE '{}'::jsonb END
) point(stamp,value)
CROSS JOIN LATERAL (
    SELECT CASE WHEN point.stamp ~ '^(0|[1-9][0-9]{0,10})$' THEN to_timestamp(point.stamp::bigint) END AS at
) sample
CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(point.value)='number' THEN point.value::numeric END AS value
) reading
CROSS JOIN LATERAL (
    SELECT coalesce(s.segment->>'model_id','unknown') AS id
) model
WHERE r.provider='withings' AND r.kind='sleep'
  AND sample.at>=r.started_at AND sample.at<r.ended_at
  AND reading.value>=metric.minimum AND (metric.maximum IS NULL OR reading.value<=metric.maximum)
ORDER BY r.subject_id, r.source, r.source_id, metric.name, sample.at, model.id, s.ordinal DESC;
