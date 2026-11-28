ALTER TABLE avatar_hsm.samsung_session_revision
    DROP CONSTRAINT samsung_session_revision_kind_check,
    ADD CHECK (kind IN ('sleep', 'workout', 'heart_rate') OR kind ~ '^metric:[a-z][a-z0-9_]*$');

CREATE FUNCTION avatar_hsm.samsung_record_scope(record_kind text, payload jsonb)
RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE WHEN record_kind = 'metric' THEN 'metric:' || (payload->>'metric') ELSE record_kind END
$$;

CREATE OR REPLACE FUNCTION avatar_hsm.samsung_record_uid(record_kind text, source_id text)
RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE
        WHEN record_kind = 'heart_rate' THEN source_id
        WHEN starts_with(record_kind, 'metric:') THEN
            CASE WHEN right(source_id, char_length(record_kind) - 6) = substring(record_kind FROM 7)
                THEN left(source_id, char_length(source_id) - char_length(record_kind) + 6)
            END
        ELSE substring(source_id FROM '^(.*):[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.][0-9]+)?Z$')
    END
$$;
