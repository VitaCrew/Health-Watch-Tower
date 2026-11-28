ALTER TABLE avatar_hsm.samsung_session_revision
    DROP CONSTRAINT samsung_session_revision_kind_check,
    ADD CHECK (kind IN ('sleep', 'workout', 'heart_rate'));

CREATE FUNCTION avatar_hsm.samsung_record_uid(record_kind text, source_id text)
RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE WHEN record_kind = 'heart_rate' THEN source_id
        ELSE substring(source_id FROM '^(.*):[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.][0-9]+)?Z$')
    END
$$;
