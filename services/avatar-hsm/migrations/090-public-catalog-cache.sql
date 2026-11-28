-- Public reference catalogs are kept separate from person-scoped health facts.
CREATE SCHEMA IF NOT EXISTS avatar_catalog;

CREATE TABLE IF NOT EXISTS avatar_catalog.reference_item (
    source text NOT NULL,
    source_id text NOT NULL,
    payload jsonb NOT NULL,
    license_id text NOT NULL,
    license_uri text NOT NULL,
    source_uri text NOT NULL,
    retrieved_at timestamptz NOT NULL,
    PRIMARY KEY (source, source_id)
);

COMMENT ON SCHEMA avatar_catalog IS
    'Public catalog cache only; it has no subject_id and is not a personal health record store.';
