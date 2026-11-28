CREATE TABLE IF NOT EXISTS watch_tower.clinical_patient_binding (
    binding_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_id text NOT NULL,
    source_system text NOT NULL,
    patient_reference text NOT NULL,
    confirmed_by text NOT NULL,
    consent_version bigint NOT NULL CHECK (consent_version > 0),
    confirmed_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz,
    UNIQUE (subject_id, source_system)
);

CREATE TABLE IF NOT EXISTS watch_tower.clinical_record (
    record_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_id text NOT NULL,
    source_system text NOT NULL,
    patient_reference text NOT NULL,
    resource_type text NOT NULL CHECK (resource_type IN (
        'Condition', 'AllergyIntolerance', 'Observation', 'MedicationRequest'
    )),
    source_record_id text NOT NULL,
    source_version text NOT NULL,
    fhir_version text NOT NULL CHECK (fhir_version = '4.0.1'),
    profile_uri text NOT NULL,
    resource_sha256 text NOT NULL CHECK (resource_sha256 ~ '^[0-9a-f]{64}$'),
    occurred_at timestamptz,
    normalized_fact jsonb NOT NULL,
    resource jsonb NOT NULL,
    supersedes_record_id uuid REFERENCES watch_tower.clinical_record(record_id),
    actor_id text NOT NULL,
    consent_version bigint NOT NULL CHECK (consent_version > 0),
    imported_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (subject_id, source_system, resource_type, source_record_id, source_version)
);

CREATE INDEX IF NOT EXISTS clinical_record_subject_time_idx
    ON watch_tower.clinical_record (subject_id, occurred_at DESC NULLS LAST, imported_at DESC);

CREATE TABLE IF NOT EXISTS watch_tower.clinical_hsm_outbox (
    event_id text PRIMARY KEY CHECK (event_id ~ '^[0-9a-f]{64}$'),
    record_id uuid NOT NULL REFERENCES watch_tower.clinical_record(record_id) ON DELETE CASCADE,
    subject_id text NOT NULL,
    occurred_at timestamptz NOT NULL,
    consent_version bigint NOT NULL CHECK (consent_version > 0),
    fact jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    applied_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_owner text,
    lease_until timestamptz,
    last_failure_code text
);

CREATE INDEX IF NOT EXISTS clinical_hsm_outbox_ready_idx
    ON watch_tower.clinical_hsm_outbox (next_attempt_at, created_at)
    WHERE applied_at IS NULL;
