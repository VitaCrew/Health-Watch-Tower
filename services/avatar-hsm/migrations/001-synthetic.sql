CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE SCHEMA IF NOT EXISTS avatar_hsm;

CREATE TABLE IF NOT EXISTS avatar_hsm.subject (
    id text PRIMARY KEY CHECK (id = 'synthetic-mvp'),
    timezone text NOT NULL
);
CREATE TABLE IF NOT EXISTS avatar_hsm.product (
    item_seq text PRIMARY KEY,
    display_name text NOT NULL,
    source_name text NOT NULL
);
CREATE INDEX IF NOT EXISTS hsm_product_name_trgm ON avatar_hsm.product
    USING gin (display_name gin_trgm_ops);

CREATE TABLE IF NOT EXISTS avatar_hsm.regimen (
    subject_id text NOT NULL REFERENCES avatar_hsm.subject,
    item_seq text NOT NULL REFERENCES avatar_hsm.product,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    PRIMARY KEY (subject_id, item_seq),
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);
CREATE TABLE IF NOT EXISTS avatar_hsm.dose (
    id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES avatar_hsm.subject,
    item_seq text NOT NULL REFERENCES avatar_hsm.product,
    scheduled_at timestamptz NOT NULL,
    eligible_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    requested_at timestamptz,
    response text CHECK (response IN ('taken', 'not_yet', 'skipped')),
    responded_at timestamptz,
    UNIQUE (subject_id, item_seq, scheduled_at),
    CHECK (eligible_at >= scheduled_at AND expires_at > eligible_at),
    CHECK ((response IS NULL) = (responded_at IS NULL)),
    CHECK (response IS NULL OR requested_at IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS hsm_due_dose ON avatar_hsm.dose (subject_id, eligible_at)
    WHERE requested_at IS NULL;

CREATE TABLE IF NOT EXISTS avatar_hsm.dur_coverage (
    item_seq text PRIMARY KEY REFERENCES avatar_hsm.product,
    dataset_version text NOT NULL,
    availability text NOT NULL CHECK (availability IN ('available', 'failed'))
);
CREATE TABLE IF NOT EXISTS avatar_hsm.dur_pair (
    first_item_seq text NOT NULL REFERENCES avatar_hsm.dur_coverage,
    second_item_seq text NOT NULL REFERENCES avatar_hsm.dur_coverage,
    reason text NOT NULL,
    source_url text NOT NULL,
    PRIMARY KEY (first_item_seq, second_item_seq),
    CHECK (first_item_seq < second_item_seq)
);

CREATE TABLE IF NOT EXISTS avatar_hsm.sleep_session (
    id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES avatar_hsm.subject,
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    CHECK (completed_at > started_at)
);
CREATE TABLE IF NOT EXISTS avatar_hsm.acute_decision (
    event_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES avatar_hsm.subject,
    signal text NOT NULL,
    observed_at timestamptz NOT NULL,
    rule_version text NOT NULL,
    emitted boolean NOT NULL
);
CREATE TABLE IF NOT EXISTS avatar_hsm.health_outbox (
    event_id text PRIMARY KEY REFERENCES avatar_hsm.acute_decision,
    created_at timestamptz NOT NULL DEFAULT now(),
    received_at timestamptz,
    retry_at timestamptz NOT NULL DEFAULT now(),
    tries integer NOT NULL DEFAULT 0 CHECK (tries >= 0)
);
