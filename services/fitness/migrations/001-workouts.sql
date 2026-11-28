CREATE SCHEMA IF NOT EXISTS fitness;

CREATE TABLE IF NOT EXISTS fitness.schema_migration (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fitness.workout_sessions (
    id text PRIMARY KEY CHECK (id ~ '^[a-f0-9]{32}$'),
    owner_id text NOT NULL,
    workflow_run_id text NOT NULL,
    request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
    performed_at timestamptz NOT NULL,
    timezone text NOT NULL,
    source text NOT NULL CHECK (source IN ('manual', 'workflow')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (owner_id, workflow_run_id)
);

CREATE INDEX IF NOT EXISTS workout_sessions_owner_performed
    ON fitness.workout_sessions (owner_id, performed_at DESC, id);

CREATE TABLE IF NOT EXISTS fitness.workout_sets (
    id text PRIMARY KEY CHECK (id ~ '^[a-f0-9]{32}$'),
    workout_id text NOT NULL REFERENCES fitness.workout_sessions(id) ON DELETE CASCADE,
    position integer NOT NULL CHECK (position BETWEEN 1 AND 30),
    exercise_id text NOT NULL CHECK (exercise_id IN ('squat', 'bench_press', 'deadlift', 'push_up')),
    variant_id text NOT NULL CHECK (variant_id IN ('bodyweight', 'barbell')),
    reps integer NOT NULL CHECK (reps BETWEEN 0 AND 1000),
    load_kg double precision,
    load_mode text,
    rpe double precision CHECK (rpe IS NULL OR rpe BETWEEN 1 AND 10),
    set_type text NOT NULL CHECK (set_type IN ('work', 'warmup', 'drop', 'failure')),
    revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
    deleted_at timestamptz,
    UNIQUE (workout_id, position),
    CHECK ((variant_id = 'barbell') = (load_kg IS NOT NULL)),
    CHECK (load_mode IS NULL OR (variant_id = 'barbell' AND load_mode = 'barbell_total')),
    CHECK ((exercise_id IN ('bench_press', 'deadlift') AND variant_id = 'barbell') OR
           (exercise_id = 'push_up' AND variant_id = 'bodyweight') OR exercise_id = 'squat')
);

CREATE INDEX IF NOT EXISTS workout_sets_workout_position
    ON fitness.workout_sets (workout_id, position) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS fitness.training_profiles (
    owner_id text PRIMARY KEY,
    revision bigint NOT NULL CHECK (revision > 0),
    settings jsonb NOT NULL CHECK (jsonb_typeof(settings) = 'object'),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fitness.training_plans (
    id text PRIMARY KEY CHECK (id ~ '^[a-f0-9]{32}$'),
    owner_id text NOT NULL,
    request_key text NOT NULL CHECK (request_key ~ '^[a-f0-9]{32}$'),
    request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
    revision bigint NOT NULL CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,
    UNIQUE (owner_id, request_key)
);

CREATE INDEX IF NOT EXISTS training_plans_owner_created
    ON fitness.training_plans (owner_id, created_at DESC, id) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS fitness.training_plan_revisions (
    plan_id text NOT NULL REFERENCES fitness.training_plans(id) ON DELETE CASCADE,
    revision bigint NOT NULL CHECK (revision > 0),
    definition jsonb NOT NULL CHECK (jsonb_typeof(definition) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (plan_id, revision)
);

CREATE TABLE IF NOT EXISTS fitness.training_plan_executions (
    set_id text PRIMARY KEY REFERENCES fitness.workout_sets(id) ON DELETE CASCADE,
    plan_id text NOT NULL REFERENCES fitness.training_plans(id) ON DELETE CASCADE,
    plan_revision bigint NOT NULL,
    target_position integer NOT NULL CHECK (target_position BETWEEN 1 AND 30),
    linked_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (plan_id, plan_revision)
        REFERENCES fitness.training_plan_revisions(plan_id, revision) ON DELETE CASCADE,
    UNIQUE (plan_id, plan_revision, target_position)
);

INSERT INTO fitness.schema_migration(version) VALUES (1) ON CONFLICT DO NOTHING;
