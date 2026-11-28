ALTER TABLE avatar_hsm.diary_entry ADD COLUMN IF NOT EXISTS occurred_at timestamptz;
UPDATE avatar_hsm.diary_entry SET occurred_at=recorded_at WHERE occurred_at IS NULL;
ALTER TABLE avatar_hsm.diary_entry ALTER COLUMN occurred_at SET DEFAULT now();
ALTER TABLE avatar_hsm.diary_entry ALTER COLUMN occurred_at SET NOT NULL;

CREATE TABLE IF NOT EXISTS avatar_hsm.nutrition_daily_state (
    subject_id text NOT NULL REFERENCES avatar_hsm.subject,
    local_day date NOT NULL,
    meal_count integer NOT NULL,
    calories_kcal double precision,
    protein_g double precision,
    carbs_g double precision,
    fat_g double precision,
    unresolved_meals integer NOT NULL,
    revision bigint NOT NULL DEFAULT 1,
    rebuilt_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(subject_id, local_day)
);
