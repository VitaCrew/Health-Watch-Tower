ALTER TABLE avatar_hsm.google_workout_tcx ADD COLUMN route_points jsonb;
DROP INDEX avatar_hsm.google_workout_tcx_due;
CREATE INDEX google_workout_tcx_due ON avatar_hsm.google_workout_tcx (next_attempt)
    WHERE route_points IS NULL AND attempts < 6;
