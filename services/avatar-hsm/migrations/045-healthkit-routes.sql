ALTER TABLE avatar_hsm.wearable_workout_route
    DROP CONSTRAINT wearable_workout_route_provider_check;
ALTER TABLE avatar_hsm.wearable_workout_route
    ADD CHECK (provider IN ('samsung_health', 'health_connect', 'healthkit'));
