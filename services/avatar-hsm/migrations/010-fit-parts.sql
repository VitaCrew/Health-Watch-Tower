ALTER TABLE avatar_hsm.wearable_fit ADD COLUMN part smallint NOT NULL DEFAULT 0
    CHECK (part BETWEEN 0 AND 16);
ALTER TABLE avatar_hsm.wearable_fit DROP CONSTRAINT wearable_fit_pkey;
ALTER TABLE avatar_hsm.wearable_fit ADD PRIMARY KEY (subject_id, provider, activity_id, part);

UPDATE avatar_hsm.wearable_fit_job
SET status='pending', attempts=0, next_attempt=now(), job_id=NULL, leased_until=NULL
WHERE provider='coros' AND status='stored';
