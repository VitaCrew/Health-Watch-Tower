ALTER TABLE avatar_hsm.subject
    DROP CONSTRAINT IF EXISTS subject_id_check;

ALTER TABLE avatar_hsm.dose
    DROP CONSTRAINT IF EXISTS dose_pkey;

ALTER TABLE avatar_hsm.dose
    ADD CONSTRAINT dose_pkey PRIMARY KEY (subject_id, id);

ALTER TABLE avatar_hsm.sleep_session
    DROP CONSTRAINT IF EXISTS sleep_session_pkey;

ALTER TABLE avatar_hsm.sleep_session
    ADD CONSTRAINT sleep_session_pkey PRIMARY KEY (subject_id, id);
