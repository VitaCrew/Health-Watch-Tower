UPDATE avatar_hsm.wearable_fit
SET detail_state = 'pending', detail = NULL
WHERE detail_state <> 'pending';
