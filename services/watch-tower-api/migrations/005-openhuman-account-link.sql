CREATE UNIQUE INDEX IF NOT EXISTS identity_link_openhuman_account_idx
    ON watch_tower.identity_link (openhuman_user_id)
    WHERE client_id = 'openhuman-local'
      AND openhuman_user_id IS NOT NULL
      AND revoked_at IS NULL;
