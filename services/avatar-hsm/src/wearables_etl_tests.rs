use super::*;

#[test]
fn snapshot_fingerprint_ignores_lease_and_window_but_keeps_data_changes() -> Result<()> {
    let snapshot = |lease: &str, start: &str, end: &str| -> Result<Snapshot> {
        Ok(serde_json::from_value(json!({
            "provider":"oura",
            "lease":lease,
            "start":start,
            "end":end,
            "grants":["steps"],
            "records":[]
        }))?)
    };
    let first = snapshot("lease-a", "2026-11-19T00:00:00Z", "2026-11-21T00:00:00Z")?;
    let replay = snapshot("lease-b", "2026-11-20T00:00:00Z", "2026-11-22T00:00:00Z")?;
    assert_eq!(
        snapshot_content_sha256(&first)?,
        snapshot_content_sha256(&replay)?,
        "lease rotation and query-window changes must not create a new ETL event identity"
    );

    let mut changed = snapshot("lease-c", "2026-11-20T00:00:00Z", "2026-11-22T00:00:00Z")?;
    changed.grants.push("workout".into());
    assert_ne!(
        snapshot_content_sha256(&first)?,
        snapshot_content_sha256(&changed)?,
        "a changed data contract must receive a different content identity"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn identical_cloud_content_reuses_receipt_and_tracks_latest_run() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let subject = format!("etl-run-replay-{suffix}");
    let first_run = "c64a4746-e082-4445-a24e-0785381cc821";
    let second_run = "bde64e5f-5f20-4cf8-b22d-33b505d3864d";
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject(id,timezone) VALUES($1,'UTC')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider,state)
         VALUES($1,'oura','synced')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,backfill_from)
         VALUES($1,'oura','2026-11-20T00:00:00Z')",
        &[&subject],
    )
    .await?;

    for (index, run_id) in [first_run, second_run].into_iter().enumerate() {
        db.execute(
            "UPDATE watch_tower.wearable_sync_schedule
             SET job_id=$3::text::uuid,job_until=now()+interval '5 minutes'
             WHERE subject_id=$1 AND provider=$2",
            &[&subject, &"oura", &run_id],
        )
        .await?;
        let lease = issue_job_lease(&pool, &subject, "oura", run_id).await?;
        let snapshot: Snapshot = serde_json::from_value(json!({
            "provider":"oura",
            "lease":lease["lease"],
            "start":"2026-11-20T00:00:00Z",
            "end":"2026-11-21T00:00:00Z",
            "grants":[],
            "records":[]
        }))?;
        crate::state_owner::apply_normalized_snapshot(
            &pool,
            &subject,
            "oura",
            snapshot,
            false,
            Some(run_id),
        )
        .await?;
        if index == 0 {
            db.execute(
                "UPDATE avatar_hsm.watch_tower_record_outbox SET delivered_at=now()
                 WHERE subject_id=$1 AND provider='oura'",
                &[&subject],
            )
            .await?;
        }
    }

    let queued = db
        .query_one(
            "SELECT count(*),(array_agg(run_id::text))[1],bool_or(delivered_at IS NULL)
             FROM avatar_hsm.watch_tower_record_outbox
             WHERE subject_id=$1 AND provider='oura'",
            &[&subject],
        )
        .await?;
    assert_eq!(queued.get::<_, i64>(0), 1);
    assert_eq!(queued.get::<_, String>(1), second_run);
    assert!(queued.get::<_, bool>(2));

    db.execute(
        "DELETE FROM avatar_hsm.watch_tower_record_outbox WHERE subject_id=$1",
        &[&subject],
    )
    .await?;
    db.execute(
        "DELETE FROM avatar_hsm.device_connection WHERE subject_id=$1",
        &[&subject],
    )
    .await?;
    db.execute("DELETE FROM avatar_hsm.subject WHERE id=$1", &[&subject])
        .await?;
    Ok(())
}
