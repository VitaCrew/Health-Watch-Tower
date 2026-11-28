use super::*;

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database in WATCH_TOWER_TEST_DATABASE_URL"]
async fn wearable_sync_and_replay_queues_are_migrated_to_watch_tower() -> anyhow::Result<()> {
    let url = std::env::var("WATCH_TOWER_TEST_DATABASE_URL")?;

    // Exercise the Watch Tower-first order too: it records its idempotent
    // migration before HSM's historical migrations create the old tables.
    let _policy_pool = store::connect(&url).await?;
    let hsm_pool = avatar_hsm::store::connect(&url).await?;
    let hsm_db = hsm_pool.get().await?;
    let subject = "google-tcx-queue-migration-fixture";
    hsm_db
        .execute(
            "INSERT INTO avatar_hsm.subject(id,timezone) VALUES($1,'UTC')",
            &[&subject],
        )
        .await?;
    hsm_db
        .execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider,state)
             VALUES($1,'google_health','synced')",
            &[&subject],
        )
        .await?;
    hsm_db
        .execute(
            "INSERT INTO avatar_hsm.wearable_record
             (subject_id,provider,kind,source,source_id,started_at,ended_at,modified_at,payload)
             VALUES($1,'google_health','workout','Pixel Watch','123',now(),now(),now(),'{}')",
            &[&subject],
        )
        .await?;
    hsm_db
        .execute(
            "INSERT INTO avatar_hsm.google_workout_tcx(subject_id,source,source_id,fingerprint)
             VALUES($1,'Pixel Watch','123',decode(repeat('ab',32),'hex'))",
            &[&subject],
        )
        .await?;
    hsm_db
        .execute(
            "UPDATE avatar_hsm.google_workout_tcx SET attempts=2,next_attempt=now()+interval '3 minutes',
             job_id='90fd01e0-e5e6-4f73-b9d4-f7c0c6d512ce',leased_until=now()+interval '2 minutes'
             WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
    let policy_pool = store::connect(&url).await?;
    let db = policy_pool.get().await?;
    assert!(db
        .query_one(
            "SELECT to_regclass('watch_tower.wearable_etl_run') IS NOT NULL",
            &[],
        )
        .await?
        .get::<_, bool>(0));
    let row = db
        .query_one(
            "SELECT to_regclass('watch_tower.wearable_sync_schedule') IS NOT NULL,
                    to_regclass('watch_tower.wearable_target_day') IS NOT NULL,
                    to_regclass('avatar_hsm.wearable_sync_schedule') IS NULL,
                    to_regclass('avatar_hsm.wearable_target_day') IS NULL,
                    to_regclass('watch_tower.wearable_fit_job') IS NOT NULL,
                    to_regclass('watch_tower.wearable_webhook_document') IS NOT NULL,
                    to_regclass('watch_tower.wearable_webhook_receipt') IS NOT NULL,
                    to_regclass('watch_tower.garmin_backfill') IS NOT NULL,
                    to_regclass('watch_tower.garmin_pull_job') IS NOT NULL,
                    to_regclass('watch_tower.withings_notify_cleanup') IS NOT NULL,
                    to_regclass('watch_tower.google_workout_tcx_queue') IS NOT NULL,
                    to_regclass('avatar_hsm.wearable_fit_job') IS NULL,
                    to_regclass('avatar_hsm.wearable_webhook_document') IS NULL,
                    to_regclass('avatar_hsm.wearable_webhook_receipt') IS NULL,
                    to_regclass('avatar_hsm.garmin_backfill') IS NULL,
                    to_regclass('avatar_hsm.garmin_pull_job') IS NULL,
                    to_regclass('avatar_hsm.withings_notify_cleanup') IS NULL,
                    NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='avatar_hsm' AND table_name='google_workout_tcx' AND column_name IN ('fingerprint','attempts','next_attempt','job_id','leased_until'))",
            &[],
        )
        .await?;
    assert!(row.get::<_, bool>(0));
    assert!(row.get::<_, bool>(1));
    assert!(row.get::<_, bool>(2));
    assert!(row.get::<_, bool>(3));
    for index in 4..=10 {
        assert!(row.get::<_, bool>(index));
    }
    for index in 11..=16 {
        assert!(row.get::<_, bool>(index));
    }
    assert!(row.get::<_, bool>(17));
    let moved = db
        .query_one(
            "SELECT q.attempts,q.job_id::text,q.leased_until>now(),
                    (SELECT count(*) FROM avatar_hsm.google_workout_tcx t WHERE t.subject_id=$1),
                    NOT EXISTS(SELECT 1 FROM information_schema.columns
                               WHERE table_schema='avatar_hsm' AND table_name='google_workout_tcx'
                               AND column_name IN ('fingerprint','attempts','next_attempt','job_id','leased_until'))
             FROM watch_tower.google_workout_tcx_queue q
             WHERE q.subject_id=$1 AND q.source='Pixel Watch' AND q.source_id='123'",
            &[&subject],
        )
        .await?;
    assert_eq!(moved.get::<_, i32>(0), 2);
    assert_eq!(
        moved.get::<_, String>(1),
        "90fd01e0-e5e6-4f73-b9d4-f7c0c6d512ce"
    );
    assert!(moved.get::<_, bool>(2));
    assert_eq!(moved.get::<_, i64>(3), 1);
    assert!(moved.get::<_, bool>(4));
    let lease_columns = db.query_one(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='watch_tower' AND table_name='withings_notify_cleanup' AND column_name='job_id'),
                EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='watch_tower' AND table_name='withings_notify_cleanup' AND column_name='leased_until'),
                EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='watch_tower' AND table_name='withings_notify_cleanup' AND column_name='attempts')",
        &[],
    ).await?;
    assert!(lease_columns.get::<_, bool>(0));
    assert!(lease_columns.get::<_, bool>(1));
    assert!(lease_columns.get::<_, bool>(2));
    Ok(())
}
