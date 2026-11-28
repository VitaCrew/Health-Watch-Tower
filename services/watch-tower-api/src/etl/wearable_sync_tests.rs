use super::*;
use anyhow::Context;

#[test]
fn target_window_covers_adjacent_days_and_caps_at_now() {
    let day = NaiveDate::from_ymd_opt(2026, 11, 20).unwrap();
    let now = day.and_hms_opt(12, 0, 0).unwrap().and_utc();
    let (start, end) = target_window(day, now);
    assert_eq!(
        start,
        day.and_hms_opt(0, 0, 0).unwrap().and_utc() - Duration::days(1)
    );
    assert_eq!(end, now);
}

#[test]
fn single_day_replay_splits_requests_around_midnight() {
    let day = NaiveDate::from_ymd_opt(2026, 11, 20).unwrap();
    let now = day.and_hms_opt(23, 0, 0).unwrap().and_utc();
    let windows = single_day_target_windows(day, now);
    assert_eq!(windows.len(), 3);
    assert!(windows
        .iter()
        .all(|(start, end)| *end - *start <= Duration::days(1)));
}

#[test]
fn only_google_health_target_replay_replaces_the_source_window() {
    assert!(replaces_source_window("google_health", true));
    assert!(!replaces_source_window("google_health", false));
    assert!(!replaces_source_window("sensorbio", true));
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database in WATCH_TOWER_TEST_DATABASE_URL"]
async fn watch_tower_claims_work_and_records_payload_free_run_lifecycle() -> Result<()> {
    let url = std::env::var("WATCH_TOWER_TEST_DATABASE_URL")?;
    let _hsm_pool = avatar_hsm::store::connect(&url).await?;
    let pool = crate::store::connect(&url).await?;
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject(id,timezone) VALUES('synthetic-mvp','UTC')
         ON CONFLICT(id) DO NOTHING",
        &[],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider,state)
         VALUES('synthetic-mvp','oura','synced')
         ON CONFLICT(subject_id,provider) DO UPDATE SET state='synced'",
        &[],
    )
    .await?;
    db.execute(
        "INSERT INTO watch_tower.wearable_etl_run
            (subject_id,run_id,provider,status,started_at)
         VALUES('synthetic-mvp','c64a4746-e082-4445-a24e-0785381cc821','oura','running',
                now()-interval '10 minutes')
         ON CONFLICT(subject_id,run_id) DO UPDATE SET status='running',failure_code=NULL,
             finished_at=NULL,started_at=now()-interval '10 minutes'",
        &[],
    )
    .await?;
    db.execute(
        "INSERT INTO watch_tower.wearable_sync_schedule
            (subject_id,provider,next_sync,backfill_from,backfill_to,job_id,job_until)
         VALUES('synthetic-mvp','oura',now()-interval '1 second',now()-interval '2 days',now(),
                'c64a4746-e082-4445-a24e-0785381cc821',now()-interval '1 second')
         ON CONFLICT(subject_id,provider) DO UPDATE SET
            next_sync=now()-interval '1 second',backfill_from=now()-interval '2 days',
            backfill_to=now(),job_id='c64a4746-e082-4445-a24e-0785381cc821',
            job_until=now()-interval '1 second',throttled_until=NULL",
        &[],
    )
    .await?;
    let replay_day = Utc::now().date_naive() - Duration::days(2);
    db.execute(
        "INSERT INTO watch_tower.wearable_target_day(subject_id,provider,day)
         VALUES('synthetic-mvp','oura',$1)
         ON CONFLICT(subject_id,provider,day) DO UPDATE SET next_attempt=now()",
        &[&replay_day],
    )
    .await?;

    let job = claim(&pool)
        .await?
        .context("Watch Tower did not claim due work")?;
    assert_eq!(job.subject, "synthetic-mvp");
    assert_eq!(job.provider, "oura");
    assert_eq!(
        job.target.as_ref().map(|target| target.day),
        Some(replay_day)
    );
    let abandoned = db
        .query_one(
            "SELECT status,failure_code FROM watch_tower.wearable_etl_run
             WHERE subject_id=$1 AND run_id='c64a4746-e082-4445-a24e-0785381cc821'::uuid",
            &[&job.subject],
        )
        .await?;
    assert_eq!(abandoned.get::<_, String>(0), "failed");
    assert_eq!(
        abandoned.get::<_, String>(1),
        "collection_or_state_owner_failed"
    );
    assert!(db
        .query_one(
            "SELECT job_id::text=$1 FROM watch_tower.wearable_sync_schedule
             WHERE subject_id='synthetic-mvp' AND provider='oura'",
            &[&job.id],
        )
        .await?
        .get::<_, bool>(0));

    begin_run(&pool, &job).await?;
    assert_eq!(
        db.query_one(
            "SELECT status FROM watch_tower.wearable_etl_run
             WHERE subject_id=$1 AND run_id=$2::text::uuid",
            &[&job.subject, &job.id],
        )
        .await?
        .get::<_, String>(0),
        "running"
    );
    finish_run(
        &pool,
        &job,
        Some(&anyhow::anyhow!("private provider response")),
    )
    .await?;
    let failed = db
        .query_one(
            "SELECT status,failure_code,finished_at IS NOT NULL
             FROM watch_tower.wearable_etl_run WHERE subject_id=$1 AND run_id=$2::text::uuid",
            &[&job.subject, &job.id],
        )
        .await?;
    assert_eq!(failed.get::<_, String>(0), "failed");
    assert_eq!(
        failed.get::<_, String>(1),
        "collection_or_state_owner_failed"
    );
    assert!(failed.get::<_, bool>(2));
    assert!(begin_run(&pool, &job).await.is_err());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM information_schema.columns
             WHERE table_schema='watch_tower' AND table_name='wearable_etl_run'
               AND column_name IN ('payload','provider_response','error_message')",
            &[],
        )
        .await?
        .get::<_, i64>(0),
        0
    );
    release_job(&pool, &job, None, true).await?;
    let successful = claim(&pool)
        .await?
        .context("Watch Tower did not claim the retry")?;
    begin_run(&pool, &successful).await?;
    release_job(&pool, &successful, None, false).await?;
    let completed = db
        .query_one(
            "SELECT r.status,s.job_id IS NULL FROM watch_tower.wearable_etl_run r
             JOIN watch_tower.wearable_sync_schedule s
               ON s.subject_id=r.subject_id AND s.provider=r.provider
             WHERE r.subject_id=$1 AND r.run_id=$2::text::uuid",
            &[&successful.subject, &successful.id],
        )
        .await?;
    assert_eq!(completed.get::<_, String>(0), "succeeded");
    assert!(completed.get::<_, bool>(1));
    db.execute(
        "DELETE FROM watch_tower.wearable_etl_run WHERE subject_id=$1
         AND run_id IN ($2::text::uuid,$3::text::uuid,$4::text::uuid)",
        &[
            &job.subject,
            &job.id,
            &successful.id,
            &"c64a4746-e082-4445-a24e-0785381cc821",
        ],
    )
    .await?;
    Ok(())
}
