use super::*;

struct EnvRestore(Option<std::ffi::OsString>);

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match self.0.take() {
            Some(value) => std::env::set_var("AVATAR_HEVY_API_KEY", value),
            None => std::env::remove_var("AVATAR_HEVY_API_KEY"),
        }
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL in AVATAR_HSM_TEST_DATABASE_URL"]
async fn manual_hevy_sync_enqueues_for_watch_tower_instead_of_fetching_inline() -> Result<()> {
    let original = std::env::var_os("AVATAR_HEVY_API_KEY");
    std::env::set_var("AVATAR_HEVY_API_KEY", "test-only-key");
    let _restore = EnvRestore(original);
    let url = std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?;
    let pool = crate::store::connect(&url).await?;
    pool.get()
        .await?
        .execute(
            "INSERT INTO avatar_hsm.subject(id,timezone) VALUES('synthetic-mvp','UTC')
             ON CONFLICT(id) DO NOTHING",
            &[],
        )
        .await?;
    let app = crate::mcp_app(
        pool.clone(),
        "read-test-token".into(),
        "control-test-token".into(),
    )?;
    let result = crate::mcp::call(
        &app,
        crate::mcp::Surface::Control,
        "sync_cloud_device",
        &json!({"provider":"hevy"}),
    )
    .await?;
    assert_eq!(result["state"], "queued");
    let row = pool
        .get()
        .await?
        .query_one(
            "SELECT backfill_from < backfill_to,
                    next_sync <= now(),
                    job_id IS NULL
             FROM watch_tower.wearable_sync_schedule
             WHERE subject_id='synthetic-mvp' AND provider='hevy'",
            &[],
        )
        .await?;
    assert!(row.get::<_, bool>(0));
    assert!(row.get::<_, bool>(1));
    assert!(row.get::<_, bool>(2));
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn coros_targeted_refetch_updates_an_older_day_and_retries_throttling() -> Result<()> {
    use axum::{extract::Query, http::StatusCode, response::IntoResponse, routing::get, Json};
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicU32, Ordering},
            Arc,
        },
    };

    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let subject = "coros-scheduled-revision-fixture";
    let day = (Utc::now() - Duration::days(40)).date_naive();
    let midnight = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let old_start = midnight - Duration::days(2);
    let old_end = midnight + Duration::days(5);
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES ($1,'UTC')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'coros')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,next_sync,backfill_from,backfill_to)
         VALUES($1,'coros','1970-01-01',$2,$3)", &[&subject,&old_start,&old_end],
    ).await?;

    let steps = Arc::new(AtomicU32::new(1100));
    let upstream = {
        let steps = Arc::clone(&steps);
        let day_number: u32 = day.format("%Y%m%d").to_string().parse()?;
        axum::Router::new()
            .route("/v2/coros/sport/list", get(|| async { Json(json!({"result":"0000","data":[]})) }))
            .route("/coros/daily/query", get(move |Query(query): Query<HashMap<String, String>>| {
                let steps = steps.load(Ordering::SeqCst);
                async move {
                    let day_text = day_number.to_string();
                    if query["openId"] != "coros-fixture-account" || query["token"] != "synthetic"
                        || query["startDate"] > day_text || query["endDate"] < day_text
                    {
                        return StatusCode::BAD_REQUEST.into_response();
                    }
                    if steps == 0 {
                        return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "600")]).into_response();
                    }
                    Json(json!({"result":"0000","data":{"dailyList":[{"happenDay":day_number,"step":steps}]}})).into_response()
                }
            }))
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let api = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await });
    let client = cloud_wearables::http_client()?;
    let fetch = |start, end, lease: Value| {
        let (client, api) = (&client, &api);
        async move {
            let (records, _) = cloud_wearables::manufacturers::coros_at(
                client,
                "synthetic",
                "coros-fixture-account",
                start,
                end,
                api,
            )
            .await?;
            Ok(serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":end,"grants":["workout","sleep","metric","steps"],"records":records
            }))?)
        }
    };
    let daily_steps = || async {
        let daily = wearables::daily_for(
            &pool,
            subject,
            wearables::DailyQuery {
                start_day: day,
                end_day: day,
                kind: Some("steps".into()),
                source: None,
                provider: Some("coros".into()),
            },
        )
        .await?;
        Ok::<_, anyhow::Error>(daily["records"][0]["stats"]["total"].as_u64())
    };

    let initial = claim(&pool).await?.context("missing COROS backfill")?;
    assert_eq!(initial.subject, subject);
    assert!(initial.target.is_none());
    run_batches(&pool, &initial, fetch).await?;
    assert_eq!(daily_steps().await?, Some(1100));

    db.execute(
        "INSERT INTO watch_tower.wearable_target_day(subject_id,provider,day) VALUES($1,'coros',$2)",
        &[&subject, &day],
    )
    .await?;
    let wake = || async {
        db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET next_sync='1970-01-01',throttled_until=NULL
         WHERE subject_id=$1 AND provider='coros'", &[&subject],
        ).await
    };
    wake().await?;
    let targeted = claim(&pool).await?.context("missing COROS re-fetch")?;
    assert_eq!(targeted.subject, subject);
    assert_eq!(targeted.target.as_ref().map(|target| target.day), Some(day));
    steps.store(0, Ordering::SeqCst);
    let error = run_batches(&pool, &targeted, fetch).await.unwrap_err();
    assert!(error.is::<cloud_wearables::ProviderThrottled>());
    defer_job(&pool, &targeted, &error).await?;
    assert_eq!(daily_steps().await?, Some(1100));
    let throttled = db.query_one(
        "SELECT throttled_until > now()+interval '9 minutes' FROM watch_tower.wearable_sync_schedule WHERE subject_id=$1 AND provider='coros'",
        &[&subject],
    ).await?.get::<_, bool>(0);
    assert!(throttled);

    steps.store(2400, Ordering::SeqCst);
    wake().await?;
    let retry = claim(&pool).await?.context("missing COROS retry")?;
    assert_eq!(retry.target.as_ref().map(|target| target.day), Some(day));
    run_batches(&pool, &retry, fetch).await?;
    assert_eq!(daily_steps().await?, Some(2400));
    assert!(db
        .query_opt(
            "SELECT 1 FROM watch_tower.wearable_target_day WHERE subject_id=$1 AND provider='coros'",
            &[&subject]
        )
        .await?
        .is_none());
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn rate_limit_survives_wake_and_restart_without_moving_backfill_cursor() -> Result<()> {
    let url = std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?;
    let pool = crate::store::connect(&url).await?;
    let db = pool.get().await?;
    let subject = "wearable-throttle-fixture";
    let provider = Provider::Oura;
    let slug = provider.slug();
    let end = Utc::now() - Duration::days(1);
    let start = end - Duration::days(30);
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES ($1,'UTC')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES ($1,$2)",
        &[&subject, &slug],
    )
    .await?;
    db.execute(
        "INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,backfill_from,backfill_to)
         VALUES ($1,$2,$3,$4)", &[&subject,&slug,&start,&end],
    ).await?;
    let day = end.date_naive();
    db.execute(
        "INSERT INTO watch_tower.wearable_target_day(subject_id,provider,day) VALUES($1,$2,$3)",
        &[&subject, &slug, &day],
    )
    .await?;
    let job = claim(&pool).await?.context("missing targeted sync")?;
    assert_eq!(job.subject, subject);
    let revision = job
        .target
        .as_ref()
        .context("missing target day")?
        .revision
        .clone();
    let upstream = axum::Router::new().route(
        "/",
        axum::routing::get(|| async {
            (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", "900")],
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await });
    let client = cloud_wearables::http_client()?;
    let before = Utc::now();
    let error = run_batches(&pool, &job, async |_, _, _| {
        cloud_wearables::body(client.get(&endpoint).send().await?).await?;
        anyhow::bail!("fixture must be rate limited")
    })
    .await
    .expect_err("rate limited sync must remain queued");
    defer_job(&pool, &job, &error).await?;
    let limited = status(&pool, subject, provider).await?;
    assert_eq!(limited["schedule"]["last_error"], "provider_rate_limited");
    assert_eq!(limited["schedule"]["failures"], 0);
    let boundary: DateTime<Utc> = limited["schedule"]["backfill_remaining_to"]
        .as_str()
        .context("missing backfill cursor")?
        .parse()?;
    assert_eq!(boundary, job.upper);
    let due: DateTime<Utc> = limited["schedule"]["next_sync"]
        .as_str()
        .context("missing retry time")?
        .parse()?;
    assert!(due - before >= Duration::seconds(899));
    assert_eq!(
        limited["schedule"]["next_sync"],
        limited["schedule"]["throttled_until"]
    );
    assert!(request_live_refresh(&pool, subject, provider).await?);
    let restarted = crate::store::connect(&url).await?;
    assert!(claim(&restarted).await?.is_none());
    db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET throttled_until=now()-interval '1 second'
                WHERE subject_id=$1 AND provider=$2",
        &[&subject, &slug],
    )
    .await?;
    let recovered = claim(&restarted).await?.context("missing deferred job")?;
    assert!(recovered.live_first);
    assert_eq!(
        recovered.target.as_ref().context("lost target")?.revision,
        revision
    );
    assert_eq!(recovered.upper, job.upper);
    finish_job(&restarted, &recovered, None).await?;
    let resumed = status(&pool, subject, provider).await?;
    assert!(resumed["schedule"]["throttled_until"].is_null());
    let polling = claim(&restarted).await?.context("pending backfill job")?;
    db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET throttled_until=now()+interval '15 minutes'
         WHERE subject_id=$1 AND provider=$2",
        &[&subject, &slug],
    )
    .await?;
    finish_job(&restarted, &polling, None).await?;
    let limited_again = status(&pool, subject, provider).await?;
    assert_eq!(
        limited_again["schedule"]["throttled_until"],
        limited_again["schedule"]["next_sync"]
    );
    assert_eq!(
        limited_again["schedule"]["last_error"],
        "provider_rate_limited"
    );
    assert!(claim(&restarted).await?.is_none());
    wearables::revoke_for(&pool, subject, slug).await?;
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and a synthetic Garmin Personal key"]
async fn garmin_personal_backfill_resume_and_refresh_share_a_queue() -> Result<()> {
    let url = std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?;
    let pool = crate::store::connect(&url).await?;
    let provider = Provider::GarminPersonal;
    let slug = provider.slug();
    let read_key = "garmin-personal-schedule-read-01234567890123";
    let control = "garmin-personal-schedule-control-01234567890123";
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let base = format!("http://{}", listener.local_addr()?);
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: read_key.into(),
        control_token: control.into(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let client = cloud_wearables::http_client()?;
    let end: DateTime<Utc> = "2026-04-30T18:00:00Z".parse()?;
    let start = end - Duration::days(30);
    let call = |surface: &str, key: &str, name: &str, args: Value| {
        let request = client.post(format!("{base}/mcp/{surface}")).bearer_auth(key)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":args}}));
        async move {
            let body: Value = request.send().await?.error_for_status()?.json().await?;
            ensure!(
                body["error"].is_null() && body["result"]["isError"] != true,
                "unexpected MCP result: {body}"
            );
            Ok::<Value, anyhow::Error>(body["result"]["structuredContent"].clone())
        }
    };
    let scheduled = call(
        "control",
        control,
        "schedule_cloud_sync",
        json!({"provider":slug,"start":start,"end":end}),
    )
    .await?;
    assert_eq!(scheduled["state"], "queued");
    let old = claim(&pool)
        .await?
        .context("scheduled Garmin job missing")?;
    assert_eq!(old.provider.slug(), slug);
    let db = pool.get().await?;
    db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET job_until=now()+interval '1 second'
        WHERE subject_id=$1 AND provider=$2",
        &[&SUBJECT, &slug],
    )
    .await?;
    let lease = wearables::issue_job_lease(&pool, SUBJECT, slug, &old.id).await?;
    let expiry = db.query_one("SELECT d.lease_expires_at=j.job_until
        FROM avatar_hsm.device_connection d JOIN watch_tower.wearable_sync_schedule j USING(subject_id,provider)
        WHERE d.subject_id=$1 AND d.provider=$2", &[&SUBJECT,&slug]).await?;
    assert!(expiry.get::<_, bool>(0));
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let restarted_pool = crate::store::connect(&url).await?;
    let recovered = claim(&restarted_pool)
        .await?
        .context("expired Garmin job not recovered")?;
    assert_ne!(recovered.id, old.id);
    assert_eq!(recovered.upper, old.upper);
    let snapshot = |from: DateTime<Utc>, until: DateTime<Utc>, lease: Value| {
        serde_json::from_value::<wearables::Snapshot>(json!({
            "lease":lease["lease"],"start":from,"end":until,"grants":["metric"],
            "records":[{"kind":"metric","source":"Garmin Connect","source_id":format!("cycle:{}",from.date_naive()),
                "started_at":from,"ended_at":from,"modified_at":until,
                "payload":{"metric":"menstrual_cycle","provider_data":{"predictedCycleLength":28}}}]
        })).map_err(anyhow::Error::from)
    };
    assert!(wearables::store_snapshot(
        &pool,
        SUBJECT,
        slug,
        snapshot(end - Duration::days(7), end, lease)?,
        false
    )
    .await
    .is_err());
    let active_lease =
        wearables::issue_job_lease(&restarted_pool, SUBJECT, slug, &recovered.id).await?;
    let refresh = call(
        "control",
        control,
        "sync_cloud_device",
        json!({"provider":slug}),
    )
    .await?;
    assert_eq!(refresh["state"], "queued");
    let unaffected = db.query_one(
        "SELECT d.lease_hash=sha256(convert_to($3,'UTF8')), j.job_id::text
         FROM avatar_hsm.device_connection d JOIN watch_tower.wearable_sync_schedule j USING(subject_id,provider)
         WHERE d.subject_id=$1 AND d.provider=$2",
        &[&SUBJECT, &slug, &active_lease["lease"].as_str().unwrap()],
    ).await?;
    assert!(unaffected.get::<_, bool>(0));
    assert_eq!(unaffected.get::<_, String>(1), recovered.id);
    assert!(
        run_batches(&restarted_pool, &recovered, async |_, _, _| anyhow::bail!(
            "synthetic rate limit"
        ))
        .await
        .is_err()
    );
    retry_job(&restarted_pool, &recovered).await?;
    let state = status(&pool, SUBJECT, provider).await?;
    assert_eq!(state["schedule"]["failures"], 1);
    assert_eq!(
        state["schedule"]["backfill_remaining_to"],
        json!(end.to_rfc3339())
    );
    let refresh = call(
        "control",
        control,
        "sync_cloud_device",
        json!({"provider":slug}),
    )
    .await?;
    assert_eq!(refresh["state"], "queued");
    let live = claim(&restarted_pool)
        .await?
        .context("requested live refresh missing")?;
    assert!(live.live_first);
    let captured = std::cell::RefCell::new(Vec::new());
    let fetch = |from, until, lease| {
        captured.borrow_mut().push((from, until));
        std::future::ready(snapshot(from, until, lease))
    };
    run_batches(&restarted_pool, &live, fetch).await?;
    assert!(captured.borrow()[0].1 > end);
    while let Some(job) = claim(&restarted_pool).await? {
        assert_eq!(job.subject, SUBJECT);
        assert_eq!(job.provider.slug(), slug);
        run_batches(&restarted_pool, &job, fetch).await?;
    }
    let spans = captured.into_inner();
    assert!(spans.len() > 2);
    assert!(spans[1..]
        .iter()
        .all(|(from, until)| *until - *from <= Duration::days(7)));
    assert_eq!(spans.last().unwrap().0, start);
    let state = call(
        "read",
        read_key,
        "cloud_device_status",
        json!({"provider":slug}),
    )
    .await?;
    assert_eq!(state["schedule"]["backfill_remaining_to"], Value::Null);
    assert_eq!(state["schedule"]["failures"], 0);
    let next: DateTime<Utc> = state["schedule"]["next_sync"]
        .as_str()
        .context("no recurring time")?
        .parse()?;
    assert!(next > Utc::now() + Duration::minutes(50));
    let rows = call(
        "read",
        read_key,
        "wearable_samples",
        json!({
            "kind":"metric","metric":"menstrual_cycle","provider":slug,"start":start,"end":end
        }),
    )
    .await?;
    assert!(rows["records"]
        .as_array()
        .is_some_and(|rows| rows.len() >= 4));
    wearables::revoke(&pool, slug).await?;
    assert!(status(&pool, SUBJECT, provider).await?["schedule"].is_null());
    server.abort();
    Ok(())
}

#[test]
fn one_day_replay_covers_both_midnights_with_bounded_requests() -> Result<()> {
    let midnight: DateTime<Utc> = "2026-04-12T00:00:00Z".parse()?;
    let now = midnight + Duration::days(2);
    let windows = single_day_target_windows(midnight.date_naive(), now);
    assert_eq!(windows.len(), 3);
    assert!(windows
        .iter()
        .all(|(from, until)| *until - *from == Duration::days(1)));
    for end in [
        midnight + Duration::hours(6),
        midnight + Duration::hours(30),
    ] {
        let overnight = end - Duration::hours(8);
        assert!(windows
            .iter()
            .any(|(from, until)| overnight >= *from && end < *until));
    }
    let current = midnight + Duration::hours(4);
    assert!(single_day_target_windows(midnight.date_naive(), current)
        .iter()
        .all(|(from, until)| from < until && *until <= current));
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and a synthetic Withings webhook path key"]
async fn withings_callback_queues_the_old_day_without_moving_backfill() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let key = std::env::var("AVATAR_WITHINGS_WEBHOOK_PATH_KEY")?;
    let subject = "withings-target-day-fixture";
    let owner = "991234";
    let provider = Provider::Withings;
    let slug = provider.slug();
    let end = Utc::now() - Duration::days(1);
    let start = end - Duration::days(14);
    let day = (end - Duration::days(40)).date_naive();
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.wearable_profile VALUES
        ($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,$2)",
        &[&subject, &slug],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection
        (subject_id,provider,sealed_tokens,provider_account_hash) VALUES ($1,$2,''::bytea,sha256(convert_to($3,'UTF8')))",
        &[&subject,&slug,&owner]).await?;
    drop(db);
    schedule(
        &pool,
        subject,
        ScheduleRequest {
            provider,
            start: Some(start),
            end: Some(end),
        },
    )
    .await?;
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let callback = |key: String, owner: &str, category: &str| {
        let app = app.clone();
        let mut fields = std::collections::BTreeMap::from([
            ("userid".into(), owner.into()),
            ("appli".into(), category.into()),
        ]);
        if category == "63" {
            let at = day.and_hms_opt(8, 0, 0).unwrap().and_utc().timestamp();
            fields.insert("startdate".into(), at.to_string());
            fields.insert("enddate".into(), (at + 60).to_string());
        } else {
            fields.insert("date".into(), day.to_string());
        }
        async move {
            crate::wearable_webhooks::withings_event(
                axum::extract::State(app),
                axum::extract::Path(key),
                axum::Form(fields),
            )
            .await
        }
    };
    assert_eq!(
        callback("invalid-key".into(), owner, "63").await,
        axum::http::StatusCode::NOT_FOUND
    );
    assert_eq!(
        callback(key.clone(), "991235", "63").await,
        axum::http::StatusCode::OK
    );
    let initial = claim(&pool).await?.context("missing backfill")?;
    assert_eq!(initial.subject, subject);
    assert!(initial.target.is_none());
    assert_eq!(
        callback(key.clone(), owner, "63").await,
        axum::http::StatusCode::OK
    );
    finish_job(&pool, &initial, None).await?;

    let job = claim(&pool).await?.context("missing targeted job")?;
    assert_eq!(job.subject, subject);
    let target = job.target.as_ref().context("missing target day")?;
    assert_eq!(target.day, day);
    let (lower, upper) = target_window(target.day, Utc::now());
    assert!(upper < start);
    assert_eq!(upper - lower, Duration::days(3));
    assert_eq!(
        callback(key.clone(), owner, "63").await,
        axum::http::StatusCode::OK
    );
    assert_eq!(callback(key, owner, "44").await, axum::http::StatusCode::OK);
    finish_job(&pool, &job, None).await?;

    let following = claim(&pool).await?.context("missing updated target")?;
    let updated = following.target.as_ref().context("new delivery lost")?;
    assert_eq!(updated.day, day);
    assert_ne!(updated.revision, target.revision);
    retry_job(&pool, &following).await?;
    let state = status(&pool, subject, provider).await?;
    assert_eq!(state["schedule"]["last_error"], "target_day_unavailable");
    assert_eq!(state["schedule"]["failures"], 0);
    let resumed = claim(&pool).await?.context("backfill did not resume")?;
    assert_eq!(resumed.subject, subject);
    assert!(resumed.target.is_none());
    assert_eq!(resumed.upper.timestamp_micros(), end.timestamp_micros());
    finish_job(&pool, &resumed, Some(start)).await?;

    let db = pool.get().await?;
    assert_eq!(
        db.execute(
            "UPDATE watch_tower.wearable_target_day SET next_attempt=now()
         WHERE subject_id=$1 AND provider=$2 AND day=$3 AND failures=1 AND next_attempt>now()",
            &[&subject, &slug, &day],
        )
        .await?,
        1
    );
    db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET next_sync=now()
         WHERE subject_id=$1 AND provider=$2",
        &[&subject, &slug],
    )
    .await?;
    drop(db);
    let retry = claim(&pool)
        .await?
        .context("missing retry after cooldown")?;
    assert_eq!(
        retry.target.as_ref().context("wrong retry job")?.revision,
        updated.revision
    );
    finish_job(&pool, &retry, None).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn webhook_live_sync_survives_crash_retry_and_a_second_delivery() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "webhook-live-sync-retry-fixture";
    let owner = "fixture-live-sync-owner";
    let provider = Provider::Whoop;
    let slug = provider.slug();
    let end = Utc::now() - Duration::days(1);
    let start = end - Duration::days(12);
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.wearable_profile VALUES
        ($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,$2)",
        &[&subject, &slug],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection
        (subject_id,provider,sealed_tokens,provider_account_hash) VALUES ($1,$2,''::bytea,sha256(convert_to($3,'UTF8')))",
        &[&subject,&slug,&owner]).await?;
    drop(db);
    let request = || ScheduleRequest {
        provider,
        start: Some(start),
        end: Some(end),
    };
    schedule(&pool, subject, request()).await?;
    crate::wearable_webhooks::nudge(&pool, slug, owner, b"first signed delivery").await?;

    let first = claim(&pool).await?.context("missing first live job")?;
    assert_eq!(first.subject, subject);
    assert!(first.live_first);
    let expire =
        || async {
            let updated = pool.get().await?.execute(
            "UPDATE watch_tower.wearable_sync_schedule SET job_until=now()-interval '1 second'
             WHERE subject_id=$1 AND provider=$2", &[&subject,&slug]
            ).await?;
            Ok::<_, anyhow::Error>(updated)
        };
    expire().await?;
    let restarted = claim(&pool).await?.context("missing expired live job")?;
    assert_eq!(restarted.subject, subject);
    assert!(restarted.live_first);
    assert_ne!(first.id, restarted.id);
    assert!(finish_job(&pool, &first, None).await.is_err());

    retry_job(&pool, &restarted).await?;
    let db = pool.get().await?;
    let retry = db.query_one(
        "SELECT wake_requested,claimed_wake,next_sync>now(),backfill_to FROM watch_tower.wearable_sync_schedule
         WHERE subject_id=$1 AND provider=$2", &[&subject,&slug]
    ).await?;
    assert!(retry.get::<_, bool>(0));
    assert!(!retry.get::<_, bool>(1));
    assert!(retry.get::<_, bool>(2));
    assert_eq!(
        retry.get::<_, DateTime<Utc>>(3).timestamp_micros(),
        end.timestamp_micros()
    );
    db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET next_sync=now()-interval '1 second'
        WHERE subject_id=$1 AND provider=$2",
        &[&subject, &slug],
    )
    .await?;
    drop(db);
    let retried = claim(&pool).await?.context("missing live retry")?;
    assert_eq!(retried.subject, subject);
    assert!(retried.live_first);
    expire().await?;
    schedule(&pool, subject, request()).await?;
    let rescheduled = claim(&pool)
        .await?
        .context("missing rescheduled live job")?;
    assert_eq!(rescheduled.subject, subject);
    assert!(rescheduled.live_first);
    crate::wearable_webhooks::nudge(&pool, slug, owner, b"second signed delivery").await?;
    finish_job(&pool, &rescheduled, None).await?;
    let following = claim(&pool)
        .await?
        .context("missing second live delivery")?;
    assert_eq!(following.subject, subject);
    assert!(following.live_first);
    finish_job(&pool, &following, None).await?;

    let backfill = claim(&pool).await?.context("missing resumed backfill")?;
    assert_eq!(backfill.subject, subject);
    assert!(!backfill.live_first);
    assert_eq!(backfill.upper.timestamp_micros(), end.timestamp_micros());
    Ok(())
}
use crate::{acute, mcp, store};
use axum::{
    body::to_bytes,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Json,
};
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn scoped_reads_backfill_claims_and_revocation_are_isolated() -> Result<()> {
    let pool = store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let first = "wearable-test-first";
    let second = "wearable-test-second";
    let r1 = "a".repeat(43);
    let c1 = "b".repeat(43);
    let r2 = "c".repeat(43);
    let c2 = "d".repeat(43);
    let provider = Provider::GoogleHealth;
    let slug = provider.slug();
    for (subject, read, control) in [(first, &r1, &c1), (second, &r2, &c2)] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($3,'UTF8')))",
            &[&subject,read,control]).await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,$2)",
            &[&subject, &slug],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens) VALUES($1,$2,''::bytea)", &[&subject,&slug]).await?;
    }
    drop(db);

    assert_eq!(
        wearable_oauth::authenticate(&pool, &r1, false)
            .await?
            .as_deref(),
        Some(first)
    );
    assert!(wearable_oauth::authenticate(&pool, &r1, true)
        .await?
        .is_none());
    let end = Utc::now() - Duration::days(1);
    let default = schedule(
        &pool,
        first,
        ScheduleRequest {
            provider,
            start: None,
            end: Some(end),
        },
    )
    .await?;
    assert_eq!(default["start"], json!(end - Duration::days(30)));
    for (provider, limit) in [(provider, 3_650), (Provider::Polar, 365)] {
        let error = schedule(
            &pool,
            first,
            ScheduleRequest {
                provider,
                start: Some(end - Duration::days(limit + 1)),
                end: Some(end),
            },
        )
        .await
        .expect_err("provider limit applies before updating the queue");
        assert!(error.to_string().contains(&format!("at most {limit} days")));
    }
    let start = end - Duration::days(730);
    let request = || ScheduleRequest {
        provider,
        start: Some(start),
        end: Some(end),
    };
    schedule(&pool, first, request()).await?;
    let job = claim(&pool).await?.unwrap();
    assert_eq!(job.subject, first);
    assert_eq!(job.lower.timestamp_micros(), start.timestamp_micros());
    assert!(claim(&pool).await?.is_none());
    assert!(schedule(&pool, first, request()).await.is_err());

    let window = end - Duration::hours(6);
    let lease = wearables::issue_job_lease(&pool, first, slug, &job.id).await?;
    let snapshot = |lease: Value| {
        serde_json::from_value(json!({
            "provider":slug,"lease":lease["lease"],"start":window,"end":end,"grants":["metric"],
            "records":[{"kind":"metric","source":"fixture","source_id":"snapshot","started_at":window,"ended_at":window,
                 "modified_at":end, "payload":{"metric":"daily-heart-rate-variability","provider_data":{"milliseconds":52}}}]
        }))
    };
    wearables::store_snapshot(&pool, first, slug, snapshot(lease)?, false).await?;
    let app = Arc::new(mcp::App {
        db: pool.clone(),
        read_token: "fixture".into(),
        control_token: "control".into(),
        rules: acute::rules()?,
    });
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let base = format!("http://{}", listener.local_addr()?);
    let router = crate::router(app.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let client = reqwest::Client::new();
    let page = client
        .get(format!("{base}/wearables/connect"))
        .send()
        .await?
        .error_for_status()?;
    assert!(page.headers()["content-security-policy"]
        .to_str()?
        .contains("frame-ancestors 'none'"));
    assert!(page.text().await?.contains("portal.js"));
    let denied = client
        .post(format!("{base}/mcp/wearable-control"))
        .bearer_auth(&r1)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await?;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let tools: Value = client
        .post(format!("{base}/mcp/wearable-read"))
        .bearer_auth(&r1)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .all(|item| item["name"] != "connect_cloud_oauth"));
    server.abort();
    let read = |token: &str, args: Value| {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let app = app.clone();
        async move {
            let response = mcp::handle(State(app),Path("wearable-read".into()),headers,Json(json!({
                "jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wearable_samples","arguments":args}
            }))).await;
            assert_eq!(response.status(), StatusCode::OK);
            Ok::<Value, anyhow::Error>(serde_json::from_slice(
                &to_bytes(response.into_body(), 32_768).await?,
            )?)
        }
    };
    let arguments = json!({"kind":"metric","metric":"daily-heart-rate-variability","provider":slug,"start":window,"end":end});
    let result = read(&r1, arguments.clone()).await?;
    assert_eq!(
        result["result"]["structuredContent"]["records"][0]["payload"]["provider_data"]
            ["milliseconds"],
        52
    );
    let result = read(&r2, arguments.clone()).await?;
    assert_eq!(result["result"]["structuredContent"]["records"], json!([]));
    let mut invalid = arguments;
    invalid["subject"] = json!(first);
    assert_eq!(read(&r2, invalid).await?["result"]["isError"], true);
    let next = end - Duration::hours(12);
    finish_job(&pool, &job, Some(next)).await?;
    assert!(wearables::issue_job_lease(&pool, first, slug, &job.id)
        .await
        .is_err());
    pool.get()
        .await?
        .execute(
            "UPDATE watch_tower.wearable_sync_schedule
             SET next_live_sync=now()-interval '1 second'
             WHERE subject_id=$1 AND provider=$2",
            &[&first, &slug],
        )
        .await?;
    let live = claim(&pool).await?.context("missing periodic live sync")?;
    assert!(live.live_first);
    assert!(live.target.is_none());
    assert_eq!(live.upper.timestamp_micros(), next.timestamp_micros());
    run_batches(&pool, &live, |from, until, lease| async move {
        assert!(from > next);
        assert_eq!(until - from, Duration::days(1));
        Ok(serde_json::from_value(json!({
            "provider":slug,"lease":lease["lease"],"start":from,"end":until,
            "grants":["metric"],"records":[]
        }))?)
    })
    .await?;
    let job = claim(&pool).await?.unwrap();
    assert!(!job.live_first);
    assert_eq!(job.upper.timestamp_micros(), next.timestamp_micros());
    finish_job(&pool, &job, Some(start)).await?;
    let periodic_timer_preserved: bool = pool
        .get()
        .await?
        .query_one(
            "SELECT next_sync=next_live_sync AND next_sync>now()
             FROM watch_tower.wearable_sync_schedule WHERE subject_id=$1 AND provider=$2",
            &[&first, &slug],
        )
        .await?
        .get(0);
    assert!(periodic_timer_preserved);
    assert!(request_live_refresh(&pool, first, provider).await?);
    let job = claim(&pool).await?.context("missing requested refresh")?;
    let lease = wearables::issue_job_lease(&pool, first, slug, &job.id).await?;
    wearables::revoke_for(&pool, first, slug).await?;
    assert!(
        wearables::store_snapshot(&pool, first, slug, snapshot(lease)?, false)
            .await
            .is_err()
    );
    assert!(wearables::issue_job_lease(&pool, first, slug, &job.id)
        .await
        .is_err());
    assert!(claim(&pool).await?.is_none());
    assert_eq!(status(&pool, first, provider).await?["state"], "revoked");
    assert_eq!(status(&pool, second, provider).await?["state"], "pending");
    Ok(())
}
