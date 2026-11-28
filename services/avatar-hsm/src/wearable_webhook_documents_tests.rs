use super::*;
use anyhow::Context;
use axum::{
    http::{HeaderMap, StatusCode},
    routing::get,
    Json, Router,
};
use ring::hmac;
use serde_json::json;
use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};

#[tokio::test]
#[ignore = "requires isolated PostgreSQL in AVATAR_HSM_TEST_DATABASE_URL"]
async fn webhook_document_retry_after_survives_new_notification() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let subject = "oura-retry-after-fixture";
    let other = "oura-retry-after-other";
    for (subject, owner) in [(subject, "first-owner"), (other, "other-owner")] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'oura')",
            &[&subject],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile
             VALUES($1,sha256(convert_to($1,'UTF8')),sha256(convert_to($1||':control','UTF8')))",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash)
                    VALUES($1,'oura',''::bytea,$2)", &[&subject,&wearable_webhooks::hash(owner.as_bytes())]).await?;
        db.execute(
            "INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,backfill_from)
                    VALUES($1,'oura',now())",
            &[&subject],
        )
        .await?;
    }
    let at = Utc::now();
    let target = |event_at| Target {
        kind: "daily_readiness",
        id: "readiness-01",
        delete: false,
        event_at: Some(event_at),
    };
    enqueue(&pool, "oura", "first-owner", target(at), b"first-event").await?;
    let job = claim(&pool).await?.context("first webhook job")?;
    let provider = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", provider.local_addr()?);
    let responses = Arc::new(AtomicU16::new(0));
    let mock = {
        let responses = Arc::clone(&responses);
        async move {
            let router = Router::new().route(
                "/daily_readiness/readiness-01",
                get(move || {
                    let response_index = responses.fetch_add(1, Ordering::Relaxed);
                    async move {
                        if response_index == 0 {
                            (
                                StatusCode::TOO_MANY_REQUESTS,
                                [("retry-after", "900")],
                                Json(json!({})),
                            )
                        } else {
                            (
                                StatusCode::OK,
                                [("retry-after", "0")],
                                Json(json!({
                                    "id":"readiness-01","day":Utc::now().date_naive(),"score":78
                                })),
                            )
                        }
                    }
                }),
            );
            axum::serve(provider, router).await
        }
    };
    let task = tokio::spawn(mock);
    let client = cloud_wearables::http_client()?;
    let credentials = ["test-readiness-access".to_string(), "daily".to_string()];
    let Err(error) = fetch_document(&client, &origin, &credentials, &job).await else {
        anyhow::bail!("mock HTTP 429 should defer the webhook");
    };
    retry(&pool, &job, &error).await?;
    assert!(db
        .query_one(
            "SELECT attempts=0 AND job_id IS NULL AND next_attempt>now()+interval '14 minutes'
         FROM watch_tower.wearable_webhook_document WHERE subject_id=$1",
            &[&subject],
        )
        .await?
        .get::<_, bool>(0));
    enqueue(
        &pool,
        "oura",
        "first-owner",
        target(at + chrono::Duration::seconds(1)),
        b"changed-event",
    )
    .await?;
    enqueue(&pool, "oura", "other-owner", target(at), b"other-event").await?;
    let other_job = claim(&pool).await?.context("unthrottled owner job")?;
    assert_eq!(other_job.subject, other);
    assert!(claim(&pool).await?.is_none());
    assert_eq!(responses.load(Ordering::Relaxed), 1);

    db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET throttled_until=now()-interval '1 second'
         WHERE subject_id=$1",
        &[&subject],
    )
    .await?;
    let resumed = claim(&pool).await?.context("resumed webhook job")?;
    assert_eq!(resumed.subject, subject);
    let doc = fetch_document(&client, &origin, &credentials, &resumed).await?;
    finish(&pool, &resumed, doc).await?;
    assert!(db.query_one(
        "SELECT attempts=0 AND completed_at IS NOT NULL FROM watch_tower.wearable_webhook_document
         WHERE subject_id=$1", &[&subject],
    ).await?.get::<_, bool>(0));
    assert_eq!(responses.load(Ordering::Relaxed), 2);
    task.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and a synthetic Polar signing key"]
async fn polar_physical_info_uses_its_durable_owner_bound_job() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let secret = std::env::var("AVATAR_POLAR_WEBHOOK_SECRET")?;
    let subject = "polar-physical-info-fixture";
    let other = "polar-physical-info-other";
    let token = |subject: &str| format!("{subject}-synthetic-read-0123456789");
    let db = pool.get().await?;
    for (subject, provider_owner) in [(subject, "778991"), (other, "778992")] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($1||'c','UTF8')))", &[&subject, &token(subject)]).await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'polar')",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash,linked_at)
            VALUES($1,'polar',''::bytea,$2,now()-interval '2 hours')",
            &[&subject,&wearable_webhooks::hash(provider_owner.as_bytes())]).await?;
    }
    schedule_polar_physical_info(&pool, subject).await?;
    schedule_polar_physical_info(&pool, subject).await?;
    let hsm = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let hsm_url = format!("http://{}", hsm.local_addr()?);
    let app = Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let hsm_server = tokio::spawn(async move { axum::serve(hsm, crate::router(app)).await });
    let client = cloud_wearables::http_client()?;
    let at = Utc::now();
    let body = json!({"event":"PHYSICAL_INFORMATION","user_id":778991,"timestamp":at,
        "url":"https://unused.invalid/ignored-by-hsm","weight":1})
    .to_string();
    let sign = |body: &str| {
        let tag = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()),
            body.as_bytes(),
        );
        tag.as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    for (notification, signature, expected) in [
        (body.clone(), "invalid".into(), StatusCode::UNAUTHORIZED),
        (body.clone(), sign(&body), StatusCode::OK),
        (body.clone(), sign(&body), StatusCode::OK),
    ] {
        assert_eq!(
            client
                .post(format!("{hsm_url}/wearables/webhooks/polar"))
                .header("Polar-Webhook-Signature", signature)
                .body(notification)
                .send()
                .await?
                .status(),
            expected
        );
    }
    assert_eq!(
        db.query_one(
            "SELECT subject_id,event_at FROM watch_tower.wearable_webhook_document
        WHERE provider='polar' AND data_type='physical_info'",
            &[]
        )
        .await?
        .get::<_, String>(0),
        subject
    );
    let receipt = db.query_one("SELECT count(*) FROM watch_tower.wearable_webhook_receipt WHERE provider='polar' AND event_hash=$1",
        &[&wearable_webhooks::hash(body.as_bytes())]).await?;
    assert_eq!(receipt.get::<_, i64>(0), 1);
    db.execute("UPDATE watch_tower.wearable_webhook_document SET next_attempt='1900-01-01' WHERE subject_id=$1",
        &[&subject]).await?;
    let interrupted = claim(&pool).await?.context("missing Polar document")?;
    assert_eq!(interrupted.subject, subject);
    db.execute("UPDATE watch_tower.wearable_webhook_document SET leased_until=now()-interval '1 minute' WHERE subject_id=$1",
        &[&subject]).await?;
    // Reconnect the pool to reproduce a worker picking up a job left behind by a dead process.
    let restarted = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let job = claim(&restarted)
        .await?
        .context("missing Polar job after restart")?;
    assert_ne!(job.id, interrupted.id);
    let api = Router::new().route("/physical-info", get(|headers: HeaderMap| async move {
        assert_eq!(headers["authorization"], "Bearer fixture-polar-owner");
        Json(json!({"weight":72.5,"height":174,"modified":"2026-07-01T15:00:00Z","created":"2026-06-01T10:00:00Z",
            "vo2_max":48,"maximum_heart_rate":180,"sleep_goal":"PT8H","birthday":"1990-01-01"}))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let api_url = format!("http://{}", listener.local_addr()?);
    let api_server = tokio::spawn(async move { axum::serve(listener, api).await });
    let credentials = ["fixture-polar-owner".into(), "accesslink.read_all".into()];
    let doc = fetch_document(&client, &api_url, &credentials, &interrupted).await?;
    finish(&restarted, &interrupted, doc).await?;
    assert!(db
        .query_opt(
            "SELECT 1 FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='polar'",
            &[&subject]
        )
        .await?
        .is_none());
    let doc = fetch_document(&client, &api_url, &credentials, &job).await?;
    finish(&restarted, &job, doc).await?;
    schedule_polar_physical_info(&pool, subject).await?;
    assert!(db.query_one("SELECT completed_at IS NOT NULL FROM watch_tower.wearable_webhook_document WHERE subject_id=$1",
        &[&subject]).await?.get::<_,bool>(0));
    let query = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wearable_samples","arguments":{
        "provider":"polar","kind":"metric","metric":"physical_info","start":"2026-07-01T00:00:00Z","end":"2026-07-02T00:00:00Z"
    }}});
    for (owner, count) in [(subject, 1), (other, 0)] {
        let result: Value = client
            .post(format!("{hsm_url}/mcp/wearable-read"))
            .bearer_auth(token(owner))
            .json(&query)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let rows = result["result"]["structuredContent"]["records"]
            .as_array()
            .context("missing records")?;
        assert_eq!(rows.len(), count);
        if let Some(row) = rows.first() {
            let data = &row["payload"]["provider_data"];
            assert_eq!(data["weight_kg"], 72.5);
            assert_eq!(data["height_cm"].as_f64(), Some(174.0));
            assert_eq!(data["sleep_goal"], "PT8H");
            assert!(data.get("birthday").is_none());
            assert_eq!(row["payload"]["timestamp_basis"], "modified");
        }
    }
    let latest = |owner: &str| {
        client.post(format!("{hsm_url}/mcp/wearable-read")).bearer_auth(token(owner))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"polar_physical_info","arguments":{}}}))
            .send()
    };
    let ready: Value = latest(subject).await?.error_for_status()?.json().await?;
    assert_eq!(ready["result"]["structuredContent"]["state"], "ready");
    assert_eq!(
        ready["result"]["structuredContent"]["at"],
        "2026-07-01T15:00:00Z"
    );
    let empty: Value = latest(other).await?.error_for_status()?.json().await?;
    assert_eq!(empty["result"]["structuredContent"]["state"], "pending");
    db.execute("UPDATE watch_tower.wearable_webhook_document SET completed_at=now()-interval '2 days' WHERE subject_id=$1",
        &[&subject]).await?;
    schedule_polar_physical_info(&pool, subject).await?;
    assert!(db.query_one("SELECT completed_at IS NULL AND attempts=0 FROM watch_tower.wearable_webhook_document WHERE subject_id=$1",
        &[&subject]).await?.get::<_,bool>(0));
    crate::wearables::revoke_for(&pool, subject, "polar").await?;
    assert!(db
        .query_opt(
            "SELECT 1 FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='polar'",
            &[&subject]
        )
        .await?
        .is_none());
    let disconnected: Value = latest(subject).await?.error_for_status()?.json().await?;
    assert_eq!(
        disconnected["result"]["structuredContent"]["state"],
        "not_connected"
    );
    api_server.abort();
    hsm_server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and synthetic Intervals.icu webhook secret"]
async fn intervals_webhook_targets_only_the_authenticated_athletes_own_workout() -> Result<()> {
    use axum::extract::Path;
    use std::sync::atomic::AtomicUsize;

    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let secret = std::env::var("AVATAR_INTERVALS_ICU_WEBHOOK_SECRET")?;
    let db = pool.get().await?;
    let subject = "intervals-webhook-owner";
    let owner = "i123";
    for (subject, owner) in [
        (subject, owner),
        ("another-intervals-webhook-owner", "i456"),
    ] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))", &[&subject]).await?;
        db.execute("INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'intervals_icu')", &[&subject]).await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash,linked_at)
            VALUES($1,'intervals_icu',''::bytea,$2,now()-interval '1 day')", &[&subject,&wearable_webhooks::hash(owner.as_bytes())]).await?;
        db.execute("INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,backfill_from,next_sync)
            VALUES($1,'intervals_icu',now(),now()+interval '1 hour')", &[&subject]).await?;
    }
    let app = Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let hsm = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let webhook = format!(
        "http://{}/wearables/webhooks/intervals_icu",
        hsm.local_addr()?
    );
    let hsm_server = tokio::spawn(async move { axum::serve(hsm, crate::router(app)).await });
    let timestamp = Utc::now();
    let event = json!({"type":"ACTIVITY_ANALYZED","athlete_id":owner,"timestamp":timestamp,
        "activity":{"id":"i900","start_date":"2026-08-28T10:00:00Z","distance":123}});
    let client = cloud_wearables::http_client()?;
    let send = |secret: &str, events: Vec<Value>| {
        client
            .post(&webhook)
            .json(&json!({"secret":secret,"events":events}))
            .send()
    };
    assert_eq!(
        send("wrong-secret", vec![event.clone()]).await?.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send(&secret, vec![event.clone()]).await?.status(),
        StatusCode::OK
    );
    assert_eq!(
        send(&secret, vec![event.clone(), event]).await?.status(),
        StatusCode::OK
    );
    assert_eq!(db.query_one(
        "SELECT count(*) FROM watch_tower.wearable_webhook_receipt WHERE provider='intervals_icu'",
        &[],
    ).await?.get::<_,i64>(0), 1);
    assert!(db
        .query_opt(
            "SELECT 1 FROM avatar_hsm.wearable_record WHERE subject_id=$1",
            &[&subject]
        )
        .await?
        .is_none());
    db.execute("UPDATE watch_tower.wearable_webhook_document SET next_attempt='1900-01-01' WHERE subject_id=$1 AND provider='intervals_icu'", &[&subject]).await?;
    let job = claim(&pool)
        .await?
        .context("missing Intervals.icu webhook job")?;
    assert_eq!(job.subject, subject);
    assert_eq!(job.object, "i900");

    let requests = Arc::new(AtomicUsize::new(0));
    let upstream_calls = Arc::clone(&requests);
    let router = Router::new().route(
        "/activity/{id}",
        get(move |Path(id): Path<String>, headers: HeaderMap| {
            upstream_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                assert_eq!(headers["authorization"], "Bearer fixture-access");
                Json(
                    json!({"id":id,"icu_athlete_id":owner,"start_date":"2026-08-15T22:00:00Z",
                "start_date_local":"2026-08-16T07:00:00"}),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let test_server = tokio::spawn(async move { axum::serve(listener, router).await });
    let token =
        |scope: &str, athlete: &str| ["fixture-access".into(), scope.into(), athlete.into()];
    assert!(matches!(
        fetch_document(&client, &origin, &token("WELLNESS:READ", owner), &job).await?,
        Document::NoAccess
    ));
    assert_eq!(requests.load(Ordering::SeqCst), 0);
    assert!(matches!(
        fetch_document(&client, &origin, &token("ACTIVITY:READ", "i456"), &job).await?,
        Document::NoAccess
    ));
    let document = fetch_document(&client, &origin, &token("ACTIVITY:READ", owner), &job).await?;
    finish(&pool, &job, document).await?;
    let targets = db.query("SELECT subject_id,day::text FROM watch_tower.wearable_target_day WHERE provider='intervals_icu' ORDER BY day", &[]).await?;
    assert_eq!(
        targets
            .iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>(),
        [
            (subject.into(), "2026-08-15".into()),
            (subject.into(), "2026-08-16".into())
        ]
    );
    hsm_server.abort();
    test_server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn strava_authorization_jobs_follow_the_link_generation() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let owner = "strava-revocation-owner";
    let other = "strava-revocation-other";
    let account_hash = wearable_webhooks::hash(b"590");
    let current = DateTime::from_timestamp(Utc::now().timestamp(), 0).unwrap();
    let old = current - chrono::Duration::minutes(2);
    let linked = current - chrono::Duration::minutes(1);
    for (subject, account) in [(owner, "590"), (other, "591")] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
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
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'strava')",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash,linked_at)
            VALUES($1,'strava',''::bytea,$2,$3)", &[&subject,&wearable_webhooks::hash(account.as_bytes()),&linked]).await?;
        db.execute(
            "INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,backfill_from)
            VALUES($1,'strava',now())",
            &[&subject],
        )
        .await?;
    }
    enqueue(
        &pool,
        "strava",
        "590",
        strava_revocation("590", old),
        b"old strava revoke",
    )
    .await?;
    assert!(claim(&pool).await?.is_none());
    enqueue(
        &pool,
        "strava",
        "590",
        strava_revocation("590", current),
        b"in-flight strava revoke",
    )
    .await?;
    let old_job = claim(&pool)
        .await?
        .context("missing first Strava revocation")?;
    assert_eq!(old_job.subject, owner);
    crate::wearables::revoke_for(&pool, owner, "strava").await?;
    db.execute(
        "UPDATE avatar_hsm.device_connection SET state='pending'
        WHERE subject_id=$1 AND provider='strava'",
        &[&owner],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash,linked_at)
        VALUES($1,'strava',''::bytea,$2,$3)", &[&owner,&account_hash,&(current+chrono::Duration::seconds(1))]).await?;
    db.execute(
        "INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,backfill_from)
        VALUES($1,'strava',now())",
        &[&owner],
    )
    .await?;
    let client = Client::new();
    run(&pool, &client, &old_job).await?;
    enqueue(
        &pool,
        "strava",
        "590",
        strava_revocation("590", current),
        b"delayed strava revoke",
    )
    .await?;
    enqueue(
        &pool,
        "strava",
        "590",
        strava_revocation("590", current + chrono::Duration::seconds(1)),
        b"indeterminate strava revoke",
    )
    .await?;
    assert!(claim(&pool).await?.is_none());
    let lease = crate::wearables::issue_lease_for(&pool, owner, "strava").await?;
    let started = current - chrono::Duration::hours(2);
    let ended = started + chrono::Duration::minutes(30);
    let snapshot = |lease: &Value| {
        serde_json::from_value(json!({"lease":lease["lease"],"start":started,
        "end":ended,"grants":["workout"],"records":[{"kind":"workout","source":"Strava",
        "source_id":"590","started_at":started,"ended_at":ended,"modified_at":current,
        "payload":{"exercise_type":"Ride"}}]}))
    };
    crate::wearables::store_snapshot(&pool, owner, "strava", snapshot(&lease)?, false).await?;
    let in_flight = crate::wearables::issue_lease_for(&pool, owner, "strava").await?;
    let fresh = current + chrono::Duration::seconds(2);
    enqueue(
        &pool,
        "strava",
        "590",
        strava_revocation("590", fresh),
        b"live strava revoke",
    )
    .await?;
    let job = claim(&pool)
        .await?
        .context("missing current Strava revocation")?;
    run(&pool, &client, &job).await?;
    run(&pool, &client, &job).await?;
    assert!(
        crate::wearables::store_snapshot(&pool, owner, "strava", snapshot(&in_flight)?, false)
            .await
            .is_err()
    );
    let rows = db
        .query(
            "SELECT d.subject_id,d.state,o.generation IS NOT NULL,
           s.subject_id IS NOT NULL, EXISTS(SELECT 1 FROM avatar_hsm.wearable_record r
             WHERE r.subject_id=d.subject_id AND r.provider=d.provider)
         FROM avatar_hsm.device_connection d
         LEFT JOIN avatar_hsm.wearable_oauth_connection o USING(subject_id,provider)
         LEFT JOIN watch_tower.wearable_sync_schedule s USING(subject_id,provider)
         WHERE d.provider='strava' AND d.subject_id IN ($1,$2) ORDER BY d.subject_id",
            &[&owner, &other],
        )
        .await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), other);
    assert!(rows[0].get::<_, bool>(2) && rows[0].get::<_, bool>(3));
    assert_eq!(rows[1].get::<_, String>(0), owner);
    assert_eq!(rows[1].get::<_, String>(1), "revoked");
    assert!((2..=4).all(|index| !rows[1].get::<_, bool>(index)));
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn strava_reconciles_visibility_and_prevents_a_stale_snapshot_from_resurrecting_a_record(
) -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let owner = "strava-workout-owner";
    let other = "strava-workout-other";
    let db = pool.get().await?;
    let day = Utc::now().date_naive() - chrono::Duration::days(60);
    let started = day.and_hms_opt(7, 0, 0).unwrap().and_utc();
    let ended = started + chrono::Duration::minutes(30);
    let snapshot = |lease: &Value| {
        serde_json::from_value(json!({"lease":lease["lease"],"start":started,
        "end":ended+chrono::Duration::hours(1),"grants":["workout"],"records":[{
            "kind":"workout","source":"Strava","source_id":"888","started_at":started,"ended_at":ended,
            "modified_at":Utc::now(),"payload":{"exercise_type":"Ride"}
        }]}))
    };
    for (subject, account) in [(owner, "101"), (other, "102")] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
            &[&subject],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($1||'r','UTF8')),
            sha256(convert_to($1||'c','UTF8')))",
            &[&subject],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'strava')",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash)
            VALUES($1,'strava',''::bytea,$2)", &[&subject,&wearable_webhooks::hash(account.as_bytes())]).await?;
        db.execute("INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,next_sync,backfill_from)
            VALUES($1,'strava',now()+interval '1 day',now())", &[&subject]).await?;
        let lease = crate::wearables::issue_lease_for(&pool, subject, "strava").await?;
        crate::wearables::store_snapshot(&pool, subject, "strava", snapshot(&lease)?, false)
            .await?;
    }
    let in_flight = crate::wearables::issue_lease_for(&pool, owner, "strava").await?;
    let status = Arc::new(AtomicU16::new(403));
    let server_status = status.clone();
    let upstream = Router::new().route(
        "/activities/888",
        get(move |headers: HeaderMap| {
            let code = server_status.load(Ordering::Relaxed);
            async move {
                assert_eq!(headers["authorization"], "Bearer strava-workout-fixture");
                if code == 200 {
                    Ok(Json(
                        json!({"id":888,"start_date":started,"elapsed_time":1800}),
                    ))
                } else {
                    Err(StatusCode::from_u16(code).unwrap())
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await });
    let client = Client::new();
    let credentials: [String; 2] = ["strava-workout-fixture".into(), "read,activity:read".into()];
    let observed = Utc::now() - chrono::Duration::seconds(10);
    enqueue(
        &pool,
        "strava",
        "101",
        strava_activity("888", "delete", observed).unwrap(),
        b"delete activity",
    )
    .await?;
    let deletion = claim(&pool).await?.context("missing Strava deletion job")?;
    assert_eq!(deletion.subject, owner);
    assert!(matches!(
        fetch_document(
            &client,
            &origin,
            &[credentials[0].clone(), "read".into()],
            &deletion
        )
        .await?,
        Document::NoAccess
    ));
    assert!(fetch_document(&client, &origin, &credentials, &deletion)
        .await
        .is_err());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM avatar_hsm.wearable_record WHERE provider='strava' AND subject_id IN ($1,$2)",
            &[&owner,&other]
        )
        .await?
        .get::<_, i64>(0),
        2
    );
    status.store(404, Ordering::Relaxed);
    let confirmed = fetch_document(&client, &origin, &credentials, &deletion).await?;
    assert!(matches!(confirmed, Document::Deleted));
    finish(&pool, &deletion, confirmed).await?;
    crate::wearables::store_snapshot(&pool, owner, "strava", snapshot(&in_flight)?, false).await?;
    let accounts = db
        .query(
            "SELECT subject_id FROM avatar_hsm.wearable_record WHERE provider='strava' AND subject_id IN ($1,$2)",
            &[&owner,&other],
        )
        .await?;
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].get::<_, String>(0), other);
    enqueue(
        &pool,
        "strava",
        "101",
        strava_activity("888", "update", observed - chrono::Duration::seconds(1)).unwrap(),
        b"late activity update",
    )
    .await?;
    assert!(claim(&pool).await?.is_none());
    status.store(200, Ordering::Relaxed);
    enqueue(
        &pool,
        "strava",
        "101",
        strava_activity("888", "create", observed + chrono::Duration::seconds(1)).unwrap(),
        b"activity shared again",
    )
    .await?;
    let reopened = claim(&pool)
        .await?
        .context("missing Strava visibility job")?;
    finish(
        &pool,
        &reopened,
        fetch_document(&client, &origin, &credentials, &reopened).await?,
    )
    .await?;
    let targets = db.query("SELECT subject_id,day FROM watch_tower.wearable_target_day WHERE provider='strava' AND subject_id=$1",&[&owner]).await?;
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].get::<_, NaiveDate>(1), day);
    let lease = crate::wearables::issue_lease_for(&pool, owner, "strava").await?;
    crate::wearables::store_snapshot(&pool, owner, "strava", snapshot(&lease)?, false).await?;
    assert_eq!(db.query_one("SELECT count(*) FROM avatar_hsm.wearable_record WHERE provider='strava' AND subject_id=$1",&[&owner]).await?.get::<_,i64>(0),1);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn oura_optional_webhooks_fetch_with_their_own_scopes() -> Result<()> {
    let router = Router::new()
        .route(
            "/enhanced_tag/tag-1",
            get(|| async {
                Json(
                    json!({"id":"tag-1","start_day":"2026-04-24","end_day":"2026-04-26",
                "start_time":"2026-04-24T23:00:00","end_time":"2026-04-26T08:00:00"}),
                )
            }),
        )
        .route(
            "/vO2_max/vo2-1",
            get(|| async { Json(json!({"id":"vo2-1","day":"2026-04-24","vo2_max":45})) }),
        )
        .route(
            "/rest_mode_period/rest-1",
            get(|| async { Json(json!({"id":"rest-1","start_day":"2026-04-24","end_day":null})) }),
        )
        .route(
            "/rest_mode_period/no-access",
            get(|| async { StatusCode::FORBIDDEN }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let client = Client::new();
    for (kind, id, scope, expected) in [
        (
            "enhanced_tag",
            "tag-1",
            "tag",
            vec!["2026-04-24", "2026-04-26"],
        ),
        ("vo2_max", "vo2-1", "heart_health", vec!["2026-04-24"]),
        ("rest_mode_period", "rest-1", "daily", vec!["2026-04-24"]),
    ] {
        let job = Job {
            provider: "oura".into(),
            kind: kind.into(),
            object: id.into(),
            subject: String::new(),
            id: String::new(),
            delete_requested: false,
        };
        let wrong_scope = ["synthetic".into(), "workout".into()];
        assert!(matches!(
            fetch_document(&client, &origin, &wrong_scope, &job).await?,
            Document::NoAccess
        ));
        let credentials = ["synthetic".into(), scope.into()];
        let Document::Live { days, record: None } =
            fetch_document(&client, &origin, &credentials, &job).await?
        else {
            anyhow::bail!("missing Oura {kind} document");
        };
        assert_eq!(
            days.iter().map(ToString::to_string).collect::<Vec<_>>(),
            expected
        );
    }
    let job = Job {
        provider: "oura".into(),
        kind: "rest_mode_period".into(),
        object: "no-access".into(),
        subject: String::new(),
        id: String::new(),
        delete_requested: true,
    };
    assert!(matches!(
        fetch_document(
            &client,
            &origin,
            &["synthetic".into(), "daily".into()],
            &job
        )
        .await?,
        Document::NoAccess
    ));
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and a synthetic WHOOP app secret"]
async fn whoop_recovery_webhook_uses_the_associated_sleep_uuid() -> Result<()> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let secret = std::env::var("AVATAR_WHOOP_CLIENT_SECRET")?;
    let subject = "whoop-v2-webhook-fixture";
    let id = "550e8400-e29b-41d4-a716-446655440000";
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
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
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'whoop')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.wearable_oauth_connection
        (subject_id,provider,sealed_tokens,provider_account_hash) VALUES($1,'whoop',''::bytea,$2)",
        &[&subject, &wearable_webhooks::hash(b"456")],
    )
    .await?;
    db.execute("INSERT INTO watch_tower.wearable_sync_schedule
        (subject_id,provider,next_sync,backfill_from) VALUES($1,'whoop',now()+interval '1 day',now())",&[&subject]).await?;
    let app = Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let webhook_url = format!("http://{}/wearables/webhooks/whoop", listener.local_addr()?);
    let webhook = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let client = cloud_wearables::http_client()?;
    let body = json!({"user_id":456,"id":id,"type":"recovery.updated","trace_id":"signed-fixture"})
        .to_string();
    let timestamp = Utc::now().timestamp_millis().to_string();
    let signed = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()),
        [timestamp.as_bytes(), body.as_bytes()].concat().as_slice(),
    );
    let signature = STANDARD.encode(signed.as_ref());
    for _ in 0..2 {
        assert_eq!(
            client
                .post(&webhook_url)
                .header("X-WHOOP-Signature-Timestamp", &timestamp)
                .header("X-WHOOP-Signature", &signature)
                .body(body.clone())
                .send()
                .await?
                .status(),
            StatusCode::OK
        );
    }
    webhook.abort();
    let job = claim(&pool).await?.context("missing WHOOP v2 job")?;
    assert_eq!(job.provider, "whoop");
    assert_eq!(job.subject, subject);
    assert_eq!(job.object, id);
    assert!(claim(&pool).await?.is_none());
    let route = format!("/activity/sleep/{id}");
    let recovery_code = Arc::new(AtomicU16::new(200));
    let replacement = "550e8400-e29b-41d4-a716-446655440001";
    let upstream = Router::new().route(
        &route,
        get(move |headers: HeaderMap| async move {
            assert_eq!(headers["authorization"], "Bearer whoop-fixture-access");
            Json(json!({"id":id,"cycle_id":93845,"start":"2026-03-10T22:00:00Z","end":"2026-03-11T06:00:00Z"}))
        }),
    ).route("/cycle/93845/recovery",get({
        let recovery_code = recovery_code.clone();
        move |headers:HeaderMap| {
            let code = recovery_code.load(Ordering::Relaxed);
            async move {
                assert_eq!(headers["authorization"],"Bearer whoop-fixture-access");
                let sleep_id = match code {
                    200 => id,
                    299 => replacement,
                    _ => return Err(StatusCode::from_u16(code).unwrap()),
                };
                Ok(Json(json!({"cycle_id":93845,"sleep_id":sleep_id,"created_at":"2026-03-11T06:30:00Z"})))
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await });
    assert!(matches!(
        fetch_document(
            &client,
            &origin,
            &["invalid".into(), "read:recovery".into()],
            &job
        )
        .await?,
        Document::NoAccess
    ));
    let document = fetch_document(
        &client,
        &origin,
        &[
            "whoop-fixture-access".into(),
            "read:sleep read:recovery".into(),
        ],
        &job,
    )
    .await?;
    finish(&pool, &job, document).await?;
    let stored = db
        .query(
            "SELECT day::text FROM watch_tower.wearable_target_day
        WHERE subject_id=$1 AND provider='whoop' ORDER BY day",
            &[&subject],
        )
        .await?;
    assert_eq!(
        stored
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>(),
        ["2026-03-10", "2026-03-11"]
    );
    let lease = crate::wearables::issue_lease_for(&pool, subject, "whoop").await?;
    let snapshot = serde_json::from_value(json!({
        "lease":lease["lease"],"start":"2026-03-10T00:00:00Z","end":"2026-03-12T00:00:00Z",
        "grants":["workout"],"records":[{
            "kind":"workout","source":"WHOOP","source_id":id,
            "started_at":"2026-03-10T12:00:00Z","ended_at":"2026-03-10T13:00:00Z",
            "modified_at":"2026-03-12T00:00:00Z","payload":{"exercise_type":"cycling"}
        }]
    }))?;
    crate::wearables::store_snapshot(&pool, subject, "whoop", snapshot, false).await?;
    let deleted_event = json!({"id":id,"type":"workout.deleted"});
    enqueue(
        &pool,
        "whoop",
        "456",
        target("whoop", &deleted_event).unwrap(),
        b"whoop-workout-fixture-delete",
    )
    .await?;
    let deletion = claim(&pool)
        .await?
        .context("missing WHOOP workout deletion")?;
    let result = fetch_document(
        &client,
        &origin,
        &["whoop-fixture-access".into(), "read:workout".into()],
        &deletion,
    )
    .await?;
    assert!(matches!(result, Document::Deleted));
    finish(&pool, &deletion, result).await?;
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM avatar_hsm.wearable_record
        WHERE subject_id=$1 AND provider='whoop' AND kind='workout' AND source_id=$2",
            &[&subject, &id]
        )
        .await?
        .get::<_, i64>(0),
        0
    );
    let recovery_rows = [
        ("recovery:93845", 93845, id),
        ("recovery:93846", 93846, replacement),
    ]
    .into_iter()
    .map(|(source, cycle, sleep)| {
        json!({
            "kind":"metric","source":"WHOOP","source_id":source,
            "started_at":"2026-03-11T06:30:00Z","ended_at":"2026-03-11T06:30:00Z",
            "modified_at":"2026-03-12T00:00:00Z",
            "payload":{"metric":"recovery","provider_data":{"cycle_id":cycle,"sleep_id":sleep}}
        })
    })
    .collect::<Vec<_>>();
    let snapshot = |lease: &Value| {
        serde_json::from_value(json!({
            "lease":lease["lease"],"start":"2026-03-10T00:00:00Z","end":"2026-03-12T00:00:00Z",
            "grants":["metric"],"records":recovery_rows
        }))
    };
    let lease = crate::wearables::issue_lease_for(&pool, subject, "whoop").await?;
    crate::wearables::store_snapshot(&pool, subject, "whoop", snapshot(&lease)?, false).await?;
    let recovery_event = json!({"id":id,"type":"recovery.deleted"});
    enqueue(
        &pool,
        "whoop",
        "456",
        target("whoop", &recovery_event).unwrap(),
        b"whoop-recovery-fixture-delete",
    )
    .await?;
    let deletion = claim(&pool)
        .await?
        .context("missing WHOOP recovery deletion")?;
    let recovery_credentials = [
        "whoop-fixture-access".into(),
        "read:sleep read:recovery".into(),
    ];
    assert!(
        fetch_document(&client, &origin, &recovery_credentials, &deletion)
            .await
            .is_err()
    );
    recovery_code.store(403, Ordering::Relaxed);
    assert!(
        fetch_document(&client, &origin, &recovery_credentials, &deletion)
            .await
            .is_err()
    );
    for code in [404, 299] {
        recovery_code.store(code, Ordering::Relaxed);
        assert!(matches!(
            fetch_document(&client, &origin, &recovery_credentials, &deletion).await?,
            Document::Deleted
        ));
    }
    finish(
        &pool,
        &deletion,
        fetch_document(&client, &origin, &recovery_credentials, &deletion).await?,
    )
    .await?;
    let lease = crate::wearables::issue_lease_for(&pool, subject, "whoop").await?;
    crate::wearables::store_snapshot(&pool, subject, "whoop", snapshot(&lease)?, false).await?;
    let remaining = db
        .query(
            "SELECT source_id FROM avatar_hsm.wearable_record
        WHERE subject_id=$1 AND provider='whoop' AND payload->>'metric'='recovery'",
            &[&subject],
        )
        .await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].get::<_, String>(0), "recovery:93846");
    server.abort();
    crate::wearables::revoke_for(&pool, subject, "whoop").await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and synthetic AVATAR_OURA_CLIENT_SECRET"]
async fn signed_document_changes_and_deletions_are_owner_bound_and_resist_stale_sync() -> Result<()>
{
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let secret = std::env::var("AVATAR_OURA_CLIENT_SECRET")?;
    let subject = "oura-notify-object-fixture";
    let other = "oura-notify-other-fixture";
    let owner = "oura-signed-owner";
    let db = pool.get().await?;
    for (id, owner) in [(subject, owner), (other, "oura-other-owner")] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&id],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES
             ($1,sha256(convert_to($1||'-local-test-read-0123456789','UTF8')),sha256(convert_to($1||'c','UTF8')))",
            &[&id],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'oura')",
            &[&id],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection
            (subject_id,provider,sealed_tokens,provider_account_hash) VALUES($1,'oura',''::bytea,$2)",
            &[&id,&wearable_webhooks::hash(owner.as_bytes())]).await?;
        db.execute("INSERT INTO watch_tower.wearable_sync_schedule
            (subject_id,provider,next_sync,backfill_from) VALUES($1,'oura',now()+interval '1 day',now())",&[&id]).await?;
    }
    let app = Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let webhook_url = format!("http://{}/wearables/webhooks/oura", listener.local_addr()?);
    let mcp_url = format!("http://{}/mcp/wearable-read", listener.local_addr()?);
    let webhook = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let client = cloud_wearables::http_client()?;
    let event = |operation, time| {
        json!({"event_type":operation,"data_type":"sleep","object_id":"historical-sleep-id",
            "user_id":owner,"event_time":time})
        .to_string()
    };
    let body = event("update", "2026-03-14T09:00:00+09:00");
    let timestamp = Utc::now().timestamp_millis().to_string();
    let signed_post = |body: String| {
        let signature = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()),
            [timestamp.as_bytes(), body.as_bytes()].concat().as_slice(),
        )
        .as_ref()
        .iter()
        .map(|v| format!("{v:02X}"))
        .collect::<String>();
        client
            .post(&webhook_url)
            .header("X-Oura-Timestamp", &timestamp)
            .header("X-Oura-Signature", signature)
            .body(body)
            .send()
    };
    assert_eq!(
        client
            .post(&webhook_url)
            .body(body.clone())
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    for _ in 0..2 {
        assert_eq!(signed_post(body.clone()).await?.status(), StatusCode::OK);
    }
    let job = claim(&pool).await?.context("missing signed Oura job")?;
    assert_eq!(job.subject, subject);
    assert!(claim(&pool).await?.is_none());
    let upstream_code = Arc::new(AtomicU16::new(200));
    let activity_revision = Arc::new(AtomicU16::new(1));
    let sleep_type = Arc::new(AtomicU16::new(0));
    let upstream=Router::new().route("/sleep/historical-sleep-id",get({
        let upstream_code = upstream_code.clone();
        let sleep_type = sleep_type.clone();
        move |headers:HeaderMap| {
            let status = upstream_code.load(Ordering::Relaxed);
            let deleted = sleep_type.load(Ordering::Relaxed) == 1;
            async move {
                assert_eq!(headers["authorization"],"Bearer oura-sleep-synthetic");
                if status != 200 {
                    return Err(StatusCode::from_u16(status).unwrap());
                }
                Ok(Json(json!({"id":"historical-sleep-id","day":"2026-03-11","bedtime_start":"2026-03-10T23:00:00+09:00",
                    "bedtime_end":"2026-03-11T07:00:00+09:00","sleep_phase_30_sec":"1234",
                    "type":if deleted {"deleted"} else {"long_sleep"},"total_sleep_duration":28000,
                    "hrv":{"timestamp":"2026-03-10T23:00:00+09:00","interval":300,"items":[39]}})))
            }
        }
    })).route("/daily_activity/historical-activity-id",get({
        let upstream_code = upstream_code.clone();
        let activity_revision = activity_revision.clone();
        move |headers:HeaderMap| {
            let status = upstream_code.load(Ordering::Relaxed);
            let revision = activity_revision.load(Ordering::Relaxed);
            async move {
                assert_eq!(headers["authorization"],"Bearer oura-sleep-synthetic");
                if status != 200 {
                    return Err(StatusCode::from_u16(status).unwrap());
                }
                Ok(Json(json!({"id":"historical-activity-id","day":"2026-03-11",
                    "timestamp":"2026-03-11T04:00:00+09:00",
                    "class_5_min":if revision==1 {"012"} else {"345"},
                    "met":{"interval":60,"timestamp":"2026-03-11T04:00:00+09:00","items":[revision,null]}
                })))
            }
        }
    })).route("/workout/historical-workout-id", get(|headers: HeaderMap| async move {
        assert_eq!(headers["authorization"],"Bearer oura-sleep-synthetic");
        Json(json!({"id":"historical-workout-id","day":"2026-03-11",
            "start_datetime":"2026-03-11T15:00:00+09:00","end_datetime":"2026-03-11T15:45:00+09:00",
            "activity":"running","distance":5100.0,"calories":360.0}))
    })).route("/{kind}/historical-summary-id", get({
        let upstream_code = upstream_code.clone();
        move |axum::extract::Path(kind): axum::extract::Path<String>, headers: HeaderMap| {
            let status = upstream_code.load(Ordering::Relaxed);
            async move {
                assert_eq!(headers["authorization"],"Bearer oura-sleep-synthetic");
                if status != 200 {
                    return Err(StatusCode::from_u16(status).unwrap());
                }
                let mut summary = json!({"id":"historical-summary-id","day":"2026-03-11"});
                match kind.as_str() {
                    "daily_readiness" => { summary["score"]=json!(86); }
                    "daily_stress" => { summary["stress_high"]=json!(960); }
                    "daily_spo2" => { summary["spo2_percentage"]=json!({"average":97.25}); }
                    "daily_cardiovascular_age" => { summary["vascular_age"]=json!(47); }
                    "daily_sleep" => { summary["score"]=json!(88); }
                    "daily_resilience" => { summary["level"]=json!("solid"); }
                    _ => return Err(StatusCode::BAD_REQUEST),
                }
                Ok(Json(summary))
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await });
    assert!(matches!(
        fetch_document(
            &client,
            &origin,
            &["unrelated-token".into(), "workout".into()],
            &job
        )
        .await?,
        Document::NoAccess
    ));
    let credentials = ["oura-sleep-synthetic".into(), "daily".into()];
    let document = fetch_document(&client, &origin, &credentials, &job).await?;
    finish(&pool, &job, document).await?;
    let sessions = async |kind: &str| -> Result<Value> {
        let reply: Value = client
            .post(&mcp_url)
            .bearer_auth(format!("{subject}-local-test-read-0123456789"))
            .json(
                &json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                    "name":"wearable_sessions","arguments":{"kind":kind,"provider":"oura",
                        "start":"2026-03-10T00:00:00Z","end":"2026-03-12T00:00:00Z"}
                }}),
            )
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(reply["result"]["structuredContent"]["records"].clone())
    };
    let sleep = sessions("sleep").await?;
    let sleep = &sleep[0]["payload"];
    assert_eq!(sleep["provider_total_sleep_seconds"], 28000);
    assert_eq!(sleep["stage_resolution_seconds"], 30);
    assert_eq!(sleep["stages"].as_array().map(Vec::len), Some(4));
    assert_eq!(sleep["provider_data"]["hrv"]["items"], json!([39]));

    let stored = db
        .query(
            "SELECT subject_id,day::text FROM watch_tower.wearable_target_day
        WHERE subject_id=ANY($1) AND provider='oura' ORDER BY day",
            &[&vec![subject, other]],
        )
        .await?;
    assert!(stored.iter().all(|row| row.get::<_, String>(0) == subject));
    assert_eq!(
        stored
            .iter()
            .map(|row| row.get::<_, String>(1))
            .collect::<Vec<_>>(),
        ["2026-03-10", "2026-03-11"]
    );
    assert!(claim(&pool).await?.is_none());
    finish(
        &pool,
        &job,
        Document::Live {
            days: vec!["2026-03-08".parse()?],
            record: None,
        },
    )
    .await?;
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM watch_tower.wearable_target_day
        WHERE subject_id=$1 AND provider='oura'",
            &[&subject]
        )
        .await?
        .get::<_, i64>(0),
        2
    );
    let snapshot = |lease: &Value| {
        serde_json::from_value(json!({
            "lease":lease["lease"],"start":"2026-03-10T00:00:00Z","end":"2026-03-12T00:00:00Z",
            "grants":["sleep"],"records":[{
                "kind":"sleep","source":"Oura","source_id":"historical-sleep-id",
                "started_at":"2026-03-10T14:00:00Z","ended_at":"2026-03-10T22:00:00Z",
                "modified_at":"2026-03-12T00:00:00Z","payload":{"stages":[]}
            }]
        }))
    };
    for id in [subject, other] {
        let lease = crate::wearables::issue_lease_for(&pool, id, "oura").await?;
        crate::wearables::store_snapshot(&pool, id, "oura", snapshot(&lease)?, false).await?;
    }
    let stale_lease = crate::wearables::issue_lease_for(&pool, subject, "oura").await?;
    assert_eq!(
        signed_post(event("delete", "2026-03-16T00:00:00Z"))
            .await?
            .status(),
        StatusCode::OK
    );
    let delete = claim(&pool).await?.context("missing Oura deletion job")?;
    assert!(delete.delete_requested);
    for older in ["2026-03-15T09:00:00+09:00", "2026-03-16T09:00:00+09:00"] {
        assert_eq!(
            signed_post(event("update", older)).await?.status(),
            StatusCode::OK
        );
    }
    assert!(claim(&pool).await?.is_none());
    assert!(fetch_document(&client, &origin, &credentials, &delete)
        .await
        .is_err());
    upstream_code.store(403, Ordering::Relaxed);
    assert!(fetch_document(&client, &origin, &credentials, &delete)
        .await
        .is_err());
    upstream_code.store(404, Ordering::Relaxed);
    let document = fetch_document(&client, &origin, &credentials, &delete).await?;
    assert!(matches!(document, Document::Deleted));
    finish(&pool, &delete, document).await?;
    crate::wearables::store_snapshot(&pool, subject, "oura", snapshot(&stale_lease)?, false)
        .await?;
    let owners = db
        .query(
            "SELECT subject_id FROM avatar_hsm.wearable_record
         WHERE subject_id=ANY($1) AND provider='oura' AND source_id='historical-sleep-id'",
            &[&vec![subject, other]],
        )
        .await?;
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].get::<_, String>(0), other);

    upstream_code.store(200, Ordering::Relaxed);
    assert_eq!(
        signed_post(event("create", "2026-03-18T09:00:00+09:00"))
            .await?
            .status(),
        StatusCode::OK
    );
    let recreate = claim(&pool)
        .await?
        .context("missing recreated Oura document")?;
    let document = fetch_document(&client, &origin, &credentials, &recreate).await?;
    finish(&pool, &recreate, document).await?;
    assert_eq!(
        signed_post(event("delete", "2026-03-17T00:00:00Z"))
            .await?
            .status(),
        StatusCode::OK
    );
    assert!(claim(&pool).await?.is_none());
    let last = db
        .query_one(
            "SELECT event_at,completed_at IS NOT NULL FROM watch_tower.wearable_webhook_document
         WHERE subject_id=$1 AND object_id='historical-sleep-id'",
            &[&subject],
        )
        .await?;
    assert_eq!(
        last.get::<_, DateTime<Utc>>(0),
        "2026-03-18T00:00:00Z".parse::<DateTime<Utc>>()?
    );
    assert!(last.get::<_, bool>(1));
    let lease = crate::wearables::issue_lease_for(&pool, subject, "oura").await?;
    crate::wearables::store_snapshot(&pool, subject, "oura", snapshot(&lease)?, false).await?;
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM avatar_hsm.wearable_record
        WHERE subject_id=$1 AND provider='oura' AND source_id='historical-sleep-id'",
            &[&subject]
        )
        .await?
        .get::<_, i64>(0),
        1
    );
    let activity_event = |operation, time| {
        json!({"event_type":operation,"data_type":"daily_activity",
        "object_id":"historical-activity-id","user_id":owner,"event_time":time})
        .to_string()
    };
    let sample = async |metric: &str, owner_id: &str| -> Result<Value> {
        let response: Value = client.post(&mcp_url)
            .bearer_auth(format!("{owner_id}-local-test-read-0123456789"))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"wearable_samples","arguments":{"kind":"metric","metric":metric,"provider":"oura",
                   "start":"2026-03-10T19:00:00Z","end":"2026-03-10T19:01:00Z"}
            }})).send().await?.error_for_status()?.json().await?;
        Ok(response["result"]["structuredContent"]["records"].clone())
    };
    for (operation, event_at, revision, slots) in [
        (
            "create",
            "2026-03-19T00:00:00Z",
            1,
            json!(["non_wear", "rest", "inactive"]),
        ),
        (
            "update",
            "2026-03-20T00:00:00Z",
            2,
            json!(["low", "medium", "high"]),
        ),
    ] {
        activity_revision.store(revision, Ordering::Relaxed);
        assert_eq!(
            signed_post(activity_event(operation, event_at))
                .await?
                .status(),
            StatusCode::OK
        );
        let job = claim(&pool).await?.context("missing Oura activity job")?;
        let document = fetch_document(&client, &origin, &credentials, &job).await?;
        finish(&pool, &job, document).await?;
        let met = sample("activity_met", subject).await?;
        assert_eq!(met.as_array().map(Vec::len), Some(1));
        assert_eq!(met[0]["payload"]["provider_data"]["value"], revision);
        let activity = sample("daily_activity", subject).await?;
        assert_eq!(
            activity[0]["payload"]["activity_classification"]["slots"],
            slots
        );
        assert_eq!(sample("activity_met", other).await?, json!([]));
        assert!(claim(&pool).await?.is_none());
    }
    let stale_lease = wearables::issue_lease_for(&pool, subject, "oura").await?;
    upstream_code.store(404, Ordering::Relaxed);
    assert_eq!(
        signed_post(activity_event("delete", "2026-03-21T00:00:00Z"))
            .await?
            .status(),
        StatusCode::OK
    );
    let job = claim(&pool)
        .await?
        .context("missing Oura activity delete")?;
    let document = fetch_document(&client, &origin, &credentials, &job).await?;
    finish(&pool, &job, document).await?;
    let stale = serde_json::from_value(json!({
        "lease":stale_lease["lease"],"start":"2026-03-10T00:00:00Z","end":"2026-03-12T00:00:00Z",
        "grants":["metric"],"records":[{
            "kind":"metric","source":"Oura","source_id":"daily_activity:historical-activity-id",
            "started_at":"2026-03-10T19:00:00Z","ended_at":"2026-03-10T19:00:00Z",
            "modified_at":Utc::now(),"payload":{"metric":"daily_activity","provider_data":{"class_5_min":"012",
                "met":{"interval":60,"timestamp":"2026-03-11T04:00:00+09:00","items":[9]}}}
        }]
    }))?;
    wearables::store_snapshot(&pool, subject, "oura", stale, false).await?;
    assert_eq!(sample("activity_met", subject).await?, json!([]));
    assert_eq!(sample("daily_activity", subject).await?, json!([]));
    let summary_event = |kind, operation, at| {
        json!({"event_type":operation,
        "data_type":kind,"object_id":"historical-summary-id","user_id":owner,"event_time":at})
        .to_string()
    };
    let sample_summary = async |metric: &str| -> Result<Value> {
        let reply: Value = client.post(&mcp_url).bearer_auth(format!("{subject}-local-test-read-0123456789"))
            .json(&json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
                "name":"wearable_samples","arguments":{"kind":"metric","metric":metric,"provider":"oura",
                    "start":"2026-03-11T00:00:00Z","end":"2026-03-12T00:00:00Z"}
            }})).send().await?.error_for_status()?.json().await?;
        Ok(reply["result"]["structuredContent"]["records"].clone())
    };
    upstream_code.store(200, Ordering::Relaxed);
    assert_eq!(
        signed_post(summary_event(
            "daily_spo2",
            "create",
            "2026-03-22T00:00:00Z"
        ))
        .await?
        .status(),
        StatusCode::OK
    );
    let job = claim(&pool)
        .await?
        .context("missing Oura SpO2 scope check")?;
    let document = fetch_document(&client, &origin, &credentials, &job).await?;
    assert!(matches!(document, Document::NoAccess));
    finish(&pool, &job, document).await?;
    assert_eq!(sample_summary("oxygen_saturation").await?, json!([]));

    for (kind, scope, metric, field, expected) in [
        (
            "daily_readiness",
            "daily",
            "readiness_score",
            "value",
            json!(86),
        ),
        (
            "daily_stress",
            "daily",
            "high_stress_duration",
            "value",
            json!(960),
        ),
        (
            "daily_spo2",
            "spo2",
            "oxygen_saturation",
            "value",
            json!(97.25),
        ),
        (
            "daily_cardiovascular_age",
            "heart_health",
            "vascular_age",
            "value",
            json!(47),
        ),
        ("daily_sleep", "daily", "daily_sleep", "score", json!(88)),
        (
            "daily_resilience",
            "daily",
            "daily_resilience",
            "level",
            json!("solid"),
        ),
    ] {
        assert_eq!(
            signed_post(summary_event(kind, "update", "2026-03-23T00:00:00Z"))
                .await?
                .status(),
            StatusCode::OK
        );
        let job = claim(&pool).await?.context("missing Oura summary job")?;
        let credentials = ["oura-sleep-synthetic".into(), scope.into()];
        let document = fetch_document(&client, &origin, &credentials, &job).await?;
        finish(&pool, &job, document).await?;
        let saved = sample_summary(metric).await?;
        assert_eq!(saved.as_array().map(Vec::len), Some(1));
        assert_eq!(saved[0]["payload"]["provider_data"][field], expected);
    }
    upstream_code.store(404, Ordering::Relaxed);
    assert_eq!(
        signed_post(summary_event(
            "daily_spo2",
            "delete",
            "2026-03-24T00:00:00Z"
        ))
        .await?
        .status(),
        StatusCode::OK
    );
    let job = claim(&pool).await?.context("missing Oura SpO2 deletion")?;
    let document = fetch_document(
        &client,
        &origin,
        &["oura-sleep-synthetic".into(), "spo2".into()],
        &job,
    )
    .await?;
    finish(&pool, &job, document).await?;
    assert_eq!(sample_summary("oxygen_saturation").await?, json!([]));
    assert_eq!(
        sample_summary("readiness_score")
            .await?
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    upstream_code.store(200, Ordering::Relaxed);
    let workout_event =
        json!({"event_type":"create","data_type":"workout","object_id":"historical-workout-id",
        "user_id":owner,"event_time":"2026-03-25T00:00:00Z"})
        .to_string();
    assert_eq!(signed_post(workout_event).await?.status(), StatusCode::OK);
    let job = claim(&pool).await?.context("missing Oura workout job")?;
    assert!(matches!(
        fetch_document(&client, &origin, &credentials, &job).await?,
        Document::NoAccess
    ));
    let document = fetch_document(
        &client,
        &origin,
        &["oura-sleep-synthetic".into(), "workout".into()],
        &job,
    )
    .await?;
    finish(&pool, &job, document).await?;
    let workout = sessions("workout").await?;
    assert_eq!(workout.as_array().map(Vec::len), Some(1));
    assert_eq!(workout[0]["payload"]["duration_seconds"], 2700);
    assert_eq!(workout[0]["payload"]["exercise_type"], "running");
    assert_eq!(
        workout[0]["payload"]["distance_meters"].as_f64(),
        Some(5100.0)
    );

    sleep_type.store(1, Ordering::Relaxed);
    assert_eq!(
        signed_post(event("delete", "2026-03-26T00:00:00Z"))
            .await?
            .status(),
        StatusCode::OK
    );
    let job = claim(&pool)
        .await?
        .context("missing Oura sleep tombstone")?;
    let document = fetch_document(&client, &origin, &credentials, &job).await?;
    assert!(matches!(document, Document::Deleted));
    finish(&pool, &job, document).await?;
    assert_eq!(sessions("sleep").await?, json!([]));
    assert_eq!(sessions("workout").await?.as_array().map(Vec::len), Some(1));
    webhook.abort();
    server.abort();
    crate::wearables::revoke_for(&pool, subject, "oura").await?;
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM watch_tower.wearable_webhook_document WHERE subject_id=$1",
            &[&subject],
        )
        .await?
        .get::<_, i64>(0),
        0
    );
    Ok(())
}
