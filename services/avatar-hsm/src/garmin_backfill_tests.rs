use super::*;

#[test]
fn backfill_request_targets_one_fixed_garmin_feed() -> Result<()> {
    let mut job = Job {
        subject: "fixture".into(),
        start: "2026-04-01T00:00:00Z".parse()?,
        end: "2026-04-03T00:00:00Z".parse()?,
        feed: "sleeps".into(),
        lease: "fixture".into(),
    };
    let client = crate::cloud_wearables::http_client()?;
    let request_fixture = request(&client, &job, "fixture-token")?.build()?;
    assert_eq!(request_fixture.method(), reqwest::Method::GET);
    assert_eq!(request_fixture.url().host_str(), Some("apis.garmin.com"));
    assert_eq!(
        request_fixture.url().path(),
        "/wellness-api/rest/backfill/sleeps"
    );
    assert_eq!(
        request_fixture.url().query(),
        Some("summaryStartTimeInSeconds=1775001600&summaryEndTimeInSeconds=1775174400")
    );
    job.feed = "unrecognized".into();
    assert!(request(&client, &job, "fixture-token").is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn backfill_is_deduplicated_per_permitted_feed_and_reports_request_status() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let subject = "garmin-backfill-fixture";
    tx.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'garmin')",
        &[&subject],
    )
    .await?;
    tx.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))", &[&subject]).await?;
    tx.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,garmin_permissions) VALUES($1,'garmin',''::bytea,$2)", &[&subject,&HEALTH_EXPORT]).await?;
    let end = now();
    let start = end - Duration::days(2);
    assert_eq!(enqueue(&tx, subject, HEALTH_EXPORT, start, end).await?, 3);
    assert_eq!(enqueue(&tx, subject, HEALTH_EXPORT, start, end).await?, 0);
    tx.commit().await?;
    let first = claim(&pool).await?.context("backfill job missing")?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/", listener.local_addr()?);
    let upstream = axum::Router::new().route(
        "/",
        axum::routing::get(|| async { (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "720")]) }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await });
    let response = cloud_wearables::http_client()?.get(endpoint).send().await?;
    finish(&pool, &first, classify_response(response)).await?;
    server.abort();
    let rate_limited = status(&pool, subject).await?;
    assert_eq!(rate_limited["requests"][0]["http_status"], 429);
    assert_eq!(rate_limited["requests"][0]["status"], "queued");
    assert_eq!(
        rate_limited["requests"][0]["next_attempt"],
        rate_limited["requests"][1]["next_attempt"]
    );
    let row = db.query_one(
        "SELECT attempts,next_attempt>now()+interval '11 minutes' FROM watch_tower.garmin_backfill
         WHERE subject_id=$1 AND feed=$2", &[&subject,&first.feed],
    ).await?;
    assert_eq!(row.get::<_, i32>(0), 0);
    assert!(row.get::<_, bool>(1));
    assert!(claim(&pool).await?.is_none());
    db.execute("UPDATE avatar_hsm.wearable_oauth_connection SET garmin_retry_after=now() WHERE subject_id=$1", &[&subject]).await?;
    db.execute(
        "UPDATE watch_tower.garmin_backfill SET next_attempt=now() WHERE subject_id=$1",
        &[&subject],
    )
    .await?;
    let resumed = claim(&pool).await?.context("deferred backfill missing")?;
    assert_eq!(resumed.feed, first.feed);
    finish(&pool, &resumed, Ok(StatusCode::ACCEPTED)).await?;
    let second = claim(&pool).await?.context("backfill job missing")?;
    finish(&pool, &second, Ok(StatusCode::CONFLICT)).await?;
    let states = status(&pool, subject).await?;
    assert_eq!(states["requests"][0]["status"], "accepted");
    assert_eq!(states["requests"][1]["status"], "duplicate");
    assert_eq!(states["requests"][2]["status"], "queued");
    assert!(!states["requests"].to_string().contains("activity"));
    crate::wearables::revoke_for(&pool, subject, "garmin").await?;
    assert!(status(&pool, subject).await?["requests"]
        .as_array()
        .is_some_and(Vec::is_empty));
    Ok(())
}
