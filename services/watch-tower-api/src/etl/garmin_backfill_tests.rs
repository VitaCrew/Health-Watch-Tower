use super::*;
use anyhow::Context;

#[test]
fn retry_after_accepts_seconds_and_http_dates() {
    let now = DateTime::parse_from_rfc3339("2026-11-28T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    assert_eq!(retry_after(Some("45"), now), now + Duration::seconds(45));
    assert_eq!(
        retry_after(Some("Sat, 28 Nov 2026 00:02:00 GMT"), now),
        now + Duration::minutes(2)
    );
}

#[test]
fn invalid_retry_after_uses_five_minute_default() {
    let now = Utc::now();
    assert_eq!(
        retry_after(Some("invalid"), now),
        now + Duration::minutes(5)
    );
}

#[test]
fn request_uses_a_fixed_garmin_host_and_allowed_feed() -> Result<()> {
    let client = Client::new();
    let start = DateTime::parse_from_rfc3339("2026-04-01T00:00:00Z")?.with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339("2026-04-03T00:00:00Z")?.with_timezone(&Utc);
    let built_request = request(&client, "sleeps", start, end, "fixture-token".into())?.build()?;
    assert_eq!(built_request.method(), reqwest::Method::GET);
    assert_eq!(built_request.url().host_str(), Some("apis.garmin.com"));
    assert_eq!(
        built_request.url().path(),
        "/wellness-api/rest/backfill/sleeps"
    );
    assert_eq!(
        built_request.url().query(),
        Some("summaryStartTimeInSeconds=1775001600&summaryEndTimeInSeconds=1775174400")
    );
    assert!(request(&client, "unknown", start, end, "token".into()).is_err());
    assert!(request(&client, "sleeps", end, start, "token".into()).is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database in WATCH_TOWER_TEST_DATABASE_URL"]
async fn watch_tower_claim_is_exclusive_and_hsm_rechecks_consent() -> Result<()> {
    let url = env::var("WATCH_TOWER_TEST_DATABASE_URL")?;
    let _watch_tower_migrations = crate::store::connect(&url).await?;
    let hsm_pool = avatar_hsm::store::connect(&url).await?;
    let pool = crate::store::connect(&url).await?;
    let mut db = pool.get().await?;
    let subject: String = db
        .query_one("SELECT 'garmin-claim-' || gen_random_uuid()::text", &[])
        .await?
        .get(0);
    let start = Utc::now() - Duration::days(2);
    let end = Utc::now() - Duration::days(1);
    let tx = db.transaction().await?;
    tx.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
        &[&subject],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'garmin')",
        &[&subject],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_profile
         VALUES($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8'))) ",
        &[&subject],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_oauth_connection
         (subject_id,provider,sealed_tokens,garmin_permissions)
         VALUES($1,'garmin',''::bytea,0)",
        &[&subject],
    )
    .await?;
    tx.execute(
        "INSERT INTO watch_tower.garmin_backfill
         (subject_id,start_at,end_at,feed,permission)
         VALUES($1,$2,$3,'sleeps',2)",
        &[&subject, &start, &end],
    )
    .await?;
    tx.commit().await?;
    drop(db);

    let (left, right) = tokio::join!(claim(&pool), claim(&pool));
    let left = left?;
    let right = right?;
    assert_ne!(
        left.is_some(),
        right.is_some(),
        "one lease must win the claim"
    );
    let claimed = left
        .or(right)
        .context("the due queue item was not claimed")?;
    let authorization =
        etl_adapter::garmin_backfill_token(&hsm_pool, &Client::new(), &claimed.job).await?;
    assert!(matches!(
        authorization,
        GarminBackfillAuthorization::PermissionRemoved
    ));

    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    tx.execute(
        "DELETE FROM watch_tower.garmin_backfill WHERE subject_id=$1",
        &[&subject],
    )
    .await?;
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1 AND provider='garmin'",
        &[&subject],
    )
    .await?;
    tx.execute(
        "DELETE FROM avatar_hsm.device_connection WHERE subject_id=$1",
        &[&subject],
    )
    .await?;
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_profile WHERE subject_id=$1",
        &[&subject],
    )
    .await?;
    tx.execute("DELETE FROM avatar_hsm.subject WHERE id=$1", &[&subject])
        .await?;
    tx.commit().await?;
    Ok(())
}
