use super::*;
use anyhow::{Context, Result};
use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};

async fn fixture(
    status: StatusCode,
    retry_after: Option<&'static str>,
) -> (Client, String, tokio::task::JoinHandle<()>) {
    let app = Router::new().route(
        "/",
        get(move || async move {
            let mut response = (status, r#"[{"id":1}]"#).into_response();
            if let Some(value) = retry_after {
                response
                    .headers_mut()
                    .insert(reqwest::header::RETRY_AFTER, value.parse().unwrap());
            }
            response
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (Client::new(), url, server)
}

#[tokio::test]
async fn provider_json_is_read_by_watch_tower() {
    let (client, url, server) = fixture(StatusCode::OK, None).await;
    let response = client.get(url).send().await.unwrap();
    let value = response_body(response).await.unwrap();
    assert_eq!(value[0]["id"], 1);
    server.abort();
}

#[test]
fn normalizes_garmin_pull_rows_and_removes_credentials() {
    let received = DateTime::parse_from_rfc3339("2026-04-24T12:00:00Z")
        .unwrap()
        .to_utc();
    let rows = serde_json::json!([{
        "userId":"owner-1","userAccessToken":"private-token","activityId":123,
        "startTimeInSeconds":1777012200,"durationInSeconds":1500,
        "activityType":"RUNNING","distanceInMeters":5000
    }]);
    let records = normalize_records(&rows, "activities", received).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].owner_id.as_deref(), Some("owner-1"));
    assert_eq!(records[0].source_id, "123");
    assert_eq!(records[0].kind, "workout");
    assert!(records[0].payload["provider_data"]["userAccessToken"].is_null());
}

#[tokio::test]
async fn provider_throttle_preserves_retry_after_for_queue() {
    let (client, url, server) = fixture(StatusCode::TOO_MANY_REQUESTS, Some("120")).await;
    let response = client.get(url).send().await.unwrap();
    let error = response_body(response).await.unwrap_err();
    let retry_at = error
        .downcast_ref::<ProviderRateLimited>()
        .unwrap()
        .retry_at;
    assert!(retry_at > Utc::now() + chrono::Duration::seconds(100));
    server.abort();
}

#[tokio::test]
async fn provider_error_body_is_not_treated_as_health_data() {
    let (client, url, server) = fixture(StatusCode::BAD_GATEWAY, None).await;
    let response = client.get(url).send().await.unwrap();
    assert!(response_body(response).await.is_err());
    server.abort();
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database in WATCH_TOWER_TEST_DATABASE_URL"]
async fn watch_tower_claim_is_exclusive_and_hsm_rechecks_garmin_consent() -> Result<()> {
    let url = std::env::var("WATCH_TOWER_TEST_DATABASE_URL")?;
    let pool = crate::store::connect(&url).await?;
    let hsm = avatar_hsm::store::connect(&url).await?;
    let db = pool.get().await?;
    let subject: String = db
        .query_one("SELECT 'garmin-pull-' || gen_random_uuid()::text", &[])
        .await?
        .get(0);
    let account: Vec<u8> = db
        .query_one(
            "SELECT sha256(convert_to('pull-owner-fixture','UTF8'))",
            &[],
        )
        .await?
        .get(0);
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'garmin')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8'))) ", &[&subject]).await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash,garmin_permissions) VALUES($1,'garmin',''::bytea,$2,0)", &[&subject,&account]).await?;
    db.execute("INSERT INTO watch_tower.garmin_pull_job(subject_id,feed,permission,request_hash,sealed_source) VALUES($1,'activities',1,$2,'opaque-test'::bytea)", &[&subject,&vec![9u8;32]]).await?;
    drop(db);

    let (first, second) = tokio::join!(claim(&pool), claim(&pool));
    let (first, second) = (first?, second?);
    assert_ne!(
        first.is_some(),
        second.is_some(),
        "one concurrent worker must own the lease"
    );
    let job = first
        .or(second)
        .context("due Garmin callback was not claimed")?;
    assert!(matches!(
        etl_adapter::authorize_garmin_pull(
            &hsm,
            &Client::new(),
            &job.id,
            &job.lease,
            &job.subject,
            &job.feed,
            job.sealed.clone(),
            job.request_hash.clone()
        )
        .await?,
        avatar_hsm::etl_adapter::GarminPullAuthorizationResult::PermissionRemoved
    ));

    let db = pool.get().await?;
    db.execute(
        "UPDATE avatar_hsm.wearable_oauth_connection SET garmin_permissions=1 WHERE subject_id=$1 AND provider='garmin'",
        &[&subject],
    ).await?;
    let at = Utc::now() - chrono::Duration::minutes(20);
    assert!(
        etl_adapter::commit_garmin_pull(
            &pool,
            &job.id,
            &job.lease,
            &subject,
            "activities",
            account,
            vec![avatar_hsm::etl_adapter::GarminPullRecord {
                owner_id: Some("pull-owner-fixture".to_owned()),
                kind: "workout".to_owned(),
                source: "Garmin Connect".to_owned(),
                source_id: "fixture-activity".to_owned(),
                started_at: at,
                ended_at: at + chrono::Duration::minutes(20),
                modified_at: at,
                payload: serde_json::json!({"exercise_type":"running","duration_unknown":false}),
            }],
        )
        .await?
    );
    let state = db.query_one(
        "SELECT j.status,(SELECT count(*) FROM avatar_hsm.wearable_record r WHERE r.subject_id=j.subject_id AND r.provider='garmin' AND r.source_id='fixture-activity') FROM watch_tower.garmin_pull_job j WHERE j.id=$1::text::uuid",
        &[&job.id],
    ).await?;
    assert_eq!(state.get::<_, String>(0), "stored");
    assert_eq!(state.get::<_, i64>(1), 1);

    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    tx.execute(
        "DELETE FROM watch_tower.garmin_pull_job WHERE subject_id=$1",
        &[&subject],
    )
    .await?;
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='garmin'",
        &[&subject],
    )
    .await?;
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_daily WHERE subject_id=$1 AND provider='garmin'",
        &[&subject],
    )
    .await?;
    tx.execute("DELETE FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1 AND provider='garmin'", &[&subject]).await?;
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
