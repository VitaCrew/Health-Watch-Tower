use super::*;
use serde_json::json;

const SAMPLE_URL: &str = "https://apis.garmin.com/wellness-api/rest/activities?uploadStartTimeInSeconds=1777012200&uploadEndTimeInSeconds=1777012300&token=fixture";

fn activity(user: &str, id: i64) -> Value {
    json!({"userId":user,"activityId":id,"startTimeInSeconds":1777012200,"durationInSeconds":1500,"activityType":"RUNNING"})
}

#[test]
fn callback_is_bound_to_feed_and_response_to_account() -> Result<()> {
    assert!(source("activities", SAMPLE_URL).is_ok());
    assert!(source("sleeps", SAMPLE_URL).is_err());
    assert!(source(
        "activities",
        &SAMPLE_URL.replace("apis.garmin.com", "apis.garmin.com.invalid")
    )
    .is_err());
    assert!(source(
        "activities",
        &SAMPLE_URL.replace("activities?", "activityFile?")
    )
    .is_err());
    assert!(source("activities", &format!("{SAMPLE_URL}&userId=other")).is_err());
    assert!(source(
        "activities",
        &format!("{SAMPLE_URL}&uploadEndTimeInSeconds=1")
    )
    .is_err());
    let account = hash(b"fixture-owner");
    assert_eq!(
        decode_records(
            &json!([activity("fixture-owner", 1)]),
            "activities",
            &account,
            Utc::now()
        )?
        .len(),
        1
    );
    assert!(decode_records(
        &json!([activity("another-owner", 2)]),
        "activities",
        &account,
        Utc::now()
    )
    .is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn pull_completion_is_atomic_with_storage_and_connection_revocation() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let subject = "garmin-pull-fixture";
    let account = hash(b"fixture-owner");
    let first_url = source("activities", SAMPLE_URL)?;
    let second_url = source(
        "activities",
        &SAMPLE_URL.replace("token=fixture", "token=second"),
    )?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))", &[&subject]).await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'garmin')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash,garmin_permissions)
        VALUES($1,'garmin',''::bytea,$2,1)", &[&subject,&account]).await?;
    // The worker's database and record steps need no Garmin credentials in this fixture.
    if let (Ok(path_key), Ok(client_id), Some(_)) = (
        std::env::var("AVATAR_GARMIN_WEBHOOK_PATH_KEY"),
        std::env::var("AVATAR_GARMIN_CLIENT_ID"),
        std::env::var_os("AVATAR_WEARABLE_OAUTH_KEY"),
    ) {
        let app = std::sync::Arc::new(crate::mcp::App {
            db: pool.clone(),
            read_token: String::new(),
            control_token: String::new(),
            rules: crate::acute::rules()?,
        });
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let endpoint = format!(
            "http://{}/wearables/webhooks/garmin",
            listener.local_addr()?
        );
        let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
        let client = cloud_wearables::http_client()?;
        let payload = json!({"activities":[
            {"userId":"fixture-owner","callbackURL":first_url.as_str()},
            {"userId":"fixture-owner","callbackURL":second_url.as_str()},
            {"userId":"another-owner","callbackURL":second_url.as_str()}
        ]});
        let valid_url = format!("{endpoint}/{path_key}");
        assert_eq!(
            client
                .post(format!("{endpoint}/bad-key"))
                .json(&payload)
                .send()
                .await?
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            client
                .post(&valid_url)
                .json(&payload)
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
        for _ in 0..2 {
            assert_eq!(
                client
                    .post(&valid_url)
                    .header("garmin-client-id", &client_id)
                    .json(&payload)
                    .send()
                    .await?
                    .status(),
                StatusCode::OK
            );
        }
        server.abort();
    } else if std::env::var_os("AVATAR_WEARABLE_OAUTH_KEY").is_some() {
        let mut db = pool.get().await?;
        let tx = db.transaction().await?;
        enqueue(&tx, subject, "activities", 1, &first_url).await?;
        enqueue(&tx, subject, "activities", 1, &first_url).await?;
        enqueue(&tx, subject, "activities", 1, &second_url).await?;
        tx.commit().await?;
    } else {
        for url in [&first_url, &second_url] {
            db.execute("INSERT INTO watch_tower.garmin_pull_job(subject_id,feed,permission,request_hash,sealed_source)
                VALUES($1,'activities',1,$2,'test'::bytea)", &[&subject,&hash(url.as_str().as_bytes())]).await?;
        }
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM watch_tower.garmin_pull_job WHERE subject_id=$1",
            &[&subject]
        )
        .await?
        .get::<_, i64>(0),
        2
    );
    let original = claim(&pool).await?.context("missing pull job")?;
    let retry_at = Utc::now() + chrono::Duration::minutes(15);
    let error = cloud_wearables::ProviderThrottled { retry_at }.into();
    retry(&pool, &original, &error).await?;
    let waiting = db
        .query_one(
            "SELECT attempts,sealed_source IS NOT NULL, next_attempt>now()+interval '14 minutes'
         FROM watch_tower.garmin_pull_job WHERE id=$1::text::uuid",
            &[&original.id],
        )
        .await?;
    assert_eq!(waiting.get::<_, i16>(0), 0);
    assert!(waiting.get::<_, bool>(1) && waiting.get::<_, bool>(2));
    // The other callback is due, but both requests share this connection's cooldown.
    assert!(claim(&pool).await?.is_none());
    db.execute("UPDATE avatar_hsm.wearable_oauth_connection SET garmin_retry_after=now() WHERE subject_id=$1", &[&subject]).await?;
    db.execute("UPDATE watch_tower.garmin_pull_job SET next_attempt=now()-interval '1 hour' WHERE id=$1::text::uuid", &[&original.id]).await?;
    let job = claim(&pool).await?.context("missing deferred pull job")?;
    assert_eq!(job.id, original.id);
    assert_ne!(job.lease, original.lease);
    assert_ne!(job.sealed, SAMPLE_URL.as_bytes());
    save(
        &pool,
        &job,
        &decode_records(
            &json!([activity("fixture-owner", 123)]),
            "activities",
            &account,
            job.received,
        )?,
    )
    .await?;
    let stored = db
        .query_one(
            "SELECT status,sealed_source FROM watch_tower.garmin_pull_job WHERE id=$1::text::uuid",
            &[&job.id],
        )
        .await?;
    assert_eq!(stored.get::<_, String>(0), "stored");
    assert!(stored.get::<_, Option<Vec<u8>>>(1).is_none());
    assert_eq!(db.query_one("SELECT source_id FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='garmin' AND kind='workout'", &[&subject]).await?.get::<_,String>(0),"123");
    let pending = claim(&pool).await?.context("missing second pull job")?;
    crate::wearables::revoke_for(&pool, subject, "garmin").await?;
    save(
        &pool,
        &pending,
        &decode_records(
            &json!([activity("fixture-owner", 456)]),
            "activities",
            &account,
            pending.received,
        )?,
    )
    .await?;
    assert!(claim(&pool).await?.is_none());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM avatar_hsm.wearable_record WHERE subject_id=$1",
            &[&subject]
        )
        .await?
        .get::<_, i64>(0),
        0
    );
    Ok(())
}
