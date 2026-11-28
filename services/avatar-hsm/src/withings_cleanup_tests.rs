use super::*;
use anyhow::Context;
use axum::{
    extract::{Form, State},
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[tokio::test]
#[ignore = "requires isolated PostgreSQL, synthetic public callback and OAuth key"]
async fn unlink_durably_revokes_only_matching_remote_subscriptions() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let subject = format!("withings-cleanup-owner-{suffix}");
    let account_label = format!("withings-cleanup-account-{suffix}");
    let account = crate::wearable_webhooks::hash(account_label.as_bytes());
    let credentials = wearable_oauth::synthetic_withings_credentials(&subject)?;
    let callback = withings_notify::configured_callback().context("missing synthetic callback")?;
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
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'withings')",
        &[&subject],
    )
    .await?;
    let insert = "INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash)
                  VALUES($1,'withings',$2,$3)";
    db.execute(insert, &[&subject, &credentials, &account])
        .await?;
    crate::wearables::revoke_for(&pool, &subject, "withings").await?;
    let job = next(&pool).await?.context("unlink did not queue cleanup")?;
    assert_eq!(job.subject, subject);
    let lease_id = "a223644f-0f0c-46e0-9167-b839f705c8db";
    db.execute(
        "UPDATE watch_tower.withings_notify_cleanup SET job_id=$2::text::uuid,leased_until=now()+interval '2 minutes' WHERE subject_id=$1",
        &[&subject,&lease_id],
    ).await?;
    match authorize_watch_tower(&pool, &subject, &account, &job.generation, lease_id).await? {
        WatchTowerAuthorization::Ready {
            token,
            callback: authorized_callback,
        } => {
            assert_eq!(token, "withings-cleanup-fixture-access");
            assert_eq!(authorized_callback, callback.as_str());
        }
        WatchTowerAuthorization::Skip => anyhow::bail!("valid cleanup lease was denied"),
    }
    assert!(matches!(
        authorize_watch_tower(
            &pool,
            &subject,
            &account,
            &job.generation,
            "00000000-0000-0000-0000-000000000000"
        )
        .await?,
        WatchTowerAuthorization::Skip
    ));
    let row = db
        .query_one(
            "SELECT sealed_cleanup,expires_at<=now()+interval '30 minutes',
        NOT EXISTS (SELECT 1 FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1)
        FROM watch_tower.withings_notify_cleanup WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
    assert!(row.get::<_, bool>(1));
    assert!(row.get::<_, bool>(2));
    let (token, url) =
        wearable_oauth::open_withings_cleanup(&subject, &job.generation, &account, row.get(0))?;
    assert_eq!(token, "withings-cleanup-fixture-access");
    assert_eq!(url, callback.as_str());

    let vendor = Arc::new(Mutex::new((true, Vec::<String>::new())));
    let router = Router::new().route("/notify",post({
        let callback = callback.clone();
        move |State(vendor):State<Arc<Mutex<(bool,Vec<String>)>>>,headers:HeaderMap,Form(body):Form<HashMap<String,String>>| {
            let callback = callback.clone();
            async move {
                assert_eq!(headers["authorization"],"Bearer withings-cleanup-fixture-access");
                let mut vendor = vendor.lock().await;
                let action = &body["action"];
                vendor.1.push(action.clone());
                match action.as_str() {
                    "list" => {
                        let mut profiles = vec![
                            json!({"appli":16,"callbackurl":"https://unrelated.example/receive"}),
                            json!({"appli":99,"callbackurl":callback.as_str()}),
                        ];
                        if vendor.0 {
                            profiles.push(json!({"appli":16,"callbackurl":callback.as_str()}));
                        }
                        Ok(Json(json!({"status":0,"body":{"profiles":profiles}})))
                    }
                    "revoke" => {
                        assert_eq!(body["callbackurl"],callback.as_str());
                        assert_eq!(body["appli"],"16");
                        vendor.0=false;
                        Err(StatusCode::SERVICE_UNAVAILABLE)
                    }
                    _ => Err(StatusCode::BAD_REQUEST),
                }
            }
        }
    })).with_state(vendor.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/notify", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let client = crate::cloud_wearables::http_client()?;
    run(&pool, &client, &endpoint, &job).await?;
    assert!(next(&pool).await?.is_none());
    db.execute(
        "UPDATE watch_tower.withings_notify_cleanup SET next_attempt=now() WHERE subject_id=$1",
        &[&subject],
    )
    .await?;
    run(&pool, &client, &endpoint, &job).await?;
    assert_eq!(vendor.lock().await.1, ["list", "revoke", "list"]);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM watch_tower.withings_notify_cleanup WHERE subject_id=$1",
            &[&subject]
        )
        .await?
        .get::<_, i64>(0),
        0
    );

    db.execute(insert, &[&subject, &credentials, &account])
        .await?;
    crate::wearables::revoke_for(&pool, &subject, "withings").await?;
    let queued = next(&pool)
        .await?
        .context("missing cleanup before reconnect")?;
    let reconnect_lease = "b823644f-0f0c-46e0-9167-b839f705c8dc";
    db.execute(
        "UPDATE watch_tower.withings_notify_cleanup SET job_id=$2::text::uuid,leased_until=now()+interval '2 minutes' WHERE subject_id=$1",
        &[&subject,&reconnect_lease],
    ).await?;
    db.execute(insert, &[&subject, &credentials, &account])
        .await?;
    assert!(matches!(
        authorize_watch_tower(
            &pool,
            &subject,
            &account,
            &queued.generation,
            reconnect_lease
        )
        .await?,
        WatchTowerAuthorization::Skip
    ));
    run(&pool, &client, &endpoint, &queued).await?;
    assert_eq!(vendor.lock().await.1.len(), 3);
    assert!(next(&pool).await?.is_none());
    server.abort();
    Ok(())
}
