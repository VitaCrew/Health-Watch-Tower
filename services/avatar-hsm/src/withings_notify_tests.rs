use super::*;
use axum::{
    extract::{Form, State},
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

const TEST_KEY: &str = "synthetic-registration-key-012345678901234567890";

#[test]
fn callback_and_scopes_are_restricted() -> Result<()> {
    let url = callback("https://wearables.example.org", TEST_KEY).context("missing callback")?;
    assert_eq!(
        url.path(),
        format!("/wearables/webhooks/withings/{TEST_KEY}")
    );
    for invalid in [
        "http://wearables.example.org",
        "https://localhost",
        "https://127.0.0.1",
        "https://alt.localhost",
        "https://wearables.example.org:8443",
        "https://wearables.example.org/path",
    ] {
        assert!(callback(invalid, TEST_KEY).is_none());
    }
    assert!(callback("https://wearables.example.org", "short").is_none());
    assert_eq!(categories("user.activity"), [16, 44]);
    assert_eq!(
        categories("user.metrics,user.info"),
        [1, 2, 4, 54, 58, 62, 63]
    );
    assert!(categories("user.info user.activity.write").is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL in AVATAR_HSM_TEST_DATABASE_URL"]
async fn reconciles_existing_users_and_retries_ambiguous_result_without_duplicate() -> Result<()> {
    let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let subject = "withings-notify-fixture";
    let other = "withings-without-scope-fixture";
    for owner in [subject, other] {
        db.execute("INSERT INTO avatar_hsm.subject VALUES($1,'UTC')", &[&owner])
            .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES
             ($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))",
            &[&owner],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'withings')",
            &[&owner],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_oauth_connection
             (subject_id,provider,sealed_tokens,provider_account_hash)
             VALUES($1,'withings',''::bytea,sha256(convert_to($1,'UTF8')))",
            &[&owner],
        )
        .await?;
    }
    let url = callback("https://wearables.example.org", TEST_KEY).context("missing callback")?;
    let config = wearable_webhooks::hash(url.as_str().as_bytes());
    let generation: String = db
        .query_one(
            "SELECT generation::text FROM avatar_hsm.wearable_oauth_connection
         WHERE subject_id=$1 AND provider='withings'",
            &[&subject],
        )
        .await?
        .get(0);
    let other_generation: String = db
        .query_one(
            "SELECT generation::text FROM avatar_hsm.wearable_oauth_connection
         WHERE subject_id=$1 AND provider='withings'",
            &[&other],
        )
        .await?
        .get(0);
    let vendor = Arc::new(Mutex::new((vec![16u16], vec![])));
    let router = Router::new().route("/notify",post({
        let url = url.clone();
        move |State(vendor):State<Arc<Mutex<(Vec<u16>,Vec<String>)>>>,headers:HeaderMap,Form(body):Form<HashMap<String,String>>| {
            let url = url.clone();
            async move {
                if headers.get("authorization").and_then(|v|v.to_str().ok()) != Some("Bearer withings-owner-fixture") {
                    return Err(StatusCode::UNAUTHORIZED);
                }
                let mut vendor = vendor.lock().await;
                let action = &body["action"];
                vendor.1.push(action.clone());
                match action.as_str() {
                    "list" => Ok(Json(json!({"status":0,"body":{"profiles":vendor.0.iter()
                        .map(|category|json!({"callbackurl":url.as_str(),"appli":category.to_string()})).collect::<Vec<_>>()}}))),
                    "subscribe" => {
                        assert_eq!(body["callbackurl"],url.as_str());
                        let category = body["appli"].parse::<u16>().unwrap();
                        assert_eq!(category,44);
                        assert!(!vendor.0.contains(&category));
                        vendor.0.push(category);
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
    let client = cloud_wearables::http_client()?;
    let credentials = || {
        Ok(vec![
            "withings-owner-fixture".into(),
            "user.activity".into(),
        ])
    };
    reconcile(
        &pool,
        &client,
        &endpoint,
        (other, &other_generation),
        &url,
        &config,
        Ok(vec!["other-owner-fixture".into(), "user.info".into()]),
    )
    .await?;
    assert!(vendor.lock().await.1.is_empty());
    db.execute(
        "UPDATE avatar_hsm.wearable_oauth_connection SET withings_notify_next='-infinity'
         WHERE subject_id=$1 AND provider='withings'",
        &[&subject],
    )
    .await?;
    let selection = next_account(&pool, &config)
        .await?
        .context("expected existing account")?;
    assert_eq!(selection, (subject.into(), generation.clone()));
    reconcile(
        &pool,
        &client,
        &endpoint,
        (subject, &generation),
        &url,
        &config,
        credentials(),
    )
    .await?;
    let state = db
        .query_one(
            "SELECT withings_notify_state,withings_notify_next>now()
        FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1 AND provider='withings'",
            &[&subject],
        )
        .await?;
    assert_eq!(state.get::<_, String>(0), "retrying");
    assert!(state.get::<_, bool>(1));
    reconcile(
        &pool,
        &client,
        &endpoint,
        (subject, &generation),
        &url,
        &config,
        credentials(),
    )
    .await?;
    assert_eq!(vendor.lock().await.1, ["list", "subscribe"]);
    db.execute(
        "UPDATE avatar_hsm.wearable_oauth_connection SET withings_notify_next=now()
        WHERE subject_id=$1 AND provider='withings'",
        &[&subject],
    )
    .await?;
    reconcile(
        &pool,
        &client,
        &endpoint,
        (subject, &generation),
        &url,
        &config,
        credentials(),
    )
    .await?;
    assert_eq!(vendor.lock().await.1, ["list", "subscribe", "list"]);
    assert!(next_account(&pool, &config)
        .await?
        .is_none_or(|(owner, _)| owner != subject && owner != other));
    let state: String = db
        .query_one(
            "SELECT withings_notify_state FROM avatar_hsm.wearable_oauth_connection
        WHERE subject_id=$1 AND provider='withings'",
            &[&subject],
        )
        .await?
        .get(0);
    assert_eq!(state, "active");
    crate::wearables::revoke_for(&pool, subject, "withings").await?;
    reconcile(
        &pool,
        &client,
        &endpoint,
        (subject, &generation),
        &url,
        &config,
        credentials(),
    )
    .await?;
    assert_eq!(vendor.lock().await.1.len(), 3);
    server.abort();
    Ok(())
}
