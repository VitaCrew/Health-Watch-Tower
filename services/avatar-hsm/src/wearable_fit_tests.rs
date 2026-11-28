use super::*;
use axum::body::to_bytes;
use chrono::{TimeZone, Utc};
use std::io::Write;

fn summary_job(provider: Provider, activity: &str) -> Job {
    Job {
        subject: "summary-contract-test".into(),
        provider,
        activity: activity.into(),
        category: None,
        id: "summary-job-id".into(),
        sealed_source: None,
    }
}

fn normalized_summary(source_id: &str, kind: &str) -> crate::wearables::Record {
    let started_at = Utc.with_ymd_and_hms(2026, 1, 1, 7, 0, 0).unwrap();
    crate::wearables::Record {
        kind: kind.into(),
        source: "Polar".into(),
        source_id: source_id.into(),
        started_at,
        ended_at: started_at + chrono::Duration::minutes(30),
        modified_at: started_at,
        payload: serde_json::json!({"exercise_type":"running"}),
    }
}

#[test]
fn watch_tower_summary_contract_accepts_matching_normalized_workout() {
    let job = summary_job(Provider::Polar, "activity-1");
    let record = normalized_summary("activity-1", "workout");
    assert!(validate_watch_tower_summary(&job, &record).is_ok());
}

#[test]
fn watch_tower_summary_contract_rejects_wrong_identity_kind_and_provider() {
    let job = summary_job(Provider::Polar, "activity-1");
    assert!(validate_watch_tower_summary(&job, &normalized_summary("other", "workout")).is_err());
    assert!(
        validate_watch_tower_summary(&job, &normalized_summary("activity-1", "metric")).is_err()
    );
    let unsupported = summary_job(Provider::Garmin, "activity-1");
    assert!(validate_watch_tower_summary(
        &unsupported,
        &normalized_summary("activity-1", "workout")
    )
    .is_err());
}

#[test]
fn watch_tower_summary_contract_rejects_missing_workout_fields() {
    let job = summary_job(Provider::Suunto, "activity-2");
    let mut record = normalized_summary("activity-2", "workout");
    record.payload = serde_json::json!({"provider_data":{}});
    assert!(validate_watch_tower_summary(&job, &record).is_err());
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn intervals_stores_original_gzip_fit_for_the_connected_activity_only() -> Result<()> {
    use axum::{extract::Path, http::HeaderMap, routing::get, Router};
    use chrono::{Duration, Utc};
    use serde_json::json;

    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "intervals-fit-fixture";
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
        &[&subject],
    )
    .await?;
    let lease = crate::wearables::issue_lease_for(&pool, subject, "intervals_icu").await?;
    let end = Utc::now() - Duration::days(2);
    let start = end - Duration::hours(1);
    let workout = |id, file_type, source| {
        json!({
            "kind":"workout","source":"Intervals.icu","source_id":id,
            "started_at":start,"ended_at":end,"modified_at":end,
            "payload":{"exercise_type":"Ride","provider_data":{"file_type":file_type,"source":source}}
        })
    };
    let snapshot = serde_json::from_value(json!({"lease":lease["lease"],"start":start,"end":end,
        "grants":["workout"],"records":[workout("i900","fit","UPLOAD"),
            workout("i901","tcx","UPLOAD"),workout("i902","fit","STRAVA")]}))?;
    crate::wearables::store_snapshot(&pool, subject, "intervals_icu", snapshot, false).await?;
    let rows = db.query("SELECT activity_id FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider='intervals_icu'",
        &[&subject]).await?;
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>(),
        ["i900"]
    );
    db.execute("UPDATE watch_tower.wearable_fit_job SET next_attempt='1900-01-01' WHERE subject_id=$1 AND provider='intervals_icu'", &[&subject]).await?;
    let job = claim(&pool)
        .await?
        .context("missing Intervals.icu FIT job")?;
    assert_eq!(job.subject, subject);
    assert_eq!(job.activity, "i900");

    let original = fixture();
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(&original)?;
    let compressed = encoder.finish()?;
    assert!(raw_fit(compressed[..compressed.len() - 4].to_vec()).is_err());
    let router = Router::new().route(
        "/api/v1/activity/{id}/file",
        get(move |Path(activity): Path<String>, headers: HeaderMap| {
            let compressed = compressed.clone();
            async move {
                assert_eq!(activity, "i900");
                let authorization = headers["authorization"].to_str().unwrap();
                let basic = format!(
                    "Basic {}",
                    STANDARD.encode(b"API_KEY:synthetic-personal-key")
                );
                assert!(authorization == "Bearer synthetic-access" || authorization == basic);
                compressed
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let client = cloud_wearables::http_client()?;
    assert!(intervals_original(
        &client,
        &job.activity,
        &["synthetic-access".into(), "WELLNESS:READ".into()],
        &origin
    )
    .await
    .is_err());
    let downloaded = intervals_original(
        &client,
        &job.activity,
        &["synthetic-access".into(), "ACTIVITY:READ".into()],
        &origin,
    )
    .await?;
    assert_eq!(downloaded, original);
    assert_eq!(
        intervals_original(
            &client,
            &job.activity,
            &["synthetic-personal-key".into()],
            &origin
        )
        .await?,
        original
    );
    save_parts(&pool, &job, vec![(0, downloaded)]).await?;
    let stored = download(&pool, subject, "intervals_icu", "i900", None).await;
    assert_eq!(stored.status(), StatusCode::OK);
    assert_eq!(&to_bytes(stored.into_body(), MAX_FILE).await?[..], original);
    assert_eq!(
        download(&pool, "another-profile", "intervals_icu", "i900", None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    let sha: Vec<u8> = db.query_one("SELECT sha256 FROM avatar_hsm.wearable_fit WHERE subject_id=$1 AND provider='intervals_icu' AND activity_id='i900'",
        &[&subject]).await?.get(0);
    assert_eq!(sha, wearable_webhooks::hash(&original));
    crate::wearables::revoke_for(&pool, subject, "intervals_icu").await?;
    assert_eq!(
        download(&pool, subject, "intervals_icu", "i900", None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and the Garmin FIT SDK Activity.fit sample"]
async fn garmin_official_activity_fit_round_trips_through_authenticated_mcp() -> Result<()> {
    let Some(path) = std::env::var_os("AVATAR_TEST_GARMIN_SDK_FIT") else {
        return Ok(());
    };
    let original = raw_fit(std::fs::read(path)?)?;
    let hash = wearable_webhooks::hash(&original);
    let sha256: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(
        sha256,
        "2ea5c612438061490db533dd115692a7c44c5eff313bfd2f62dcdd0d328a5243"
    );
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "garmin-sdk-activity-test";
    let read_token = "garmin-sdk-file-fixture-test-read-0123456789";
    let activity = "sdk-activity";
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.wearable_profile VALUES
        ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
        &[&subject, &read_token],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'garmin')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,garmin_permissions)
        VALUES($1,'garmin',''::bytea,1)", &[&subject]).await?;
    db.execute(
        "INSERT INTO watch_tower.wearable_fit_job(subject_id,provider,activity_id,next_attempt)
        VALUES($1,'garmin',$2,'2000-01-01')",
        &[&subject, &activity],
    )
    .await?;
    drop(db);
    let job = claim(&pool)
        .await?
        .context("missing FIT job for public SDK sample")?;
    assert_eq!(job.activity, activity);
    save_parts(&pool, &job, vec![(1, original.clone())]).await?;
    let worker = tokio::spawn(crate::wearable_fit_details::worker(pool.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let app = Arc::new(App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let client = cloud_wearables::http_client()?;
    let details = async |section: &str, offset: u32| {
        let reply: serde_json::Value = client.post(format!("{origin}/mcp/wearable-read"))
            .bearer_auth(read_token).json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"wearable_fit_details","arguments":{"provider":"garmin","source_id":activity,"section":section,"offset":offset,"limit":1}
            }})).send().await?.error_for_status()?.json().await?;
        anyhow::Ok(reply["result"]["structuredContent"].clone())
    };
    let first = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let page = details("samples", 0).await?;
            if page["state"] != "pending" {
                return anyhow::Ok(page);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await??;
    assert_eq!(first["state"], "ready");
    assert_eq!(first["total"], 3601);
    assert_eq!(first["next_offset"], 1);
    let sample = &first["records"][0];
    assert_eq!(sample["timestamp"], "2021-07-20T21:11:20Z");
    assert_eq!(sample["heart_rate_bpm"].as_f64(), Some(126.0));
    assert_eq!(first["developer_fields_state"], "ready");
    let fields = sample["developer_fields"]
        .as_array()
        .context("missing SDK sample developer fields")?;
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0]["developer_data_index"], 0);
    assert_eq!(fields[0]["number"], 1);
    assert_eq!(fields[0]["name"], "Heart Rate");
    assert_eq!(fields[0]["units"], "bpm");
    assert_eq!(fields[0]["value"].as_f64(), Some(126.0));
    let second = details("samples", 1).await?;
    assert_eq!(second["records"][0]["heart_rate_bpm"].as_f64(), Some(134.0));
    assert_eq!(
        second["records"][0]["developer_fields"][0]["value"].as_f64(),
        Some(134.0)
    );
    assert_eq!(details("laps", 0).await?["total"], 1);
    let sessions = details("sessions", 0).await?;
    let session = &sessions["records"][0];
    assert_eq!(session["sport"], "stand_up_paddleboarding");
    let custom = &session["developer_fields"][0];
    assert_eq!(custom["developer_data_index"], 0);
    assert_eq!(custom["number"], 0);
    assert_eq!(custom["name"], "Doughnuts Earned");
    assert_eq!(custom["units"], "doughnuts");
    let value = custom["value"]
        .as_f64()
        .context("missing SDK session developer value")?;
    assert!((value - 3.00083327293396).abs() < 0.000001);
    assert_eq!(
        details("events", 0).await?["records"][0]["event_type"],
        "start"
    );
    assert_eq!(
        details("events", 1).await?["records"][0]["event_type"],
        "stop_all"
    );
    let creator = details("devices", 0).await?;
    assert_eq!(creator["total"], 2);
    assert_eq!(creator["records"][0]["message_type"], "file_id");
    assert_eq!(creator["records"][0]["manufacturer"], "development");
    let device = details("devices", 1).await?;
    assert_eq!(device["records"][0]["message_type"], "device_info");
    assert_eq!(device["records"][0]["device_index"], "creator");
    assert_eq!(device["records"][0]["product_name"], "FIT Cookbook");
    assert_eq!(device["records"][0]["software_version"].as_f64(), Some(1.0));
    for record in [&creator["records"][0], &device["records"][0]] {
        assert!(record.get("serial_number").is_none());
    }
    let url = format!("{origin}/wearables/fit/garmin/{activity}");
    assert_eq!(
        client.get(&url).send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    let downloaded = client
        .get(url)
        .bearer_auth(read_token)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    assert_eq!(&downloaded[..], &original);
    let stored_hash: Vec<u8> = pool.get().await?.query_one(
        "SELECT sha256 FROM avatar_hsm.wearable_fit WHERE subject_id=$1 AND provider='garmin' AND activity_id=$2",
        &[&subject,&activity],
    ).await?.get(0);
    assert_eq!(stored_hash, hash);
    worker.abort();
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn garmin_fit_uses_the_connection_cooldown_for_new_files() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let subject = "garmin-fit-cooldown-fixture";
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES ($1,'garmin')",
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
        "INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,garmin_permissions)
         VALUES($1,'garmin',''::bytea,1)", &[&subject],
    ).await?;
    db.execute(
        "INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,backfill_from)
         VALUES($1,'garmin',now())",
        &[&subject],
    )
    .await?;
    for (id, offset) in [("4291", 40), ("4292", 35)] {
        db.execute(
            "INSERT INTO watch_tower.wearable_fit_job(subject_id,provider,activity_id,sealed_source,next_attempt)
             VALUES ($1,'garmin',$2,'sealed fixture'::bytea,now()-make_interval(days=>$3))",
            &[&subject,&id,&offset],
        ).await?;
    }
    let limited = claim(&pool).await?.context("expected Garmin FIT fixture")?;
    assert_eq!(limited.activity, "4291");
    defer_job(
        &pool,
        &limited,
        Some(chrono::Utc::now() + chrono::Duration::minutes(10)),
    )
    .await?;
    let sibling = db.query_one(
        "SELECT next_attempt<now(),c.garmin_retry_after>now()+interval '9 minutes'
         FROM watch_tower.wearable_fit_job j JOIN avatar_hsm.wearable_oauth_connection c USING(subject_id,provider)
         WHERE j.subject_id=$1 AND j.activity_id='4292'", &[&subject],
    ).await?;
    assert!(sibling.get::<_, bool>(0) && sibling.get::<_, bool>(1));
    if let Some(other) = claim(&pool).await? {
        assert_ne!(other.subject, subject);
        db.execute(
            "UPDATE watch_tower.wearable_fit_job SET job_id=NULL,leased_until=NULL
             WHERE job_id=$1::text::uuid",
            &[&other.id],
        )
        .await?;
    }
    db.execute(
        "UPDATE avatar_hsm.wearable_oauth_connection SET garmin_retry_after=now() WHERE subject_id=$1",
        &[&subject],
    ).await?;
    assert!(claim(&pool).await?.is_none());
    db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET throttled_until=now()-interval '1 second'
         WHERE subject_id=$1 AND provider='garmin'",
        &[&subject],
    )
    .await?;
    let next = claim(&pool)
        .await?
        .context("expected Garmin FIT after cooldown")?;
    assert_eq!(next.activity, "4292");
    crate::wearables::revoke_for(&pool, subject, "garmin").await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated database and synthetic Suunto signing keys"]
async fn suunto_late_workout_is_restored_from_key_before_fit_storage() -> Result<()> {
    use axum::{
        extract::{Path, Query},
        http::HeaderMap,
        routing::get,
        Json, Router,
    };
    use serde_json::json;
    use std::collections::HashMap;
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "suunto-late-fit-fixture";
    let read_token = "suunto-late-read-synthetic-fixture-0123456789";
    let activity = "1234567890ab1234567890ab";
    let owner = "suunto-late-owner";
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES ($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($1||'c','UTF8')))", &[&subject,&read_token]).await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'suunto')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash)
        VALUES($1,'suunto',''::bytea,$2)", &[&subject,&wearable_webhooks::hash(owner.as_bytes())]).await?;
    drop(db);
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let hsm_origin = format!("http://{}", listener.local_addr()?);
    let hsm_server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let key = std::env::var("AVATAR_SUUNTO_WEBHOOK_PATH_KEY")?;
    let secret = std::env::var("AVATAR_SUUNTO_WEBHOOK_NOTIFICATION_SECRET")?;
    let form = serde_urlencoded::to_string([("username", owner), ("workoutid", activity)])?;
    let tag = ring::hmac::sign(
        &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes()),
        form.as_bytes(),
    );
    let signature: String = tag
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let accepted = Client::new()
        .post(format!("{hsm_origin}/wearables/webhooks/suunto/{key}"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("X-HMAC-SHA256-Signature", signature)
        .body(form)
        .send()
        .await?;
    assert_eq!(accepted.status(), StatusCode::OK);
    let mut job = Job {
        subject: subject.into(),
        provider: Provider::Suunto,
        activity: activity.into(),
        category: None,
        id: pool
            .get()
            .await?
            .query_one(
                "UPDATE watch_tower.wearable_fit_job
            SET job_id=gen_random_uuid(),leased_until=now()+interval '2 minutes'
            WHERE subject_id=$1 AND provider='suunto' AND activity_id=$2 RETURNING job_id::text",
                &[&subject, &activity],
            )
            .await?
            .get(0),
        sealed_source: None,
    };
    let start: chrono::DateTime<chrono::Utc> = "2026-04-01T05:00:00Z".parse()?;
    let router = Router::new().route("/v3/workouts/{key}",get(move |Path(key):Path<String>, Query(query):Query<HashMap<String,String>>, headers:HeaderMap| async move {
        if key != activity
            || query.get("extensions").map(String::as_str) != Some("PauseMarkerExtension,SummaryExtension")
            || headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Bearer fixture")
            || headers.get("ocp-apim-subscription-key").and_then(|v| v.to_str().ok()) != Some("fixture-key") {
            return Err(StatusCode::BAD_REQUEST);
        }
        let at = start.timestamp_millis();
        Ok(Json(json!({"payload":{"workoutKey":key,"activityId":1,"startTime":at,"totalTime":1800,
            "energyConsumption":500,"stepCount":8500,"avgSpeed":2.78,"totalAscent":120.5,"minAltitude":-6.25,
            "hrdata":{"workoutAvgHR":145,"workoutMaxHR":175,"userMaxHR":198},
            "extensions":[{"type":"PauseMarkerExtension","pauseMarkers":[
                {"startTime":at+30_000,"endTime":at+90_000},
                {"startTime":at+60_000,"endTime":at+120_000},
                {"startTime":at+3_000_000,"endTime":at+3_100_000}
            ]},{"type":"SummaryExtension","gear":{"displayName":"Fixture Watch"}}]}})))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    ensure_suunto_summary(
        &pool,
        &Client::new(),
        &job,
        &["fixture".into(), "fixture-key".into()],
        &origin,
    )
    .await?;
    server.abort();
    let retry_at = chrono::Utc::now() + chrono::Duration::minutes(15);
    defer_job(&pool, &job, Some(retry_at)).await?;
    let db = pool.get().await?;
    let row = db.query_one(
        "SELECT attempts,status,next_attempt>now()+interval '14 minutes',job_id IS NULL
         FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider='suunto' AND activity_id=$2",
        &[&subject,&activity],
    ).await?;
    assert_eq!(row.get::<_, i32>(0), 0);
    assert_eq!(row.get::<_, String>(1), "pending");
    assert!(row.get::<_, bool>(2) && row.get::<_, bool>(3));
    job.id = db
        .query_one(
            "UPDATE watch_tower.wearable_fit_job
         SET job_id=gen_random_uuid(),leased_until=now()+interval '2 minutes',next_attempt=now()
         WHERE subject_id=$1 AND provider='suunto' AND activity_id=$2 RETURNING job_id::text",
            &[&subject, &activity],
        )
        .await?
        .get(0);
    let raw = fixture();
    save_parts(&pool, &job, vec![(0, raw.clone())]).await?;
    let response: serde_json::Value = Client::new().post(format!("{hsm_origin}/mcp/wearable-read"))
        .bearer_auth(read_token).json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
            "name":"wearable_sessions","arguments":{"provider":"suunto","kind":"workout","start":start,"end":start+chrono::Duration::hours(1)}
        }})).send().await?.error_for_status()?.json().await?;
    let result = &response["result"]["structuredContent"];
    hsm_server.abort();
    assert_eq!(result["records"][0]["source_id"], activity);
    assert_eq!(
        result["records"][0]["payload"]["end_derived_from_timer"],
        true
    );
    let payload = &result["records"][0]["payload"];
    assert_eq!(payload["end_derived_from_pauses"], true);
    assert_eq!(payload["duration_seconds"], 1890);
    assert_eq!(payload["active_duration_seconds"].as_f64(), Some(1800.0));
    assert_eq!(payload["average_heart_rate_bpm"].as_f64(), Some(145.0));
    assert_eq!(payload["max_heart_rate_bpm"].as_f64(), Some(175.0));
    assert_eq!(payload["calories_kcal"].as_f64(), Some(500.0));
    assert_eq!(payload["min_altitude_meters"].as_f64(), Some(-6.25));
    assert_eq!(payload["device_model"], "Fixture Watch");
    assert!(payload["provider_data"].get("stopTime").is_none());
    assert_eq!(result["records"][0]["fit_state"], "stored");
    let response = download(&pool, subject, "suunto", activity, Some(0)).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(&to_bytes(response.into_body(), MAX_FILE).await?[..], &raw);
    crate::wearables::revoke_for(&pool, subject, "suunto").await?;
    enqueue_workout_notification(&pool, Provider::Suunto, owner, activity).await?;
    assert!(pool
        .get()
        .await?
        .query_opt(
            "SELECT 1 FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider='suunto'",
            &[&subject],
        )
        .await?
        .is_none());
    Ok(())
}

pub(crate) fn fixture() -> Vec<u8> {
    let mut bytes = vec![12, 0x10, 0, 0, 1, 0, 0, 0, b'.', b'F', b'I', b'T', 0x01];
    bytes.extend_from_slice(&crc(&bytes).to_le_bytes());
    bytes
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and a synthetic Polar signing key"]
async fn polar_signed_notification_imports_late_summary_and_fit() -> Result<()> {
    use axum::{extract::RawQuery, routing::get, Json, Router};
    use ring::hmac;
    use serde_json::json;

    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let signing_key = std::env::var("AVATAR_POLAR_WEBHOOK_SECRET")?;
    let subject = "polar-webhook-fit-fixture";
    let read_token = "polar-webhook-fit-fixture-read-key-0123456789";
    let activity = "late-Polar-Exercise";
    let db = pool.get().await?;
    for (profile, owner, token) in [
        (subject, "475", read_token),
        (
            "polar-webhook-other",
            "476",
            "polar-webhook-other-fixture-read-key-123456789",
        ),
    ] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&profile],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($1||'c','UTF8')))", &[&profile,&token]).await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'polar')",
            &[&profile],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash)
                    VALUES($1,'polar',''::bytea,$2)", &[&profile,&wearable_webhooks::hash(owner.as_bytes())]).await?;
        db.execute(
            "INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,next_sync,backfill_from)
                    VALUES($1,'polar',now()+interval '2 hours',now())",
            &[&profile],
        )
        .await?;
    }
    drop(db);
    let router = crate::router(Arc::new(App {
        db: pool.clone(), read_token: String::new(), control_token: String::new(),
        rules: crate::acute::rules()?,
    }))
    .merge(Router::new()
        .route("/v3/exercises/{id}", get(move |Path(id): Path<String>, RawQuery(query):RawQuery, headers: HeaderMap| async move {
            if id != activity || query.as_deref() != Some("zones=true")
                || headers.get("authorization").and_then(|value| value.to_str().ok()) != Some("Bearer fixture") {
                return Err(StatusCode::UNAUTHORIZED);
            }
            Ok(Json(json!({"id":activity,"device":"Polar Vantage","start_time":"2026-05-01T14:00:00",
                "start_time_utc_offset":540,"duration":"PT42M","upload_time":"2026-05-02T08:00:00Z",
                "sport":"RUNNING","distance":8000,"calories":530,"training_load":143.22,
                "heart_rate":{"average":129,"maximum":147},"running-index":51,
                "heart_rate_zones":[
                    {"index":2,"lower-limit":130,"upper-limit":150,"in-zone":"PT20M15.5S"},
                    {"index":1,"lower-limit":110,"upper-limit":130,"in-zone":"PT4S"},
                    {"index":3,"lower-limit":150,"upper-limit":170,"in-zone":"PT90M"},
                    {"index":4,"lower-limit":200,"upper-limit":160,"in-zone":"PT4S"}
                ]})))
        }))
        .route("/v3/exercises/{id}/fit", get(move |Path(id): Path<String>, headers: HeaderMap| async move {
            if id != activity || headers.get("authorization").and_then(|value| value.to_str().ok()) != Some("Bearer fixture") {
                return Err(StatusCode::UNAUTHORIZED);
            }
            Ok(fixture())
        })));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let client = Client::new();
    let url = format!("{origin}/wearables/webhooks/polar");
    let workout = json!({"event":"EXERCISE","user_id":475,"entity_id":activity,
        "timestamp":chrono::Utc::now(), "url":"https://invalid.example.test/untrusted"})
    .to_string();
    assert_eq!(
        client
            .post(&url)
            .body(workout.clone())
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let sleep =
        json!({"event":"SLEEP","user_id":475,"date":"2026-05-01","timestamp":chrono::Utc::now()})
            .to_string();
    for notice in [&workout, &workout, &sleep] {
        let sig = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_bytes()),
            notice.as_bytes(),
        );
        let signature = sig
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        assert_eq!(
            client
                .post(&url)
                .header("Polar-Webhook-Signature", signature)
                .body(notice.clone())
                .send()
                .await?
                .status(),
            StatusCode::OK
        );
    }
    let db = pool.get().await?;
    let jobs = db.query("SELECT subject_id,activity_id FROM watch_tower.wearable_fit_job WHERE provider='polar' AND activity_id=$1", &[&activity]).await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].get::<_, String>(0), subject);
    let day = db.query_one("SELECT day::text FROM watch_tower.wearable_target_day WHERE subject_id=$1 AND provider='polar'", &[&subject]).await?;
    assert_eq!(day.get::<_, String>(0), "2026-05-01");
    let id: String = db.query_one("UPDATE watch_tower.wearable_fit_job SET job_id=gen_random_uuid(),leased_until=now()+interval '2 minutes'
        WHERE subject_id=$1 AND provider='polar' AND activity_id=$2 RETURNING job_id::text", &[&subject,&activity]).await?.get(0);
    let job = Job {
        subject: subject.into(),
        provider: Provider::Polar,
        activity: activity.into(),
        category: None,
        id,
        sealed_source: None,
    };
    polar_fit(&pool, &client, &job, "fixture", &origin).await?;
    let summary = db.query_one("SELECT payload FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='polar' AND kind='workout' AND source_id=$2", &[&subject,&activity]).await?;
    assert_eq!(
        summary.get::<_, serde_json::Value>(0)["distance_meters"],
        8000
    );
    let sessions: serde_json::Value = client
        .post(format!("{origin}/mcp/wearable-read"))
        .bearer_auth(read_token)
        .json(
            &json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"wearable_sessions","arguments":{"provider":"polar","kind":"workout",
                    "start":"2026-05-01T00:00:00Z","end":"2026-05-02T00:00:00Z"}
            }}),
        )
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let records = sessions["result"]["structuredContent"]["records"]
        .as_array()
        .unwrap();
    assert_eq!(records.len(), 1);
    let payload = &records[0]["payload"];
    assert_eq!(payload["calories_kcal"].as_f64(), Some(530.0));
    assert_eq!(payload["average_heart_rate_bpm"].as_f64(), Some(129.0));
    assert_eq!(payload["max_heart_rate_bpm"].as_f64(), Some(147.0));
    assert_eq!(
        payload["heart_rate_zones"].as_array().map(Vec::len),
        Some(2)
    );
    assert_eq!(payload["heart_rate_zones"][0]["index"], 1);
    assert_eq!(
        payload["heart_rate_zones"][1]["duration_seconds"].as_f64(),
        Some(1215.5)
    );
    let response = download(&pool, subject, "polar", activity, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), MAX_FILE).await?.as_ref(),
        fixture()
    );
    assert_eq!(
        download(&pool, "polar-webhook-other", "polar", activity, None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    crate::wearables::revoke_for(&pool, subject, "polar").await?;
    crate::wearables::revoke_for(&pool, "polar-webhook-other", "polar").await?;
    server.abort();
    Ok(())
}

#[test]
fn preserves_raw_and_packaged_fit_and_rejects_corruption() -> Result<()> {
    let original = fixture();
    assert_eq!(raw_fit(original.clone())?, original);
    let mut file = zip::ZipWriter::new(Cursor::new(Vec::new()));
    file.start_file("fixture.fit", zip::write::SimpleFileOptions::default())?;
    file.write_all(&original)?;
    assert_eq!(raw_fit(file.finish()?.into_inner())?, original);
    let mut corrupted = original.clone();
    corrupted[12] ^= 1;
    assert!(raw_fit(corrupted).is_err());
    assert!(allowed_host(
        &Url::parse("https://cdn.example.com/abc?sig=fixture")?,
        "cdn.example.com"
    ));
    assert!(!allowed_host(
        &Url::parse("https://127.0.0.1/file")?,
        "127.0.0.1"
    ));
    assert!(!allowed_host(
        &Url::parse("https://cdn.example.com.attacker.invalid/file")?,
        "cdn.example.com"
    ));
    if let Some(path) = std::env::var_os("AVATAR_SUUNTO_FIT_SAMPLE_ZIP") {
        let mut samples = zip::ZipArchive::new(std::fs::File::open(path)?)?;
        let mut count = 0;
        for index in 0..samples.len() {
            let entry = samples.by_index(index)?;
            let name = entry.name()?.into_owned();
            if !name.to_ascii_lowercase().ends_with(".fit") {
                continue;
            }
            let mut bytes = Vec::new();
            entry.take(MAX_FILE as u64 + 1).read_to_end(&mut bytes)?;
            validate_fit(&bytes).with_context(|| format!("Suunto sample {name}"))?;
            count += 1;
        }
        ensure!(count > 0, "Suunto sample ZIP has no FIT files");
        eprintln!("validated {count} official Suunto FIT samples");
    }
    Ok(())
}

#[test]
fn suunto_fit_request_binds_api_origin_and_credentials() -> Result<()> {
    let client = cloud_wearables::http_client()?;
    let key = "0123456789abcdefABCDEF01";
    let request = suunto_request(&client, key, "fixture-token", "fixture-subscription")?.build()?;
    assert_eq!(request.method(), reqwest::Method::GET);
    assert_eq!(
        request.url().as_str(),
        format!("https://cloudapi.suunto.com/v3/workouts/{key}/fit")
    );
    assert_eq!(request.headers()["authorization"], "Bearer fixture-token");
    assert_eq!(
        request.headers()["ocp-apim-subscription-key"],
        "fixture-subscription"
    );
    assert!(suunto_request(&client, "wrong-workout-key", "", "").is_err());
    Ok(())
}

#[test]
fn coros_detail_keeps_every_leg_and_rejects_an_incomplete_or_untrusted_batch() -> Result<()> {
    let data = serde_json::json!({"triathlonItemList":[
        {"fitUrl":"https://cdn.example.com/swim.fit?sig=one"},
        {"fitUrl":"https://cdn.example.com/ride.fit?sig=two"},
        {"fitUrl":"https://cdn.example.com/run.fit?sig=three"}]});
    let links = coros_links(&data, "cdn.example.com")?;
    assert_eq!(links.iter().map(|row| row.0).collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(
        links[1].1.as_str(),
        "https://cdn.example.com/ride.fit?sig=two"
    );
    let mut invalid = data;
    invalid["triathlonItemList"][1]["fitUrl"] = serde_json::Value::Null;
    assert!(coros_links(&invalid, "cdn.example.com").is_err());
    invalid["triathlonItemList"][1]["fitUrl"] =
        serde_json::json!("https://cdn.example.com.invalid/ride.fit");
    assert!(coros_links(&invalid, "cdn.example.com").is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires synthetic wearable OAuth encryption key"]
async fn coros_fit_prefers_list_links_and_reloads_every_leg_after_expiry() -> Result<()> {
    use axum::{extract::Query, routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    let details = Arc::new(AtomicUsize::new(0));
    let detail_calls = Arc::clone(&details);
    let upstream = Router::new().route(
        "/v2/coros/sport/detail/fit",
        get(
            move |Query(query): Query<std::collections::HashMap<String, String>>| {
                detail_calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    assert_eq!(query["labelId"], "123");
                    assert_eq!(query["openId"], "fixture-open-id");
                    assert_eq!(query["token"], "fixture-access");
                    assert_eq!(query["mode"], "13");
                    assert_eq!(query["subMode"], "1");
                    Json(
                        json!({"result":"0000","data":{"labelId":123,"triathlonItemList":[
                            {"fitUrl":"https://fit.example.test/one.fit?sig=new%2B1"},
                            {"fitUrl":"https://fit.example.test/two.fit?sig=new%2B2"}
                        ]}}),
                    )
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await });
    let links = coros_links(
        &json!({"triathlonItemList":[
            {"fitUrl":"https://fit.example.test/one.fit?sig=old%2B1"},
            {"fitUrl":"https://fit.example.test/two.fit?sig=old%2B2"}
        ]}),
        "fit.example.test",
    )?;
    let sealed = wearable_oauth::seal_coros_fit_sources("owner", "123", &links)?;
    assert!(wearable_oauth::open_coros_fit_sources("other", "123", sealed.clone()).is_err());
    assert!(wearable_oauth::open_coros_fit_sources("owner", "124", sealed.clone()).is_err());
    let job = Job {
        subject: "owner".into(),
        provider: Provider::Coros,
        activity: "123".into(),
        category: Some("coros:13:1".into()),
        id: String::new(),
        sealed_source: Some(sealed),
    };
    let credentials = ["fixture-access".into(), "fixture-open-id".into()];
    let client = cloud_wearables::http_client()?;
    let expired = AtomicBool::new(false);
    let download = |url: Url| {
        let expired = expired.load(Ordering::SeqCst);
        async move {
            let signature = url.query().context("missing FIT signature")?;
            let value = match signature {
                "sig=old%2B1" => 1,
                "sig=old%2B2" if !expired => 2,
                "sig=new%2B1" => 3,
                "sig=new%2B2" => 4,
                _ => anyhow::bail!("expired signed link"),
            };
            let mut bytes = fixture();
            bytes.truncate(bytes.len() - 2);
            bytes[12] = value;
            bytes.extend_from_slice(&crc(&bytes).to_le_bytes());
            raw_fit(bytes)
        }
    };
    let fetched = || {
        coros_fit_parts(
            &client,
            &job,
            &credentials,
            &origin,
            "fit.example.test",
            download,
        )
    };
    let first = fetched().await?;
    assert_eq!(
        first
            .iter()
            .map(|(part, data)| (*part, data[12]))
            .collect::<Vec<_>>(),
        [(1, 1), (2, 2)]
    );
    assert_eq!(details.load(Ordering::SeqCst), 0);
    expired.store(true, Ordering::SeqCst);
    let fallback = fetched().await?;
    assert_eq!(
        fallback
            .iter()
            .map(|(part, data)| (*part, data[12]))
            .collect::<Vec<_>>(),
        [(1, 3), (2, 4)]
    );
    assert_eq!(details.load(Ordering::SeqCst), 1);
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn files_are_owner_scoped_and_revocation_invalidates_pending_download() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let mut db = pool.get().await?;
    let subject = "wearable-fit-first";
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'coros')",
        &[&subject],
    )
    .await?;
    let tx = db.transaction().await?;
    enqueue(&tx, subject, "coros", "abc", Some("coros:13:1")).await?;
    enqueue(&tx, subject, "coros", "abc", Some("coros:13:1")).await?;
    tx.commit().await?;
    drop(db);
    let job = claim(&pool).await?.unwrap();
    let original = fixture();
    save_parts(
        &pool,
        &job,
        vec![
            (1, raw_fit(original.clone())?),
            (2, raw_fit(original.clone())?),
        ],
    )
    .await?;
    let response = download(&pool, subject, "coros", "abc", None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        &to_bytes(response.into_body(), MAX_FILE).await?[..],
        &original
    );
    assert_eq!(
        download(&pool, "wearable-fit-other", "coros", "abc", Some(2))
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        download(&pool, subject, "coros", "abc", Some(2))
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        download(&pool, subject, "coros", "abc", Some(0))
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    enqueue(&tx, subject, "coros", "next", None).await?;
    tx.commit().await?;
    drop(db);
    let pending = claim(&pool).await?.unwrap();
    crate::wearables::revoke_for(&pool, subject, "coros").await?;
    assert!(save_parts(&pool, &pending, vec![(1, original)])
        .await
        .is_err());
    assert_eq!(
        download(&pool, subject, "coros", "abc", None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and synthetic wearable OAuth encryption key"]
async fn coros_oauth_summary_changes_replace_fit_without_exposing_partial_files() -> Result<()> {
    use chrono::{Duration, Utc};
    use serde_json::json;

    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "coros-fit-refresh-fixture";
    let activity = "coros-corrected-activity";
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
        &[&subject],
    )
    .await?;
    let revised_at = Utc::now();
    let start = revised_at - Duration::days(42);
    let end = start + Duration::hours(1);
    let commit = |distance, revision| {
        let pool = &pool;
        async move {
            let lease = crate::wearables::issue_lease_for(pool, subject, "coros").await?;
            let mut snapshot: crate::wearables::Snapshot = serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":end,"grants":["workout"],
                "records":[{"kind":"workout","source":"COROS","source_id":activity,
                "started_at":start,"ended_at":end,"modified_at":revision,
                "payload":{"exercise_type":"coros:13:1","distance_meters":distance}}]
            }))?;
            snapshot.coros_fit_sources.insert(
                activity.into(),
                coros_links(
                    &json!({"fitUrl":"https://fit.example.test/workout.fit?sig=fixture"}),
                    "fit.example.test",
                )?,
            );
            crate::wearables::store_snapshot(pool, subject, "coros", snapshot, false).await
        }
    };
    let claim_fixture = || async {
        db.execute(
            "UPDATE watch_tower.wearable_fit_job SET next_attempt='1900-01-01'
             WHERE subject_id=$1 AND provider='coros' AND activity_id=$2",
            &[&subject, &activity],
        )
        .await?;
        let job = claim(&pool).await?.context("missing COROS FIT job")?;
        ensure!(
            job.subject == subject && job.activity == activity,
            "claimed an unrelated FIT job"
        );
        Ok::<_, anyhow::Error>(job)
    };
    let read = |part| {
        let pool = &pool;
        async move {
            let response = download(pool, subject, "coros", activity, Some(part)).await;
            assert_eq!(response.status(), StatusCode::OK);
            Ok::<_, anyhow::Error>(to_bytes(response.into_body(), MAX_FILE).await?)
        }
    };
    let status = || async {
        Ok::<_, anyhow::Error>(db.query_one(
            "SELECT status FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider='coros' AND activity_id=$2",
            &[&subject,&activity],
        ).await?.get::<_, String>(0))
    };
    commit(1000, revised_at - Duration::seconds(3)).await?;
    let first_job = claim_fixture().await?;
    let sealed = first_job
        .sealed_source
        .clone()
        .context("missing staged COROS FIT URLs")?;
    assert_eq!(
        wearable_oauth::open_coros_fit_sources(subject, activity, sealed)?,
        [(0, "https://fit.example.test/workout.fit?sig=fixture".into())]
    );
    let first = raw_fit(fixture())?;
    save_parts(
        &pool,
        &first_job,
        vec![(1, first.clone()), (2, first.clone())],
    )
    .await?;
    assert!(db.query_one(
        "SELECT sealed_source IS NULL FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider='coros' AND activity_id=$2",
        &[&subject,&activity],
    ).await?.get::<_,bool>(0));
    commit(1000, revised_at - Duration::seconds(2)).await?;
    assert_eq!(status().await?, "stored");

    commit(1100, revised_at - Duration::seconds(1)).await?;
    assert_eq!(status().await?, "pending");
    assert_eq!(&read(2).await?[..], first);
    let outdated_job = claim_fixture().await?;
    commit(1200, revised_at).await?;
    assert!(save_parts(&pool, &outdated_job, vec![(1, first.clone())])
        .await
        .is_err());
    assert_eq!(&read(2).await?[..], first);
    let current_job = claim_fixture().await?;
    let mut corrected = first[..first.len() - 2].to_vec();
    corrected[12] = 2;
    corrected.extend_from_slice(&crc(&corrected).to_le_bytes());
    let corrected = raw_fit(corrected)?;
    save_parts(&pool, &current_job, vec![(1, corrected.clone())]).await?;
    assert_eq!(&read(1).await?[..], corrected);
    assert_eq!(
        download(&pool, subject, "coros", activity, Some(2))
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    let sha256: Vec<u8> = db.query_one(
        "SELECT sha256 FROM avatar_hsm.wearable_fit WHERE subject_id=$1 AND provider='coros' AND activity_id=$2 AND part=1",
        &[&subject,&activity],
    ).await?.get(0);
    assert_eq!(sha256, wearable_webhooks::hash(&corrected));
    commit(1100, revised_at - Duration::seconds(1)).await?;
    assert_eq!(status().await?, "stored");
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn existing_suunto_workouts_get_one_fit_job_per_valid_key() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let owner = "suunto-fit-upgrade";
    let key = "0123456789abcdefABCDEF01";
    tx.execute(
        "INSERT INTO avatar_hsm.subject VALUES ($1,'Asia/Seoul')",
        &[&owner],
    )
    .await?;
    tx.execute("INSERT INTO avatar_hsm.device_connection(subject_id,provider,state) VALUES ($1,'suunto','synced')", &[&owner]).await?;
    for id in [key, "invalid-key"] {
        tx.execute("INSERT INTO avatar_hsm.wearable_record VALUES ($1,'suunto','workout','Suunto',$2,now()-interval '1 hour',now(),now(),'{\"exercise_type\":\"suunto:2\"}')", &[&owner,&id]).await?;
    }
    let backfill = include_str!("../migrations/011-suunto-fit.sql").replace(
        "avatar_hsm.wearable_fit_job",
        "watch_tower.wearable_fit_job",
    );
    tx.batch_execute(&backfill).await?;
    enqueue(&tx, owner, "suunto", key, None).await?;
    enqueue(&tx, owner, "suunto", "invalid-key", None).await?;
    let jobs = tx
        .query(
            "SELECT activity_id,exercise_type FROM watch_tower.wearable_fit_job WHERE subject_id=$1",
            &[&owner],
        )
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].get::<_, String>(0), key);
    assert_eq!(jobs[0].get::<_, String>(1), "suunto:2");
    tx.rollback().await?;
    Ok(())
}
