use super::*;

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and a synthetic OAuth encryption key"]
async fn google_tcx_reconciles_cached_activity_and_location_consent() -> Result<()> {
    const ACTIVITY: &str =
        "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly";
    const LOCATION: &str = "https://www.googleapis.com/auth/googlehealth.location.readonly";
    let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let mut db = pool.get().await?;
    let provider = Provider::GoogleHealth;
    let source = "Pixel Watch";
    let id = "2026443605080188808";
    let start: chrono::DateTime<chrono::Utc> = "2026-04-20T10:00:00Z".parse()?;
    let end = start + chrono::Duration::minutes(20);
    let point = json!([{"timestamp_ms":start.timestamp_millis(),"latitude":37.5,"longitude":127.0,"segment":1}]);
    let raw = b"synthetic TCX fixture for consent storage";
    let read_token = |owner: &str| format!("tcx-read-token-synthetic-fixture-for-{owner}");
    let both = format!("{ACTIVITY} {LOCATION}");
    let fixtures = [
        ("tcx-location-only", LOCATION, false),
        ("tcx-activity-only", ACTIVITY, false),
        ("tcx-with-both-scopes", both.as_str(), true),
    ];
    for (subject, scope, _) in fixtures {
        let tokens = Tokens {
            access_token: "tcx-test-access-only".into(),
            refresh_token: None,
            expires_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            scope: scope.into(),
            provider_user: None,
        };
        let sealed = seal(&key()?, &tokens, subject, provider)?;
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
            &[&subject],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES
            ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
            &[&subject, &read_token(subject)],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider,state,grants)
            VALUES($1,'google_health','synced',ARRAY['workout'])",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,google_location_granted)
            VALUES($1,'google_health',$2,true)", &[&subject,&sealed]).await?;
        let tx = db.transaction().await?;
        let workout = serde_json::from_value(json!({
            "kind":"workout","source":source,"source_id":id,"started_at":start,"ended_at":end,"modified_at":end,
            "payload":{"exercise_type":"RUNNING","provider_data":{"exerciseMetadata":{"hasGps":true}}}
        }))?;
        crate::wearables::upsert_record(&tx, subject, "google_health", &workout).await?;
        crate::google_workout_tcx::enqueue(&tx, subject, start, end).await?;
        tx.execute(
            "UPDATE avatar_hsm.google_workout_tcx SET data=$2,route_points=$3 WHERE subject_id=$1",
            &[&subject, &raw.as_slice(), &point],
        )
        .await?;
        tx.commit().await?;
    }
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
    for (subject, scope, retained) in fixtures {
        let download = || {
            client
                .get(format!("{origin}/wearables/tcx/google_health/{id}"))
                .bearer_auth(read_token(subject))
                .send()
        };
        assert_eq!(download().await?.status(), StatusCode::OK);
        assert_eq!(access(&client, &pool, subject, provider).await?[1], scope);
        let tx = db.transaction().await?;
        crate::google_workout_tcx::enqueue(&tx, subject, start, end).await?;
        tx.commit().await?;
        let result: Value = client.post(format!("{origin}/mcp/wearable-read"))
            .bearer_auth(read_token(subject)).json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"wearable_sessions","arguments":{"google_route":{"source":source,"source_id":id}}
            }})).send().await?.error_for_status()?.json().await?;
        let detail = &result["result"]["structuredContent"];
        if retained {
            assert_eq!(download().await?.bytes().await?.as_ref(), raw);
            assert_eq!(detail["records"], point);
        } else {
            assert_eq!(download().await?.status(), StatusCode::NOT_FOUND);
            assert_eq!(detail["state"], "not_found");
            assert!(db
                .query_opt(
                    "SELECT 1 FROM avatar_hsm.google_workout_tcx WHERE subject_id=$1",
                    &[&subject]
                )
                .await?
                .is_none());
            assert!(db
                .query_opt(
                    "SELECT 1 FROM watch_tower.google_workout_tcx_queue WHERE subject_id=$1",
                    &[&subject]
                )
                .await?
                .is_none());
        }
    }
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and a synthetic OAuth encryption key"]
async fn narrowing_intervals_oauth_scope_retires_only_that_accounts_fit() -> Result<()> {
    let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "intervals-fit-revocation-fixture";
    let other = "intervals-fit-retained-fixture";
    let activity = "i4321";
    let db = pool.get().await?;
    let provider = Provider::IntervalsIcu;
    let raw_file = crate::wearable_fit::fit_fixture();
    let sha = crate::wearable_webhooks::hash(&raw_file);
    let details = json!({"samples":[{"heart_rate_bpm":75}]});
    let read_token = |owner: &str| format!("fitness-read-fixture-token-for-{owner}");
    for (owner, scope) in [
        (subject, "WELLNESS:READ"),
        (other, "ACTIVITY:READ,WELLNESS:READ"),
    ] {
        let tokens = Tokens {
            access_token: "intervals-fit-synthetic".into(),
            refresh_token: None,
            expires_at: None,
            scope: scope.into(),
            provider_user: Some(owner.into()),
        };
        let sealed = seal(&key()?, &tokens, owner, provider)?;
        db.execute("INSERT INTO avatar_hsm.subject VALUES($1,'UTC')", &[&owner])
            .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES
           ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
            &[&owner, &read_token(owner)],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider,state,grants)
           VALUES($1,'intervals_icu','synced',ARRAY['workout','metric'])",
            &[&owner],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,intervals_activity_granted)
           VALUES($1,'intervals_icu',$2,true)", &[&owner,&sealed]).await?;
        db.execute(
            "INSERT INTO watch_tower.wearable_fit_job(subject_id,provider,activity_id,status)
           VALUES($1,'intervals_icu',$2,'stored')",
            &[&owner, &activity],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_fit(subject_id,provider,activity_id,part,data,sha256,detail_state,detail)
           VALUES($1,'intervals_icu',$2,0,$3,$4,'ready',$5)", &[&owner,&activity,&raw_file,&sha,&details]).await?;
    }
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
    let download = |owner| {
        client
            .get(format!("{origin}/wearables/fit/intervals_icu/{activity}"))
            .bearer_auth(read_token(owner))
            .send()
    };
    let mcp_details = |owner| {
        let request = client.post(format!("{origin}/mcp/wearable-read"))
            .bearer_auth(read_token(owner))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"wearable_fit_details","arguments":{"provider":"intervals_icu","source_id":activity,"section":"samples"}
            }}));
        async move {
            let result: Value = request.send().await?.error_for_status()?.json().await?;
            Ok::<_, anyhow::Error>(result["result"]["structuredContent"].clone())
        }
    };
    assert_eq!(download(subject).await?.status(), StatusCode::OK);
    assert_eq!(mcp_details(subject).await?["records"], details["samples"]);
    assert_eq!(
        access(&client, &pool, subject, provider).await?[1],
        "WELLNESS:READ"
    );
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    crate::wearable_fit::enqueue(&tx, subject, "intervals_icu", activity, Some("Ride")).await?;
    tx.commit().await?;
    assert_eq!(download(subject).await?.status(), StatusCode::NOT_FOUND);
    assert_eq!(mcp_details(subject).await?["state"], "not_found");
    assert_eq!(download(other).await?.bytes().await?.as_ref(), raw_file);
    assert_eq!(mcp_details(other).await?["records"], details["samples"]);
    assert_eq!(db.query_one(
        "SELECT count(*) FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider='intervals_icu'",
        &[&subject],
    ).await?.get::<_,i64>(0),0);
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and a synthetic OAuth encryption key"]
async fn polling_recovers_oura_and_whoop_webhook_identity_for_the_current_link() -> Result<()> {
    use axum::{http::HeaderMap, routing::get, Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let client = cloud_wearables::http_client()?;
    let db = pool.get().await?;
    let object = "0f346bcd-ea54-4411-911a-85694b63cf10";
    for (provider, owner, profile_body, scope, event) in [
        (
            Provider::Oura,
            "fixture-oura-account",
            json!({"id":"fixture-oura-account"}),
            "daily",
            json!({"data_type":"sleep","event_type":"update","object_id":object}),
        ),
        (
            Provider::Whoop,
            "729321",
            json!({"user_id":729321}),
            "read:profile read:sleep",
            json!({"type":"sleep.updated","id":object}),
        ),
    ] {
        let slug = provider.slug();
        let subject = format!("{slug}-profile-recovery-fixture");
        let tokens = Tokens {
            access_token: "profile-recovery-test-access".into(),
            refresh_token: None,
            expires_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            scope: scope.into(),
            provider_user: None,
        };
        let sealed = seal(&key()?, &tokens, &subject, provider)?;
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
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,$2)",
            &[&subject, &slug],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens) VALUES($1,$2,$3)", &[&subject,&slug,&sealed]).await?;
        let mapped_account = || async {
            Ok::<_, anyhow::Error>(db.query_one(
                "SELECT provider_account_hash FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1 AND provider=$2",
                &[&subject,&slug],
            ).await?.get::<_,Option<Vec<u8>>>(0))
        };
        let accept_notification = || async {
            use crate::wearable_webhook_documents::{enqueue, target};
            enqueue(
                &pool,
                slug,
                owner,
                target(slug, &event).context("invalid notification fixture")?,
                &serde_json::to_vec(&event)?,
            )
            .await
        };
        accept_notification().await?;
        let queued = || async {
            Ok::<_, anyhow::Error>(db.query_one(
                "SELECT count(*) FROM watch_tower.wearable_webhook_document WHERE subject_id=$1 AND provider=$2 AND object_id=$3",
                &[&subject,&slug,&object],
            ).await?.get::<_,i64>(0))
        };
        assert_eq!(queued().await?, 0);
        let requests = Arc::new(AtomicUsize::new(0));
        let mock = Router::new().route("/", get({
            let requests = requests.clone();
            let pool = pool.clone();
            let subject = subject.clone();
            move |headers: HeaderMap| {
                let requests = requests.clone();
                let subject = subject.clone();
                let response = profile_body.clone();
                let pool = pool.clone();
                async move {
                    assert_eq!(headers["authorization"], "Bearer profile-recovery-test-access");
                    let attempt = requests.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        return (StatusCode::TOO_MANY_REQUESTS, [("retry-after","600")]).into_response();
                    }
                    if attempt == 2 {
                        pool.get().await.unwrap().execute(
                            "UPDATE avatar_hsm.wearable_oauth_connection SET generation=gen_random_uuid()
                             WHERE subject_id=$1 AND provider=$2", &[&subject,&slug],
                        ).await.unwrap();
                    }
                    Json(response).into_response()
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, mock).await });
        let profile = WebhookProfile {
            url: &url,
            ..webhook_profile(provider).unwrap()
        };
        let sync = || access_with_profile(&client, &pool, &subject, provider, profile);
        if matches!(provider, Provider::Whoop) {
            assert_eq!(
                webhook_profile_id(&client, profile, &tokens.access_token, "read:sleep").await?,
                None
            );
            assert_eq!(requests.load(Ordering::SeqCst), 0);
        }
        let limited = sync().await.unwrap_err();
        assert!(limited.is::<cloud_wearables::ProviderThrottled>());
        assert_eq!(mapped_account().await?, None);
        assert_eq!(sync().await?, [tokens.access_token, tokens.scope]);
        assert_eq!(mapped_account().await?, Some(hash(owner)));
        accept_notification().await?;
        accept_notification().await?;
        assert_eq!(queued().await?, 1);
        sync().await?;
        assert_eq!(requests.load(Ordering::SeqCst), 2);

        db.execute("UPDATE avatar_hsm.wearable_oauth_connection SET provider_account_hash=NULL WHERE subject_id=$1 AND provider=$2", &[&subject,&slug]).await?;
        sync().await?;
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        assert_eq!(
            mapped_account().await?,
            None,
            "stale profile must not bind a replacement connection"
        );
        server.abort();
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and synthetic Intervals.icu app configuration"]
async fn intervals_account_uses_pkce_and_reuses_a_durable_scoped_token() -> Result<()> {
    let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "intervals-oauth-fixture";
    let provider = Provider::IntervalsIcu;
    let slug = provider.slug();
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))", &[&subject]).await?;
    let result = begin(&pool, subject, provider).await?;
    let url = Url::parse(
        result["authorization_url"]
            .as_str()
            .context("missing authorization URL")?,
    )?;
    assert_eq!(url.host_str(), Some("intervals.icu"));
    let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
    assert_eq!(params["scope"], "ACTIVITY:READ,WELLNESS:READ");
    assert_eq!(params["code_challenge_method"], "S256");
    let verifier: String = db.query_one("SELECT verifier FROM avatar_hsm.wearable_oauth_state WHERE subject_id=$1 AND provider=$2", &[&subject, &slug]).await?.get(0);
    assert_eq!(
        params["code_challenge"],
        URL_SAFE_NO_PAD.encode(hash(&verifier))
    );
    let tokens = Tokens {
        access_token: "fixture-intervals-access".into(),
        refresh_token: None,
        expires_at: None,
        scope: "WELLNESS:READ".into(),
        provider_user: Some("fixture-athlete".into()),
    };
    let sealed = seal(&key()?, &tokens, subject, provider)?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,$2)",
        &[&subject, &slug],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens) VALUES($1,$2,$3)", &[&subject,&slug,&sealed]).await?;
    assert_eq!(
        access(&cloud_wearables::http_client()?, &pool, subject, provider).await?,
        [
            "fixture-intervals-access",
            "WELLNESS:READ",
            "fixture-athlete"
        ]
    );
    Ok(())
}

#[test]
fn sealed_tokens_are_bound_to_profile_and_supplier() -> Result<()> {
    let secret =
        aead::LessSafeKey::new(aead::UnboundKey::new(&aead::AES_256_GCM, &[3; 32]).unwrap());
    let tokens = Tokens {
        access_token: "fixture-access".into(),
        refresh_token: Some("fixture-refresh".into()),
        scope: "fixture-scope".into(),
        expires_at: None,
        provider_user: None,
    };
    let sealed = seal(&secret, &tokens, "wearable-one", Provider::GoogleHealth)?;
    assert!(!sealed
        .windows(b"fixture-access".len())
        .any(|v| v == b"fixture-access"));
    assert!(open(
        &secret,
        sealed.clone(),
        "wearable-two",
        Provider::GoogleHealth
    )
    .is_err());
    assert!(open(&secret, sealed.clone(), "wearable-one", Provider::Strava).is_err());
    assert_eq!(
        open(&secret, sealed, "wearable-one", Provider::GoogleHealth)?.access_token,
        tokens.access_token
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn denied_callback_consumes_only_the_matching_provider_state() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let profile = "wearable-redirect-fixture";
    let state = "s".repeat(43);
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&profile],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.wearable_profile VALUES($1,decode('a1','hex'),decode('a2','hex'))",
        &[&profile],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_state(state_hash,subject_id,provider,verifier,expires_at)
        VALUES($1,$2,'google_health','fixture-verifier',now()+interval '10 minutes')", &[&hash(&state),&profile]).await?;
    let reply = || Callback {
        state: Some(state.clone()),
        code: None,
        error: Some("access_denied".into()),
        scope: None,
        user_id: None,
    };
    assert_eq!(
        finish(&pool, "oura", reply())
            .await
            .unwrap_err()
            .to_string(),
        "expired or used OAuth state"
    );
    assert_eq!(
        finish(&pool, "google_health", reply())
            .await
            .unwrap_err()
            .to_string(),
        "OAuth denied"
    );
    assert_eq!(
        finish(&pool, "google_health", reply())
            .await
            .unwrap_err()
            .to_string(),
        "expired or used OAuth state"
    );
    assert!(db
        .query_opt(
            "SELECT 1 FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1",
            &[&profile]
        )
        .await?
        .is_none());
    Ok(())
}
