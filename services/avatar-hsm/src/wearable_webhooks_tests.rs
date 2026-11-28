use super::*;

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and synthetic Strava webhook configuration"]
async fn strava_requires_a_fresh_signature_and_queues_the_bound_activity() -> Result<()> {
    let Ok(path_key) = env::var("AVATAR_STRAVA_WEBHOOK_PATH_KEY") else {
        return Ok(());
    };
    let key = env::var("AVATAR_STRAVA_WEBHOOK_SIGNING_SECRET")?;
    let subscription = env::var("AVATAR_STRAVA_WEBHOOK_SUBSCRIPTION_ID")?.parse::<u64>()?;
    let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let subject = "strava-signed-webhook-fixture";
    let other = "strava-webhook-other-fixture";
    for (account, owner) in [(subject, "420"), (other, "421")] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'UTC')",
            &[&account],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES
            ($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))",
            &[&account],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'strava')",
            &[&account],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash,linked_at)
            VALUES($1,'strava',''::bytea,$2,now()-interval '1 minute')", &[&account,&hash(owner.as_bytes())]).await?;
        db.execute("INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,next_sync,backfill_from)
            VALUES($1,'strava',now()+interval '2 hours',now())", &[&account]).await?;
    }
    let server_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!(
        "http://{}/wearables/webhooks/strava/{path_key}",
        server_socket.local_addr()?
    );
    let app = Arc::new(App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(server_socket, crate::router(app)).await });
    let client = reqwest::Client::new();
    let now = Utc::now().timestamp();
    let raw =
        json!({"owner_id":420,"object_id":123456,"subscription_id":subscription,"event_time":now,
        "object_type":"activity","aspect_type":"delete","updates":{}})
        .to_string();
    let sign = |body: &str, timestamp: i64| {
        let tag = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, key.as_bytes()),
            format!("{timestamp}.{body}").as_bytes(),
        );
        let digest: String = tag
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        format!("t={timestamp},v1={digest}")
    };
    assert_eq!(
        client.post(&url).body(raw.clone()).send().await?.status(),
        StatusCode::FORBIDDEN
    );
    let signature = sign(&raw, now);
    for (supplied, payload) in [
        (signature.clone(), format!("{raw} ")),
        (sign(&raw, now - 301), raw.clone()),
        (format!("{signature},t={now}"), raw.clone()),
    ] {
        assert_eq!(
            client
                .post(&url)
                .header("X-Strava-Signature", supplied)
                .body(payload)
                .send()
                .await?
                .status(),
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM watch_tower.wearable_webhook_document WHERE provider='strava' AND subject_id IN ($1,$2)",
            &[&subject,&other]
        )
        .await?
        .get::<_, i64>(0),
        0
    );
    for _ in 0..2 {
        assert_eq!(
            client
                .post(&url)
                .header("X-Strava-Signature", &signature)
                .body(raw.clone())
                .send()
                .await?
                .status(),
            StatusCode::OK
        );
    }
    let targets = db
        .query(
            "SELECT subject_id,object_id,delete_requested FROM watch_tower.wearable_webhook_document WHERE provider='strava' AND subject_id IN ($1,$2)",
            &[&subject,&other],
        )
        .await?;
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].get::<_, String>(0), subject);
    assert_eq!(targets[0].get::<_, String>(1), "123456");
    assert!(targets[0].get::<_, bool>(2));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM watch_tower.wearable_webhook_receipt WHERE provider='strava' AND event_hash=$1",
            &[&hash(raw.as_bytes())]
        )
        .await?
        .get::<_, i64>(0),
        1
    );
    assert!(db.query_one("SELECT next_sync>now() FROM watch_tower.wearable_sync_schedule WHERE subject_id=$1 AND provider='strava'", &[&other]).await?.get::<_,bool>(0));
    for (object, expected) in [(421, StatusCode::BAD_REQUEST), (420, StatusCode::OK)] {
        let revocation = json!({"owner_id":420,"object_id":object,"subscription_id":subscription,
            "event_time":now,"object_type":"athlete","aspect_type":"update",
            "updates":{"authorized":"false"}})
        .to_string();
        assert_eq!(
            client
                .post(&url)
                .header("X-Strava-Signature", sign(&revocation, now))
                .body(revocation)
                .send()
                .await?
                .status(),
            expected
        );
    }
    let revocations = db
        .query(
            "SELECT subject_id FROM watch_tower.wearable_webhook_document
        WHERE provider='strava' AND data_type='authorization' AND subject_id IN ($1,$2)",
            &[&subject, &other],
        )
        .await?;
    assert_eq!(revocations.len(), 1);
    assert_eq!(revocations[0].get::<_, String>(0), subject);
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and synthetic Suunto secrets"]
async fn suunto_webhook_requires_signed_body_and_wakes_only_matched_account() -> Result<()> {
    let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let path_key = env::var("AVATAR_SUUNTO_WEBHOOK_PATH_KEY")?;
    let secret = env::var("AVATAR_SUUNTO_WEBHOOK_NOTIFICATION_SECRET")?;
    let subject = "suunto-webhook-fixture";
    let owner = "fixture-owner";
    let other_subject = "suunto-webhook-other-fixture";
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES ($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))", &[&subject]).await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'suunto')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash)
        VALUES($1,'suunto',''::bytea,$2)", &[&subject,&hash(owner.as_bytes())]).await?;
    db.execute(
        "INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,next_sync,backfill_from)
        VALUES($1,'suunto',now()+interval '2 hours',now())",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&other_subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.wearable_profile VALUES ($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))",
        &[&other_subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'suunto')",
        &[&other_subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash)
        VALUES($1,'suunto',''::bytea,$2)", &[&other_subject,&hash(b"other-fixture-owner")]).await?;
    db.execute(
        "INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,next_sync,backfill_from)
        VALUES($1,'suunto',now()+interval '2 hours',now())",
        &[&other_subject],
    )
    .await?;
    drop(db);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Arc::new(App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let client = reqwest::Client::new();
    let url = format!("http://{address}/wearables/webhooks/suunto/{path_key}");
    let body = r#"{"type":"WORKOUT_CREATED","username":"fixture-owner","workout":{"workoutKey":"0123456789abcdef01234567"}}"#;
    let sign = |body: &str| {
        hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()),
            body.as_bytes(),
        )
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
    };
    assert_eq!(
        client.post(&url).body(body).send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&url)
            .header("X-HMAC-SHA256-Signature", sign(body))
            .body(format!("{body} "))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let other = body.replace(owner, "unknown-owner");
    assert_eq!(
        client
            .post(&url)
            .header("X-HMAC-SHA256-Signature", sign(&other))
            .body(other)
            .send()
            .await?
            .status(),
        StatusCode::OK
    );
    assert!(!pool
        .get()
        .await?
        .query_one(
            "SELECT next_sync<=now() FROM watch_tower.wearable_sync_schedule
        WHERE subject_id=$1 AND provider='suunto'",
            &[&subject]
        )
        .await?
        .get::<_, bool>(0));
    for _ in 0..2 {
        assert_eq!(
            client
                .post(&url)
                .header("X-HMAC-SHA256-Signature", sign(body))
                .body(body)
                .send()
                .await?
                .status(),
            StatusCode::OK
        );
    }
    let db = pool.get().await?;
    let row = db
        .query_one(
            "SELECT next_sync<=now(), (SELECT count(*) FROM watch_tower.wearable_webhook_receipt
        WHERE provider='suunto' AND event_hash=$2) FROM watch_tower.wearable_sync_schedule
        WHERE subject_id=$1 AND provider='suunto'",
            &[&subject, &hash(body.as_bytes())],
        )
        .await?;
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, i64>(1), 1);
    let job = db.query_one(
        "SELECT activity_id,status FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider='suunto'",
        &[&subject],
    ).await?;
    assert_eq!(job.get::<_, String>(0), "0123456789abcdef01234567");
    assert_eq!(job.get::<_, String>(1), "pending");

    let form = "workoutid=aaaaaaaaaaaaaaaaaaaaaaaa&username=fixture%2Downer";
    let form_type = "application/x-www-form-urlencoded; charset=UTF-8";
    assert_eq!(
        client
            .post(&url)
            .header("Content-Type", form_type)
            .body(form)
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&url)
            .header("Content-Type", form_type)
            .header("X-HMAC-SHA256-Signature", sign(form))
            .body(form.replace("fixture%2Downer", "other-fixture-owner"))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    for (body, expected) in [
        (
            format!("{form}&username=other-fixture-owner"),
            StatusCode::BAD_REQUEST,
        ),
        (
            "workoutid=../not-a-key&username=fixture-owner".into(),
            StatusCode::BAD_REQUEST,
        ),
        (form.into(), StatusCode::OK),
        (form.into(), StatusCode::OK),
    ] {
        assert_eq!(
            client
                .post(&url)
                .header("Content-Type", form_type)
                .header("X-HMAC-SHA256-Signature", sign(&body))
                .body(body)
                .send()
                .await?
                .status(),
            expected
        );
    }
    let date_bearing_workout = json!({
        "type":"WORKOUT_CREATED","username":owner,
        "workout":{"workoutKey":"aaaaaaaaaaaaaaaaaaaaaaaa","startTime": "2026-04-06T16:30:00Z".parse::<DateTime<Utc>>()?.timestamp_millis(),
            "timeOffsetInMinutes":540}
    }).to_string();
    let route = r#"{"type":"ROUTE_CREATED","username":"fixture-owner","route":{"id":"fixture"}}"#;
    for body in [date_bearing_workout.as_str(), route] {
        assert_eq!(
            client
                .post(&url)
                .header("X-HMAC-SHA256-Signature", sign(body))
                .body(body.to_owned())
                .send()
                .await?
                .status(),
            StatusCode::OK
        );
    }
    let workout_owners = db
        .query(
            "SELECT subject_id FROM watch_tower.wearable_fit_job
         WHERE provider='suunto' AND activity_id='aaaaaaaaaaaaaaaaaaaaaaaa'",
            &[],
        )
        .await?;
    assert_eq!(workout_owners.len(), 1);
    assert_eq!(workout_owners[0].get::<_, String>(0), subject);
    assert!(db.query_one(
        "SELECT next_sync>now() AND NOT wake_requested FROM watch_tower.wearable_sync_schedule WHERE subject_id=$1 AND provider='suunto'",
        &[&other_subject],
    ).await?.get::<_, bool>(0));

    let events = [
        r#"{"type":"SUUNTO_247_ACTIVITY_CREATED","username":"fixture-owner","samples":[{"timestamp":"2026-04-03T00:30:00+09:00","entryData":{"StepCount":100}}]}"#,
        r#"{"type":"SUUNTO_247_SLEEP_CREATED","username":"fixture-owner","samples":[{"timestamp":"2026-04-11T23:30:00+09:00","entryData":{"BedtimeStart":"2026-04-11T23:30:00+09:00","BedtimeEnd":"2026-04-12T09:30:00+09:00"}}]}"#,
        r#"{"type":"SUUNTO_247_RECOVERY_CREATED","username":"fixture-owner","samples":[{"timestamp":"2026-04-20T15:30:00+09:00","entryData":{"Balance":0.93}}]}"#,
    ];
    for body in events {
        let unknown = body.replace(owner, "unknown-owner");
        for sample in [unknown.as_str(), body, body] {
            assert_eq!(
                client
                    .post(&url)
                    .header("X-HMAC-SHA256-Signature", sign(sample))
                    .body(sample.to_owned())
                    .send()
                    .await?
                    .status(),
                StatusCode::OK
            );
        }
    }
    let days = db
        .query(
            "SELECT day::text FROM watch_tower.wearable_target_day
         WHERE subject_id=$1 AND provider='suunto' ORDER BY day",
            &[&subject],
        )
        .await?;
    assert_eq!(
        days.iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>(),
        [
            "2026-04-02",
            "2026-04-03",
            "2026-04-06",
            "2026-04-07",
            "2026-04-11",
            "2026-04-12",
            "2026-04-20"
        ]
    );
    let notification_hashes: Vec<_> = [body, form, date_bearing_workout.as_str(), route]
        .into_iter()
        .chain(events)
        .flat_map(|event| {
            [
                hash(event.as_bytes()),
                hash(event.replace(owner, "unknown-owner").as_bytes()),
            ]
        })
        .collect();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM watch_tower.wearable_webhook_receipt
             WHERE provider='suunto' AND event_hash=ANY($1::bytea[])",
            &[&notification_hashes]
        )
        .await?
        .get::<_, i64>(0),
        6
    );
    server.abort();
    Ok(())
}

#[test]
fn signatures_bind_timestamp_and_exact_body() -> Result<()> {
    let key = "fixture-webhook-key";
    let timestamp = Utc::now().timestamp_millis().to_string();
    let body = br#"{"user_id":42}"#;
    let message = [timestamp.as_bytes(), body].concat();
    let tag = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key.as_bytes()), &message);
    assert!(signed(key, &timestamp, body, tag.as_ref()));
    assert!(!signed(key, &timestamp, br#"{"user_id":43}"#, tag.as_ref()));
    assert!(!signed(key, "0", body, tag.as_ref()));
    check_age(&timestamp, 300)?;
    assert!(check_age("0", 300).is_err());
    assert!(decode_hex_32(&"a".repeat(64)).is_ok());
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn events_coalesce_and_only_wake_the_bound_account() -> Result<()> {
    let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    for (subject, owner) in [
        ("wearable-webhook-first", "42"),
        ("wearable-webhook-second", "43"),
    ] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))",&[&subject]).await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'whoop')",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash)
            VALUES($1,'whoop',''::bytea,$2)",&[&subject,&hash(owner.as_bytes())]).await?;
        db.execute("INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,next_sync,backfill_from,job_until)
            VALUES($1,'whoop',now()+interval '2 hours',now(),now()+interval '5 minutes')",&[&subject]).await?;
    }
    nudge(&pool, "whoop", "42", b"event-a").await?;
    nudge(&pool, "whoop", "42", b"event-a").await?;
    nudge(&pool, "whoop", "99", b"event-b").await?;
    let rows = db
        .query(
            "SELECT wake_requested FROM watch_tower.wearable_sync_schedule
        WHERE subject_id LIKE 'wearable-webhook-%' ORDER BY subject_id",
            &[],
        )
        .await?;
    assert!(rows[0].get::<_, bool>(0));
    assert!(!rows[1].get::<_, bool>(0));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM watch_tower.wearable_webhook_receipt
             WHERE provider='whoop' AND event_hash IN ($1,$2)",
            &[&hash(b"event-a"), &hash(b"event-b")]
        )
        .await?
        .get::<_, i64>(0),
        1
    );
    db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET wake_requested=false WHERE subject_id LIKE 'wearable-webhook-%'",
        &[],
    ).await?;
    let owners = [hash(b"42"), hash(b"43")];
    enqueue_nudge(&pool, "whoop", &owners, b"batch-a", &[], None).await?;
    assert_eq!(db.query_one(
        "SELECT count(*) FROM watch_tower.wearable_sync_schedule WHERE subject_id LIKE 'wearable-webhook-%' AND wake_requested",
        &[],
    ).await?.get::<_, i64>(0), 2);
    db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET wake_requested=false WHERE subject_id LIKE 'wearable-webhook-%'",
        &[],
    ).await?;
    enqueue_nudge(&pool, "whoop", &owners, b"batch-a", &[], None).await?;
    assert_eq!(db.query_one(
        "SELECT count(*) FROM watch_tower.wearable_sync_schedule WHERE subject_id LIKE 'wearable-webhook-%' AND wake_requested",
        &[],
    ).await?.get::<_, i64>(0), 0);
    Ok(())
}

#[test]
fn coros_callback_signature_and_owner_batch() -> Result<()> {
    let token = "COROSfixtureToken123";
    let nonce = "test789";
    let signature = |timestamp: String| {
        let mut parts = [token, nonce, timestamp.as_str()];
        parts.sort_unstable();
        let digest = digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, parts.concat().as_bytes());
        CorosSignature {
            signature: digest
                .as_ref()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            nonce: nonce.into(),
            timestamp,
        }
    };
    let mut signed = signature(Utc::now().timestamp().to_string());
    assert!(verify_coros(token, &signed).is_ok());
    signed.nonce.push('x');
    assert!(verify_coros(token, &signed).is_err());
    let stale = signature((Utc::now().timestamp() - 301).to_string());
    assert!(verify_coros(token, &stale).is_err());

    let today = NaiveDate::from_ymd_opt(2026, 11, 23).unwrap();
    let body = json!({"batchDailyList":[
        {"openId":"first","dailyList":[{"happenDay":20261101},{"happenDay":20260822}]},
        {"openId":"first","dailyList":[{"happenDay":20261101},{"happenDay":20261103}]},
        {"openId":"second","dailyList":[{"happenDay":20260921},{"happenDay":20261125}]}
    ]});
    let owners = coros_targets(&body, today)?;
    assert_eq!(owners.len(), 2);
    let date = |month, day| NaiveDate::from_ymd_opt(2026, month, day).unwrap();
    assert_eq!(
        owners[&hash(b"first")],
        BTreeSet::from([date(11, 1), date(11, 3)])
    );
    assert_eq!(owners[&hash(b"second")], BTreeSet::from([date(9, 21)]));
    let timestamp =
        |day: NaiveDate, hour| day.and_hms_opt(hour, 0, 0).unwrap().and_utc().timestamp();
    let workout = json!({"sportDataList":[
        {"openId":"first","startTime":timestamp(date(10, 21), 23),"endTime":timestamp(date(10, 22), 1)},
        {"openId":"second"}
    ]});
    let owners = coros_targets(&workout, today)?;
    assert_eq!(
        owners[&hash(b"first")],
        BTreeSet::from([date(10, 21), date(10, 22)])
    );
    assert!(owners[&hash(b"second")].is_empty());
    assert!(coros_targets(&json!({"sportDataList":[{}]}), today).is_err());
    assert!(coros_targets(&json!({"sportDataList":[],"batchDailyList":[]}), today).is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and synthetic COROS secrets"]
async fn coros_webhook_queues_only_each_accounts_dates_and_ignores_replay() -> Result<()> {
    let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let client_id = env::var("AVATAR_COROS_WEBHOOK_CLIENT")?;
    let secret = env::var("AVATAR_COROS_WEBHOOK_SECRET")?;
    let token = env::var("AVATAR_COROS_WEBHOOK_SIGN_TOKEN")?;
    let db = pool.get().await?;
    for (subject, owner) in [
        ("coros-push-a", "first"),
        ("coros-push-b", "second"),
        ("coros-push-c", "third"),
    ] {
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
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'coros')",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash)
          VALUES($1,'coros',''::bytea,$2)", &[&subject, &hash(owner.as_bytes())]).await?;
        db.execute("INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,next_sync,backfill_from)
          VALUES($1,'coros',now()+interval '2 hours',now())", &[&subject]).await?;
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Arc::new(App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let first = (Utc::now() - chrono::Duration::days(26)).date_naive();
    let second = (Utc::now() - chrono::Duration::days(63)).date_naive();
    let day_number = |date: NaiveDate| date.format("%Y%m%d").to_string().parse::<i32>().unwrap();
    let body = json!({"batchDailyList":[
        {"openId":"first","dailyList":[{"happenDay":day_number(first),"step":12345}]},
        {"openId":"second","dailyList":[{"happenDay":day_number(second)}]},
        {"openId":"unlinked","dailyList":[{"happenDay":day_number(second)}]}
    ]});
    let timestamp = Utc::now().timestamp().to_string();
    let nonce = "fixture1234";
    let mut parts = [token.as_str(), nonce, timestamp.as_str()];
    parts.sort_unstable();
    let digest = digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, parts.concat().as_bytes());
    let signature = digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let url = format!("http://{address}/wearables/webhooks/coros?nonce={nonce}&timestamp={timestamp}&signature={signature}");
    let client = reqwest::Client::new();
    let probe = client
        .get(format!("http://{address}/wearables/webhooks/coros"))
        .send()
        .await?;
    assert_eq!(probe.status(), StatusCode::OK);
    assert!(probe.bytes().await?.is_empty());
    let submit = |secret: &str| {
        client
            .post(&url)
            .header("client", &client_id)
            .header("secret", secret)
            .json(&body)
            .send()
    };
    assert_eq!(
        submit("incorrect").await?.status(),
        StatusCode::UNAUTHORIZED
    );
    let read_targets = || {
        db.query(
            "SELECT subject_id,day,revision::text FROM watch_tower.wearable_target_day
         WHERE provider='coros' AND subject_id LIKE 'coros-push-%' ORDER BY subject_id,day",
            &[],
        )
    };
    assert!(read_targets().await?.is_empty());
    assert_eq!(submit(&secret).await?.status(), StatusCode::OK);
    let targets = read_targets().await?;
    let keys = targets
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, NaiveDate>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        vec![
            ("coros-push-a".into(), first),
            ("coros-push-b".into(), second)
        ]
    );
    let revisions = targets
        .iter()
        .map(|row| row.get::<_, String>(2))
        .collect::<Vec<_>>();
    assert_eq!(submit(&secret).await?.status(), StatusCode::OK);
    assert_eq!(
        read_targets()
            .await?
            .iter()
            .map(|row| row.get::<_, String>(2))
            .collect::<Vec<_>>(),
        revisions
    );
    for row in db
        .query(
            "SELECT subject_id,next_sync<=now(),wake_requested
          FROM watch_tower.wearable_sync_schedule WHERE provider='coros' AND subject_id LIKE 'coros-push-%' ORDER BY subject_id",
            &[],
        )
        .await?
    {
        assert_eq!(
            row.get::<_, bool>(1),
            row.get::<_, String>(0) != "coros-push-c"
        );
        assert!(!row.get::<_, bool>(2));
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM avatar_hsm.wearable_record WHERE provider='coros' AND subject_id LIKE 'coros-push-%'",
            &[]
        )
        .await?
        .get::<_, i64>(0),
        0
    );
    server.abort();
    Ok(())
}
