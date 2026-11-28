use super::*;
use chrono::Utc;

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn garmin_workout_points_are_paged_scoped_and_replaced_atomically() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "garmin-workout-samples-fixture";
    let read_token = "garmin-workout-samples-fixture-read-123456789012345";
    let activity = "123456";
    let start = 1_777_010_000;
    let received = Utc::now();
    let render = |samples: Value, received| {
        crate::garmin_push::decode_record(
            &json!({"activityId":activity,"summary":{"startTimeInSeconds":start,"durationInSeconds":120},
                "samples":samples}),
            "metric",
            "activityDetails",
            received,
        )
    };
    let original = render(
        json!([
            {"startTimeInSeconds":start+2,"powerInWatts":153,"airTemperatureCelcius":-2.5},
            {"startTimeInSeconds":start+1,"heartRate":124,"speedMetersPerSecond":2.8,"stepsPerMinute":165},
            {"startTimeInSeconds":start+1,"latitudeInDegree":37.5,"longitudeInDegree":127,"elevationInMeters":-1},
            {"startTimeInSeconds":start+3,"heartRate":500,"latitudeInDegree":200,"longitudeInDegree":127},
            {"startTimeInSeconds":start+121,"heartRate":125}
        ]),
        received,
    )?;
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    tx.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider,state,grants)
        VALUES($1,'garmin','synced','{metric}')",
        &[&subject],
    )
    .await?;
    tx.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
        &[&subject,&read_token]).await?;
    tx.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,garmin_permissions)
        VALUES($1,'garmin',''::bytea,1)", &[&subject]).await?;
    crate::wearables::upsert_record(&tx, subject, "garmin", &original).await?;
    tx.commit().await?;
    drop(db);
    assert!(rebuild_one(&pool).await?);
    assert!(!rebuild_one(&pool).await?);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: "garmin-json-fixture-operator-read-token-123456".into(),
        control_token: "garmin-json-fixture-operator-control-token-123456".into(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let client = crate::cloud_wearables::http_client()?;
    let url = format!("http://{address}/mcp/wearable-read");
    let samples = |offset| Query {
        source: "Garmin Connect".into(),
        source_id: activity.into(),
        offset,
        limit: Some(1),
    };
    let response = client.post(&url).bearer_auth(read_token)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wearable_sessions",
            "arguments":{"samples":{"source":"Garmin Connect","source_id":activity,"limit":1}}}}))
        .send().await?.error_for_status()?.json::<Value>().await?;
    let page = &response["result"]["structuredContent"];
    assert_eq!(page["total"], 2, "{response}");
    assert_eq!(page["next_offset"], 1);
    assert_eq!(
        page["records"][0],
        json!({
            "timestamp":DateTime::from_timestamp(start+1,0).unwrap(),"heart_rate_bpm":124.0,
            "speed_mps":2.8,"cadence_steps_per_min":165.0,"latitude_deg":37.5,"longitude_deg":127.0,
            "elevation_m":-1.0
        })
    );
    let next = query(&pool, subject, samples(1)).await?;
    assert_eq!(next["records"][0]["power_w"].as_f64(), Some(153.0));
    assert_eq!(next["records"][0]["temperature_c"].as_f64(), Some(-2.5));
    assert_eq!(next["next_offset"], Value::Null);
    assert_eq!(
        query(&pool, "other-workout-owner", samples(0)).await?["state"],
        "not_found"
    );

    let latest = render(
        json!([{"startTimeInSeconds":start+2,"heartRate":120}]),
        received + chrono::Duration::seconds(1),
    )?;
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    crate::garmin_push::store_records(&tx, subject, [&latest, &original].into_iter(), false)
        .await?;
    tx.commit().await?;
    let page = query(&pool, subject, samples(0)).await?;
    assert_eq!(page["total"], 1);
    assert_eq!(page["records"][0]["heart_rate_bpm"].as_f64(), Some(120.0));
    db.execute("UPDATE avatar_hsm.wearable_oauth_connection SET garmin_permissions=2 WHERE subject_id=$1 AND provider='garmin'",
        &[&subject]).await?;
    assert_eq!(
        query(&pool, subject, samples(0)).await?["state"],
        "not_found"
    );
    crate::wearables::revoke_for(&pool, subject, "garmin").await?;
    let rows: i64 = db
        .query_one(
            "SELECT count(*) FROM avatar_hsm.garmin_workout_samples WHERE subject_id=$1",
            &[&subject],
        )
        .await?
        .get(0);
    assert_eq!(rows, 0);
    server.abort();
    Ok(())
}
