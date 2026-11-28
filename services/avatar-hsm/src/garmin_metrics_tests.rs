use super::*;
use deadpool_postgres::Pool;

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn garmin_sleep_projects_legacy_naps_and_replaces_them_on_correction() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "garmin-sleep-naps-fixture";
    let token = "garmin-sleep-read-0123456789012345678901234567";
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    tx.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_profile VALUES
         ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
        &[&subject, &token],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider,state,grants)
         VALUES($1,'garmin','synced','{sleep}')",
        &[&subject],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_oauth_connection
         (subject_id,provider,sealed_tokens,garmin_permissions,garmin_daily_projected)
         VALUES($1,'garmin',''::bytea,2,true)",
        &[&subject],
    )
    .await?;
    let at: DateTime<Utc> = "2026-04-24T00:00:00Z".parse()?;
    let nap = at + Duration::hours(12);
    let mut raw = json!({"summaryId":"night-1",
        "startTimeInSeconds":at.timestamp(),"durationInSeconds":25200,
        "deepSleepDurationInSeconds":3600,"remSleepInSeconds":4200,
        "overallSleepScore":{"value":83,"qualifier":"GOOD"},
        "sleepScores":{"duration":{"qualifierKey":"EXCELLENT"}},
        "validation":"AUTO_FINAL",
        "naps":[{"napStartTimeInSeconds":nap.timestamp(),"napDurationInSeconds":1800,"napOffsetInSeconds":32400},
                {"napStartTimeInSeconds":nap.timestamp()+4000,"napDurationInSeconds":0}]
    });
    let original =
        crate::garmin_push::decode_record(&raw, "sleep", "sleeps", at + Duration::hours(18))?;
    wearables::upsert_record(&tx, subject, PROVIDER, &original).await?;
    tx.batch_execute(include_str!("../migrations/052-garmin-sleep-details.sql"))
        .await?;
    tx.commit().await?;
    while rebuild_one(&pool).await? {}
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/mcp/wearable-read", listener.local_addr()?);
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let client = crate::cloud_wearables::http_client()?;
    let read = async || -> Result<Value> {
        let reply: Value = client.post(&url).bearer_auth(token).json(&json!({
            "jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wearable_sessions",
            "arguments":{"provider":"garmin","kind":"sleep","start":at,"end":at+Duration::days(1)}}
        })).send().await?.error_for_status()?.json().await?;
        Ok(reply["result"]["structuredContent"]["records"].clone())
    };
    let first = read().await?;
    assert_eq!(first.as_array().map(Vec::len), Some(2));
    assert_eq!(first[0]["payload"]["sleep_score"], 83);
    assert_eq!(
        first[0]["payload"]["sleep_score_components"]["duration"],
        "EXCELLENT"
    );
    assert_eq!(
        first[0]["payload"]["provider_stage_summary"]["rem_seconds"],
        4200
    );
    assert_eq!(first[1]["payload"]["nap"], true);
    assert_eq!(first[1]["payload"]["utc_offset_seconds"], 32400);
    let ended_at: DateTime<Utc> = first[1]["ended_at"].as_str().unwrap().parse()?;
    assert_eq!(ended_at, nap + Duration::minutes(30));
    raw["naps"] = json!([]);
    raw["overallSleepScore"] = json!({"value":85});
    let corrected =
        crate::garmin_push::decode_record(&raw, "sleep", "sleeps", at + Duration::hours(19))?;
    for record in [&corrected, &original] {
        let tx = db.transaction().await?;
        crate::garmin_push::store_records(&tx, subject, std::iter::once(record), false).await?;
        tx.commit().await?;
    }
    let revised = read().await?;
    assert_eq!(revised.as_array().map(Vec::len), Some(1));
    assert_eq!(revised[0]["payload"]["sleep_score"], 85);
    wearables::revoke_for(&pool, subject, PROVIDER).await?;
    assert_eq!(read().await?, json!([]));
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and synthetic Garmin webhook configuration"]
async fn garmin_cycle_and_move_iq_webhooks_are_scoped_and_queryable() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let path_key = std::env::var("AVATAR_GARMIN_WEBHOOK_PATH_KEY")?;
    let client_id = std::env::var("AVATAR_GARMIN_CLIENT_ID")?;
    let subject = "garmin-cycle-owner";
    let read_key = "garmin-cycle-read-0123456789012345678901234567";
    let db = pool.get().await?;
    for (owner, account, permissions, token) in [
        (subject, "cycle-remote-owner", 2i16, read_key),
        (
            "garmin-cycle-denied",
            "cycle-remote-denied",
            1i16,
            "garmin-cycle-denied-key-012345678901234567",
        ),
    ] {
        let fingerprint = crate::wearable_webhooks::hash(account.as_bytes());
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&owner],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider,state,grants)
            VALUES($1,'garmin','synced','{metric}')",
            &[&owner],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile
            VALUES($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
            &[&owner, &token],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,
            provider_account_hash,garmin_permissions,garmin_daily_projected) VALUES($1,'garmin',''::bytea,$2,$3,true)",
            &[&owner,&fingerprint,&permissions]).await?;
    }
    let through: DateTime<Utc> = "2026-05-01T23:00:00Z".parse()?;
    let payload = |account, version: i64, phase| {
        json!({
            "userId":account,"summaryId":"cycle-1","calendarDate":"2026-05-01","periodStartDate":"2026-04-01",
            "dayInCycle":31,"cycleLength":30,"currentPhaseType":phase,"isPredictedCycle":false,
            "predictedCycleLength":32,"lengthOfFertileWindow":0,"hasSpecifiedPeriodLength":false,
            "lastUpdatedTimeInSeconds":through.timestamp()+version
        })
    };
    let movement = |account, id, offset: Option<i32>, duration| {
        json!({"userId":account,"summary":{
            "summaryId":id,"startTimeInSeconds":through.timestamp(),
            "startTimeOffsetInSeconds":offset,"durationInSeconds":duration,
            "activityType":"WALKING"
        }})
    };
    let mut old = crate::garmin_push::decode_record(
        &json!({"summaryId":"old-prediction","periodStartDate":"2026-05-05","predictedCycleLength":32,
            "isPredictedCycle":true,"lastUpdatedTimeInSeconds":through.timestamp()}),
        "metric",
        "mct",
        Utc::now(),
    )?;
    old.modified_at = Utc::now();
    let mut writer = pool.get().await?;
    let tx = writer.transaction().await?;
    wearables::upsert_record(&tx, subject, PROVIDER, &old).await?;
    let old_movement = crate::garmin_push::decode_record(
        &movement("cycle-remote-owner", "old-movement", None, 600),
        "metric",
        "moveIQActivities",
        through,
    )?;
    wearables::upsert_record(&tx, subject, PROVIDER, &old_movement).await?;
    tx.batch_execute(include_str!("../migrations/047-garmin-menstrual-cycle.sql"))
        .await?;
    tx.execute(
        "UPDATE avatar_hsm.wearable_oauth_connection SET garmin_daily_projected=true
        WHERE subject_id=$1 AND provider='garmin'",
        &[&subject],
    )
    .await?;
    tx.batch_execute(include_str!("../migrations/061-garmin-move-iq.sql"))
        .await?;
    tx.commit().await?;
    assert_eq!(
        db.query_one(
            "SELECT modified_at FROM avatar_hsm.wearable_record
            WHERE subject_id=$1 AND provider='garmin' AND source_id=$2",
            &[&subject, &old.source_id]
        )
        .await?
        .get::<_, DateTime<Utc>>(0),
        through
    );
    while rebuild_one(&pool).await? {}
    assert!(db
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM avatar_hsm.wearable_record
         WHERE subject_id=$1 AND provider='garmin' AND source_id='move_iq:old-movement')",
            &[&subject]
        )
        .await?
        .get::<_, bool>(0));
    drop(writer);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: read_key.into(),
        control_token: "synthetic-cycle-control-key-0123456789".into(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let client = crate::cloud_wearables::http_client()?;
    let endpoint = format!("{origin}/wearables/webhooks/garmin/{path_key}");
    let fresh = json!({"mct":[payload("cycle-remote-owner", 1, "LUTEAL"),
        payload("cycle-remote-denied", 1, "FOLLICULAR"),payload("unmapped", 1, "FOLLICULAR")],
        "moveIQActivities":[movement("cycle-remote-owner","movement",Some(32400),660),
            movement("cycle-remote-denied","movement",Some(32400),660)]});
    assert_eq!(
        client.post(&endpoint).json(&fresh).send().await?.status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    for body in [
        &fresh,
        &fresh,
        &json!({"mct":[payload("cycle-remote-owner",0,"STALE")]}),
    ] {
        client
            .post(&endpoint)
            .header("garmin-client-id", &client_id)
            .json(body)
            .send()
            .await?
            .error_for_status()?;
    }
    let response: Value = client.post(format!("{origin}/mcp/wearable-read"))
        .bearer_auth(read_key).json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
            "name":"wearable_samples","arguments":{"kind":"metric","provider":"garmin","metric":"menstrual_cycle",
            "start":"2026-04-20T00:00:00Z","end":"2026-05-20T00:00:00Z"}
        }})).send().await?.error_for_status()?.json().await?;
    let records = response["result"]["structuredContent"]["records"]
        .as_array()
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(
        db.query_one(
            "SELECT payload->'provider_data'->>'currentPhaseType'
        FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND source_id='mct:cycle-1'",
            &[&subject]
        )
        .await?
        .get::<_, String>(0),
        "LUTEAL"
    );
    let data = &records[0]["payload"]["provider_data"];
    assert_eq!(data["current_phase_type"], "LUTEAL");
    assert_eq!(data["fertile_window_length_days"], 0);
    assert_eq!(data["has_specified_period_length"], false);
    assert_eq!(records[0]["ended_at"], "2026-05-01T00:00:00+00:00");
    assert_eq!(records[1]["started_at"], records[1]["ended_at"]);
    assert_eq!(
        records[1]["payload"]["provider_data"]["predicted_cycle_length_days"],
        32
    );
    client.post(&endpoint).header("garmin-client-id", &client_id)
        .json(&json!({"moveIQActivities":[movement("cycle-remote-owner","movement",Some(32400),780)]}))
        .send().await?.error_for_status()?;
    let response: Value = client.post(format!("{origin}/mcp/wearable-read"))
        .bearer_auth(read_key).json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
            "name":"wearable_samples","arguments":{"kind":"metric","provider":"garmin","metric":"garmin_move_iq",
            "start":"2026-05-01T00:00:00Z","end":"2026-05-03T00:00:00Z"}
        }})).send().await?.error_for_status()?.json().await?;
    let events = response["result"]["structuredContent"]["records"]
        .as_array()
        .unwrap();
    assert_eq!(events.len(), 2);
    let event = events
        .iter()
        .find(|event| event["source_id"] == "move_iq:movement")
        .unwrap();
    let data = &event["payload"]["provider_data"];
    assert_eq!(data["activity_type"], "walking");
    assert_eq!(data["duration_seconds"], 780);
    assert_eq!(data["utc_offset_seconds"], 32400);
    assert_eq!(data["local_day"], "2026-05-02");
    assert_eq!(data["automatically_detected"], true);
    let end: DateTime<Utc> = event["ended_at"].as_str().unwrap().parse()?;
    assert_eq!(end, through + Duration::seconds(780));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM avatar_hsm.wearable_record
         WHERE subject_id=$1 AND provider='garmin' AND kind='workout'",
            &[&subject]
        )
        .await?
        .get::<_, i64>(0),
        0
    );
    let count = db
        .query_one(
            "SELECT count(*) FROM avatar_hsm.wearable_record
        WHERE subject_id='garmin-cycle-denied'",
            &[],
        )
        .await?
        .get::<_, i64>(0);
    assert_eq!(count, 0);
    client
        .post(&endpoint)
        .header("garmin-client-id", &client_id)
        .json(&json!({"deregistrations":[{"userId":"cycle-remote-owner"}]}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM avatar_hsm.wearable_record WHERE subject_id=$1",
            &[&subject]
        )
        .await?
        .get::<_, i64>(0),
        0
    );
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn garmin_daily_totals_prefer_provider_and_deduplicate_epoch_revisions() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let mut db = pool.get().await?;
    let subject = "garmin-metrics-fixture";
    let read_token = "garmin-metrics-fixture-read-123456789012345678901234";
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.device_connection(subject_id,provider,state,grants) VALUES($1,'garmin','synced','{metric}')", &[&subject]).await?;
    db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($1||'c','UTF8')))", &[&subject,&read_token]).await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,garmin_permissions,garmin_daily_projected)
        VALUES($1,'garmin',''::bytea,2,true)", &[&subject]).await?;
    let first: DateTime<Utc> = "2026-04-24T00:00:00Z".parse()?;
    let second = first + Duration::days(1);
    let received = Utc::now();
    let render = |fields: Value, feed, received| {
        crate::garmin_push::decode_record(&fields, "metric", feed, received)
    };
    let daily = render(
        json!({"summaryId":"daily-1","calendarDate":"2026-04-24","startTimeInSeconds":first.timestamp(),
        "startTimeOffsetInSeconds":32400,"durationInSeconds":3600,"steps":900,"timeOffsetHeartRateSamples":{"60":72,"120":84,"180":0,"9999":75},
        "distanceInMeters":1050.5,"activeKilocalories":120.5,"bmrKilocalories":1000,
        "floorsClimbed":0,"activeTimeInSeconds":1800,"moderateIntensityDurationInSeconds":600,"vigorousIntensityDurationInSeconds":240,
        "restingHeartRateInBeatsPerMinute":55}),
        "dailies",
        received,
    )?;
    let epoch = |id, start: DateTime<Utc>, category, count, at| {
        render(
            json!({"summaryId":id,"startTimeInSeconds":start.timestamp(),
        "durationInSeconds":900,"activityType":category,"steps":count,
        "activeKilocalories":count,"activeTimeInSeconds":150,
        "meanHeartRateInBeatsPerMinute":80}),
            "epochs",
            at,
        )
    };
    let stress = |values: Value, at| {
        render(
            json!({"summaryId":"stress-1","startTimeInSeconds":first.timestamp(),
            "durationInSeconds":3600,"timeOffsetStressLevelValues":values,
            "bodyBatteryValues":{"120":77}}),
            "stressDetails",
            at,
        )
    };
    let initial_stress = stress(json!({"0":0,"60":48,"180":62,"3601":45}), received)?;
    let initial_oxygen = render(
        json!({"summaryId":"oxygen-1","startTimeInSeconds":first.timestamp(),
            "calendarDate":"2026-04-24","durationInSeconds":3600,
            "avgSpo2":97,"timeOffsetSpo2Values":{"120":97.5,"240":255}}),
        "pulseox",
        received,
    )?;
    let rows = [
        daily,
        epoch("first", first, "WALKING", 40, received)?,
        epoch(
            "corrected",
            first,
            "WALKING",
            42,
            received + Duration::seconds(1),
        )?,
        epoch("running", first, "RUNNING", 30, received)?,
        epoch("day-2", second, "WALKING", 25, received)?,
        render(
            json!({"summaryId":"breath-1","startTimeInSeconds":first.timestamp(),
            "calendarDate":"2026-04-24","durationInSeconds":3600,
            "avgWakingRespirationValue":15.25,"timeOffsetEpochToBreaths":{"120":14.5,"240":0}}),
            "allDayRespiration",
            received,
        )?,
        render(
            json!({"summaryId":"hrv-1","calendarDate":"2026-04-24","startTimeInSeconds":first.timestamp(),
                "durationInSeconds":3600,"lastNightAvg":51,"hrvValues":{"120":46.5,"600":55,"9999":60}}),
            "hrv",
            received,
        )?,
        render(
            json!({"summaryId":"body-1","measurementTimeInSeconds":first.timestamp()+300,
                "measurementTimeOffsetInSeconds":32400,"weightInGrams":73450,"muscleMassInGrams":32800,
                "boneMassInGrams":3200,"bodyFatInPercent":24.5,"bodyWaterInPercent":55,"bodyMassIndex":24.2}),
            "bodyComps",
            received,
        )?,
        render(
            json!({"summaryId":"bp-1","startTimeInSeconds":first.timestamp()+60,
            "measurementTimestampGMT":first+Duration::seconds(300),"measurementTimeOffsetInSeconds":32400,
            "systolic":118,"diastolic":76}),
            "bloodPressures",
            received,
        )?,
        render(
            json!({"summaryId":"user-1","calendarDate":"2026-04-24","vo2Max":42.5,"fitnessAge":39}),
            "userMetrics",
            received,
        )?,
        render(
            json!({"summaryId":"skin-1","calendarDate":"2026-04-24","avgDeviationCelsius":-0.4}),
            "skinTemp",
            received,
        )?,
        render(
            json!({"summaryId":"snapshot-1","startTimeInSeconds":first.timestamp()+600,"durationInSeconds":120,
            "summaries":[{"summaryType":"heart_rate","avgValue":67},{"summaryType":"rmssd_hrv","avgValue":48},
                         {"summaryType":"sdrr_hrv","avgValue":54},{"summaryType":"stress","avgValue":0},
                         {"summaryType":"pulse_ox","avgValue":98},{"summaryType":"respiration","avgValue":13.5}]}),
            "healthSnapshot",
            received,
        )?,
    ];
    let tx = db.transaction().await?;
    for row in &rows {
        wearables::upsert_record(&tx, subject, PROVIDER, row).await?;
    }
    wearables::upsert_record(&tx, subject, PROVIDER, &initial_stress).await?;
    wearables::upsert_record(&tx, subject, PROVIDER, &initial_oxygen).await?;
    tx.batch_execute(include_str!(
        "../migrations/053-garmin-respiration-oxygen-averages.sql"
    ))
    .await?;
    tx.commit().await?;
    drop(db);
    assert!(rebuild_one(&pool).await?);
    assert!(!rebuild_one(&pool).await?);
    let activity = daily_query(&pool, subject, "activity").await?;
    assert_eq!(
        activity["records"][0]["stats"]["total_energy_kcal"].as_f64(),
        Some(1120.5)
    );
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let latest = epoch(
        "day-2-corrected",
        second,
        "WALKING",
        25,
        received + Duration::seconds(2),
    )?;
    let mut updated_summary = rows[0].payload["provider_data"].clone();
    updated_summary["activeKilocalories"] = json!(124.5);
    let revised = render(updated_summary, "dailies", received + Duration::seconds(3))?;
    let revised_stress = stress(
        json!({"0":0,"60":-1,"180":63,"3601":45}),
        received + Duration::seconds(3),
    )?;
    let oxygen_summary_only = render(
        json!({"summaryId":"oxygen-1","startTimeInSeconds":first.timestamp(),
            "calendarDate":"2026-04-24","durationInSeconds":3600,"avgSpo2":98}),
        "pulseox",
        received + Duration::seconds(4),
    )?;
    crate::garmin_push::store_records(
        &tx,
        subject,
        [
            &latest,
            &revised,
            &rows[0],
            &revised_stress,
            &initial_stress,
            &oxygen_summary_only,
            &initial_oxygen,
        ]
        .into_iter(),
        false,
    )
    .await?;
    tx.commit().await?;
    drop(db);
    let result = daily_query(&pool, subject, "steps").await?;
    assert_eq!(result["records"].as_array().map(Vec::len), Some(2));
    assert_eq!(result["records"][0]["stats"]["total"], 900);
    assert_eq!(result["records"][0]["calculated_by"], "provider");
    assert_eq!(result["records"][1]["stats"]["total"], 25);
    assert_eq!(result["records"][1]["calculated_by"], "hsm");
    let db = pool.get().await?;
    let computed: Value=db.query_one("SELECT stats FROM avatar_hsm.wearable_daily WHERE subject_id=$1 AND kind='steps' AND local_day='2026-04-24' AND calculated_by='hsm'", &[&subject]).await?.get(0);
    assert_eq!(computed["total"], 72);
    let heart = daily_query(&pool, subject, "heart_rate").await?;
    assert_eq!(heart["records"][0]["stats"]["sample_count"], 2);
    assert_eq!(heart["records"][0]["stats"]["average"].as_f64(), Some(78.0));
    let stress: Value = db.query_one(
        "SELECT payload FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='garmin' AND source_id='stressDetails:stress-1'",
        &[&subject]
    ).await?.get(0);
    assert_eq!(
        stress["provider_data"]["timeOffsetStressLevelValues"]["60"],
        -1
    );

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: "synthetic-operator-fixture-read-token-123456".into(),
        control_token: "synthetic-operator-fixture-control-token-123456".into(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let client = crate::cloud_wearables::http_client()?;
    let response = client.post(format!("http://{address}/mcp/wearable-read"))
        .bearer_auth(read_token)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wearable_daily","arguments":{
            "provider":"garmin","kind":"activity","start_day":"2026-04-24","end_day":"2026-04-25"
        }}}))
        .send().await?.error_for_status()?.json::<Value>().await?;
    let activity = &response["result"]["structuredContent"]["records"];
    assert_eq!(activity.as_array().map(Vec::len), Some(1), "{response}");
    assert_eq!(activity[0]["timezone"], "UTC+09:00");
    assert_eq!(activity[0]["calculated_by"], "provider");
    assert_eq!(
        activity[0]["stats"],
        json!({
            "distance_meters":1050.5,"active_energy_kcal":124.5,"basal_energy_kcal":1000,
        "total_energy_kcal":1124.5,"floors_climbed":0,"active_seconds":1800,"moderate_intensity_seconds":600,
            "vigorous_intensity_seconds":240,"resting_heart_rate_bpm":55
        })
    );
    for (metric, unit, expected) in [
        ("garmin_stress_level", "score", json!([0.0, 63.0])),
        ("garmin_body_battery", "score", json!([77.0])),
        ("respiratory_rate", "breaths/min", json!([14.5])),
        ("oxygen_saturation", "%", json!([97.5])),
        (
            "garmin_average_waking_respiratory_rate",
            "breaths/min",
            json!([15.25]),
        ),
        ("garmin_average_oxygen_saturation", "%", json!([98.0])),
        ("hrv_rmssd", "ms", json!([46.5, 55.0])),
        ("garmin_nightly_hrv_rmssd", "ms", json!([51.0])),
        ("epoch_active_energy", "kcal", json!([30.0, 42.0])),
        ("epoch_active_time", "s", json!([150.0, 150.0])),
        ("epoch_average_heart_rate", "bpm", json!([80.0, 80.0])),
        ("weight", "kg", json!([73.45])),
        ("skeletal_muscle_mass", "kg", json!([32.8])),
        ("bone_mass", "kg", json!([3.2])),
        ("body_fat", "%", json!([24.5])),
        ("body_water_percentage", "%", json!([55.0])),
        ("body_mass_index", "kg/m²", json!([24.2])),
        ("blood_pressure_systolic", "mmHg", json!([118.0])),
        ("blood_pressure_diastolic", "mmHg", json!([76.0])),
        ("vo2_max", "mL/kg/min", json!([42.5])),
        ("garmin_fitness_age", "years", json!([39.0])),
        ("skin_temperature_deviation", "°C", json!([-0.4])),
        ("health_snapshot_average_heart_rate", "bpm", json!([67.0])),
        ("health_snapshot_hrv_rmssd", "ms", json!([48.0])),
        ("health_snapshot_hrv_sdrr", "ms", json!([54.0])),
        ("health_snapshot_stress", "score", json!([0.0])),
        ("health_snapshot_oxygen_saturation", "%", json!([98.0])),
        (
            "health_snapshot_respiratory_rate",
            "breaths/min",
            json!([13.5]),
        ),
    ] {
        let response = client.post(format!("http://{address}/mcp/wearable-read"))
            .bearer_auth(read_token)
            .json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"wearable_samples","arguments":{
                "provider":"garmin","kind":"metric","metric":metric,"start":first,"end":second
            }}}))
            .send().await?.error_for_status()?.json::<Value>().await?;
        let records = response["result"]["structuredContent"]["records"]
            .as_array()
            .expect("MCP returned records");
        let mut values = Vec::new();
        for record in records {
            let data = &record["payload"]["provider_data"];
            assert_eq!(data["unit"], unit);
            let timestamp: DateTime<Utc> = record["started_at"].as_str().unwrap().parse()?;
            let end: DateTime<Utc> = record["ended_at"].as_str().unwrap().parse()?;
            if let Some(offset) = data["offset_seconds"].as_i64() {
                assert_eq!((timestamp - first).num_seconds(), offset);
                assert_eq!(timestamp, end);
            } else if metric.starts_with("epoch_") {
                assert_eq!(timestamp, first);
                assert_eq!(end - timestamp, Duration::seconds(900));
            } else if data.get("utc_offset_seconds").is_some() {
                assert_eq!(data["utc_offset_seconds"], 32400);
                assert_eq!(timestamp, first + Duration::seconds(300));
                assert_eq!(timestamp, end);
            } else if data["aggregation"] == "average" {
                assert_eq!(timestamp, first + Duration::seconds(600));
                assert_eq!(end - timestamp, Duration::seconds(120));
            } else if metric.starts_with("garmin_average_") {
                assert_eq!(data["aggregation"], "provider_average");
                assert_eq!(data["local_day"], "2026-04-24");
                assert_eq!(timestamp, first);
                assert_eq!(end - timestamp, Duration::hours(1));
            } else if data["time_basis"] == "provider_day" {
                assert_eq!(data["local_day"], "2026-04-24");
                assert_eq!(timestamp, first);
                assert_eq!(timestamp, end);
            } else {
                assert_eq!(data["local_day"], "2026-04-24");
                assert_eq!(end - timestamp, Duration::seconds(3600));
            }
            values.push(data["value"].as_f64().unwrap());
        }
        assert_eq!(json!(values), expected, "{response}");
    }
    let mut connection = pool.get().await?;
    let tx = connection.transaction().await?;
    let withdrawn_average = render(
        json!({"summaryId":"oxygen-1","startTimeInSeconds":first.timestamp(),"avgSpo2":255}),
        "pulseox",
        received + Duration::seconds(5),
    )?;
    crate::garmin_push::store_records(&tx, subject, std::iter::once(&withdrawn_average), false)
        .await?;
    tx.commit().await?;
    assert_eq!(
        connection
            .query_one(
                "SELECT count(*) FROM avatar_hsm.wearable_record WHERE subject_id=$1
         AND payload->>'metric'='garmin_average_oxygen_saturation'",
                &[&subject],
            )
            .await?
            .get::<_, i64>(0),
        0
    );
    server.abort();
    Ok(())
}

async fn daily_query(pool: &Pool, subject: &str, kind: &str) -> Result<Value> {
    wearables::daily_for(
        pool,
        subject,
        wearables::DailyQuery {
            start_day: "2026-04-24".parse()?,
            end_day: "2026-04-25".parse()?,
            source: None,
            provider: Some("garmin".into()),
            kind: Some(kind.into()),
        },
    )
    .await
}
