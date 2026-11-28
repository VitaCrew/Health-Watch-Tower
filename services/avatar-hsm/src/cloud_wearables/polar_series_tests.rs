use super::*;

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn polar_reported_total_wins_and_corrected_intervals_replace_old_rows() -> Result<()> {
    let start = "2026-04-24T00:00:00Z".parse()?;
    let first_end: DateTime<Utc> = "2026-04-25T00:00:00Z".parse()?;
    let day = "2026-04-24";
    let initial = json!({"date":day,"steps":{"total_steps":820,
    "interval_ms":60000,"samples":[
        {"steps":14,"timestamp":"2026-04-24T12:00:00+09:00"},
        {"steps":12,"timestamp":"2026-04-24T12:03:00+09:00"},
        {"steps":18,"timestamp":"2026-04-24T13:00"}
    ]}});
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "polar-activity-fixture";
    pool.get()
        .await?
        .execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
    for (item, end) in [
        (initial, first_end),
        (
            json!({"date":day,"steps":{"total_steps":0,"interval_ms":60000,"samples":[]}}),
            first_end + CalendarDuration::hours(1),
        ),
    ] {
        let window = Window { start, end };
        let mut rows = vec![record(
            "metric",
            "Polar",
            "activity_samples:2026-04-24",
            start,
            start,
            end,
            json!({"metric":"activity_samples","provider_data":item}),
        )];
        activity_steps(&mut rows, &item, &window)?;
        let lease = wearables::issue_lease_for(&pool, subject, "polar").await?;
        wearables::store_snapshot(&pool, subject, "polar", serde_json::from_value(json!({
            "lease":lease["lease"],"start":start,"end":end,"grants":["steps","metric"],"records":rows
        }))?, false).await?;
        let daily = wearables::daily_for(
            &pool,
            subject,
            wearables::DailyQuery {
                start_day: day.parse()?,
                end_day: day.parse()?,
                source: None,
                provider: Some("polar".into()),
                kind: Some("steps".into()),
            },
        )
        .await?;
        assert_eq!(daily["records"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            daily["records"][0]["stats"]["total"],
            item["steps"]["total_steps"]
        );
        assert_eq!(daily["records"][0]["calculated_by"], "provider");
        assert!(daily["records"][0]["timezone"].is_null());
        let steps = wearables::records_for(
            &pool,
            subject,
            wearables::RecordQuery {
                kind: "steps".into(),
                start,
                end,
                offset: 0,
                source: None,
                provider: Some("polar".into()),
                metric: None,
            },
            false,
        )
        .await?;
        let expect = if end == first_end { 2 } else { 0 };
        assert_eq!(steps["records"].as_array().map(Vec::len), Some(expect));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn polar_sleep_series_keep_midnight_and_offset_in_storage() -> Result<()> {
    let start = "2026-04-24T00:00:00Z".parse()?;
    let end = "2026-04-26T00:00:00Z".parse()?;
    let window = Window { start, end };
    let item = json!({
        "sleep_start_time":"2026-04-24T23:55:17+09:00","sleep_end_time":"2026-04-25T07:00:00+09:00",
        "hypnogram":{"23:55":2,"23:58":4,"00:05":1},
        "heart_rate_samples":{"00:10":62},"date":"2026-04-25",
        "sleep_score":80,"group_duration_score":99.5,"group_solidity_score":75,"group_regeneration_score":54.2,
        "total_interruption_duration":1000,"light_sleep":10000,"deep_sleep":1000,"rem_sleep":2000,
        "unrecognized_sleep_stage":0,"short_interruption_duration":300,"long_interruption_duration":700,
        "sleep_charge":3,"continuity":2.1,"continuity_class":2,"sleep_cycles":6,"sleep_rating":0,"sleep_goal":28800,
        "hrv_samples":{"23:59":30,"00:15":44},"breathing_samples":{"00:20":14.5}
    });
    let night = Night::parse(&item)?;
    let (from, until) = night.utc_bounds();
    let stages = night.stages(&item["hypnogram"]);
    assert_eq!(stages[0]["start"], "2026-04-24T14:55:17Z");
    assert_eq!(stages[1]["stage"], 5);
    assert_eq!(stages[2]["start"], "2026-04-24T15:05:00Z");
    assert_eq!(stages[2]["stage"], 6);
    let mut rows = vec![record(
        "sleep",
        "Polar",
        "2026-04-25",
        from,
        until,
        end,
        json!({"stages":stages,"provider_data":item}),
    )];
    night.heart_rate(
        &mut rows,
        "2026-04-25",
        &item["heart_rate_samples"],
        &window,
    );
    night.recharge(&mut rows, &item, &window);
    assert_eq!(rows.len(), 5);
    let dst = Night::parse(&json!({
        "sleep_start_time":"2026-03-28T23:50:00+01:00","sleep_end_time":"2026-03-29T07:00:00+02:00"
    }))?;
    assert!(dst.stages(&item["hypnogram"]).is_empty());

    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "polar-series-fixture";
    pool.get()
        .await?
        .execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
    let lease = wearables::issue_lease_for(&pool, subject, "polar").await?;
    wearables::store_snapshot(
        &pool,
        subject,
        "polar",
        serde_json::from_value(json!({
            "lease":lease["lease"],"start":start,"end":end,
            "grants":["heart_rate","metric","sleep"],"records":rows
        }))?,
        false,
    )
    .await?;
    let result = wearables::records_for(
        &pool,
        subject,
        wearables::RecordQuery {
            kind: "metric".into(),
            start,
            end,
            offset: 0,
            source: None,
            provider: Some("polar".into()),
            metric: Some("hrv_rmssd".into()),
        },
        false,
    )
    .await?;
    assert_eq!(result["records"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        result["records"][1]["payload"]["provider_data"]["value"],
        44
    );
    assert_eq!(
        result["records"][1]["started_at"],
        "2026-04-24T15:15:00+00:00"
    );
    let read_token = "polar-sleep-fixture-read-key-0123456789-abcdef";
    pool.get().await?.execute(
        "INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
        &[&subject, &read_token],
    ).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/mcp/wearable-read", listener.local_addr()?);
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let sessions = async || -> Result<Value> {
        let response: Value = Client::new().post(&url).bearer_auth(read_token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"wearable_sessions","arguments":{"provider":"polar","kind":"sleep","start":start,"end":end}
            }})).send().await?.error_for_status()?.json().await?;
        Ok(response["result"]["structuredContent"].clone())
    };
    let sleep = sessions().await?;
    let payload = &sleep["records"][0]["payload"];
    assert_eq!(payload["stages"].as_array().map(Vec::len), Some(3));
    assert_eq!(payload["sleep_score"], 80);
    assert_eq!(
        payload["sleep_score_components"]["duration"].as_f64(),
        Some(99.5)
    );
    assert_eq!(payload["provider_stage_summary"]["unrecognized_seconds"], 0);
    assert_eq!(payload["provider_stage_summary"]["awake_seconds"], 1000);
    assert_eq!(
        payload["sleep_metrics"]["sleep_window_seconds"].as_f64(),
        Some(25483.0)
    );
    assert_eq!(
        payload["sleep_metrics"]["asleep_seconds"].as_f64(),
        Some(24483.0)
    );
    assert_eq!(
        payload["sleep_metrics"]["sleep_efficiency_percent"].as_f64(),
        Some(96.08)
    );
    assert_eq!(payload["sleep_metrics"]["sleep_rating"], Value::Null);
    let mut corrected = rows[0].clone();
    corrected["modified_at"] = json!(end + CalendarDuration::hours(1));
    corrected["payload"]["provider_data"]["total_interruption_duration"] = json!(90000);
    corrected["payload"]["provider_data"]["sleep_score"] = Value::Null;
    let lease = wearables::issue_lease_for(&pool, subject, "polar").await?;
    wearables::store_snapshot(&pool, subject, "polar", serde_json::from_value(json!({
        "lease":lease["lease"],"start":start,"end":end,"grants":["heart_rate","metric","sleep"],"records":[corrected]
    }))?,false).await?;
    let revised = sessions().await?;
    let payload = &revised["records"][0]["payload"];
    assert_eq!(
        payload["sleep_metrics"]["sleep_efficiency_percent"],
        Value::Null
    );
    assert_eq!(payload["sleep_score"], Value::Null);
    server.abort();
    Ok(())
}
