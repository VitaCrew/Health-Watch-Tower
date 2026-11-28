use super::*;

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn sensorbio_http2_samples_reach_postgres_daily_and_detail() -> Result<()> {
    use axum::{
        extract::{Query, Request},
        http::{HeaderMap, Version},
        routing::get,
        Json, Router,
    };
    let start: DateTime<Utc> = "2026-04-25T00:00:00Z".parse()?;
    let end = start + CalendarDuration::days(1);
    let cursor = start.timestamp_millis() - 1;
    let app = Router::new().route("/", get(move |headers: HeaderMap,
        Query(query): Query<std::collections::HashMap<String, i64>>, request: Request| async move {
        assert_eq!(request.version(), Version::HTTP_2);
        assert_eq!(headers["authorization"], "Bearer fixture");
        assert_eq!(query["limit"], 50);
        assert_eq!(query["last-timestamp"], cursor);
        Json(json!({"data":[{"timestamp": start.timestamp_millis(),"bpm":61,"hrv":31,"brpm":15.4,"spo2":98}]}))
    }));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let url = format!("http://{}/", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let rows = sensorbio_at(
        &Client::builder().http2_prior_knowledge().build()?,
        "fixture",
        start,
        end,
        &url,
    )
    .await;
    server.abort();
    let rows = rows?;
    assert_eq!(rows.len(), 5);
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "sensorbio-metric-fixture";
    pool.get()
        .await?
        .execute(
            "INSERT INTO avatar_hsm.subject VALUES ($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
    let lease = wearables::issue_lease_for(&pool, subject, "sensorbio").await?;
    wearables::store_snapshot(&pool, subject, "sensorbio", serde_json::from_value(json!({
        "lease":lease["lease"],"start":start,"end":end,"grants":["heart_rate","metric"],"records":rows
    }))?, false).await?;
    let detail = wearables::records_for(
        &pool,
        subject,
        wearables::RecordQuery {
            kind: "metric".into(),
            start,
            end,
            offset: 0,
            source: None,
            provider: Some("sensorbio".into()),
            metric: Some("respiratory_rate".into()),
        },
        false,
    )
    .await?;
    assert_eq!(
        detail["records"][0]["payload"]["provider_data"]["value"],
        15.4
    );
    let daily = wearables::daily_for(
        &pool,
        subject,
        wearables::DailyQuery {
            start_day: start.date_naive(),
            end_day: start.date_naive(),
            kind: Some("heart_rate".into()),
            source: None,
            provider: Some("sensorbio".into()),
        },
    )
    .await?;
    assert_eq!(daily["records"][0]["stats"]["average"].as_f64(), Some(61.0));
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn suunto_activity_samples_and_daily_totals_are_queryable() -> Result<()> {
    use axum::{
        extract::OriginalUri,
        http::{HeaderMap, StatusCode},
        routing::get,
        Json, Router,
    };
    let router = Router::new().fallback(get(
        |OriginalUri(uri): OriginalUri, headers: HeaderMap| async move {
            let authorization = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok());
            if !matches!(
                authorization,
                Some("Bearer fixture" | "Bearer fixture-revision")
            ) || headers
                .get("ocp-apim-subscription-key")
                .and_then(|value| value.to_str().ok())
                != Some("fixture-key")
            {
                return Err(StatusCode::UNAUTHORIZED);
            }
            let bpm = if authorization == Some("Bearer fixture-revision") {
                79
            } else {
                73
            };
            let daily_joules = if authorization == Some("Bearer fixture-revision") {
                585760
            } else {
                418400
            };
            let data = match uri.path() {
                "/247samples/activity" => json!({"payload":[
                    {"timestamp":"2026-04-25T00:10:00+02:00","entryData":{
                        "HR":bpm,"StepCount":13,"EnergyConsumption":4184,"HRV":47,"SpO2":0.96
                    }},
                    {"timestamp":"2026-04-25T00:20:00+02:00","entryData":{"HR":0}}
                ]}),
                "/247samples/sleep" => json!({"payload":[
                    {"timestamp":"2026-04-24T13:00:00+02:00","entryData":{
                        "SleepId":721,"BedtimeStart":"2026-04-24T13:00:00+02:00",
                        "BedtimeEnd":"2026-04-24T13:30:00+02:00","IsNap":true,
                        "DeepSleepDuration":0,"LightSleepDuration":1200,
                        "SleepQualityScore":78,"MaxSpo2":if bpm==73 { json!(0.98) } else { Value::Null },
                        "HRAvg":bpm,"AvgHRV":if bpm==73 {47} else {51},
                        "HRMin":if bpm==73 { json!(43) } else { json!("unavailable") },
                        "AvgHRVSampleCount":23
                    }}
                ]}),
                "/247/daily-activity-statistics" => json!([
                    {"Name":"stepcount","Sources":[{"Name":"watch","Samples":[
                        {"TimeISO8601":"2026-04-25T00:00:00+02:00","Value":8200}
                    ]}]},
                    {"Name":"energyconsumption","Sources":[{"Name":"watch","Samples":[
                        {"TimeISO8601":"2026-04-25T00:10:00+02:00","Value":daily_joules},
                        {"TimeISO8601":"2026-04-25T00:00:00+02:00","Value":41840}
                    ]},{"Name":"backup","Samples":[
                        {"TimeISO8601":"2026-04-25T00:00:00+02:00","Value":0}
                    ]}]}
                ]),
                _ => return Err(StatusCode::NO_CONTENT),
            };
            Ok(Json(data))
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let start = "2026-04-24T00:00:00Z".parse()?;
    let end = "2026-04-25T00:00:00Z".parse()?;
    let rows = suunto_at(
        &Client::new(),
        "fixture",
        "fixture-key",
        Window { start, end },
        &origin,
    )
    .await?;
    assert_eq!(rows.len(), 12);
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "suunto-activity-fixture";
    pool.get()
        .await?
        .execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
    let lease = wearables::issue_lease_for(&pool, subject, "suunto").await?;
    wearables::store_snapshot(&pool,subject,"suunto",serde_json::from_value(json!({
        "lease":lease["lease"],"start":start,"end":end,"grants":["steps","heart_rate","metric","sleep"],"records":rows
    }))?,false).await?;
    let day = "2026-04-25".parse()?;
    let daily = wearables::daily_for(
        &pool,
        subject,
        wearables::DailyQuery {
            start_day: day,
            end_day: day,
            source: None,
            provider: Some("suunto".into()),
            kind: Some("steps".into()),
        },
    )
    .await?;
    assert_eq!(daily["records"][0]["stats"]["total"], 8200);
    assert_eq!(daily["records"][0]["source"], "Suunto:watch");
    assert_eq!(daily["records"][0]["timezone"], "+02:00");
    let read_energy = || wearables::DailyQuery {
        start_day: day,
        end_day: day,
        source: None,
        provider: Some("suunto".into()),
        kind: Some("activity".into()),
    };
    let energy = wearables::daily_for(&pool, subject, read_energy()).await?;
    assert_eq!(energy["state"], "ready");
    assert_eq!(energy["records"].as_array().map(Vec::len), Some(2));
    assert_eq!(energy["records"][0]["source"], "Suunto:backup");
    assert_eq!(
        energy["records"][0]["stats"]["active_energy_kcal"].as_f64(),
        Some(0.0)
    );
    assert_eq!(energy["records"][1]["source"], "Suunto:watch");
    assert_eq!(energy["records"][1]["timezone"], "+02:00");
    assert_eq!(
        energy["records"][1]["stats"]["active_energy_kcal"].as_f64(),
        Some(100.0)
    );
    assert!(energy["records"][1]["stats"]
        .get("total_energy_kcal")
        .is_none());
    for (kind, metric, field, expected) in [
        ("heart_rate", None, "bpm", 73.0),
        ("metric", Some("activity_step_sample"), "value", 13.0),
        ("metric", Some("daily_energyconsumption"), "value", 100.0),
        ("metric", Some("oxygen_saturation"), "value", 96.0),
        ("metric", Some("suunto_sleep_max_spo2"), "value", 98.0),
        ("metric", Some("suunto_sleep_hrv_rmssd"), "value", 47.0),
        ("metric", Some("suunto_sleep_min_heart_rate"), "value", 43.0),
    ] {
        let result = wearables::records_for(
            &pool,
            subject,
            wearables::RecordQuery {
                kind: kind.into(),
                start,
                end,
                offset: 0,
                source: (metric == Some("daily_energyconsumption"))
                    .then(|| "Suunto:watch".to_owned()),
                provider: Some("suunto".into()),
                metric: metric.map(str::to_owned),
            },
            false,
        )
        .await?;
        assert_eq!(result["records"].as_array().map(Vec::len), Some(1));
        let payload = &result["records"][0]["payload"];
        let data = if kind == "heart_rate" {
            &payload["samples"][0]
        } else {
            &payload["provider_data"]
        };
        assert_eq!(data[field].as_f64(), Some(expected));
    }
    let narrower_end = end - chrono::Duration::hours(1);
    let revised = suunto_at(
        &Client::new(),
        "fixture-revision",
        "fixture-key",
        Window {
            start,
            end: narrower_end,
        },
        &origin,
    )
    .await?;
    server.abort();
    let lease = wearables::issue_lease_for(&pool, subject, "suunto").await?;
    wearables::store_snapshot(&pool,subject,"suunto",serde_json::from_value(json!({
        "lease":lease["lease"],"start":start,"end":narrower_end,"grants":["steps","heart_rate","metric","sleep"],
        "records":revised
    }))?,false).await?;
    let corrected = wearables::records_for(
        &pool,
        subject,
        wearables::RecordQuery {
            kind: "heart_rate".into(),
            start,
            end,
            offset: 0,
            source: None,
            provider: Some("suunto".into()),
            metric: None,
        },
        false,
    )
    .await?;
    assert_eq!(corrected["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(corrected["records"][0]["payload"]["samples"][0]["bpm"], 79);
    let corrected = wearables::daily_for(&pool, subject, read_energy()).await?;
    assert_eq!(
        corrected["records"][1]["stats"]["active_energy_kcal"].as_f64(),
        Some(140.0)
    );
    let sleep = wearables::records_for(
        &pool,
        subject,
        wearables::RecordQuery {
            kind: "sleep".into(),
            start,
            end,
            offset: 0,
            source: None,
            provider: Some("suunto".into()),
            metric: None,
        },
        true,
    )
    .await?;
    assert_eq!(sleep["records"].as_array().map(Vec::len), Some(1));
    let payload = &sleep["records"][0]["payload"];
    assert_eq!(payload["nap"], true);
    assert_eq!(payload["sleep_score"], 78);
    assert_eq!(
        payload["provider_stage_summary"],
        json!({"deep_seconds":0,"light_seconds":1200})
    );
    assert_eq!(payload["sleep_metrics"]["max_spo2_percent"], Value::Null);
    assert_eq!(
        payload["sleep_metrics"]["average_heart_rate_bpm"].as_f64(),
        Some(79.0)
    );
    assert_eq!(
        payload["sleep_metrics"]["average_hrv_ms"].as_f64(),
        Some(51.0)
    );
    assert_eq!(payload["sleep_metrics"]["hrv_sample_count"], 23);
    assert_eq!(
        payload["sleep_metrics"]["minimum_heart_rate_bpm"],
        Value::Null
    );
    let read_token = "suunto-metric-fixture-read-key-0123456789-abcdef";
    pool.get()
        .await?
        .execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES
           ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
            &[&subject, &read_token],
        )
        .await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/mcp/wearable-read", listener.local_addr()?);
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let samples = async |metric| -> Result<Value> {
        let response: Value = Client::new().post(&url).bearer_auth(read_token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"wearable_samples","arguments":{"provider":"suunto","kind":"metric","metric":metric,"start":start,"end":end}
            }})).send().await?.error_for_status()?.json().await?;
        Ok(response["result"]["structuredContent"].clone())
    };
    let hrv = samples("suunto_sleep_hrv_rmssd").await?;
    assert_eq!(hrv["records"].as_array().map(Vec::len), Some(1));
    let point = &hrv["records"][0]["payload"]["provider_data"];
    assert_eq!(point["value"], 51);
    assert_eq!(point["statistic"], "average");
    assert_eq!(point["nap"], true);
    assert_eq!(point["sleep_source_id"], "sleep:721");
    assert_eq!(point["interval_start"], "2026-04-24T13:00:00+02:00");
    assert_eq!(
        samples("suunto_sleep_max_spo2").await?["records"],
        json!([])
    );
    let spot = samples("oxygen_saturation").await?;
    assert_eq!(spot["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        spot["records"][0]["payload"]["provider_data"]["value"].as_f64(),
        Some(96.0)
    );
    wearables::revoke_for(&pool, subject, "suunto").await?;
    assert_eq!(samples("suunto_sleep_hrv_rmssd").await?["state"], "revoked");
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn withings_paged_activity_and_intraday_are_readable_in_mcp() -> Result<()> {
    use axum::{
        extract::Form,
        http::{HeaderMap, StatusCode},
        routing::post,
        Json, Router,
    };
    use std::{collections::HashMap, sync::Arc};

    let start: DateTime<Utc> = "2026-04-24T06:00:00Z".parse()?;
    let middle = start + CalendarDuration::days(1);
    let end = middle + CalendarDuration::hours(6);
    let app = Router::new().fallback(post(
        move |headers: HeaderMap, Form(form): Form<HashMap<String, String>>| async move {
            if headers.get("authorization").and_then(|value| value.to_str().ok())
                != Some("Bearer withings-fixture") {
                return Err(StatusCode::UNAUTHORIZED);
            }
            let date_range = form.get("startdateymd").map(String::as_str) == Some("2026-04-23")
                && form.get("enddateymd").map(String::as_str) == Some("2026-04-26");
            let page = match form.get("action").map(String::as_str) {
                Some("getactivity") if date_range => json!({"activities":[{
                    "date":"2026-04-25","timezone":"Europe/Paris","steps":830,
                    "distance":650.5,"elevation":0,"soft":300,"moderate":120,"intense":60,"active":180,
                    "calories":600.25,"totalcalories":1800.5
                }]}),
                Some("getworkouts") if date_range => match form.get("offset").map(String::as_str) {
                    Some("0") => json!({"series":[],"more":true,"offset":10}),
                    Some("10") => json!({"series":[{"id":51,"category":16,
                        "startdate": (middle + CalendarDuration::hours(1)).timestamp(),
                        "enddate": (middle + CalendarDuration::hours(2)).timestamp(),
                        "data":{"calories":190.4,"distance":1975,"manual_distance":2100,"manual_calories":193,
                            "hr_average":115,"hr_min":0,"hr_max":169,"hr_zone_0":0,"hr_zone_1":1200,
                            "hr_zone_2":4000,"hr_zone_3":"invalid","spo2_average":97.25,"elevation":0,"steps":650,
                            "pool_laps":20,"strokes":185,"pool_length":25,"pause_duration":90,"algo_pause_duration":120}
                    }],"more":false}),
                    _ => return Err(StatusCode::BAD_REQUEST),
                },
                Some("getintradayactivity") => {
                    let epoch = |name| form.get(name).and_then(|value| value.parse::<i64>().ok());
                    match (epoch("startdate"), epoch("enddate")) {
                        (Some(from), Some(to)) if from == start.timestamp() && to == middle.timestamp() =>
                            json!({"series":{(middle.timestamp().to_string()):{"heart_rate":99}}}),
                        (Some(from), Some(to)) if from == middle.timestamp() && to == end.timestamp() =>
                            json!({"series":{(middle.timestamp().to_string()):{
                                "heart_rate":73,"rmssd":46.2,"sdnn1":36.1,"steps":0,"distance":2.5,
                                "spo2_auto":98.4,"rr":14.25,"stroke":4,"pool_lap":1,"duration":60,
                                "core_body_temperature":36.8,"elevation":-2,"hrv_quality":null
                            }}}),
                        _ => return Err(StatusCode::BAD_REQUEST),
                    }
                }
                _ => return Err(StatusCode::BAD_REQUEST),
            };
            Ok(Json(json!({"status":0,"body":page})))
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let client = Client::new();
    let rows = withings_at(
        &client,
        "withings-fixture",
        &Window { start, end },
        &format!("http://{address}"),
    )
    .await?;
    server.abort();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0]["payload"]["provider_data"]["steps"], 830);
    let stored_workout = rows
        .iter()
        .find(|row| row["kind"] == "workout")
        .context("missing workout")?;
    assert!(stored_workout["payload"].get("heart_rate_zones").is_none());
    let mut revised = rows[0].clone();
    let mut revised_intraday = rows
        .iter()
        .find(|row| row["payload"]["metric"] == "intraday_activity")
        .context("missing original Withings intraday record")?
        .clone();

    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "withings-activity-fixture";
    let token = "withings-activity-fixture-read-token-0123456789abc";
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES ($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.wearable_profile VALUES
         ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
        &[&subject, &token],
    )
    .await?;
    drop(db);
    let lease = wearables::issue_lease_for(&pool, subject, "withings").await?;
    wearables::store_snapshot(
        &pool,
        subject,
        "withings",
        serde_json::from_value(json!({
            "lease":lease["lease"],"start":start,"end":end,
            "grants":["metric","workout","heart_rate","steps"],"records":rows
        }))?,
        false,
    )
    .await?;
    let daily = wearables::daily_for(
        &pool,
        subject,
        wearables::DailyQuery {
            start_day: "2026-04-25".parse()?,
            end_day: "2026-04-25".parse()?,
            source: None,
            provider: Some("withings".into()),
            kind: Some("steps".into()),
        },
    )
    .await?;
    assert_eq!(daily["records"][0]["stats"]["total"], 830);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let activity = async || -> Result<Value> {
        let response = client
            .post(format!("http://{address}/mcp/wearable-read"))
            .bearer_auth(token)
            .json(
                &json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                    "name":"wearable_daily","arguments":{"provider":"withings","kind":"activity",
                        "start_day":"2026-04-25","end_day":"2026-04-25"}
                }}),
            )
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        Ok(response["result"]["structuredContent"].clone())
    };
    let summary = activity().await?;
    assert_eq!(summary["state"], "ready");
    assert_eq!(summary["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(summary["records"][0]["timezone"], "Europe/Paris");
    assert_eq!(
        summary["records"][0]["stats"],
        json!({
            "distance_meters":650.5,"floors_climbed":0,"active_seconds":180,"light_intensity_seconds":300,
            "moderate_intensity_seconds":120,"vigorous_intensity_seconds":60,
            "active_energy_kcal":600.25,"total_energy_kcal":1800.5,"passive_energy_kcal":1200.25
        })
    );
    for (tool, kind, metric) in [
        ("wearable_samples", "metric", Some("intraday_activity")),
        ("wearable_samples", "heart_rate", None),
        ("wearable_sessions", "workout", None),
    ] {
        let mut args = json!({"provider":"withings","kind":kind,"start":start,"end":end});
        if let Some(metric) = metric {
            args["metric"] = json!(metric);
        }
        let response = client.post(format!("http://{address}/mcp/wearable-read"))
            .bearer_auth(token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":tool,"arguments":args}}))
            .send().await?.error_for_status()?.json::<Value>().await?;
        let rows = &response["result"]["structuredContent"]["records"];
        assert_eq!(rows.as_array().map(Vec::len), Some(1), "{response}");
        let payload = &rows[0]["payload"];
        match kind {
            "metric" => assert_eq!(payload["provider_data"]["rmssd"], 46.2),
            "heart_rate" => assert_eq!(payload["samples"][0]["bpm"].as_f64(), Some(73.0)),
            _ => {
                assert_eq!(payload["duration_seconds"], 3600);
                assert_eq!(payload["provider_data"]["data"]["hr_average"], 115);
                assert_eq!(payload["average_heart_rate_bpm"], 115);
                assert_eq!(payload["max_heart_rate_bpm"], 169);
                assert!(payload.get("min_heart_rate_bpm").is_none());
                assert_eq!(
                    payload["heart_rate_zones"],
                    json!([
                        {"index":0,"duration_seconds":0},{"index":1,"duration_seconds":1200}
                    ])
                );
                assert_eq!(payload["average_spo2_percent"], 97.25);
                assert_eq!(payload["distance_meters"], 1975);
                assert_eq!(payload["manual_distance_meters"], 2100);
                assert_eq!(payload["calories_kcal"], 190.4);
                assert_eq!(payload["manual_calories_kcal"], 193);
                assert_eq!(payload["manual_pause_seconds"], 90);
                assert_eq!(payload["device_pause_seconds"], 120);
                assert_eq!(payload["step_count"], 650);
                assert_eq!(payload["floors_climbed"], 0);
                assert_eq!(payload["swimming_lap_count"], 20);
                assert_eq!(payload["swimming_stroke_count"], 185);
                assert_eq!(payload["provider_data"]["data"]["pool_length"], 25);
            }
        }
    }
    let samples = async |name: &str| -> Result<Value> {
        let response = client.post(format!("http://{address}/mcp/wearable-read"))
            .bearer_auth(token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"wearable_samples","arguments":{"provider":"withings","kind":"metric","metric":name,
                    "start":start,"end":end}
            }}))
            .send().await?.error_for_status()?.json::<Value>().await?;
        Ok(response["result"]["structuredContent"].clone())
    };
    for (name, field, value, unit) in [
        ("step_count", "steps", 0.0, "count"),
        ("activity_distance", "distance", 2.5, "m"),
        ("oxygen_saturation", "spo2_auto", 98.4, "%"),
        ("hrv_rmssd", "rmssd", 46.2, "ms"),
        ("hrv_sdnn", "sdnn1", 36.1, "ms"),
        ("respiratory_rate", "rr", 14.25, "breaths/min"),
        ("swimming_stroke_count", "stroke", 4.0, "count"),
        ("swimming_lap_count", "pool_lap", 1.0, "count"),
        ("activity_duration", "duration", 60.0, "s"),
        (
            "core_body_temperature",
            "core_body_temperature",
            36.8,
            "celsius",
        ),
    ] {
        let result = samples(name).await?;
        let records = result["records"]
            .as_array()
            .context("missing Withings sample records")?;
        assert_eq!(records.len(), 1, "{result}");
        let detail = &records[0]["payload"]["provider_data"];
        assert_eq!(detail["value"].as_f64(), Some(value));
        assert_eq!(detail["unit"], unit);
        assert_eq!(detail["provider_field"], field);
        assert_eq!(detail["measurement_context"], "intraday_activity");
        assert_eq!(
            detail["raw_record_id"],
            format!("intraday:{}", middle.timestamp())
        );
    }
    assert_eq!(samples("floors_climbed").await?["records"], json!([]));
    assert_eq!(samples("hrv_quality").await?["records"], json!([]));
    assert_eq!(
        samples("daily_activity").await?["records"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    let later = end + CalendarDuration::minutes(5);
    revised["modified_at"] = json!(later);
    revised_intraday["modified_at"] = json!(later);
    revised_intraday["payload"]["provider_data"]["rmssd"] = json!(47.5);
    revised_intraday["payload"]["provider_data"]["spo2_auto"] = json!(102);
    revised["payload"]["provider_data"] = json!({
        "date":"2026-04-25","steps":830,"distance":"unknown","calories":650,"totalcalories":640
    });
    let lease = wearables::issue_lease_for(&pool, subject, "withings").await?;
    wearables::store_snapshot(
        &pool,
        subject,
        "withings",
        serde_json::from_value(json!({
            "lease":lease["lease"],"start":start,"end":later,
            "grants":["metric","workout","heart_rate","steps"],"records":[revised,revised_intraday]
        }))?,
        false,
    )
    .await?;
    let latest = activity().await?;
    assert_eq!(latest["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        latest["records"][0]["stats"],
        json!({"active_energy_kcal":650,"total_energy_kcal":640})
    );
    let updated = samples("hrv_rmssd").await?;
    assert_eq!(updated["records"].as_array().unwrap().len(), 1);
    assert_eq!(
        updated["records"][0]["payload"]["provider_data"]["value"],
        47.5
    );
    assert_eq!(samples("oxygen_saturation").await?["records"], json!([]));
    let lease = wearables::issue_lease_for(&pool, subject, "withings").await?;
    wearables::store_snapshot(
        &pool,
        subject,
        "withings",
        serde_json::from_value(json!({
            "lease":lease["lease"],"start":start,"end":later,
            "grants":["metric"],"records":[]
        }))?,
        false,
    )
    .await?;
    assert_eq!(samples("hrv_rmssd").await?["records"], json!([]));
    assert_eq!(samples("intraday_activity").await?["records"], json!([]));
    assert_eq!(samples("daily_activity").await?["records"], json!([]));
    assert_eq!(activity().await?["state"], "permission_missing");
    wearables::revoke_for(&pool, subject, "withings").await?;
    assert_eq!(activity().await?["state"], "revoked");
    server.abort();
    Ok(())
}

#[tokio::test]
async fn polar_imports_activity_details_and_sleepwise_periods() -> Result<()> {
    use axum::{extract::OriginalUri, http::StatusCode, routing::get, Json, Router};

    let router = Router::new().fallback(get(|OriginalUri(uri): OriginalUri| async move {
        let expected = if uri.path() == "/activities" {
            "from=2026-04-24&to=2026-04-25&inactivity_stamps=true"
        } else {
            "from=2026-04-24&to=2026-04-25"
        };
        if uri.query() != Some(expected) {
            return Err(StatusCode::BAD_REQUEST);
        }
        let data = match uri.path() {
            "/continuous-heart-rate" => json!({"date":"2026-04-24","heart_rate_samples":[
                {"heart_rate":72,"sample_time":"06:03:00"},
                {"heart_rate":73,"sample_time":"2026-04-24T06:08:00+09:00"},
                {"heart_rate":74,"sample_time":"2026-04-24T11:08:00+09:00"}
            ]}),
            "/activities/samples" => json!([{"date":"2026-04-24","steps":{
                "total_steps":830,"interval_ms":30000,"samples":[{"steps":14,"timestamp":"2026-04-24T06:00"}]
            }}]),
            "/activities" => json!([{"start_time":"2026-04-24T06:00:00","end_time":"2026-04-24T23:59:59",
                "samples":{"inactivity_stamps":[{"stamp":"2026-04-24T10:00"}]}}]),
            "/cardio-load/date" => json!([{"date":"2026-04-24","cardio_load":0.2}]),
            "/sleepwise/alertness/date" => json!([{
                "period_start_time":"2026-04-24T04:10:00","period_end_time":"2026-04-24T22:30:00",
                "grade":9.9,"sleep_timezone_offset_minutes":540
            }]),
            "/sleepwise/circadian-bedtime/date" => json!([{
                "period_start_time":"2026-04-24T20:15:00","period_end_time":"2026-04-24T23:30:00",
                "sleep_gate_start_time":"2026-04-24T21:00:00"
            }]),
            _ => return Err(StatusCode::NO_CONTENT),
        };
        Ok(Json(data))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let window = Window {
        start: "2026-04-24T00:00:00Z".parse()?,
        end: "2026-04-25T00:00:00Z".parse()?,
    };
    let rows = polar_at(
        &Client::new(),
        "polar-fixture",
        &window,
        &origin,
        window.end.date_naive(),
    )
    .await?;
    server.abort();
    assert_eq!(rows.len(), 9);
    assert_eq!(rows[0]["payload"]["metric"], "continuous_heart_rate");
    assert_eq!(rows[1]["payload"]["time_precision"], "provider_civil_time");
    assert_eq!(rows[2]["kind"], "heart_rate");
    assert_eq!(rows[2]["started_at"], "2026-04-24T02:08:00Z");
    let rows = &rows[3..];
    assert_eq!(
        rows[0]["payload"]["provider_data"]["samples"]["inactivity_stamps"][0]["stamp"],
        "2026-04-24T10:00"
    );
    let rows = &rows[1..];
    assert_eq!(
        rows[0]["payload"]["provider_data"]["steps"]["samples"][0]["steps"],
        14
    );
    assert_eq!(rows[1]["payload"]["metric"], "activity_step_sample");
    assert_eq!(rows[1]["ended_at"], "2026-04-24T06:00:30Z");
    assert_eq!(rows[2]["payload"]["metric"], "cardio_load");
    assert_eq!(rows[3]["started_at"], "2026-04-24T04:10:00Z");
    assert_eq!(rows[3]["ended_at"], "2026-04-24T22:30:00Z");
    assert_eq!(rows[3]["payload"]["time_precision"], "instant");
    assert_eq!(rows[4]["payload"]["metric"], "sleepwise_circadian_bedtime");
    Ok(())
}

#[tokio::test]
async fn polar_rechecks_activity_retention_before_fetching_a_queued_window() -> Result<()> {
    use axum::{extract::OriginalUri, http::StatusCode, routing::get, Router};
    use std::sync::{Arc, Mutex};

    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&requests);
    let router = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
        observed.lock().unwrap().push(uri);
        async { StatusCode::NO_CONTENT }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let window = Window {
        start: "2025-11-22T00:00:00Z".parse()?,
        end: "2025-11-27T00:00:00Z".parse()?,
    };
    let client = Client::new();
    polar_at(&client, "fixture", &window, &origin, "2026-11-25".parse()?).await?;
    let observed = std::mem::take(&mut *requests.lock().unwrap());
    assert!(observed.iter().any(|uri| {
        uri.path() == "/activities"
            && uri.query() == Some("from=2025-11-25&to=2025-11-27&inactivity_stamps=true")
    }));
    assert!(observed.iter().any(|uri| {
        uri.path() == "/activities/samples" && uri.query() == Some("from=2025-11-25&to=2025-11-27")
    }));
    assert!(observed.iter().any(|uri| {
        uri.path() == "/continuous-heart-rate"
            && uri.query() == Some("from=2025-11-22&to=2025-11-27")
    }));
    polar_at(&client, "fixture", &window, &origin, "2026-11-28".parse()?).await?;
    assert!(requests
        .lock()
        .unwrap()
        .iter()
        .all(|uri| !uri.path().starts_with("/activities")));
    server.abort();
    Ok(())
}

#[test]
fn suunto_sleep_deduplicates_revisions_inside_wrapped_samples() -> Result<()> {
    let window = Window {
        start: "2026-04-24T00:00:00Z".parse()?,
        end: "2026-04-25T00:00:00Z".parse()?,
    };
    let data = json!({"payload":[
        {"timestamp":"2026-04-24T12:00:00+09:00","entryData":{"SleepId":34,"Duration":1800,
            "BedtimeStart":"2026-04-24T11:30:00+09:00","BedtimeEnd":"2026-04-24T12:00:00+09:00","IsNap":true}},
        {"timestamp":"2026-04-24T12:00:00+09:00","entryData":{"SleepId":34,"Duration":2400,
            "BedtimeStart":"2026-04-24T11:20:00+09:00","BedtimeEnd":"2026-04-24T12:00:00+09:00","IsNap":true}}
    ]});
    let rows = suunto_series(&data, "sleep_samples", &window)?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["kind"], "metric");
    assert_eq!(rows[1]["kind"], "sleep");
    assert_eq!(rows[1]["source_id"], "sleep:34");
    assert_eq!(rows[1]["started_at"], "2026-04-24T02:20:00Z");
    assert_eq!(rows[1]["payload"]["provider_data"]["IsNap"], true);
    assert_eq!(
        suunto_series(&data["payload"], "sleep_samples", &window)?,
        rows
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn polar_elixir_uses_biosensing_routes_and_keeps_waveform_through_mcp() -> Result<()> {
    use axum::{
        extract::OriginalUri,
        http::{HeaderMap, StatusCode},
        routing::get,
        Json, Router,
    };
    use std::sync::Arc;
    let router = Router::new().fallback(get(|OriginalUri(uri): OriginalUri, headers: HeaderMap| async move {
        let expected = if uri.path() == "/activities" {
            "from=2026-04-24&to=2026-04-25&inactivity_stamps=true"
        } else {
            "from=2026-04-24&to=2026-04-25"
        };
        if uri.query() != Some(expected)
            || headers.get("authorization").and_then(|value| value.to_str().ok()) != Some("Bearer fixture")
        {
            return Err(StatusCode::BAD_REQUEST);
        }
        let data = match uri.path() {
            "/biosensing/bodytemperature" => json!([{
                "source_device_id":"device-fixture","measurement_type":"TM_CORE_TEMPERATURE",
                "sensor_location":"SL_PROXIMAL","start_time":"2026-04-24T04:00:00",
                "end_time":"2026-04-24T05:00:00","samples":[
                    {"recording_time_delta_milliseconds":123,"temperature_celsius":36.5},
                    {"recording_time_delta_milliseconds":-123,"temperature_celsius":36.7}
                ]
            }]),
            "/biosensing/skintemperature" => json!([{
                "sleep_date":"2026-04-24","sleep_time_skin_temperature_celsius":34.5,
                "deviation_from_baseline_celsius":0.5
            }]),
            "/biosensing/skincontacts" => json!([{
                "source_device_id":"device-fixture","start_time":"2026-04-24T04:00:00",
                "end_time":"2026-04-24T05:00:00","skin_contact_changes":[
                    {"recording_time_delta_milliseconds":123,"skin_contact":true}
                ]
            }]),
            "/biosensing/ecg" => json!([{
                "source_device_id":"device-fixture","test_time":1777003200,
                "average_heart_rate_bpm":60,"time_zone_offset":540,
                "samples":[{"recording_time_delta_ms":123,"amplitude_mv":-0.1}],
                "quality_measurements":[{"recording_time_delta_ms":123,"quality_level":"ECG_QUALITY_HIGH"}]
            }]),
            "/biosensing/spo2" => json!([{
                "source_device_id":"device-fixture","test_time":1777003200,
                "test_status":"SPO2_TEST_PASSED","blood_oxygen_percent":95
            }]),
            _ => return Err(StatusCode::NO_CONTENT),
        };
        Ok(Json(data))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let window = Window {
        start: "2026-04-24T00:00:00Z".parse()?,
        end: "2026-04-25T00:00:00Z".parse()?,
    };
    let client = Client::new();
    let rows = polar_at(
        &client,
        "fixture",
        &window,
        &origin,
        window.end.date_naive(),
    )
    .await?;
    server.abort();
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0]["started_at"], "2026-04-24T04:00:00.123Z");
    assert_eq!(rows[1]["ended_at"], "2026-04-24T05:00:00Z");
    assert_eq!(
        rows[2]["payload"]["provider_data"]["deviation_from_baseline_celsius"],
        0.5
    );
    assert_eq!(
        rows[3]["payload"]["provider_data"]["skin_contact_changes"][0]["skin_contact"],
        true
    );
    assert_eq!(
        rows[5]["payload"]["provider_data"]["blood_oxygen_percent"],
        95
    );

    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "polar-elixir-fixture";
    let token = "polar-elixir-fixture-read-token-0123456789abcdef";
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES ($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.wearable_profile VALUES
         ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
        &[&subject, &token],
    )
    .await?;
    drop(db);
    let lease = wearables::issue_lease_for(&pool, subject, "polar").await?;
    wearables::store_snapshot(&pool, subject, "polar", serde_json::from_value(json!({
        "lease":lease["lease"],"start":window.start,"end":window.end,"grants":["metric"],"records":rows
    }))?, false).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Arc::new(crate::mcp::App {
        db: pool,
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let response = client.post(format!("http://{address}/mcp/wearable-read"))
        .bearer_auth(token)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
            "name":"wearable_samples","arguments":{"provider":"polar","kind":"metric","metric":"ecg",
                "start":window.start,"end":window.end}
        }}))
        .send().await?.error_for_status()?.json::<Value>().await?;
    server.abort();
    let payload = &response["result"]["structuredContent"]["records"][0]["payload"];
    assert_eq!(payload["signal_unit"], "mV", "{response}");
    assert_eq!(
        payload["provider_data"]["samples"][0],
        json!({"recording_time_delta_ms":123,"amplitude_mv":-0.1})
    );
    assert_eq!(
        payload["provider_data"]["quality_measurements"][0]["quality_level"],
        "ECG_QUALITY_HIGH"
    );
    Ok(())
}
