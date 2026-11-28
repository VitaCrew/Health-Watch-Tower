use super::*;
use axum::{
    extract::Form,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use std::{collections::HashMap, sync::Arc};

#[test]
fn withings_sleep_summary_uses_available_duration_sources() {
    let mut imported = json!({"provider_data":{"startdate":1777000000,"enddate":1777000200,
        "data":{"total_timeinbed":200,"asleepduration":150,"sleep_efficiency":75}}});
    sleep_summary(&mut imported);
    assert_eq!(imported["sleep_metrics"]["asleep_seconds"], 150);
    assert_eq!(
        imported["sleep_metrics"]["asleep_seconds_source"],
        "asleepduration"
    );
    assert_eq!(
        imported["sleep_metrics"]["sleep_efficiency_percent"].as_f64(),
        Some(75.0)
    );
    assert_eq!(
        imported["sleep_metrics"]["sleep_efficiency_source"],
        "duration_ratio"
    );

    let mut staged = json!({"provider_data":{"startdate":1777000000,"enddate":1777000200,
        "data":{"lightsleepduration":90,"deepsleepduration":55,"remsleepduration":5}}});
    sleep_summary(&mut staged);
    assert_eq!(staged["sleep_metrics"]["asleep_seconds"], 150);
    assert_eq!(
        staged["sleep_metrics"]["asleep_seconds_source"],
        "stage_durations"
    );
    assert!(staged["sleep_metrics"]
        .get("sleep_efficiency_percent")
        .is_none());

    staged["provider_data"]["data"]["deepsleepduration"] = Value::Null;
    staged.as_object_mut().unwrap().remove("sleep_metrics");
    sleep_summary(&mut staged);
    assert!(staged["sleep_metrics"].get("asleep_seconds").is_none());
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn withings_details_survive_postgres_and_authenticated_mcp() -> Result<()> {
    let start: DateTime<Utc> = "2026-05-01T00:00:00Z".parse()?;
    let end = start + CalendarDuration::days(2);
    let asleep = start + CalendarDuration::hours(4);
    let first_end = asleep + CalendarDuration::hours(4);
    let awake = first_end + CalendarDuration::hours(3);
    let recording_at = awake + CalendarDuration::hours(2);
    let app = Router::new()
        .route("/sleep", post(move |headers: HeaderMap, Form(fields): Form<HashMap<String, String>>| async move {
            if headers.get("authorization").and_then(|value| value.to_str().ok()) != Some("Bearer fixture") {
                return Err(StatusCode::UNAUTHORIZED);
            }
            let result = match fields.get("action").map(String::as_str) {
                Some("getsummary") => {
                    let requested: Vec<_> = fields["data_fields"].split(',').collect();
                    for field in ["night_events","out_of_bed_count","core_body_temperature_status"] {
                        assert!(requested.contains(&field));
                    }
                    json!({"series":[{"id":42,"model_id":94,
                        "startdate":asleep.timestamp(),"enddate":awake.timestamp(),"data":{
                            "sleep_score":84,"total_sleep_time":21600,"asleepduration":21000,
                            "total_timeinbed":24000,"sleep_efficiency":0.885,"sleep_latency":600,
                            "wakeupcount":0,"out_of_bed_count":2,"wakeupduration":1800,
                            "hr_average":54,"hr_min":0,"rr_average":14.25,"rmssd_start_avg":41.5,
                            "core_body_temperature_avg":36.75,"core_body_temperature_status":"usual",
                            "night_events":{"1":[0],"2":[600],"3":[25200],"4":[4860,11640,-1,5],"6":[25201]}
                        }
                    }]})
                }
                Some("get") if fields.get("startdate") == Some(&asleep.timestamp().to_string())
                    && fields.get("enddate") == Some(&awake.timestamp().to_string()) => json!({"series":[
                    {"startdate":first_end.timestamp(),"enddate":awake.timestamp(),"state":3,"model_id":94},
                    {"startdate":(asleep-CalendarDuration::minutes(10)).timestamp(),
                     "enddate":first_end.timestamp(),"state":2,"model_id":94,
                     "hr":{(asleep.timestamp().to_string()):52},
                     "rr":{(asleep.timestamp().to_string()):14.25},
                     "snoring":{(asleep.timestamp().to_string()):0},
                     "rmssd":{(asleep.timestamp().to_string()):45,
                        ((asleep-CalendarDuration::seconds(1)).timestamp().to_string()):80,
                        (awake.timestamp().to_string()):85,
                        ((asleep+CalendarDuration::seconds(1)).timestamp().to_string()):null,
                        "bad":15},
                     "sdnn_1":{(asleep.timestamp().to_string()):38}},
                    {"startdate":asleep.timestamp(),"enddate":awake.timestamp(),"state":0,"model_id":63}
                ]}),
                _ => return Err(StatusCode::BAD_REQUEST),
            };
            Ok(Json(json!({"status":0,"body":result})))
        }))
        .route("/measure", post(move |headers: HeaderMap, Form(fields): Form<HashMap<String, String>>| async move {
            if headers.get("authorization").and_then(|value| value.to_str().ok()) != Some("Bearer fixture")
                || fields["action"] != "getmeas" || fields["category"] != "1" {
                return Err(StatusCode::BAD_REQUEST);
            }
            let result = match fields["offset"].as_str() {
                "0" => json!({"more":1,"offset":1,"measuregrps":[
                    {"grpid":12,"date":asleep.timestamp(),"category":1,"measures":[
                        {"type":1,"value":72400,"unit":-3},
                        {"type":91,"value":780,"unit":-2},
                        {"type":91,"value":790,"unit":-2},
                        {"type":173,"value":440,"unit":-2},
                        {"type":9,"value":80,"unit":200}
                    ]}
                ]}),
                "1" => json!({"measuregrps":[
                    {"grpid":13,"date":awake.timestamp(),"category":1,"measures":[
                        {"type":77,"value":0,"unit":-2},
                        {"type":147,"value":65,"unit":-1},
                        {"type":148,"value":1015,"unit":-3},
                        {"type":151,"value":0,"unit":-2},
                        {"type":204,"value":11,"unit":-1},
                        {"type":205,"value":2,"unit":200},
                        {"type":205,"value":75,"unit":-2},
                        {"type":248,"value":13,"unit":-1},
                        {"type":249,"value":48,"unit":-1},
                        {"type":251,"value":27,"unit":-2},
                        {"type":251,"value":31,"unit":-2}
                    ]}
                ]}),
                _ => return Err(StatusCode::BAD_REQUEST),
            };
            Ok(Json(json!({"status":0,"body":result})))
        }))
        .route("/heart", post(move |headers: HeaderMap, Form(fields): Form<HashMap<String, String>>| async move {
            if headers.get("authorization").and_then(|value| value.to_str().ok()) != Some("Bearer fixture") {
                return Err(StatusCode::UNAUTHORIZED);
            }
            let result = match fields.get("action").map(String::as_str) {
                Some("list") if fields["offset"] == "0" => json!({"more":true,"offset":5,"series":[
                    {"timestamp":recording_at.timestamp(),"ecg":{"signalid":812,"afib":2}},
                    {"timestamp":recording_at.timestamp(),"heart_rate":70}
                ]}),
                Some("list") if fields["offset"] == "5" => json!({"series":[
                    {"timestamp":recording_at.timestamp(),"ecg":{"signalid":812}}
                ]}),
                Some("get") if fields["signalid"] == "812" => json!({"signal":[-24,0,19],"sampling_frequency":300}),
                _ => return Err(StatusCode::BAD_REQUEST),
            };
            Ok(Json(json!({"status":0,"body":result})))
        }));

    let provider = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", provider.local_addr()?);
    let provider = tokio::spawn(async move { axum::serve(provider, app).await });
    let client = Client::new();
    let window = Window { start, end };
    let sleep_url = format!("{origin}/sleep");
    let heart_url = format!("{origin}/heart");
    let measure_url = format!("{origin}/measure");
    let (mut rows, ecg, measurements) = tokio::try_join!(
        manufacturers::withings_sleep_at(&client, "fixture", &window, &sleep_url),
        ecg_at(&client, "fixture", &window, &heart_url),
        manufacturers::withings_measurements_at(&client, "fixture", &window, &measure_url),
    )?;
    provider.abort();
    assert_eq!(rows.len(), 1);
    assert_eq!(ecg.len(), 1);
    rows.extend(ecg);
    assert_eq!(measurements.len(), 5);
    rows.extend(measurements);

    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "withings-detail-fixture";
    let token = "withings-detail-fixture-read-token-0123456789abc";
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
            "grants":["metric","sleep"],"records":rows
        }))?,
        false,
    )
    .await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    for (tool, kind) in [
        ("wearable_samples", "metric"),
        ("wearable_sessions", "sleep"),
    ] {
        let mut args = json!({"provider":"withings","kind":kind,"start":start,"end":end});
        if kind == "metric" {
            args["metric"] = json!("ecg");
        }
        let response = client.post(format!("http://{address}/mcp/wearable-read"))
            .bearer_auth(token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":tool,"arguments":args}}))
            .send().await?.error_for_status()?.json::<Value>().await?;
        let rows = &response["result"]["structuredContent"]["records"];
        assert_eq!(rows.as_array().map(Vec::len), Some(1), "{response}");
        let payload = &rows[0]["payload"];
        if kind == "metric" {
            assert_eq!(
                payload["provider_data"]["waveform"]["signal"],
                json!([-24, 0, 19])
            );
            assert_eq!(
                payload["provider_data"]["waveform"]["sampling_frequency"],
                300
            );
        } else {
            assert_eq!(payload["stages"][0]["start"], json!(asleep));
            assert_eq!(payload["stages"][0]["stage"], 5);
            assert_eq!(payload["stages"][1]["stage"], 6);
            let metrics = &payload["sleep_metrics"];
            for (field, value) in [
                ("asleep_seconds", 21600.0),
                ("time_in_bed_seconds", 24000.0),
                ("sleep_efficiency_percent", 88.5),
                ("sleep_latency_seconds", 600.0),
                ("wake_count", 0.0),
                ("out_of_bed_count", 2.0),
                ("awake_seconds", 1800.0),
                ("average_heart_rate_bpm", 54.0),
                ("average_respiratory_rate_brpm", 14.25),
                ("start_average_hrv_rmssd_ms", 41.5),
                ("average_core_body_temperature_celsius", 36.75),
            ] {
                assert_eq!(metrics[field].as_f64(), Some(value), "{field}");
            }
            assert_eq!(payload["sleep_score"], 84);
            assert_eq!(metrics["asleep_seconds_source"], "total_sleep_time");
            assert_eq!(metrics["sleep_efficiency_source"], "provider_ratio");
            assert!(metrics.get("minimum_heart_rate_bpm").is_none());
            assert_eq!(
                payload["sleep_events"],
                json!([
                    {"at":asleep,"kind":"got_in_bed"},
                    {"at":asleep+CalendarDuration::seconds(600),"kind":"fell_asleep"},
                    {"at":asleep+CalendarDuration::seconds(4860),"kind":"got_out_of_bed"},
                    {"at":asleep+CalendarDuration::seconds(16500),"kind":"got_out_of_bed"},
                    {"at":awake,"kind":"woke_up"}
                ])
            );
            assert_eq!(
                payload["provider_sleep_segments"].as_array().map(Vec::len),
                Some(2)
            );
            assert_eq!(
                payload["provider_sleep_segments"][0]["rmssd"][asleep.timestamp().to_string()],
                45
            );
        }
    }
    let samples =
        async |name: &str| -> Result<Value> {
            let response = client.post(format!("http://{address}/mcp/wearable-read"))
            .bearer_auth(token)
            .json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                "name":"wearable_samples","arguments":{"provider":"withings","kind":"metric",
                "metric":name,"start":start,"end":end}
            }}))
            .send().await?.error_for_status()?.json::<Value>().await?;
            Ok(response["result"]["structuredContent"].clone())
        };
    for (name, value, unit, code) in [
        ("urinary_ph", 6.5, "pH", 147),
        ("urine_specific_gravity", 1.015, "ratio", 148),
        ("urinary_nitrites", 0.0, "µmol/L", 151),
        ("urinary_ketones", 1.1, "mmol/L", 204),
        ("urinary_vitamin_c", 0.75, "mmol/L", 205),
        ("urinary_calcium", 1.3, "mmol/L", 248),
        ("urinary_creatinine", 4.8, "mmol/L", 249),
        ("urinary_calcium_creatinine_ratio", 0.27, "mmol/mmol", 251),
    ] {
        let response = samples(name).await?;
        let records = response["records"]
            .as_array()
            .context("missing urine biomarkers")?;
        assert_eq!(records.len(), 1, "{response}");
        let sample = &records[0];
        let detail = &sample["payload"]["provider_data"];
        assert_eq!(detail["value"].as_f64(), Some(value));
        assert_eq!(detail["unit"], unit);
        assert_eq!(detail["measurement"]["type"], code);
        assert_eq!(detail["raw_record_id"], "measurements:13");
        assert_eq!(
            sample["started_at"]
                .as_str()
                .unwrap()
                .parse::<DateTime<Utc>>()?,
            awake
        );
    }
    for (name, count, value, unit) in [
        ("sleep_heart_rate", 1, Some(52.0), "bpm"),
        ("respiratory_rate", 1, Some(14.25), "breaths/min"),
        ("hrv_rmssd", 1, Some(45.0), "ms"),
        ("hrv_sdnn", 1, Some(38.0), "ms"),
        ("snoring_duration", 1, Some(0.0), "s"),
        ("pulse_wave_velocity", 1, Some(7.8), "m/s"),
        ("weight", 1, Some(72.4), "kg"),
        ("body_water_mass", 1, Some(0.0), "kg"),
        ("blood_pressure_diastolic", 0, None, "mmHg"),
        ("measurements", 2, None, ""),
    ] {
        let response = client
            .post(format!("http://{address}/mcp/wearable-read"))
            .bearer_auth(token)
            .json(
                &json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                    "name":"wearable_samples","arguments":{"provider":"withings","kind":"metric",
                    "metric":name,"start":start,"end":end}
                }}),
            )
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let rows = &response["result"]["structuredContent"]["records"];
        assert_eq!(rows.as_array().map(Vec::len), Some(count), "{response}");
        if let Some(value) = value {
            let data = &rows[0]["payload"]["provider_data"];
            assert!((data["value"].as_f64().unwrap() - value).abs() < 1e-8);
            assert_eq!(data["unit"], unit);
            if let Some(sleep_id) = data.get("sleep_id") {
                assert_eq!(sleep_id, "42");
                assert_eq!(data["model_id"], 94);
                let at = rows[0]["started_at"]
                    .as_str()
                    .unwrap()
                    .parse::<DateTime<Utc>>()?;
                assert_eq!(at, asleep);
            }
        }
        if name == "measurements" {
            assert!(rows
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["payload"]["provider_data"]["measures"][3]["type"] == 173));
        }
    }
    let lease = wearables::issue_lease_for(&pool, subject, "withings").await?;
    let mut activity_only: wearables::Snapshot = serde_json::from_value(json!({
        "lease":lease["lease"],"start":start,"end":end,"grants":["sleep","steps","metric"],"records":[]
    }))?;
    activity_only.withings_measurement_access = Some(false);
    wearables::store_snapshot(&pool, subject, "withings", activity_only, false).await?;
    assert_eq!(samples("urinary_ph").await?["records"], json!([]));
    assert_eq!(samples("measurements").await?["records"], json!([]));
    assert_eq!(samples("ecg").await?["records"], json!([]));
    let remaining: i64 = pool.get().await?.query_one(
        "SELECT count(*) FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='withings' AND kind='sleep'",
        &[&subject],
    ).await?.get(0);
    assert_eq!(remaining, 1);
    server.abort();
    Ok(())
}
