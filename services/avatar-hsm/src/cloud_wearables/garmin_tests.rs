use super::*;

#[tokio::test]
#[ignore = "requires isolated PostgreSQL in AVATAR_HSM_TEST_DATABASE_URL"]
async fn personal_cycle_dayview_is_stored_and_readable_through_mcp() -> Result<()> {
    use axum::{
        extract::{Path, Query},
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::get,
        Json, Router,
    };
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "garmin-personal-cycle-test";
    let read_key = "garmin-personal-test-read-012345678901234567890123456789";
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.wearable_profile
        VALUES($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
        &[&subject, &read_key],
    )
    .await?;
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: "garmin-personal-service-read-0123456789012345".into(),
        control_token: "garmin-personal-test-control-012345678901".into(),
        rules: crate::acute::rules()?,
    });
    let router = crate::router(app).merge(Router::new().route(
        "/periodichealth-service/menstrualcycle/dayview/{day}",
        get(
            |Path(day): Path<String>,
             Query(query): Query<std::collections::HashMap<String, String>>,
             headers: HeaderMap| async move {
                if headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    != Some("Bearer fixture-garmin-personal")
                    || query.get("includeAnalysis").map(String::as_str) != Some("true")
                {
                    return StatusCode::UNAUTHORIZED.into_response();
                }
                match day.as_str() {
                    "2026-04-21" => Json(json!({
                        "dayLog":{"calendarDate":{"localDate":day,"date":"2026-04-20T15:00:00Z"},
                            "flow":"LIGHT","symptoms":[],"notes":"synthetic fixture"},
                        "daySummary":{"startDate":{"localDate":"2026-04-20"},"dayInCycle":2,
                            "predictedCycle":false,"predictedCycleLength":30},
                        "weightGoals":{"weekNum":0,"lowWeightGoalInGrams":0}
                    }))
                    .into_response(),
                    "2026-04-22" => StatusCode::NO_CONTENT.into_response(),
                    "2026-04-23" => StatusCode::FORBIDDEN.into_response(),
                    "2026-04-24" => StatusCode::NOT_FOUND.into_response(),
                    "2026-04-25" => StatusCode::TOO_MANY_REQUESTS.into_response(),
                    "2026-04-26" => {
                        Json(json!({"dayLog":{"calendarDate":{"localDate":"2026-04-25"}}}))
                            .into_response()
                    }
                    _ => StatusCode::BAD_REQUEST.into_response(),
                }
            },
        ),
    ));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let client = http_client()?;
    let window = Window {
        start: "2026-04-20T12:00:00Z".parse()?,
        end: "2026-04-25T00:00:00Z".parse()?,
    };
    let rows = cycle_days_at(&client, &origin, "fixture-garmin-personal", &window).await?;
    assert_eq!(rows.len(), 3);
    for (start, end) in [
        ("2026-04-25T00:00:00Z", "2026-04-26T00:00:00Z"),
        ("2026-04-26T00:00:00Z", "2026-04-27T00:00:00Z"),
    ] {
        assert!(cycle_days_at(
            &client,
            &origin,
            "fixture-garmin-personal",
            &Window {
                start: start.parse()?,
                end: end.parse()?,
            }
        )
        .await
        .is_err());
    }
    for _ in 0..2 {
        let lease = wearables::issue_lease_for(&pool, subject, "garmin_personal").await?;
        let snapshot = serde_json::from_value(json!({
            "lease":lease["lease"],"start":window.start,"end":window.end,"grants":["metric"],"records":rows
        }))?;
        wearables::store_snapshot(&pool, subject, "garmin_personal", snapshot, false).await?;
    }
    for (metric, field, expected) in [
        ("menstrual_cycle_day", "flow", json!("LIGHT")),
        ("menstrual_cycle", "predictedCycleLength", json!(30)),
        (
            "cycle_pregnancy_weight_goals",
            "lowWeightGoalInGrams",
            json!(0),
        ),
    ] {
        let body: Value = client.post(format!("{origin}/mcp/wearable-read")).bearer_auth(read_key)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"wearable_samples","arguments":{"provider":"garmin_personal","kind":"metric",
                    "metric":metric,"start":window.start,"end":window.end}
            }})).send().await?.error_for_status()?.json().await?;
        let rows = body["result"]["structuredContent"]["records"]
            .as_array()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["payload"]["local_day"], "2026-04-21");
        assert_eq!(rows[0]["payload"]["provider_data"][field], expected);
    }
    server.abort();
    Ok(())
}

#[test]
fn projects_personal_api_records() -> Result<()> {
    let window = Window {
        start: "2026-11-21T00:00:00Z".parse()?,
        end: "2026-11-22T00:00:00Z".parse()?,
    };
    let sleep = sleep_point(
        &json!({"id":7,"sleepStartTimestampGMT":"2026-11-21T00:30:00.0",
        "sleepEndTimestampGMT":"2026-11-21T08:30:00.0","deepSleepSeconds":900}),
        &window,
    )?
    .unwrap();
    assert_eq!(sleep["started_at"], "2026-11-21T00:30:00Z");
    assert_eq!(
        sleep["payload"]["provider_stage_summary"]["deep_seconds"],
        900
    );
    let workout = workout_point(
        &json!({"activityId":4,"startTimeGMT":"2026-11-21T09:00:00.0",
        "duration":60.25,"activityType":{"typeKey":"running"}}),
        &window,
    )?
    .unwrap();
    assert_eq!(workout["ended_at"], "2026-11-21T09:01:00.250Z");
    let at = window.start + CalendarDuration::hours(14);
    let mut hours = HeartHours::new();
    heart_points(
        &json!({"heartRateValues":[[at.timestamp_millis(),84],
        [(at + CalendarDuration::seconds(1)).timestamp_millis(),null]]}),
        &window,
        &mut hours,
    )?;
    assert_eq!(hours.values().next().unwrap()[&at], 84);
    Ok(())
}
