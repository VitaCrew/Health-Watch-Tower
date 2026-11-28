use super::*;
use axum::{extract::Query, routing::get, Router};
use serde_json::{json, Value};
use std::collections::HashMap;

const TCX: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
    xmlns:ae="http://www.garmin.com/xmlschemas/ActivityExtension/v2">
  <Activities><Activity Sport="Running"><Id>2026-04-20T10:00:00Z</Id><Lap StartTime="2026-04-20T10:00:00Z"><Track>
    <Trackpoint><Time>2026-04-20T10:00:01Z</Time><Position>
      <LatitudeDegrees>37.0</LatitudeDegrees><LongitudeDegrees>127.0</LongitudeDegrees>
    </Position><AltitudeMeters>57.25</AltitudeMeters><DistanceMeters>25.5</DistanceMeters></Trackpoint>
    <Trackpoint><Time>2026-04-20T10:00:02Z</Time><Position>
      <LatitudeDegrees>91</LatitudeDegrees><LongitudeDegrees>127.1</LongitudeDegrees>
    </Position></Trackpoint>
    <Trackpoint><Time>2026-04-20T10:00:02.500Z</Time><DistanceMeters>NaN</DistanceMeters>
      <HeartRateBpm><Value>137</Value></HeartRateBpm><Cadence>89</Cadence>
      <Extensions><ae:TPX><ae:Speed>3.5</ae:Speed><ae:RunCadence>82</ae:RunCadence>
        <ae:Watts>215</ae:Watts></ae:TPX></Extensions></Trackpoint>
  </Track><Track>
    <Trackpoint><Time>2026-04-20T19:00:03+09:00</Time><Position>
      <LatitudeDegrees>37.125</LatitudeDegrees><LongitudeDegrees>127.25</LongitudeDegrees>
    </Position></Trackpoint>
  </Track></Lap></Activity></Activities>
</TrainingCenterDatabase>"#;

#[test]
fn tcx_requires_correct_xml_and_opt_in_scope() -> Result<()> {
    let (points, telemetry) = parse_track(TCX.as_bytes())?;
    assert_eq!(points.len(), 2);
    assert_eq!(points[0].altitude, Some(57.25));
    assert_eq!(points[1].segment, 2);
    assert_eq!(
        points[1].timestamp_ms,
        "2026-04-20T10:00:03Z"
            .parse::<DateTime<Utc>>()?
            .timestamp_millis()
    );
    assert_eq!(telemetry.len(), 2);
    assert_eq!(telemetry[0]["distance_m"], 25.5);
    assert_eq!(
        telemetry[1],
        json!({
            "timestamp_ms": "2026-04-20T10:00:02.500Z".parse::<DateTime<Utc>>()?.timestamp_millis(),
            "segment": 1, "heart_rate_bpm": 137, "cycling_cadence_rpm": 89,
            "run_cadence": 82, "speed_mps": 3.5, "power_w": 215
        })
    );
    assert!(parse_track(b"<html>unexpected gateway</html>").is_err());
    assert!(parse_track(b"<!DOCTYPE TrainingCenterDatabase><TrainingCenterDatabase/>").is_err());
    assert!(parse_track(&TCX.as_bytes()[..TCX.len() - 10]).is_err());
    assert!(!valid_id("../999"));
    assert!(!wearable_oauth::google_tcx_scope(
        "https://www.googleapis.com/auth/googlehealth.location.readonly"
    ));
    assert!(wearable_oauth::google_tcx_scope(concat!(
        "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly ",
        "https://www.googleapis.com/auth/googlehealth.location.readonly"
    )));
    Ok(())
}

#[test]
fn hsm_commit_contract_rejects_invalid_normalized_route() -> Result<()> {
    let bytes = b"<TrainingCenterDatabase/>";
    let telemetry = json!([]);
    let valid = json!([{
        "timestamp_ms": 1000,
        "latitude": 37.0,
        "longitude": 127.0,
        "altitude": null,
        "segment": 1
    }]);
    validate_normalized_payload(bytes, &valid, &telemetry)?;
    let invalid = json!([{
        "timestamp_ms": 1000,
        "latitude": 91.0,
        "longitude": 127.0,
        "segment": 1
    }]);
    assert!(validate_normalized_payload(bytes, &invalid, &telemetry).is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL in AVATAR_HSM_TEST_DATABASE_URL"]
async fn raw_tcx_download_and_mcp_are_owner_bound_and_revocable() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let subject = "tcx-fixture-owner";
    let other = "tcx-fixture-other";
    let owner_token = "tcx-fixture-owner-read-0123456789012345678901234";
    let other_token = "tcx-fixture-other-read-0123456789012345678901234";
    let activity = "2026443605080188808";
    let provider = "google_health";
    let start = "2026-04-20T00:00:00Z";
    let end = "2026-04-21T00:00:00Z";
    for (owner, token, location) in [(subject, owner_token, true), (other, other_token, false)] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES ($1,'UTC')",
            &[&owner],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES
            ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
            &[&owner, &token],
        )
        .await?;
        let lease = crate::wearables::issue_lease_for(&pool, owner, provider).await?;
        db.execute(
            "INSERT INTO watch_tower.wearable_sync_schedule(subject_id,provider,backfill_from)
             VALUES($1,$2,now())",
            &[&owner, &provider],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_oauth_connection
             (subject_id,provider,sealed_tokens,google_location_granted) VALUES($1,$2,''::bytea,$3)",
            &[&owner,&provider,&location],
        ).await?;
        let snapshot = serde_json::from_value(json!({
            "lease":lease["lease"],"start":start,"end":end,"grants":["workout"],"records":[{
                "kind":"workout","source":"Pixel Watch","source_id":activity,
                "started_at":"2026-04-20T10:00:00Z","ended_at":"2026-04-20T10:20:00Z","modified_at":end,
                "payload":{"exercise_type":"RUNNING","provider_data":{"exerciseMetadata":{"hasGps":true}}}
            }]
        }))?;
        crate::wearables::store_snapshot(&pool, owner, provider, snapshot, false).await?;
    }
    let mut job = claim(&pool).await?.context("expected opted-in TCX job")?;
    assert_eq!(job.subject, subject);
    assert!(claim(&pool).await?.is_none());
    let upstream = Router::new().route(
        "/v4/users/me/dataTypes/exercise/dataPoints/2026443605080188808:exportExerciseTcx",
        get(
            |headers: HeaderMap, Query(query): Query<HashMap<String, String>>| async move {
                assert_eq!(query["alt"], "media");
                if headers["authorization"] == "Bearer tcx-fixture-throttle" {
                    return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "720")])
                        .into_response();
                }
                assert_eq!(headers["authorization"], "Bearer tcx-fixture-upstream");
                TCX.into_response()
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let upstream_server = tokio::spawn(async move { axum::serve(listener, upstream).await });
    let client = crate::cloud_wearables::http_client()?;
    let limited = fetch(&client, &origin, "tcx-fixture-throttle", activity)
        .await
        .expect_err("provider must respond 429");
    let throttle = limited
        .downcast_ref::<cloud_wearables::ProviderThrottled>()
        .context("TCX downloader dropped Retry-After")?;
    defer_job(&pool, &job, Some(throttle.retry_at)).await?;
    assert!(claim(&pool).await?.is_none());
    let row = db
        .query_one(
            "UPDATE watch_tower.google_workout_tcx_queue SET next_attempt=now()
         WHERE subject_id=$1 RETURNING attempts",
            &[&subject],
        )
        .await?;
    assert_eq!(row.get::<_, i32>(0), 0);
    assert!(claim(&pool).await?.is_none());
    db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET throttled_until=now()-interval '1 second'
         WHERE subject_id=$1 AND provider='google_health'",
        &[&subject],
    )
    .await?;
    job = claim(&pool)
        .await?
        .context("expected rescheduled TCX job")?;
    let data = fetch(&client, &origin, "tcx-fixture-upstream", activity).await?;
    upstream_server.abort();
    assert_eq!(data, TCX.as_bytes());
    assert!(save(&pool, &job, &data).await?);
    db.execute(
        "UPDATE watch_tower.wearable_sync_schedule SET throttled_until=now()+interval '10 minutes'
         WHERE subject_id=$1 AND provider='google_health'",
        &[&subject],
    )
    .await?;
    db.execute(
        "UPDATE avatar_hsm.google_workout_tcx SET telemetry_points=NULL WHERE subject_id=$1",
        &[&subject],
    )
    .await?;
    let backfill = claim(&pool)
        .await?
        .context("expected stored TCX telemetry backfill")?;
    assert_eq!(backfill.stored_data.as_deref(), Some(TCX.as_bytes()));
    run(&pool, &client, &backfill).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let app = Arc::new(App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let path = format!("/wearables/tcx/google_health/{activity}");
    let url = format!("{base}{path}");
    assert_eq!(
        client.get(&url).send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(other_token)
            .send()
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    let response = client
        .get(&url)
        .bearer_auth(owner_token)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        response.headers()["content-disposition"],
        "attachment; filename=activity.tcx"
    );
    assert_eq!(response.bytes().await?.as_ref(), TCX.as_bytes());
    let metadata: Value = client.post(format!("{base}/mcp/wearable-read")).bearer_auth(owner_token)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
            "name":"wearable_sessions","arguments":{"provider":provider,"kind":"workout","start":start,"end":end}
        }})).send().await?.error_for_status()?.json().await?;
    let tcx = &metadata["result"]["structuredContent"]["records"][0]["tcx"];
    assert_eq!(tcx["download_path"], path);
    assert_eq!(tcx["bytes"], TCX.len());
    assert_eq!(tcx["telemetry_point_count"], 2);
    assert_eq!(tcx["sha256"].as_str().map(str::len), Some(64));
    assert_eq!(
        metadata["result"]["structuredContent"]["records"][0]["route_point_count"],
        2
    );
    let query_page = |token, offset, section: Option<&str>| {
        let mut detail =
            json!({"source":"Pixel Watch","source_id":activity,"offset":offset,"limit":1});
        if let Some(section) = section {
            detail["section"] = json!(section);
        }
        client
            .post(format!("{base}/mcp/wearable-read"))
            .bearer_auth(token)
            .json(
                &json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                    "name":"wearable_sessions","arguments":{"google_route":detail}
                }}),
            )
            .send()
    };
    for (offset, expected_latitude, next_offset) in [(0, 37.0, json!(1)), (1, 37.125, Value::Null)]
    {
        let page: Value = query_page(owner_token, offset, None)
            .await?
            .error_for_status()?
            .json()
            .await?;
        let page = &page["result"]["structuredContent"];
        assert_eq!(page["state"], "ready", "{page}");
        assert_eq!(page["stored_point_count"], 2);
        assert_eq!(
            page["records"][0]["latitude"].as_f64(),
            Some(expected_latitude)
        );
        assert_eq!(page["records"][0]["segment"], offset + 1);
        assert_eq!(page["next_offset"], next_offset);
    }
    let telemetry: Value = query_page(owner_token, 1, Some("telemetry"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    let telemetry = &telemetry["result"]["structuredContent"];
    assert_eq!(telemetry["state"], "ready");
    assert_eq!(telemetry["stored_point_count"], 2);
    assert_eq!(telemetry["next_offset"], Value::Null);
    assert_eq!(telemetry["records"][0]["heart_rate_bpm"], 137);
    assert!(telemetry["records"][0].get("latitude").is_none());
    let denied: Value = query_page(other_token, 0, Some("telemetry"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(denied["result"]["structuredContent"]["state"], "not_found");
    crate::wearables::revoke_for(&pool, subject, provider).await?;
    let revoked: Value = query_page(owner_token, 0, Some("telemetry"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(revoked["result"]["structuredContent"]["state"], "not_found");
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(owner_token)
            .send()
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    assert!(!save(&pool, &job, &data).await?);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM avatar_hsm.google_workout_tcx WHERE subject_id=$1",
            &[&subject]
        )
        .await?
        .get::<_, i64>(0),
        0
    );
    server.abort();
    Ok(())
}
