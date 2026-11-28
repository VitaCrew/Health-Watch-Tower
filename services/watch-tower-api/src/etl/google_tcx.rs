use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::{self, GoogleTcxJob, GoogleTcxPlan};
use chrono::{DateTime, Duration, Utc};
use deadpool_postgres::Pool;
use quick_xml::{events::Event, Reader};
use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde_json::{json, Value};
use std::time::Duration as StdDuration;

const ORIGIN: &str = "https://health.googleapis.com";
const MAX_BYTES: usize = 12 * 1024 * 1024;
const MAX_POINTS: usize = 100_000;

#[derive(Debug)]
struct ProviderRateLimited {
    retry_at: DateTime<Utc>,
}
impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Google Health rate limited TCX export")
    }
}
impl std::error::Error for ProviderRateLimited {}

fn retry_after(header: Option<&str>, now: DateTime<Utc>) -> DateTime<Utc> {
    header
        .and_then(|value| {
            let value = value.trim();
            value
                .parse::<i64>()
                .ok()
                .and_then(Duration::try_seconds)
                .and_then(|delay| now.checked_add_signed(delay))
                .or_else(|| {
                    DateTime::parse_from_rfc2822(value)
                        .ok()
                        .map(|at| at.with_timezone(&Utc))
                })
        })
        .filter(|at| *at > now)
        .unwrap_or_else(|| now + Duration::minutes(5))
}

async fn fetch(client: &Client, token: &str, activity: &str) -> Result<Vec<u8>> {
    ensure!(
        !activity.is_empty()
            && activity.len() <= 20
            && activity.bytes().all(|b| b.is_ascii_digit()),
        "invalid Google Health exercise ID"
    );
    let mut response = client
        .get(format!(
            "{ORIGIN}/v4/users/me/dataTypes/exercise/dataPoints/{activity}:exportExerciseTcx"
        ))
        .query(&[("alt", "media")])
        .bearer_auth(token)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        return Err(ProviderRateLimited {
            retry_at: retry_after(
                response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()),
                Utc::now(),
            ),
        }
        .into());
    }
    ensure!(
        response.status().is_success(),
        "Google Health TCX export returned HTTP {}",
        response.status().as_u16()
    );
    let mut data = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        ensure!(
            data.len() + chunk.len() <= MAX_BYTES,
            "Google Health TCX exceeds size limit"
        );
        data.extend_from_slice(&chunk);
    }
    Ok(data)
}

#[derive(Default)]
struct TrackPoint {
    timestamp_ms: Option<i64>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    altitude: Option<f64>,
    telemetry: serde_json::Map<String, Value>,
}
#[derive(Serialize)]
struct Point {
    timestamp_ms: i64,
    latitude: f64,
    longitude: f64,
    altitude: Option<f64>,
    segment: usize,
}
fn measurement(value: &str) -> Option<f64> {
    value
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && *v >= 0.0)
}

fn parse_track(data: &[u8]) -> Result<(Value, Value)> {
    let text = std::str::from_utf8(data)?.trim_start_matches('\u{feff}');
    let mut reader = Reader::from_str(text);
    reader.config_mut().trim_text(true);
    let mut path = Vec::new();
    let mut root = false;
    let mut activity = false;
    let mut track_point = None::<TrackPoint>;
    let mut segment = 0;
    let mut points = Vec::new();
    let mut telemetry = Vec::new();
    loop {
        match reader.read_event()? {
            Event::Start(node) => {
                let name = node.local_name();
                let name = name.as_ref();
                if path.is_empty() {
                    ensure!(
                        !root && name == "TrainingCenterDatabase",
                        "invalid TCX root"
                    );
                    root = true;
                } else if path == ["TrainingCenterDatabase", "Activities"] && name == "Activity" {
                    activity = true;
                } else if path == ["TrainingCenterDatabase", "Activities", "Activity", "Lap"]
                    && name == "Track"
                {
                    segment += 1;
                } else if path
                    == [
                        "TrainingCenterDatabase",
                        "Activities",
                        "Activity",
                        "Lap",
                        "Track",
                    ]
                    && name == "Trackpoint"
                {
                    track_point = Some(TrackPoint::default());
                }
                ensure!(path.len() < 64, "invalid TCX nesting");
                path.push(name.to_owned());
            }
            Event::Text(text) => {
                let value = text.as_ref();
                if let Some(point) = &mut track_point {
                    match (path.len(), path.last().map(String::as_str)) {
                        (7, Some("Time")) => {
                            point.timestamp_ms = DateTime::parse_from_rfc3339(value)
                                .ok()
                                .map(|time| time.timestamp_millis())
                        }
                        (7, Some("AltitudeMeters")) => {
                            point.altitude = value.parse::<f64>().ok().filter(|v| v.is_finite())
                        }
                        (7, Some("DistanceMeters")) => {
                            if let Some(v) = measurement(value) {
                                point.telemetry.insert("distance_m".into(), json!(v));
                            }
                        }
                        (7, Some("Cadence")) => {
                            if let Ok(v) = value.parse::<u8>() {
                                point
                                    .telemetry
                                    .insert("cycling_cadence_rpm".into(), json!(v));
                            }
                        }
                        (8, Some("Value")) if path[6] == "HeartRateBpm" => {
                            if let Ok(v) = value.parse::<u8>() {
                                point.telemetry.insert("heart_rate_bpm".into(), json!(v));
                            }
                        }
                        (9, Some(field)) if path[6..8] == ["Extensions", "TPX"] => {
                            let sample = match field {
                                "RunCadence" => {
                                    value.parse::<u8>().ok().map(|v| ("run_cadence", json!(v)))
                                }
                                "Speed" => measurement(value).map(|v| ("speed_mps", json!(v))),
                                "Watts" => value.parse::<u16>().ok().map(|v| ("power_w", json!(v))),
                                _ => None,
                            };
                            if let Some((key, value)) = sample {
                                point.telemetry.insert(key.into(), value);
                            }
                        }
                        (8, Some("LatitudeDegrees")) if path[6] == "Position" => {
                            point.latitude = value
                                .parse::<f64>()
                                .ok()
                                .filter(|v| (-90.0..=90.0).contains(v))
                        }
                        (8, Some("LongitudeDegrees")) if path[6] == "Position" => {
                            point.longitude = value
                                .parse::<f64>()
                                .ok()
                                .filter(|v| (-180.0..=180.0).contains(v))
                        }
                        _ => {}
                    }
                } else if path.is_empty() {
                    ensure!(value.is_empty(), "invalid TCX text");
                }
            }
            Event::End(_) => {
                if path.len() == 6 {
                    if let Some(TrackPoint {
                        timestamp_ms: Some(timestamp_ms),
                        latitude,
                        longitude,
                        altitude,
                        telemetry: mut measurements,
                    }) = track_point.take()
                    {
                        if let (Some(latitude), Some(longitude)) = (latitude, longitude) {
                            ensure!(points.len() < MAX_POINTS, "too many TCX route points");
                            points.push(Point {
                                timestamp_ms,
                                latitude,
                                longitude,
                                altitude,
                                segment,
                            });
                        }
                        if !measurements.is_empty() {
                            ensure!(
                                telemetry.len() < MAX_POINTS,
                                "too many TCX telemetry points"
                            );
                            measurements.insert("timestamp_ms".into(), json!(timestamp_ms));
                            measurements.insert("segment".into(), json!(segment));
                            telemetry.push(Value::Object(measurements));
                        }
                    }
                }
                path.pop().context("invalid TCX closing tag")?;
            }
            Event::DocType(_) => anyhow::bail!("TCX contains a document type"),
            Event::Empty(_) if path.is_empty() => anyhow::bail!("invalid TCX root"),
            Event::Eof => {
                ensure!(
                    root && activity && path.is_empty(),
                    "incomplete TCX activity"
                );
                return Ok((serde_json::to_value(points)?, Value::Array(telemetry)));
            }
            _ => {}
        }
    }
}

async fn run_one(pool: &Pool, client: &Client, job: &GoogleTcxJob) -> Result<()> {
    let (bytes, route, telemetry) = match etl_adapter::prepare_google_tcx(pool, client, job).await?
    {
        GoogleTcxPlan::Stored(bytes) => {
            let (r, t) = parse_track(&bytes)?;
            (bytes, r, t)
        }
        GoogleTcxPlan::Fetch { token, activity_id } => {
            let bytes = fetch(client, &token, &activity_id).await?;
            let (r, t) = parse_track(&bytes)?;
            (bytes, r, t)
        }
        GoogleTcxPlan::NoAccess => {
            etl_adapter::defer_google_tcx(pool, job, None).await?;
            return Ok(());
        }
    };
    etl_adapter::commit_google_tcx(pool, job, bytes, route, telemetry).await?;
    Ok(())
}

pub async fn worker(pool: Pool, client: Client) {
    loop {
        match etl_adapter::next_google_tcx(&pool).await {
            Ok(Some(job)) => {
                let result = tokio::time::timeout(
                    StdDuration::from_secs(100),
                    run_one(&pool, &client, &job),
                )
                .await;
                if !matches!(result, Ok(Ok(()))) {
                    let retry_at = match &result {
                        Ok(Err(error)) => error
                            .downcast_ref::<ProviderRateLimited>()
                            .map(|limited| limited.retry_at),
                        _ => None,
                    };
                    let _ = etl_adapter::defer_google_tcx(&pool, &job, retry_at).await;
                }
                tokio::time::sleep(StdDuration::from_secs(1)).await;
            }
            Ok(None) | Err(_) => tokio::time::sleep(StdDuration::from_secs(12)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TCX: &str = r#"<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2" xmlns:ae="http://www.garmin.com/xmlschemas/ActivityExtension/v2"><Activities><Activity Sport="Run"><Id>workout</Id><Lap><Track><Trackpoint><Time>2026-04-20T10:00:01Z</Time><Position><LatitudeDegrees>37.0</LatitudeDegrees><LongitudeDegrees>127.0</LongitudeDegrees></Position><AltitudeMeters>57.25</AltitudeMeters><DistanceMeters>25.5</DistanceMeters></Trackpoint><Trackpoint><Time>2026-04-20T10:00:02.500Z</Time><HeartRateBpm><Value>137</Value></HeartRateBpm><Cadence>89</Cadence><Extensions><ae:TPX><ae:Speed>3.5</ae:Speed><ae:Watts>215</ae:Watts></ae:TPX></Extensions></Trackpoint></Track></Lap></Activity></Activities></TrainingCenterDatabase>"#;
    #[test]
    fn rejects_non_tcx_and_document_types() {
        assert!(parse_track(b"<html>bad</html>").is_err());
        assert!(
            parse_track(b"<!DOCTYPE TrainingCenterDatabase><TrainingCenterDatabase/>").is_err()
        );
    }

    #[test]
    fn normalizes_route_coordinates_and_telemetry_from_tcx() -> Result<()> {
        let (route, telemetry) = parse_track(TCX.as_bytes())?;
        assert_eq!(route.as_array().context("route array")?.len(), 1);
        assert_eq!(route[0]["latitude"], 37.0);
        assert_eq!(route[0]["longitude"], 127.0);
        assert_eq!(route[0]["altitude"], 57.25);
        assert_eq!(telemetry[0]["distance_m"], 25.5);
        assert_eq!(telemetry[1]["heart_rate_bpm"], 137);
        assert_eq!(telemetry[1]["cycling_cadence_rpm"], 89);
        assert_eq!(telemetry[1]["speed_mps"], 3.5);
        assert_eq!(telemetry[1]["power_w"], 215);
        Ok(())
    }
    #[test]
    fn bounded_retry_after_handles_seconds_and_http_dates() {
        let now = DateTime::parse_from_rfc3339("2026-11-28T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(retry_after(Some("30"), now), now + Duration::seconds(30));
        assert_eq!(
            retry_after(Some("Sat, 28 Nov 2026 00:02:00 GMT"), now),
            now + Duration::minutes(2)
        );
    }
}
