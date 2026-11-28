#[cfg(test)]
use crate::cloud_wearables::{self, Provider};
use crate::{mcp::App, wearable_fit, wearable_oauth};
use anyhow::Context;
use anyhow::{ensure, Result};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
#[cfg(test)]
use quick_xml::{events::Event, Reader};
#[cfg(test)]
use reqwest::Client;
use serde::Deserialize;
#[cfg(test)]
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;
use tokio_postgres::Transaction;

#[cfg(test)]
#[path = "google_workout_tcx_tests.rs"]
mod tests;

const ORIGIN: &str = "https://health.googleapis.com";
const MAX_BYTES: usize = 12 * 1024 * 1024;
const MAX_POINTS: usize = 100_000;

pub(crate) fn validate_normalized_payload(
    bytes: &[u8],
    route: &Value,
    telemetry: &Value,
) -> Result<()> {
    ensure!(
        bytes.len() <= MAX_BYTES,
        "Google TCX exceeds the HSM contract limit"
    );
    let routes = route.as_array().context("Google route must be an array")?;
    let samples = telemetry
        .as_array()
        .context("Google telemetry must be an array")?;
    ensure!(
        routes.len() <= MAX_POINTS && samples.len() <= MAX_POINTS,
        "Google TCX point limit exceeded"
    );
    for point in routes {
        let lat = point["latitude"]
            .as_f64()
            .context("route latitude missing")?;
        let lon = point["longitude"]
            .as_f64()
            .context("route longitude missing")?;
        ensure!(
            (-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lon),
            "route coordinate out of range"
        );
        ensure!(
            point["timestamp_ms"].as_i64().is_some() && point["segment"].as_u64().is_some(),
            "route point timestamp or segment invalid"
        );
    }
    ensure!(
        samples.iter().all(Value::is_object),
        "Google telemetry point must be an object"
    );
    Ok(())
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 20 && id.bytes().all(|byte| byte.is_ascii_digit())
}

pub async fn enqueue(
    tx: &Transaction<'_>,
    subject: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<()> {
    // Keep the consent check and queue changes ordered with OAuth scope changes.
    let allowed = tx
        .query_opt(
            "SELECT google_location_granted FROM avatar_hsm.wearable_oauth_connection
         WHERE subject_id=$1 AND provider='google_health' FOR SHARE",
            &[&subject],
        )
        .await?
        .is_some_and(|row| row.get::<_, bool>(0));
    if !allowed {
        return Ok(());
    }
    tx.execute(
        "DELETE FROM watch_tower.google_workout_tcx_queue q
         WHERE q.subject_id=$1 AND EXISTS (
           SELECT 1 FROM avatar_hsm.wearable_record r
           WHERE r.subject_id=q.subject_id AND r.provider='google_health' AND r.kind='workout'
             AND r.source=q.source AND r.source_id=q.source_id
             AND r.started_at >= $2 AND r.ended_at <= $3
             AND (r.payload#>'{provider_data,exerciseMetadata,hasGps}') IS DISTINCT FROM 'true'::jsonb
         )",
        &[&subject,&start,&end],
    ).await?;
    tx.execute(
        "DELETE FROM avatar_hsm.google_workout_tcx t USING avatar_hsm.wearable_record r
         WHERE t.subject_id=$1 AND r.subject_id=t.subject_id AND r.provider=t.provider AND r.kind=t.kind
           AND r.source=t.source AND r.source_id=t.source_id AND r.started_at >= $2 AND r.ended_at <= $3
           AND (r.payload#>'{provider_data,exerciseMetadata,hasGps}') IS DISTINCT FROM 'true'::jsonb",
        &[&subject,&start,&end],
    ).await?;
    tx.execute(
        "DELETE FROM watch_tower.google_workout_tcx_queue q
         WHERE q.subject_id=$1 AND NOT EXISTS (
           SELECT 1 FROM avatar_hsm.google_workout_tcx t
           WHERE t.subject_id=q.subject_id AND t.source=q.source AND t.source_id=q.source_id
         )",
        &[&subject],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.google_workout_tcx(subject_id,source,source_id)
         SELECT r.subject_id,r.source,r.source_id
         FROM avatar_hsm.wearable_record r
         WHERE r.subject_id=$1 AND r.provider='google_health' AND r.kind='workout'
           AND r.source_id ~ '^[0-9]{1,20}$' AND r.started_at >= $2 AND r.started_at < $3 AND r.ended_at <= $3
           AND r.payload#>'{provider_data,exerciseMetadata,hasGps}'='true'::jsonb
         ON CONFLICT (subject_id,source,source_id) DO NOTHING",
        &[&subject,&start,&end],
    ).await?;
    tx.execute(
        "WITH changed AS (
         INSERT INTO watch_tower.google_workout_tcx_queue AS q
            (subject_id,source,source_id,fingerprint)
         SELECT r.subject_id,r.source,r.source_id,f.fingerprint
         FROM avatar_hsm.wearable_record r
         CROSS JOIN LATERAL (SELECT sha256(convert_to((r.payload->'provider_data')::text,'UTF8')) AS fingerprint) f
         WHERE r.subject_id=$1 AND r.provider='google_health' AND r.kind='workout'
           AND r.started_at >= $2 AND r.started_at < $3 AND r.ended_at <= $3
           AND r.payload#>'{provider_data,exerciseMetadata,hasGps}'='true'::jsonb
         ON CONFLICT(subject_id,source,source_id) DO UPDATE
         SET fingerprint=EXCLUDED.fingerprint,attempts=0,next_attempt=now(),job_id=NULL,leased_until=NULL
         WHERE q.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint
         RETURNING subject_id,source,source_id
         )
         UPDATE avatar_hsm.google_workout_tcx t
         SET data=NULL,route_points=NULL,telemetry_points=NULL
         FROM changed WHERE (t.subject_id,t.source,t.source_id)=(changed.subject_id,changed.source,changed.source_id)",
        &[&subject,&start,&end],
    ).await?;
    Ok(())
}

pub(crate) struct Job {
    pub(crate) subject: String,
    pub(crate) source: String,
    pub(crate) activity: String,
    pub(crate) id: String,
    pub(crate) stored_data: Option<Vec<u8>>,
}

pub(crate) async fn claim(pool: &Pool) -> Result<Option<Job>> {
    let row = pool.get().await?.query_opt(
        "WITH due AS (
           SELECT q.subject_id,q.source,q.source_id FROM watch_tower.google_workout_tcx_queue q
           JOIN avatar_hsm.google_workout_tcx t USING(subject_id,source,source_id)
           JOIN avatar_hsm.wearable_oauth_connection c ON c.subject_id=t.subject_id AND c.provider='google_health'
           WHERE c.google_location_granted AND (t.route_points IS NULL OR t.telemetry_points IS NULL)
             AND q.attempts<6 AND q.next_attempt<=now()
             AND (q.leased_until IS NULL OR q.leased_until<=now())
             AND (t.data IS NOT NULL OR NOT EXISTS (
               SELECT 1 FROM watch_tower.wearable_sync_schedule s
               WHERE s.subject_id=t.subject_id AND s.provider='google_health' AND s.throttled_until>now()
             ))
           ORDER BY q.next_attempt LIMIT 1 FOR UPDATE OF q SKIP LOCKED
         )
         UPDATE watch_tower.google_workout_tcx_queue q SET job_id=gen_random_uuid(),leased_until=now()+interval '2 minutes'
         FROM due,avatar_hsm.google_workout_tcx t
         WHERE (q.subject_id,q.source,q.source_id)=(due.subject_id,due.source,due.source_id)
           AND (t.subject_id,t.source,t.source_id)=(q.subject_id,q.source,q.source_id)
         RETURNING q.subject_id,q.source,q.source_id,q.job_id::text,t.data",
        &[],
    ).await?;
    Ok(row.map(|row| Job {
        subject: row.get(0),
        source: row.get(1),
        activity: row.get(2),
        id: row.get(3),
        stored_data: row.get(4),
    }))
}

#[cfg(test)]
async fn fetch(client: &Client, origin: &str, token: &str, id: &str) -> Result<Vec<u8>> {
    ensure!(valid_id(id), "invalid Google Health exercise ID");
    let response = client
        .get(format!(
            "{origin}/v4/users/me/dataTypes/exercise/dataPoints/{id}:exportExerciseTcx"
        ))
        .query(&[("alt", "media")])
        .bearer_auth(token)
        .send()
        .await?;
    let mut response = cloud_wearables::check_provider_response(response)?;
    let mut data = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            data.len() + chunk.len() <= MAX_BYTES,
            "Google Health TCX exceeds size limit"
        );
        data.extend_from_slice(&chunk);
    }
    Ok(data)
}

#[cfg(test)]
#[derive(Default)]
struct TrackPoint {
    timestamp_ms: Option<i64>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    altitude: Option<f64>,
    telemetry: serde_json::Map<String, Value>,
}

#[cfg(test)]
#[derive(Serialize)]
struct Point {
    timestamp_ms: i64,
    latitude: f64,
    longitude: f64,
    altitude: Option<f64>,
    segment: usize,
}

#[cfg(test)]
fn measurement(value: &str) -> Option<f64> {
    value
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && *v >= 0.0)
}

#[cfg(test)]
fn parse_track(data: &[u8]) -> Result<(Vec<Point>, Vec<Value>)> {
    let mut reader = Reader::from_str(std::str::from_utf8(data)?.trim_start_matches('\u{feff}'));
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
                                .map(|time| time.timestamp_millis());
                        }
                        (7, Some("AltitudeMeters")) => {
                            point.altitude = value.parse::<f64>().ok().filter(|v| v.is_finite());
                        }
                        (7, Some("DistanceMeters")) => {
                            if let Some(distance) = measurement(value) {
                                point.telemetry.insert("distance_m".into(), json!(distance));
                            }
                        }
                        (7, Some("Cadence")) => {
                            if let Ok(cadence) = value.parse::<u8>() {
                                point
                                    .telemetry
                                    .insert("cycling_cadence_rpm".into(), json!(cadence));
                            }
                        }
                        (8, Some("Value")) if path[6] == "HeartRateBpm" => {
                            if let Ok(heart_rate) = value.parse::<u8>() {
                                point
                                    .telemetry
                                    .insert("heart_rate_bpm".into(), json!(heart_rate));
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
                                .filter(|lat| (-90.0..=90.0).contains(lat));
                        }
                        (8, Some("LongitudeDegrees")) if path[6] == "Position" => {
                            point.longitude = value
                                .parse::<f64>()
                                .ok()
                                .filter(|lon| (-180.0..=180.0).contains(lon));
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
                return Ok((points, telemetry));
            }
            _ => {}
        }
    }
}

#[cfg(test)]
async fn save(pool: &Pool, job: &Job, data: &[u8]) -> Result<bool> {
    let (route, telemetry) = parse_track(data)?;
    let route = serde_json::to_value(route)?;
    let telemetry = Value::Array(telemetry);
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let count = tx.execute(
        "UPDATE avatar_hsm.google_workout_tcx t SET data=COALESCE(t.data,$5),route_points=$6,
             telemetry_points=$7
         WHERE t.subject_id=$1 AND t.source=$2 AND t.source_id=$3
           AND EXISTS(SELECT 1 FROM watch_tower.google_workout_tcx_queue q
                      WHERE q.subject_id=t.subject_id AND q.source=t.source AND q.source_id=t.source_id
                        AND q.job_id=$4::text::uuid AND q.leased_until>now())
           AND EXISTS (SELECT 1 FROM avatar_hsm.wearable_oauth_connection c
                       WHERE c.subject_id=t.subject_id AND c.provider=t.provider AND c.google_location_granted)",
        &[&job.subject,&job.source,&job.activity,&job.id,&data,&route,&telemetry],
    ).await?;
    if count == 1 {
        tx.execute(
            "UPDATE watch_tower.google_workout_tcx_queue SET job_id=NULL,leased_until=NULL
                    WHERE subject_id=$1 AND source=$2 AND source_id=$3 AND job_id=$4::text::uuid",
            &[&job.subject, &job.source, &job.activity, &job.id],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(count == 1)
}

#[cfg(test)]
async fn run(pool: &Pool, client: &Client, job: &Job) -> Result<()> {
    if let Some(data) = &job.stored_data {
        save(pool, job, data).await?;
        return Ok(());
    }
    let credentials =
        wearable_oauth::access(client, pool, &job.subject, Provider::GoogleHealth).await?;
    ensure!(
        credentials
            .get(1)
            .is_some_and(|scope| wearable_oauth::google_tcx_scope(scope)),
        "Google Health activity and location read permissions are required"
    );
    let data = fetch(client, ORIGIN, &credentials[0], &job.activity).await?;
    save(pool, job, &data).await?;
    Ok(())
}

#[cfg(test)]
pub async fn worker(pool: Pool, client: Client) {
    loop {
        let Ok(Some(job)) = claim(&pool).await else {
            tokio::time::sleep(Duration::from_secs(12)).await;
            continue;
        };
        if let Err(error) = run(&pool, &client, &job).await {
            let retry_at = error
                .downcast_ref::<cloud_wearables::ProviderThrottled>()
                .map(|throttle| throttle.retry_at);
            let _ = defer_job(&pool, &job, retry_at).await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub(crate) async fn defer_job(
    pool: &Pool,
    job: &Job,
    retry_at: Option<DateTime<Utc>>,
) -> Result<()> {
    pool.get()
        .await?
        .execute(
            "WITH connection AS (
                 SELECT 1 FROM avatar_hsm.device_connection
                 WHERE $5::timestamptz IS NOT NULL AND subject_id=$1 AND provider='google_health'
                 FOR UPDATE
             ), deferred AS (
                 UPDATE watch_tower.google_workout_tcx_queue
                 SET attempts=attempts+CASE WHEN $5::timestamptz IS NULL THEN 1 ELSE 0 END,
                     next_attempt=COALESCE($5,now()+make_interval(hours=>LEAST(attempts+1,24))),
                     job_id=NULL,leased_until=NULL
                 WHERE subject_id=$1 AND source=$2 AND source_id=$3 AND job_id=$4::text::uuid
                   AND ($5 IS NULL OR EXISTS(SELECT 1 FROM connection))
                 RETURNING subject_id
             )
             UPDATE watch_tower.wearable_sync_schedule AS sync
             SET throttled_until=GREATEST(sync.throttled_until,$5)
             FROM deferred WHERE $5 IS NOT NULL
               AND sync.subject_id=deferred.subject_id AND sync.provider='google_health'",
            &[&job.subject, &job.source, &job.activity, &job.id, &retry_at],
        )
        .await?;
    Ok(())
}

#[derive(Default, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum TrackSection {
    #[default]
    Route,
    Telemetry,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteQuery {
    source: String,
    source_id: String,
    #[serde(default)]
    offset: i64,
    limit: Option<i64>,
    #[serde(default)]
    section: TrackSection,
}

pub async fn query_route(pool: &Pool, subject: &str, query: RouteQuery) -> Result<Value> {
    let limit = query.limit.unwrap_or(100);
    let telemetry = query.section == TrackSection::Telemetry;
    ensure!(
        valid_id(&query.source_id)
            && !query.source.trim().is_empty()
            && query.source.len() <= 256
            && (0..=MAX_POINTS as i64).contains(&query.offset)
            && (1..=100).contains(&limit),
        "invalid Google Health route query"
    );
    let row = pool
        .get()
        .await?
        .query_opt(
            "SELECT CASE WHEN track.points IS NOT NULL THEN 'ready'
             WHEN q.attempts >= 6 THEN 'unavailable' ELSE 'pending' END,
             COALESCE(jsonb_array_length(track.points),0), COALESCE((
                 SELECT jsonb_agg(point ORDER BY ord)
                 FROM jsonb_array_elements(track.points) WITH ORDINALITY AS points(point,ord)
                 WHERE ord>$4::bigint AND ord<=$4::bigint+$5::bigint
             ),'[]'::jsonb)
         FROM avatar_hsm.google_workout_tcx g
         JOIN watch_tower.google_workout_tcx_queue q USING(subject_id,source,source_id)
         JOIN avatar_hsm.wearable_oauth_connection o USING(subject_id,provider)
         JOIN avatar_hsm.device_connection c USING(subject_id,provider)
         CROSS JOIN LATERAL (SELECT CASE WHEN $6 THEN g.telemetry_points ELSE g.route_points END AS points) track
         WHERE g.subject_id=$1 AND g.source=$2 AND g.source_id=$3
           AND o.google_location_granted AND c.state='synced' AND 'workout'=ANY(c.grants)",
            &[
                &subject,
                &query.source,
                &query.source_id,
                &query.offset,
                &limit,
                &telemetry,
            ],
        )
        .await?;
    let Some(row) = row else {
        return Ok(json!({"state":"not_found","records":[]}));
    };
    let total = i64::from(row.get::<_, i32>(1));
    let next = query.offset + limit;
    Ok(json!({
        "state":row.get::<_,String>(0),"provider":"google_health","source":query.source,
        "source_id":query.source_id,"stored_point_count":total,
        "next_offset":(next<total).then_some(next),"records":row.get::<_,Value>(2)
    }))
}

pub async fn user_download(
    State(app): State<Arc<App>>,
    Path(activity): Path<String>,
    headers: HeaderMap,
) -> Response {
    let subject =
        match wearable_oauth::authenticate(&app.db, wearable_fit::bearer(&headers), false).await {
            Ok(Some(subject)) => subject,
            Ok(None) => return StatusCode::UNAUTHORIZED.into_response(),
            Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        };
    if !valid_id(&activity) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let read = async {
        Ok::<_, anyhow::Error>(app.db.get().await?.query_opt(
            "SELECT t.data FROM avatar_hsm.google_workout_tcx t
             JOIN avatar_hsm.wearable_oauth_connection c USING(subject_id,provider)
             WHERE t.subject_id=$1 AND t.source_id=$2 AND c.google_location_granted AND t.data IS NOT NULL
             ORDER BY t.source LIMIT 1",
            &[&subject,&activity],
        ).await?)
    }.await;
    match read {
        Ok(Some(row)) => (
            [
                ("content-type", "application/octet-stream"),
                ("content-disposition", "attachment; filename=activity.tcx"),
                ("cache-control", "private, no-store"),
                ("x-content-type-options", "nosniff"),
            ],
            row.get::<_, Vec<u8>>(0),
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
