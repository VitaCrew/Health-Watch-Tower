use std::{collections::HashSet, env, time::Duration};

#[cfg(test)]
use anyhow::bail;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Duration as CalendarDuration, Utc};
use reqwest::{Client, Response};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::wearables;

pub(crate) mod garmin;
#[cfg(test)]
mod google_metrics;
pub(crate) mod manufacturers;
mod polar_series;
pub(crate) use polar_series::sleep_details as polar_sleep_details;
mod sensorbio_activity;
pub(crate) use sensorbio_activity::daily_hourly_details as sensorbio_daily_hourly_details;
mod vendor_metrics;
pub(crate) use vendor_metrics::{suunto_sleep_details, withings_workout_details};
mod withings_detail;
pub(crate) use withings_detail::sleep_summary as withings_sleep_summary;

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    GoogleHealth,
    Hevy,
    Liftosaur,
    Strava,
    Oura,
    Whoop,
    Polar,
    IntervalsIcu,
    Withings,
    Suunto,
    Coros,
    Ultrahuman,
    Sensorbio,
    GarminPersonal,
    Garmin,
}

impl Provider {
    pub fn parse(slug: &str) -> Result<Self> {
        serde_json::from_value(json!(slug)).map_err(|_| anyhow::anyhow!("unknown cloud provider"))
    }
    pub fn slug(self) -> &'static str {
        match self {
            Self::GoogleHealth => "google_health",
            Self::Hevy => "hevy",
            Self::Liftosaur => "liftosaur",
            Self::Strava => "strava",
            Self::Oura => "oura",
            Self::Whoop => "whoop",
            Self::Polar => "polar",
            Self::IntervalsIcu => "intervals_icu",
            Self::Withings => "withings",
            Self::Suunto => "suunto",
            Self::Coros => "coros",
            Self::Ultrahuman => "ultrahuman",
            Self::Sensorbio => "sensorbio",
            Self::GarminPersonal => "garmin_personal",
            Self::Garmin => "garmin",
        }
    }

    fn credentials(self) -> &'static [&'static str] {
        match self {
            Self::GoogleHealth => &[
                "AVATAR_GOOGLE_HEALTH_CLIENT_ID",
                "AVATAR_GOOGLE_HEALTH_CLIENT_SECRET",
                "AVATAR_GOOGLE_HEALTH_REFRESH_TOKEN",
            ],
            Self::Hevy => &["AVATAR_HEVY_API_KEY"],
            Self::Liftosaur => &["AVATAR_LIFTOSAUR_API_KEY"],
            Self::Strava => &["AVATAR_STRAVA_ACCESS_TOKEN"],
            Self::Oura => &["AVATAR_OURA_ACCESS_TOKEN"],
            Self::Whoop => &["AVATAR_WHOOP_ACCESS_TOKEN"],
            Self::Polar => &["AVATAR_POLAR_ACCESS_TOKEN"],
            Self::IntervalsIcu => &["AVATAR_INTERVALS_ICU_API_KEY"],
            Self::Withings => &["AVATAR_WITHINGS_ACCESS_TOKEN"],
            Self::Suunto => &[
                "AVATAR_SUUNTO_ACCESS_TOKEN",
                "AVATAR_SUUNTO_SUBSCRIPTION_KEY",
            ],
            Self::Coros => &["AVATAR_COROS_ACCESS_TOKEN", "AVATAR_COROS_OPEN_ID"],
            Self::Ultrahuman => &["AVATAR_ULTRAHUMAN_ACCESS_TOKEN"],
            Self::Sensorbio => &["AVATAR_SENSORBIO_ACCESS_TOKEN"],
            Self::GarminPersonal => &["AVATAR_GARMIN_PERSONAL_API_KEY"],
            Self::Garmin => &[],
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub provider: Provider,
}

pub fn configured(provider: Provider) -> Option<Vec<String>> {
    if provider.credentials().is_empty() {
        return None;
    }
    provider
        .credentials()
        .iter()
        .map(|key| env::var(key).ok().filter(|value| !value.trim().is_empty()))
        .collect()
}

pub fn http_client() -> Result<Client> {
    Ok(Client::builder()
        .timeout(Duration::from_secs(12))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent("OpenHuman-Avatar/0.1")
        .build()?)
}

pub fn window_size(provider: Provider) -> CalendarDuration {
    CalendarDuration::days(
        if matches!(
            provider,
            Provider::GoogleHealth | Provider::Sensorbio | Provider::Suunto | Provider::Ultrahuman
        ) {
            1
        } else {
            7
        },
    )
}

pub async fn prepare(
    client: &Client,
    provider: Provider,
    credentials: Vec<String>,
) -> Result<Vec<String>> {
    if matches!(provider, Provider::GoogleHealth) {
        Ok(vec![
            refresh(client, "https://oauth2.googleapis.com/token", &credentials).await?,
        ])
    } else {
        Ok(credentials)
    }
}

#[cfg(test)]
pub async fn fetch_range(
    client: &Client,
    provider: Provider,
    credentials: &[String],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    lease: &Value,
) -> Result<wearables::Snapshot> {
    ensure!(
        start < end && end - start <= window_size(provider),
        "invalid cloud sync window"
    );
    #[cfg(test)]
    let client = client.clone();
    #[cfg(not(test))]
    let _ = (client, credentials);
    #[cfg(test)]
    let mut google_metric_coverage = Vec::new();
    #[cfg(test)]
    let mut withings_measurement_access = None;
    #[cfg(not(test))]
    let withings_measurement_access = None;
    #[cfg(test)]
    let mut coros_fit_sources = crate::wearable_fit::CorosFitSources::new();
    let (records, grants): (Vec<Value>, Vec<&'static str>) =
        tokio::time::timeout(Duration::from_secs(48), async {
            match provider {
                Provider::Hevy
                | Provider::Liftosaur
                | Provider::Strava
                | Provider::Oura
                | Provider::IntervalsIcu
                | Provider::Polar
                | Provider::Ultrahuman
                | Provider::Whoop
                | Provider::GarminPersonal => {
                    bail!("this provider collection is owned by Watch Tower ETL")
                }
                #[cfg(not(test))]
                Provider::Withings => bail!("this provider collection is owned by Watch Tower ETL"),
                #[cfg(test)]
                Provider::Withings => {
                    let revision = Utc::now();
                    let scope = credentials.get(1).map(String::as_str);
                    let mut rows = Vec::new();
                    let mut grants = Vec::new();
                    if manufacturers::has_scope(scope, "user.activity") {
                        let (sleep, activity) = tokio::try_join!(
                            manufacturers::withings(&client, &credentials[0], start, end),
                            vendor_metrics::withings(&client, &credentials[0], start, end),
                        )?;
                        rows.extend(sleep);
                        rows.extend(activity);
                        grants.extend(["sleep", "workout", "heart_rate", "steps", "metric"]);
                    }
                    let measurement_access = manufacturers::has_scope(scope, "user.metrics");
                    withings_measurement_access = Some(measurement_access);
                    if measurement_access {
                        let window = Window { start, end };
                        let (measurements, ecg) = tokio::try_join!(
                            manufacturers::withings_measurements(&client, &credentials[0], &window),
                            withings_detail::ecg(&client, &credentials[0], start, end),
                        )?;
                        rows.extend(measurements);
                        rows.extend(ecg);
                        if !grants.contains(&"metric") {
                            grants.push("metric");
                        }
                    }
                    for row in &mut rows {
                        row["modified_at"] = json!(revision);
                    }
                    Ok((rows, grants))
                }
                #[cfg(not(test))]
                Provider::Suunto => bail!("this provider collection is owned by Watch Tower ETL"),
                #[cfg(not(test))]
                Provider::Coros => bail!("this provider collection is owned by Watch Tower ETL"),
                #[cfg(not(test))]
                Provider::Sensorbio => {
                    bail!("this provider collection is owned by Watch Tower ETL")
                }
                #[cfg(not(test))]
                Provider::GoogleHealth => {
                    bail!("this provider collection is owned by Watch Tower ETL")
                }
                #[cfg(test)]
                Provider::GoogleHealth => {
                    let scope = credentials
                        .get(1)
                        .map(String::as_str)
                        .filter(|s| !s.is_empty());
                    let ((mut rows, mut grants), (metrics, covered)) = tokio::try_join!(
                        google_records(
                            &client,
                            &credentials[0],
                            start,
                            end,
                            scope,
                            "https://health.googleapis.com",
                        ),
                        google_metrics::fetch(&client, &credentials[0], start, end, scope),
                    )?;
                    rows.extend(metrics);
                    if !covered.is_empty() {
                        grants.push("metric");
                    }
                    google_metric_coverage = covered;
                    ensure!(
                        rows.len() <= wearables::HIGH_VOLUME_CLOUD_RECORD_LIMIT,
                        "Google Health combined record limit exceeded"
                    );
                    Ok::<_, anyhow::Error>((rows, grants))
                }
                #[cfg(test)]
                Provider::Suunto => {
                    let mut rows = manufacturers::suunto(
                        &client,
                        &credentials[0],
                        &credentials[1],
                        start,
                        end,
                    )
                    .await?;
                    rows.extend(
                        vendor_metrics::suunto(
                            &client,
                            &credentials[0],
                            &credentials[1],
                            start,
                            end,
                        )
                        .await?,
                    );
                    Ok((
                        rows,
                        vec!["workout", "sleep", "heart_rate", "steps", "metric"],
                    ))
                }
                #[cfg(test)]
                Provider::Coros => {
                    let (rows, sources) =
                        manufacturers::coros(&client, &credentials[0], &credentials[1], start, end)
                            .await?;
                    coros_fit_sources = sources;
                    Ok((rows, vec!["workout", "sleep", "metric", "steps"]))
                }
                Provider::Garmin => bail!("Garmin business uses push delivery"),
                #[cfg(test)]
                Provider::Sensorbio => {
                    let (mut rows, biometrics, activity) = tokio::try_join!(
                        manufacturers::daily_sleep(&client, &credentials[0], start, end),
                        vendor_metrics::sensorbio(&client, &credentials[0], start, end),
                        sensorbio_activity::fetch(&client, &credentials[0], start, end),
                    )?;
                    rows.extend(biometrics);
                    rows.extend(activity);
                    ensure!(
                        rows.len() <= wearables::HIGH_VOLUME_CLOUD_RECORD_LIMIT,
                        "Sensor Bio combined record limit exceeded"
                    );
                    Ok((
                        rows,
                        vec!["sleep", "heart_rate", "workout", "steps", "metric"],
                    ))
                }
            }
        })
        .await??;
    let mut snapshot: wearables::Snapshot = serde_json::from_value(json!({
        "lease": lease["lease"], "start": start, "end": end, "grants": grants, "records": records,
    }))?;
    #[cfg(test)]
    {
        snapshot.google_metric_coverage = google_metric_coverage;
    }
    snapshot.withings_measurement_access = withings_measurement_access;
    #[cfg(test)]
    {
        snapshot.coros_fit_sources = coros_fit_sources;
    }
    Ok(snapshot)
}

#[cfg(test)]
fn google_page_token(response: &Value) -> Result<Option<&str>> {
    ensure!(
        response.is_object() && response.get("error").is_none(),
        "invalid Google Health page"
    );
    match response.get("nextPageToken") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(token)) => Ok((!token.is_empty()).then_some(token.as_str())),
        _ => bail!("invalid Google Health page token"),
    }
}

#[derive(Debug)]
pub(crate) struct ProviderThrottled {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderThrottled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("provider returned HTTP 429")
    }
}

impl std::error::Error for ProviderThrottled {}

fn retry_after(header: Option<&str>, now: DateTime<Utc>) -> DateTime<Utc> {
    let retry_at = header.and_then(|value| {
        if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
            Some(
                value
                    .parse::<i64>()
                    .ok()
                    .and_then(CalendarDuration::try_seconds)
                    .and_then(|delay| now.checked_add_signed(delay))
                    .unwrap_or(DateTime::<Utc>::MAX_UTC),
            )
        } else {
            DateTime::parse_from_rfc2822(value)
                .ok()
                .map(|date| date.with_timezone(&Utc))
        }
    });
    retry_at
        .unwrap_or(now + CalendarDuration::minutes(5))
        .max(now)
}

pub(crate) fn check_provider_response(response: Response) -> Result<Response> {
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let header = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok());
        return Err(ProviderThrottled {
            retry_at: retry_after(header, Utc::now()),
        }
        .into());
    }
    ensure!(
        status.is_success(),
        "provider returned HTTP {}",
        status.as_u16()
    );
    Ok(response)
}

pub async fn body(response: Response) -> Result<Value> {
    let mut response = check_provider_response(response)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= 8 * 1024 * 1024,
            "provider response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

async fn refresh(client: &Client, token_url: &str, keys: &[String]) -> Result<String> {
    let result = body(
        client
            .post(token_url)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", &keys[0]),
                ("client_secret", &keys[1]),
                ("refresh_token", &keys[2]),
            ])
            .send()
            .await?,
    )
    .await?;
    Ok(required_text(&result["access_token"])?.to_owned())
}

fn required_text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .context("required text field missing")
}

fn integer(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .context("invalid whole number")
}

fn date(value: &Value) -> Result<DateTime<Utc>> {
    Ok(required_text(value)?.parse()?)
}

fn records(value: &Value) -> Result<&Vec<Value>> {
    value.as_array().context("missing provider record list")
}

struct Window {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

impl Window {
    fn contains(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
        start >= self.start && start < self.end && end >= start && end <= self.end
    }
}

fn record(
    kind: &str,
    source: &str,
    id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    revision: DateTime<Utc>,
    payload: Value,
) -> Value {
    json!({"kind":kind,"source":source,"source_id":id,"started_at":start,"ended_at":end,
           "modified_at":revision,"payload":payload})
}

#[cfg(test)]
pub(crate) fn end_at(start: DateTime<Utc>, seconds: i64) -> Result<DateTime<Utc>> {
    ensure!(
        (0..=7 * 86_400).contains(&seconds),
        "unsupported workout duration"
    );
    start
        .checked_add_signed(CalendarDuration::seconds(seconds))
        .context("invalid workout end")
}

#[cfg(test)]
async fn google_records(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    scope: Option<&str>,
    origin: &str,
) -> Result<(Vec<Value>, Vec<&'static str>)> {
    let window = Window { start, end };
    let mut output = Vec::new();
    let mut grants = Vec::new();
    for (endpoint, field, filter_field, kind) in [
        ("steps", "steps", "steps.interval.start_time", "steps"),
        (
            "heart-rate",
            "heartRate",
            "heart_rate.sample_time.physical_time",
            "heart_rate",
        ),
        ("sleep", "sleep", "sleep.interval.end_time", "sleep"),
        (
            "exercise",
            "exercise",
            "exercise.interval.civil_start_time",
            "workout",
        ),
    ] {
        let required = match kind {
            "heart_rate" => "health_metrics_and_measurements",
            "sleep" => "sleep",
            _ => "activity_and_fitness",
        };
        if !google_metrics::granted(scope, required) {
            continue;
        }
        grants.push(kind);
        let base = format!("{origin}/v4/users/me/dataTypes/{endpoint}/dataPoints");
        let url = if matches!(kind, "sleep" | "workout") {
            format!("{base}:reconcile")
        } else {
            base
        };
        let (from, until) = if kind == "workout" {
            // Korean test profile. Include an extra civil day in either direction for travel and apply the exact UTC window below.
            (
                (start - CalendarDuration::days(1))
                    .format("%Y-%m-%dT%H:%M:%S")
                    .to_string(),
                (end + CalendarDuration::days(1))
                    .format("%Y-%m-%dT%H:%M:%S")
                    .to_string(),
            )
        } else {
            (start.to_rfc3339(), end.to_rfc3339())
        };
        let filter = format!("{filter_field} >= \"{from}\" AND {filter_field} < \"{until}\"");
        let page_size = if matches!(kind, "sleep" | "workout") {
            "25"
        } else {
            "1000"
        };
        let mut page = String::new();
        let mut seen = HashSet::new();
        loop {
            let mut request = client.get(&url).bearer_auth(token).query(&[
                ("filter", filter.as_str()),
                ("pageSize", page_size),
                (
                    "dataSourceFamily",
                    "users/me/dataSourceFamilies/google-wearables",
                ),
            ]);
            if !page.is_empty() {
                request = request.query(&[("pageToken", page.as_str())]);
            }
            let result = body(request.send().await?).await?;
            let next = google_page_token(&result)?;
            if let Some(items) = result.get("dataPoints") {
                for item in records(items)? {
                    if let Some(mut row) = google_point(item, field, kind, &window)? {
                        // Reconciled sessions omit DataSource. Read it from the same resource
                        // without introducing overlapping sessions or changing stored keys.
                        if matches!(kind, "sleep" | "workout") && !item["dataSource"].is_object() {
                            if let Some(source) = google_session_source(
                                client,
                                token,
                                origin,
                                endpoint,
                                item["dataPointName"].as_str(),
                            )
                            .await?
                            {
                                row["payload"]["provider_source"] = source;
                            }
                        }
                        output.push(row);
                    }
                }
            }
            ensure!(
                output.len() <= wearables::HIGH_VOLUME_CLOUD_RECORD_LIMIT,
                "Google Health snapshot exceeds the record limit"
            );
            let Some(next) = next else {
                break;
            };
            ensure!(
                seen.len() < 32 && seen.insert(next.to_owned()),
                "incomplete Google Health pagination"
            );
            page = next.to_owned();
        }
    }
    // Google interval and sample records can lack resource IDs; their source and time are used as a fallback key.
    let mut keys = HashSet::new();
    ensure!(
        output.iter().all(|row| keys.insert((
            row["kind"].clone(),
            row["source"].clone(),
            row["source_id"].clone()
        ))),
        "ambiguous repeated Google Health record"
    );
    Ok((output, grants))
}

#[cfg(test)]
async fn google_session_source(
    client: &Client,
    token: &str,
    origin: &str,
    endpoint: &str,
    resource_name: Option<&str>,
) -> Result<Option<Value>> {
    let Some(name) = resource_name else {
        return Ok(None);
    };
    let parts: Vec<_> = name.split('/').collect();
    let ["users", user, "dataTypes", data_type, "dataPoints", point] = parts.as_slice() else {
        return Ok(None);
    };
    let safe_segment = |segment: &str| {
        !segment.is_empty()
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    };
    if *data_type != endpoint || !safe_segment(user) || !safe_segment(point) {
        return Ok(None);
    }
    let Ok(response) = client
        .get(format!("{origin}/v4/{name}"))
        .bearer_auth(token)
        .timeout(Duration::from_secs(3))
        .send()
        .await
    else {
        return Ok(None);
    };
    let result = match body(response).await {
        Ok(result) => result,
        // Source metadata is optional, but the connection's request limit also applies to this lookup.
        Err(error) if error.is::<ProviderThrottled>() => return Err(error),
        Err(_) => return Ok(None),
    };
    if result["name"].as_str() != Some(name) || !result["dataSource"].is_object() {
        return Ok(None);
    }
    Ok(result.get("dataSource").cloned())
}

#[cfg(test)]
fn google_point(root: &Value, field: &str, kind: &str, window: &Window) -> Result<Option<Value>> {
    let point = &root[field];
    let (start, end) = if kind == "heart_rate" {
        let at = date(&point["sampleTime"]["physicalTime"])?;
        (at, at)
    } else {
        (
            date(&point["interval"]["startTime"])?,
            date(&point["interval"]["endTime"])?,
        )
    };
    if !window.contains(start, end) {
        return Ok(None);
    }
    let origin = &root["dataSource"];
    let label = origin["device"]["displayName"]
        .as_str()
        .or_else(|| origin["platform"].as_str())
        .unwrap_or("Google Health");
    let id = root["name"]
        .as_str()
        .or_else(|| root["dataPointName"].as_str())
        .and_then(|name| name.rsplit('/').next())
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{start}/{end}"));
    let mut payload = match kind {
        "steps" => json!({"count":integer(&point["count"])?, "unit":"steps"}),
        "heart_rate" => {
            json!({"samples":[{"at":start,"bpm":integer(&point["beatsPerMinute"])?}],"unit":"bpm"})
        }
        "sleep" => {
            let stages = point["stages"].as_array().into_iter().flatten().map(|stage| {
                let value = match stage["type"].as_str().unwrap_or_default() {
                    "AWAKE" => 1, "ASLEEP" => 2, "LIGHT" => 4, "DEEP" => 5, "REM" => 6, _ => 0,
                };
                Ok::<_, anyhow::Error>(json!({"start":date(&stage["startTime"])?, "end":date(&stage["endTime"])?, "stage":value}))
            }).collect::<Result<Vec<_>>>()?;
            // Short awakenings can overlap stages and must not alter the normalized stage timeline.
            json!({"stages":stages, "provider_data":point})
        }
        "workout" => {
            let summary = &point["metricsSummary"];
            let mut payload = json!({
                "exercise_type":required_text(&point["exerciseType"])?,
                "title":point["displayName"],
                "provider_data":point
            });
            for (field, target, divisor) in [
                ("distanceMillimeters", "distance_meters", 1000.0),
                ("elevationGainMillimeters", "elevation_gain_meters", 1000.0),
                (
                    "averageSpeedMillimetersPerSecond",
                    "average_speed_meters_per_second",
                    1000.0,
                ),
                ("caloriesKcal", "calories_kcal", 1.0),
            ] {
                if let Some(value) = summary[field].as_f64() {
                    payload[target] = json!(value / divisor);
                }
            }
            for (field, target) in [
                ("steps", "workout_steps"),
                ("averageHeartRateBeatsPerMinute", "average_heart_rate_bpm"),
                ("activeZoneMinutes", "active_zone_minutes"),
            ] {
                if let Ok(value) = integer(&summary[field]) {
                    payload[target] = json!(value);
                }
            }
            if let Some(seconds) = point["activeDuration"]
                .as_str()
                .and_then(|value| value.strip_suffix('s'))
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|value| value.is_finite() && *value >= 0.0)
            {
                payload["active_duration_seconds"] = json!(seconds);
            }
            payload
        }
        _ => bail!("unexpected Google Health data type"),
    };
    if origin.is_object() {
        payload["provider_source"] = origin.clone();
    }
    let revision = point["updateTime"]
        .as_str()
        .map(str::parse)
        .transpose()?
        .unwrap_or_else(Utc::now);
    Ok(Some(record(
        kind, label, &id, start, end, revision, payload,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_accepts_seconds_and_http_dates() -> Result<()> {
        let now: DateTime<Utc> = "2026-04-20T10:00:00Z".parse()?;
        let later = now + CalendarDuration::minutes(10);
        assert_eq!(retry_after(Some("600"), now), later);
        assert_eq!(
            retry_after(Some("Mon, 20 Apr 2026 10:10:00 GMT"), now),
            later
        );
        assert_eq!(retry_after(Some("Mon, 20 Apr 2026 09:00:00 GMT"), now), now);
        assert_eq!(retry_after(None, now), now + CalendarDuration::minutes(5));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL in AVATAR_HSM_TEST_DATABASE_URL"]
    async fn google_session_details_survive_paginated_collection_and_authenticated_mcp(
    ) -> Result<()> {
        use axum::{
            extract::Query, http::HeaderMap, response::IntoResponse, routing::get, Json, Router,
        };
        use std::{
            collections::HashMap,
            sync::{
                atomic::{AtomicBool, AtomicUsize, Ordering},
                Arc,
            },
        };

        let start: DateTime<Utc> = "2026-03-04T16:00:00Z".parse()?;
        let end: DateTime<Utc> = "2026-03-05T16:00:00Z".parse()?;
        let sleep = json!({
            "interval":{"startTime":"2026-03-04T22:00:00Z","endTime":"2026-03-05T06:00:00Z",
                        "startUtcOffset":"32400s","endUtcOffset":"32400s"},
            "type":"STAGES",
            "stages":[
                {"startTime":"2026-03-04T22:00:00Z","endTime":"2026-03-05T00:00:00Z","type":"DEEP"},
                {"startTime":"2026-03-05T00:00:00Z","endTime":"2026-03-05T06:00:00Z","type":"LIGHT"}],
            "shortAwakenings":[
                {"startTime":"2026-03-04T23:10:00Z","endTime":"2026-03-04T23:11:30Z","type":"AWAKE"}],
            "outOfBedSegments":[{"startTime":"2026-03-05T06:00:00Z","endTime":"2026-03-05T06:07:00Z"}],
            "summary":{"minutesInSleepPeriod":"480","minutesAsleep":"455","minutesAwake":"25"}
        });
        let workout = json!({
            "interval":{"startTime":"2026-03-05T09:00:00Z","endTime":"2026-03-05T09:40:00Z",
                        "startUtcOffset":"32400s","endUtcOffset":"32400s"},
            "exerciseType":"RUNNING","displayName":"Run","activeDuration":"2075.25s",
            "metricsSummary":{"distanceMillimeters":5025500,"caloriesKcal":345.5,
                "averageHeartRateBeatsPerMinute":"143","steps":"6750","elevationGainMillimeters":42000,
                "averageSpeedMillimetersPerSecond":2500,"activeZoneMinutes":"35"},
            "splitSummaries":[{"startTime":"2026-03-05T09:00:00Z","endTime":"2026-03-05T09:08:00Z",
                "metricsSummary":{"distanceMillimeters":1000000}}],
            "exerciseMetadata":{"hasGps":true},
            "updateTime":"2026-03-05T09:50:00Z"
        });
        let exercise_requests = Arc::new(AtomicUsize::new(0));
        let upstream = Router::new()
            .route(
                "/v4/users/me/dataTypes/sleep/dataPoints:reconcile",
                get({
                    let sleep = sleep.clone();
                    move |headers: HeaderMap, Query(query): Query<HashMap<String, String>>| {
                        let sleep = sleep.clone();
                        async move {
                            assert_eq!(headers["authorization"], "Bearer google-session-synthetic");
                            assert_eq!(
                                query["dataSourceFamily"],
                                "users/me/dataSourceFamilies/google-wearables"
                            );
                            assert!(query["filter"].contains("sleep.interval.end_time"));
                            if query.get("pageToken").is_none_or(|token| token != "second") {
                                return Json(json!({"nextPageToken":"second","dataPoints":[]}));
                            }
                            Json(json!({"dataPoints":[{
                                "dataPointName":"users/123456/dataTypes/sleep/dataPoints/overnight-sleep-1",
                                "sleep":sleep
                            }]}))
                        }
                    }
                }),
            )
            .route(
                "/v4/users/123456/dataTypes/sleep/dataPoints/overnight-sleep-1",
                get({
                    let first_request = Arc::new(AtomicBool::new(true));
                    move |headers: HeaderMap| {
                        let first_request = first_request.clone();
                        async move {
                            assert_eq!(headers["authorization"], "Bearer google-session-synthetic");
                            if first_request.swap(false, Ordering::SeqCst) {
                                return (axum::http::StatusCode::TOO_MANY_REQUESTS, [("retry-after", "600")])
                                    .into_response();
                            }
                            Json(json!({
                                "name":"users/123456/dataTypes/sleep/dataPoints/overnight-sleep-1",
                                "dataSource":{
                                    "device":{"displayName":"Pixel Watch","manufacturer":"Google","formFactor":"WATCH"},
                                    "recordingMethod":"PASSIVELY_MEASURED"
                                }
                            })).into_response()
                        }
                    }
                }),
            )
            .route(
                "/v4/users/me/dataTypes/steps/dataPoints",
                get(|| async { Json(json!({"dataPoints":[]})) }),
            )
            .route(
                "/v4/users/me/dataTypes/exercise/dataPoints:reconcile",
                get({
                    let workout = workout.clone();
                    let exercise_requests = exercise_requests.clone();
                    move |headers: HeaderMap, Query(query): Query<HashMap<String, String>>| {
                        let workout = workout.clone();
                        let exercise_requests = exercise_requests.clone();
                        async move {
                            exercise_requests.fetch_add(1, Ordering::SeqCst);
                            assert_eq!(headers["authorization"], "Bearer google-session-synthetic");
                            assert!(query["filter"].contains("exercise.interval.civil_start_time"));
                            Json(json!({"dataPoints":[{
                                "dataPointName":"users/123456/dataTypes/exercise/dataPoints/run-1",
                                "exercise":workout
                            }]}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let upstream_addr = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, upstream).await });
        let client = http_client()?;
        let origin = format!("http://{upstream_addr}");
        let fetch = || {
            google_records(
                &client,
                "google-session-synthetic",
                start,
                end,
                Some(concat!(
                    "https://www.googleapis.com/auth/googlehealth.sleep.readonly ",
                    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly"
                )),
                &origin,
            )
        };
        let attempt_at = Utc::now();
        let error = fetch()
            .await
            .expect_err("source lookup 429 must defer the snapshot");
        let throttle = error
            .downcast_ref::<ProviderThrottled>()
            .context("expected a connection throttle")?;
        assert!(throttle.retry_at - attempt_at >= CalendarDuration::seconds(600));
        assert_eq!(exercise_requests.load(Ordering::SeqCst), 0);
        let fetched = fetch().await;
        server.abort();
        let (records, grants) = fetched?;
        assert_eq!(exercise_requests.load(Ordering::SeqCst), 1);
        assert_eq!(grants, ["steps", "sleep", "workout"]);
        assert_eq!(records.len(), 2);

        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "google-sleep-details-fixture";
        let token = "google-sleep-details-fixture-read-token-0123456789";
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
        let lease = wearables::issue_lease_for(&pool, subject, "google_health").await?;
        wearables::store_snapshot(
            &pool,
            subject,
            "google_health",
            serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":end,"grants":grants,"records":records
            }))?,
            false,
        )
        .await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        let app = Arc::new(crate::mcp::App {
            db: pool,
            read_token: String::new(),
            control_token: String::new(),
            rules: crate::acute::rules()?,
        });
        let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
        for (kind, id, raw) in [
            ("sleep", "overnight-sleep-1", sleep),
            ("workout", "run-1", workout),
        ] {
            let response = client.post(format!("http://{local_addr}/mcp/wearable-read"))
                .bearer_auth(token)
                .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                    "name":"wearable_sessions","arguments":{"provider":"google_health","kind":kind,"start":start,"end":end}
                }}))
                .send().await?.error_for_status()?.json::<Value>().await?;
            let rows = &response["result"]["structuredContent"]["records"];
            assert_eq!(rows.as_array().map(Vec::len), Some(1), "{response}");
            assert_eq!(rows[0]["source_id"], id);
            assert_eq!(rows[0]["source"], "Google Health");
            let payload = &rows[0]["payload"];
            assert_eq!(payload["provider_data"], raw);
            if kind == "sleep" {
                assert_eq!(
                    payload["provider_source"]["device"]["displayName"],
                    "Pixel Watch"
                );
                assert_eq!(
                    payload["provider_source"]["recordingMethod"],
                    "PASSIVELY_MEASURED"
                );
                assert_eq!(payload["stages"].as_array().map(Vec::len), Some(2));
                assert_eq!(payload["stages"][0]["stage"], 5);
            } else {
                assert!(payload.get("provider_source").is_none());
                assert_eq!(payload["distance_meters"], 5025.5);
                assert_eq!(payload["calories_kcal"], 345.5);
                assert_eq!(payload["average_heart_rate_bpm"], 143);
                assert_eq!(payload["workout_steps"], 6750);
                assert_eq!(payload["active_duration_seconds"], 2075.25);
                assert_eq!(payload["average_speed_meters_per_second"], json!(2.5));
                assert_eq!(payload["elevation_gain_meters"], json!(42.0));
                assert_eq!(payload["active_zone_minutes"], 35);
            }
        }
        server.abort();
        Ok(())
    }

    #[test]
    fn projects_official_provider_shapes() -> Result<()> {
        let window = Window {
            start: "2026-03-04T00:00:00Z".parse()?,
            end: "2026-03-05T00:00:00Z".parse()?,
        };
        let google = google_point(&json!({
            "dataSource":{"platform":"FITBIT","device":{"displayName":"Charge 6"}},
            "steps":{"interval":{"startTime":"2026-03-04T07:05:00Z","endTime":"2026-03-04T07:06:00Z"},"count":"84"}
        }),"steps","steps",&window)?.unwrap();
        assert_eq!(google["source"], "Charge 6");
        assert_eq!(google["payload"]["count"], 84);
        let heart = google_point(&json!({"heartRate":{"sampleTime":{"physicalTime":"2026-03-04T07:05:00Z"},"beatsPerMinute":"95"}}),
            "heartRate","heart_rate",&window)?.unwrap();
        assert_eq!(heart["payload"]["samples"][0]["bpm"], 95);
        let sleep = google_point(&json!({"sleep":{"interval":{"startTime":"2026-03-04T04:00:00Z","endTime":"2026-03-04T06:00:00Z"},
            "stages":[{"startTime":"2026-03-04T04:00:00Z","endTime":"2026-03-04T06:00:00Z","type":"DEEP"}]}}),
            "sleep","sleep",&window)?.unwrap();
        assert_eq!(sleep["payload"]["stages"][0]["stage"], 5);
        let workout = google_point(&json!({"exercise":{"interval":{"startTime":"2026-03-04T06:00:00Z","endTime":"2026-03-04T07:00:00Z"},
            "exerciseType":"RUNNING"}}),"exercise","workout",&window)?.unwrap();
        assert_eq!(workout["payload"]["exercise_type"], "RUNNING");
        Ok(())
    }
}
