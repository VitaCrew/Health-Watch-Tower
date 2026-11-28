use super::*;
use chrono::NaiveDate;

const ACTIVITY: &str = "activity_and_fitness";
const HEALTH: &str = "health_metrics_and_measurements";

#[derive(Clone, Copy)]
enum Time {
    Interval,
    Sample,
    BetaSample,
    Day,
    Session,
    Ecg,
    DailyRollup,
    HourlyRollup,
}

type Metric = (&'static str, &'static str, Time, &'static str);

const TYPES: &[Metric] = &[
    ("distance", "distance", Time::Interval, ACTIVITY),
    ("altitude", "altitude", Time::Interval, ACTIVITY),
    ("floors", "floors", Time::Interval, ACTIVITY),
    ("active-minutes", "activeMinutes", Time::Interval, ACTIVITY),
    (
        "active-zone-minutes",
        "activeZoneMinutes",
        Time::Interval,
        ACTIVITY,
    ),
    ("activity-level", "activityLevel", Time::Interval, ACTIVITY),
    (
        "sedentary-period",
        "sedentaryPeriod",
        Time::Interval,
        ACTIVITY,
    ),
    (
        "swim-lengths-data",
        "swimLengthsData",
        Time::Interval,
        ACTIVITY,
    ),
    (
        "time-in-heart-rate-zone",
        "timeInHeartRateZone",
        Time::Interval,
        ACTIVITY,
    ),
    (
        "calories-in-heart-rate-zone",
        "caloriesInHeartRateZone",
        Time::DailyRollup,
        ACTIVITY,
    ),
    ("run-vo2-max", "runVo2Max", Time::Sample, ACTIVITY),
    ("vo2-max", "vo2Max", Time::Sample, ACTIVITY),
    (
        "active-energy-burned",
        "activeEnergyBurned",
        Time::Interval,
        ACTIVITY,
    ),
    (
        "total-calories",
        "totalCalories",
        Time::DailyRollup,
        ACTIVITY,
    ),
    (
        "total-calories",
        "totalCalories",
        Time::HourlyRollup,
        ACTIVITY,
    ),
    ("daily-vo2-max", "dailyVo2Max", Time::Day, ACTIVITY),
    (
        "heart-rate-variability",
        "heartRateVariability",
        Time::Sample,
        HEALTH,
    ),
    (
        "daily-heart-rate-variability",
        "dailyHeartRateVariability",
        Time::Day,
        HEALTH,
    ),
    (
        "daily-resting-heart-rate",
        "dailyRestingHeartRate",
        Time::Day,
        HEALTH,
    ),
    (
        "oxygen-saturation",
        "oxygenSaturation",
        Time::Sample,
        HEALTH,
    ),
    (
        "daily-oxygen-saturation",
        "dailyOxygenSaturation",
        Time::Day,
        HEALTH,
    ),
    (
        "daily-respiratory-rate",
        "dailyRespiratoryRate",
        Time::Day,
        HEALTH,
    ),
    (
        "daily-sleep-temperature-derivations",
        "dailySleepTemperatureDerivations",
        Time::Day,
        HEALTH,
    ),
    ("body-fat", "bodyFat", Time::Sample, HEALTH),
    ("weight", "weight", Time::Sample, HEALTH),
    ("height", "height", Time::Sample, HEALTH),
    ("blood-glucose", "bloodGlucose", Time::Sample, HEALTH),
    (
        "core-body-temperature",
        "coreBodyTemperature",
        Time::Sample,
        HEALTH,
    ),
    (
        "daily-heart-rate-zones",
        "dailyHeartRateZones",
        Time::Day,
        HEALTH,
    ),
    (
        "respiratory-rate-sleep-summary",
        "respiratoryRateSleepSummary",
        Time::Sample,
        HEALTH,
    ),
    ("electrocardiogram", "electrocardiogram", Time::Ecg, "ecg"),
    (
        "irregular-rhythm-notification",
        "irregularRhythmNotification",
        Time::Session,
        "irn",
    ),
    ("hydration-log", "hydrationLog", Time::Session, "nutrition"),
    ("nutrition-log", "nutritionLog", Time::Session, "nutrition"),
];
const SKIN_SENSORS: Metric = (
    "skin-temperature-sensors",
    "skinTemperatureSensors",
    Time::BetaSample,
    HEALTH,
);

pub(super) fn granted(scope: Option<&str>, suffix: &str) -> bool {
    scope.map_or_else(
        || matches!(suffix, ACTIVITY | HEALTH | "sleep"),
        |scope| {
            scope.split_whitespace().any(|token| {
                token
                    .strip_prefix("https://www.googleapis.com/auth/googlehealth.")
                    .and_then(|scope| scope.strip_suffix(".readonly"))
                    == Some(suffix)
            })
        },
    )
}

fn metric_name(endpoint: &str, time: Time) -> String {
    match time {
        Time::HourlyRollup => format!("{}_hourly", endpoint.replace('-', "_")),
        Time::DailyRollup | Time::Session | Time::Ecg | Time::BetaSample => {
            endpoint.replace('-', "_")
        }
        _ => endpoint.to_owned(),
    }
}

pub(super) async fn fetch(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    scope: Option<&str>,
) -> Result<(Vec<Value>, Vec<String>)> {
    let beta = std::env::var("AVATAR_GOOGLE_HEALTH_SKIN_TEMPERATURE_BETA").as_deref() == Ok("true");
    fetch_from(
        client,
        token,
        start,
        end,
        scope,
        "https://health.googleapis.com",
        beta,
    )
    .await
}

async fn fetch_from(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    scope: Option<&str>,
    origin: &str,
    beta_skin_sensors: bool,
) -> Result<(Vec<Value>, Vec<String>)> {
    let mut output = Vec::new();
    let mut covered = Vec::new();
    let mut pending = TYPES
        .iter()
        .copied()
        .chain(beta_skin_sensors.then_some(SKIN_SENSORS))
        .filter(|metric| granted(scope, metric.3));
    let mut in_flight = tokio::task::JoinSet::new();
    loop {
        while in_flight.len() < 3 {
            let Some(metric) = pending.next() else { break };
            let (client, token, origin) = (client.clone(), token.to_owned(), origin.to_owned());
            in_flight.spawn(async move {
                let rows =
                    fetch_type(&client, &token, &origin, Window { start, end }, metric).await?;
                Ok::<_, anyhow::Error>((rows, metric_name(metric.0, metric.2)))
            });
        }
        let Some(result) = in_flight.join_next().await else {
            break;
        };
        let (rows, name) = result??;
        output.extend(rows);
        covered.push(name);
        ensure!(
            output.len() <= wearables::HIGH_VOLUME_CLOUD_RECORD_LIMIT,
            "Google Health metric snapshot limit exceeded"
        );
    }
    Ok((output, covered))
}

async fn fetch_type(
    client: &Client,
    token: &str,
    origin: &str,
    window: Window,
    (endpoint, field, time, _): Metric,
) -> Result<Vec<Value>> {
    if matches!(time, Time::DailyRollup | Time::HourlyRollup) {
        return fetch_rollup(client, token, origin, window, endpoint, field, time).await;
    }
    let mut output = Vec::new();
    let Window { start, end } = window;
    let method = if endpoint == "floors" {
        ":reconcile"
    } else {
        ""
    };
    let version = if matches!(time, Time::BetaSample) {
        "v4beta"
    } else {
        "v4"
    };
    let url = format!("{origin}/{version}/users/me/dataTypes/{endpoint}/dataPoints{method}");
    let source_family = if matches!(
        endpoint,
        "height" | "blood-glucose" | "core-body-temperature" | "hydration-log" | "nutrition-log"
    ) {
        "users/me/dataSourceFamilies/google-sources"
    } else {
        "users/me/dataSourceFamilies/google-wearables"
    };
    let kind = endpoint.replace('-', "_");
    let (key, lower, upper) = match time {
        Time::Interval | Time::Ecg => ("interval.start_time", start.to_rfc3339(), end.to_rfc3339()),
        Time::Session => (
            "interval.civil_start_time",
            (start - CalendarDuration::days(1))
                .format("%Y-%m-%d")
                .to_string(),
            (end + CalendarDuration::days(1))
                .format("%Y-%m-%d")
                .to_string(),
        ),
        Time::Sample | Time::BetaSample => (
            "sample_time.physical_time",
            start.to_rfc3339(),
            end.to_rfc3339(),
        ),
        Time::Day => (
            "date",
            start.date_naive().to_string(),
            (end + CalendarDuration::days(1)).date_naive().to_string(),
        ),
        Time::DailyRollup | Time::HourlyRollup => unreachable!(),
    };
    let mut filter = format!("{kind}.{key} >= \"{lower}\"");
    // ECG only accepts a lower UTC bound. The exact upper bound is applied after paging.
    if !matches!(time, Time::Ecg) {
        filter.push_str(&format!(" AND {kind}.{key} < \"{upper}\""));
    }
    let page_size = if matches!(time, Time::Ecg | Time::Session) {
        "25"
    } else {
        "1000"
    };
    let mut cursor = String::new();
    let mut seen = HashSet::new();
    loop {
        let request = client.get(&url).bearer_auth(token).query(&[
            ("filter", filter.as_str()),
            ("pageSize", page_size),
            ("dataSourceFamily", source_family),
        ]);
        let request = if cursor.is_empty() {
            request
        } else {
            request.query(&[("pageToken", &cursor)])
        };
        let result = body(request.send().await?).await?;
        let next = google_page_token(&result)?;
        if let Some(raw) = result.get("dataPoints") {
            for item in records(raw)? {
                if let Some(row) = project(item, endpoint, field, time, &window)? {
                    output.push(row);
                }
            }
        }
        ensure!(
            output.len() <= wearables::HIGH_VOLUME_CLOUD_RECORD_LIMIT,
            "Google Health metric snapshot limit exceeded"
        );
        let Some(next) = next else {
            break;
        };
        ensure!(
            seen.len() < 32 && seen.insert(next.to_owned()),
            "incomplete Google Health metric pagination"
        );
        cursor = next.to_owned();
    }
    Ok(output)
}

async fn fetch_rollup(
    client: &Client,
    token: &str,
    origin: &str,
    window: Window,
    endpoint: &str,
    field: &str,
    time: Time,
) -> Result<Vec<Value>> {
    let daily = matches!(time, Time::DailyRollup);
    let metric = metric_name(endpoint, time);
    let (method, mut request) = if daily {
        let first = window.start.date_naive();
        let last = window
            .end
            .date_naive()
            .succ_opt()
            .context("invalid Google Health date")?;
        let civil = |day: NaiveDate| {
            use chrono::Datelike;
            json!({"date":{"year":day.year(),"month":day.month(),"day":day.day()},"time":{}})
        };
        (
            "dailyRollUp",
            json!({"range":{"start":civil(first),"end":civil(last)},
            "windowSizeDays":1,"pageSize":14}),
        )
    } else {
        // Align hourly buckets to UTC so retries of overlapping jobs have stable IDs.
        let floor = |at: DateTime<Utc>| {
            DateTime::<Utc>::from_timestamp(at.timestamp().div_euclid(3600) * 3600, 0)
                .context("invalid Google Health rollup time")
        };
        let first = floor(window.start)?;
        let first = if first < window.start {
            first + CalendarDuration::hours(1)
        } else {
            first
        };
        let last = floor(window.end)?;
        if first >= last {
            return Ok(Vec::new());
        }
        (
            "rollUp",
            json!({"range":{"startTime":first,"endTime":last},"windowSize":"3600s","pageSize":24}),
        )
    };
    request["dataSourceFamily"] = json!("users/me/dataSourceFamilies/google-wearables");
    let url = format!("{origin}/v4/users/me/dataTypes/{endpoint}/dataPoints:{method}");
    let revision = Utc::now();
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    loop {
        let response = body(
            client
                .post(&url)
                .bearer_auth(token)
                .json(&request)
                .send()
                .await?,
        )
        .await?;
        let next = google_page_token(&response)?;
        if let Some(raw) = response.get("rollupDataPoints") {
            for row in records(raw)? {
                ensure!(row.is_object(), "invalid Google Health rollup bucket");
                let Some(value) = row.get(field) else {
                    continue;
                };
                ensure!(value.is_object(), "invalid Google Health rollup value");
                let (start, end, key, detail, precision) = if daily {
                    let day = &row["civilStartTime"]["date"];
                    let at = civil_date(day)?;
                    (
                        at,
                        at,
                        at.date_naive().to_string(),
                        json!({"date":day,"rollup":value}),
                        "provider_calendar_date",
                    )
                } else {
                    let start: DateTime<Utc> = row["startTime"]
                        .as_str()
                        .context("missing rollup start")?
                        .parse()?;
                    let end: DateTime<Utc> = row["endTime"]
                        .as_str()
                        .context("missing rollup end")?
                        .parse()?;
                    if end - start != CalendarDuration::hours(1)
                        || start.timestamp().rem_euclid(3600) != 0
                        || start.timestamp_subsec_nanos() != 0
                    {
                        continue;
                    }
                    (
                        start,
                        end,
                        start.to_rfc3339(),
                        json!({"rollup":value}),
                        "provider_hour",
                    )
                };
                if window.contains(start, end) {
                    output.push(record(
                        "metric",
                        "Google Health",
                        &format!("{metric}:{key}"),
                        start,
                        end,
                        revision,
                        json!({"metric":metric,"time_precision":precision,
                            "provider_reconciled":true,"provider_data":detail}),
                    ));
                }
            }
        }
        let Some(next) = next else {
            break;
        };
        ensure!(
            seen.len() < 14 && seen.insert(next.to_owned()),
            "incomplete Google Health rollup pagination"
        );
        request["pageToken"] = json!(next);
    }
    Ok(output)
}

fn civil_date(day: &Value) -> Result<DateTime<Utc>> {
    Ok(NaiveDate::from_ymd_opt(
        integer(&day["year"])?.try_into()?,
        integer(&day["month"])?.try_into()?,
        integer(&day["day"])?.try_into()?,
    )
    .context("invalid Google Health date")?
    .and_hms_opt(0, 0, 0)
    .unwrap()
    .and_utc())
}

fn project(
    root: &Value,
    metric: &str,
    field: &str,
    time: Time,
    window: &Window,
) -> Result<Option<Value>> {
    let point = &root[field];
    ensure!(point.is_object(), "Google Health metric payload missing");
    let (start, end, precision) = match time {
        Time::Interval | Time::Session | Time::Ecg => (
            date(&point["interval"]["startTime"])?,
            date(&point["interval"]["endTime"])?,
            if matches!(time, Time::Interval) {
                "interval"
            } else {
                "session"
            },
        ),
        Time::Sample | Time::BetaSample => {
            let at = date(&point["sampleTime"]["physicalTime"])?;
            (at, at, "sample")
        }
        Time::Day => {
            let at = civil_date(&point["date"])?;
            (at, at, "provider_calendar_date")
        }
        Time::DailyRollup | Time::HourlyRollup => unreachable!(),
    };
    if !window.contains(start, end) {
        return Ok(None);
    }
    // Civil-date records use a stable UTC index anchor, distinct from a sampled instant.
    let source = root["dataSource"]["device"]["displayName"]
        .as_str()
        .unwrap_or("Google Health");
    let key = root["name"]
        .as_str()
        .filter(|name| !name.is_empty())
        .or_else(|| root["dataPointName"].as_str())
        .and_then(|name| name.rsplit('/').next())
        .filter(|id| !id.is_empty())
        .or_else(|| point["id"].as_str().filter(|id| !id.is_empty()))
        .map(str::to_owned)
        .unwrap_or_else(|| {
            let mut key = format!("{start}/{end}");
            if let Some(sensor) = point["metadata"]["sensorType"]
                .as_str()
                .filter(|_| matches!(time, Time::BetaSample))
            {
                key.push(':');
                key.push_str(sensor);
            }
            key
        });
    let metric_name = metric_name(metric, time);
    Ok(Some(record(
        "metric",
        source,
        &format!("{metric}:{key}"),
        start,
        end,
        Utc::now(),
        json!({"metric":metric_name,"time_precision":precision,"provider_data":point,
            "provider_reconciled":metric == "floors"}),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{Path, Query},
        http::HeaderMap,
        routing::{get, post},
        Json, Router,
    };
    use std::collections::HashMap;

    async fn fixture(
        Path(endpoint): Path<String>,
        Query(query): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> Json<Value> {
        assert_eq!(headers["authorization"], "Bearer fixture-token");
        assert_eq!(
            query["dataSourceFamily"],
            "users/me/dataSourceFamilies/google-wearables"
        );
        if endpoint == "altitude" {
            assert!(query["filter"].starts_with("altitude.interval.start_time >="));
            let start: DateTime<Utc> = "2026-04-25T00:00:00Z".parse().unwrap();
            let offset: usize = query
                .get("pageToken")
                .map(|t| t.parse().unwrap())
                .unwrap_or(0);
            let limit = (offset + 1000).min(4101);
            let points: Vec<_> = (offset..limit).map(|index| {
                let minute = start + CalendarDuration::minutes((index % 1440) as i64);
                json!({"name":format!("users/me/dataTypes/altitude/dataPoints/{index}"),
                  "dataSource":{"device":{"displayName":format!("Fixture Watch {}",index/1440)}},
                  "altitude":{"interval":{"startTime":minute,"endTime":minute+CalendarDuration::minutes(1)},
                    "gainMillimeters":"1200"}})
            }).collect();
            return Json(
                json!({"dataPoints":points,"nextPageToken":(limit<4101).then(||limit.to_string())}),
            );
        }
        assert!(TYPES
            .iter()
            .any(|metric| metric.0 == endpoint && metric.3 == ACTIVITY));
        Json(json!({"dataPoints":[]}))
    }

    #[test]
    fn projects_google_glucose_ids_and_day_scoped_zones() -> Result<()> {
        let at: DateTime<Utc> = "2026-04-25T00:00:00Z".parse()?;
        let window = Window {
            start: at,
            end: at + CalendarDuration::days(1),
        };
        let glucose = project(&json!({"bloodGlucose":{
            "id":"reading-7","sampleTime":{"physicalTime":at},"bloodGlucoseMilligramsPerDeciliter":104
        }}), "blood-glucose", "bloodGlucose", Time::Sample, &window)?.unwrap();
        assert_eq!(glucose["source_id"], "blood-glucose:reading-7");
        assert_eq!(glucose["payload"]["time_precision"], "sample");
        let zones = project(&json!({"dailyHeartRateZones":{"date":{"year":2026,"month":4,"day":25},"heartRateZones":[]}}),
            "daily-heart-rate-zones","dailyHeartRateZones",Time::Day,&window)?.unwrap();
        assert_eq!(zones["payload"]["time_precision"], "provider_calendar_date");
        Ok(())
    }

    #[tokio::test]
    async fn google_beta_skin_sensor_pair_keeps_distinct_records_without_provider_ids() -> Result<()>
    {
        let start: DateTime<Utc> = "2026-07-20T00:00:00Z".parse()?;
        let end = start + CalendarDuration::minutes(2);
        let app = Router::new().route("/v4beta/users/me/dataTypes/skin-temperature-sensors/dataPoints",
            get(move |Query(query): Query<HashMap<String,String>>| async move {
                assert!(query["filter"].starts_with("skin_temperature_sensors.sample_time.physical_time >="));
                let rows = [("SKIN_TEMPERATURE_SENSOR", 34.95), ("INTERNAL_DEVICE_TEMPERATURE_SENSOR", 31.8)]
                    .map(|(sensor, temperature)| json!({"skinTemperatureSensors":{
                        "sampleTime":{"physicalTime":start},"temperatureCelsius":temperature,"metadata":{"sensorType":sensor}
                    }}));
                Json(json!({"dataPoints":rows}))
            }));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let rows = fetch_type(
            &http_client()?,
            "fixture",
            &origin,
            Window { start, end },
            SKIN_SENSORS,
        )
        .await;
        server.abort();
        let rows = rows?;
        assert_eq!(rows.len(), 2);
        assert_ne!(rows[0]["source_id"], rows[1]["source_id"]);
        assert_eq!(
            rows[0]["payload"]["provider_data"]["temperatureCelsius"],
            34.95
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn consented_google_sessions_and_daily_zone_calories_reach_postgres_and_mcp() -> Result<()>
    {
        let start: DateTime<Utc> = "2026-04-25T00:00:00Z".parse()?;
        let end = start + CalendarDuration::days(1);
        let interval = json!({"startTime":start,"endTime":start+CalendarDuration::seconds(30)});
        let air = Router::new()
            .route("/v4/users/me/dataTypes/{endpoint}/dataPoints", get(move |Path(endpoint): Path<String>, Query(query): Query<HashMap<String,String>>, headers: HeaderMap| {
                let interval = interval.clone();
                async move {
                    assert_eq!(headers["authorization"], "Bearer fixture-google");
                    assert_eq!(query["pageSize"], "25");
                    let day_filter = endpoint.replace('-', "_") + ".interval.civil_start_time";
                    let (field, mut point) = match endpoint.as_str() {
                        "electrocardiogram" => {
                            assert_eq!(query["filter"], "electrocardiogram.interval.start_time >= \"2026-04-25T00:00:00+00:00\"");
                            ("electrocardiogram", json!({"waveformSamples":[-4,12,-7],"samplingFrequencyHertz":250,"millivoltsScalingFactor":1000}))
                        }
                        "irregular-rhythm-notification" => ("irregularRhythmNotification", json!({"alertWindows":[{"positive":true}]})),
                        "hydration-log" => ("hydrationLog", json!({"amountConsumed":{"milliliters":250}})),
                        "nutrition-log" => ("nutritionLog", json!({"foodDisplayName":"Fixture rice","mealType":"LUNCH"})),
                        _ => panic!("request without scope: {endpoint}"),
                    };
                    if endpoint != "electrocardiogram" {
                        assert_eq!(query["filter"], format!("{day_filter} >= \"2026-04-24\" AND {day_filter} < \"2026-04-27\""));
                    }
                    let source_family = if matches!(endpoint.as_str(), "hydration-log" | "nutrition-log") { "google-sources" } else { "google-wearables" };
                    assert_eq!(query["dataSourceFamily"], format!("users/me/dataSourceFamilies/{source_family}"));
                    point["interval"] = interval;
                    let mut row = json!({"name":format!("users/me/dataTypes/{endpoint}/dataPoints/first")});
                    row[field] = point;
                    Json(if endpoint == "electrocardiogram" && !query.contains_key("pageToken") {
                        json!({"dataPoints":[row],"nextPageToken":"next"})
                    } else if endpoint == "electrocardiogram" {
                        row["name"] = json!("users/me/dataTypes/electrocardiogram/dataPoints/outside");
                        row[field]["interval"] = json!({"startTime":end,"endTime":end+CalendarDuration::seconds(30)});
                        json!({"dataPoints":[row]})
                    } else {
                        json!({"dataPoints":[row]})
                    })
                }
            }))
            .route("/v4/users/me/dataTypes/calories-in-heart-rate-zone/dataPoints:dailyRollUp", post(
                |Json(request): Json<Value>| async move {
                    assert_eq!(request["range"]["start"]["date"], json!({"year":2026,"month":4,"day":25}));
                    assert_eq!(request["windowSizeDays"], 1);
                    Json(json!({"rollupDataPoints":[{"civilStartTime":{"date":{"year":2026,"month":4,"day":25}},
                        "caloriesInHeartRateZone":{"caloriesInHeartRateZones":[{"heartRateZone":"CARDIO","kcal":32}]}}]}))
                }));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, air).await });
        let client = http_client()?;
        let scopes = ["ecg", "irn", "nutrition"]
            .map(|scope| format!("https://www.googleapis.com/auth/googlehealth.{scope}.readonly"))
            .join(" ");
        assert!(!granted(None, "ecg"));
        assert!(!granted(
            Some("https://www.googleapis.com/auth/googlehealth.ecg.writeonly"),
            "ecg"
        ));
        let (mut rows, coverage) = fetch_from(
            &client,
            "fixture-google",
            start,
            end,
            Some(&scopes),
            &origin,
            false,
        )
        .await?;
        assert!(coverage.iter().any(|metric| metric == "electrocardiogram"));
        assert!(!coverage.iter().any(|metric| metric == "weight"));
        rows.extend(
            fetch_rollup(
                &client,
                "fixture-google",
                &origin,
                Window { start, end },
                "calories-in-heart-rate-zone",
                "caloriesInHeartRateZone",
                Time::DailyRollup,
            )
            .await?,
        );
        server.abort();
        assert_eq!(rows.len(), 5);

        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "fixture-google-optional-sessions";
        let reader = "fixture-google-optional-session-reader-key-0001";
        let db = pool.get().await?;
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES ($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES
            ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
            &[&subject, &reader],
        )
        .await?;
        drop(db);
        let lease = wearables::issue_lease_for(&pool, subject, "google_health").await?;
        wearables::store_snapshot(
            &pool,
            subject,
            "google_health",
            serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":end,"grants":["metric"],"records":rows
            }))?,
            false,
        )
        .await?;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let url = format!("http://{}/mcp/wearable-read", listener.local_addr()?);
        let app = std::sync::Arc::new(crate::mcp::App {
            db: pool,
            read_token: String::new(),
            control_token: String::new(),
            rules: crate::acute::rules()?,
        });
        let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
        for (metric, key, expected) in [
            ("electrocardiogram", "waveformSamples", json!([-4, 12, -7])),
            (
                "irregular_rhythm_notification",
                "alertWindows",
                json!([{"positive":true}]),
            ),
            (
                "hydration_log",
                "amountConsumed",
                json!({"milliliters":250}),
            ),
            ("nutrition_log", "foodDisplayName", json!("Fixture rice")),
            (
                "calories_in_heart_rate_zone",
                "rollup",
                json!({"caloriesInHeartRateZones":[{"heartRateZone":"CARDIO","kcal":32}]}),
            ),
        ] {
            let response = client.post(&url).bearer_auth(reader).json(&json!({
                "jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wearable_samples",
                "arguments":{"provider":"google_health","kind":"metric","start":start,"end":end,"metric":metric}}
            })).send().await?.error_for_status()?.json::<Value>().await?;
            let records = &response["result"]["structuredContent"]["records"];
            assert_eq!(records.as_array().map(Vec::len), Some(1), "{metric}");
            assert_eq!(records[0]["payload"]["provider_data"][key], expected);
        }
        server.abort();
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn scoped_google_metric_pages_over_mobile_limit_commit_in_one_cloud_snapshot(
    ) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let app = Router::new().route("/v4/users/me/dataTypes/{metric}/dataPoints", get(fixture))
            .route("/v4/users/me/dataTypes/calories-in-heart-rate-zone/dataPoints:dailyRollUp",
                post(|| async { Json(json!({"rollupDataPoints":[]})) }))
            .route("/v4/users/me/dataTypes/total-calories/dataPoints:dailyRollUp", post(
                |Json(request): Json<Value>| async move {
                    assert_eq!(request["windowSizeDays"], 1);
                    assert_eq!(request["range"]["start"]["date"], json!({"year":2026,"month":4,"day":25}));
                    Json(json!({"rollupDataPoints":[
                        {"civilStartTime":{"date":{"year":2026,"month":4,"day":25}},"totalCalories":{"kcalSum":1734.5}},
                        {"civilStartTime":{"date":{"year":2026,"month":4,"day":26}},"totalCalories":{"kcalSum":17}}
                    ]}))
                }))
            .route("/v4/users/me/dataTypes/total-calories/dataPoints:rollUp", post(
                |Json(request): Json<Value>| async move {
                    assert_eq!(request["windowSize"], "3600s");
                    assert_eq!(request["range"], json!({"startTime":"2026-04-25T00:00:00Z","endTime":"2026-04-26T00:00:00Z"}));
                    if request["pageToken"] == "second-hour" {
                        return Json(json!({"rollupDataPoints":[{"startTime":"2026-04-25T01:00:00Z",
                            "endTime":"2026-04-25T02:00:00Z","totalCalories":{"kcalSum":72.5}}]}));
                    }
                    Json(json!({"nextPageToken":"second-hour","rollupDataPoints":[
                        {"startTime":"2026-04-25T00:00:00Z","endTime":"2026-04-25T01:00:00Z","totalCalories":{"kcalSum":0}}
                    ]}))
                }))
            .route("/v4/users/me/dataTypes/floors/dataPoints:reconcile", get(
                |Query(query): Query<HashMap<String,String>>| async move {
                    assert!(query["filter"].starts_with("floors.interval.start_time >="));
                    assert_eq!(query["dataSourceFamily"], "users/me/dataSourceFamilies/google-wearables");
                    Json(json!({"dataPoints":[{"dataPointName":"users/me/dataTypes/floors/dataPoints/floor1",
                        "floors":{"interval":{"startTime":"2026-04-25T08:00:00Z",
                            "endTime":"2026-04-25T08:01:00Z"},"count":"3"}}]}))
                }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let start: DateTime<Utc> = "2026-04-25T00:00:00Z".parse()?;
        let end = start + CalendarDuration::days(1);
        let scope = format!("https://www.googleapis.com/auth/googlehealth.{ACTIVITY}.readonly");
        let rows = fetch_from(
            &http_client()?,
            "fixture-token",
            start,
            end,
            Some(&scope),
            &format!("http://{address}"),
            false,
        )
        .await;
        server.abort();
        let (rows, coverage) = rows?;
        assert!(coverage.iter().any(|metric| metric == "distance"));
        assert!(coverage
            .iter()
            .any(|metric| metric == "total_calories_hourly"));
        assert!(!coverage.iter().any(|metric| metric == "weight"));
        assert_eq!(rows.len(), 4105);
        let floors = rows
            .iter()
            .find(|row| row["payload"]["metric"] == "floors")
            .unwrap();
        assert_eq!(floors["source_id"], "floors:floor1");
        assert_eq!(floors["payload"]["provider_reconciled"], true);
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "fixture-google-expanded";
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
                &[&subject],
            )
            .await?;
        let lease = wearables::issue_lease_for(&pool, subject, "google_health").await?;
        let snapshot =
            serde_json::from_value(json!({"lease":lease["lease"], "start":start, "end":end,
            "grants":["metric"], "records":rows}))?;
        assert!(wearables::validate_snapshot(&snapshot).is_err());
        let result =
            wearables::store_snapshot(&pool, subject, "google_health", snapshot, false).await?;
        assert_eq!(result["accepted"], 4105);
        let query = wearables::RecordQuery {
            kind: "metric".into(),
            start,
            end,
            offset: 0,
            source: None,
            provider: Some("google_health".into()),
            metric: Some("altitude".into()),
        };
        let result = wearables::records_for(&pool, subject, query, false).await?;
        assert_eq!(
            result["records"][0]["payload"]["provider_data"]["gainMillimeters"],
            "1200"
        );
        let hours = wearables::records_for(
            &pool,
            subject,
            wearables::RecordQuery {
                kind: "metric".into(),
                start,
                end,
                offset: 0,
                source: None,
                provider: Some("google_health".into()),
                metric: Some("total_calories_hourly".into()),
            },
            false,
        )
        .await?;
        let kcal = hours["records"]
            .as_array()
            .context("missing calorie records")?
            .iter()
            .map(|row| &row["payload"]["provider_data"]["rollup"]["kcalSum"])
            .collect::<Vec<_>>();
        assert_eq!(kcal, vec![&json!(0), &json!(72.5)]);
        let daily = wearables::daily_for(
            &pool,
            subject,
            wearables::DailyQuery {
                start_day: start.date_naive(),
                end_day: start.date_naive(),
                source: None,
                provider: Some("google_health".into()),
                kind: Some("activity".into()),
            },
        )
        .await?;
        assert_eq!(daily["records"][0]["day"], "2026-04-25");
        assert_eq!(daily["records"][0]["stats"]["total_energy_kcal"], 1734.5);
        Ok(())
    }
}
