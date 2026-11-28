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

pub(super) async fn fetch_from(
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
            output.len() <= MAX_RECORDS,
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
            output.len() <= MAX_RECORDS,
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
