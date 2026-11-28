use super::*;
use chrono::{NaiveDate, NaiveDateTime};
use reqwest::RequestBuilder;

async fn optional(request: RequestBuilder) -> Result<Option<Value>> {
    let response = request.send().await.map_err(|error| error.without_url())?;
    if matches!(
        response.status(),
        reqwest::StatusCode::NO_CONTENT
            | reqwest::StatusCode::NOT_FOUND
            | reqwest::StatusCode::FORBIDDEN
    ) {
        return Ok(None);
    }
    super::body(response).await.map(Some)
}

fn point(value: &Value) -> Option<(DateTime<Utc>, &'static str)> {
    if let Some(number) = value.as_i64() {
        let time = if number > 10_000_000_000 {
            DateTime::from_timestamp_millis(number)
        } else {
            DateTime::from_timestamp(number, 0)
        }?;
        return Some((time, "instant"));
    }
    let text = value.as_str()?;
    if let Ok(at) = text.parse::<DateTime<Utc>>() {
        return Some((at, "instant"));
    }
    if let Ok(day) = text.parse::<NaiveDate>() {
        return Some((
            day.and_hms_opt(0, 0, 0)?.and_utc(),
            "provider_calendar_date",
        ));
    }
    let civil = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f").ok()?;
    Some((civil.and_utc(), "provider_civil_time"))
}

fn append(
    rows: &mut Vec<Value>,
    items: &Value,
    metric: &str,
    keys: &[&str],
    window: &Window,
) -> Result<()> {
    for item in super::records(items)? {
        let Some((at, precision)) = keys.iter().find_map(|key| point(&item[*key])) else {
            continue;
        };
        if !window.contains(at, at) {
            continue;
        }
        let id = item["id"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| item["id"].as_i64().map(|value| value.to_string()))
            .unwrap_or_else(|| at.to_rfc3339());
        rows.push(super::record(
            "metric",
            "Polar",
            &format!("{metric}:{id}"),
            at,
            at,
            window.end,
            json!({"metric":metric,"time_precision":precision,"provider_data":item}),
        ));
    }
    Ok(())
}

pub(super) async fn collect(
    client: &Client,
    token: &str,
    origin: &str,
    window: &Window,
) -> Result<Vec<Value>> {
    let start_day = window.start.date_naive();
    let end_day = window.end.date_naive();
    let earliest_activity = Utc::now().date_naive() - Duration::days(365);
    let to = end_day.to_string();
    let mut rows = Vec::new();
    for (path, name) in [
        ("continuous-heart-rate", "continuous_heart_rate"),
        ("biosensing/bodytemperature", "body_temperature"),
        ("biosensing/skintemperature", "sleep_skin_temperature"),
        ("biosensing/skincontacts", "skin_contact"),
        ("biosensing/ecg", "ecg"),
        ("biosensing/spo2", "spot_spo2"),
        ("activities", "daily_activity"),
        ("activities/samples", "activity_samples"),
        ("cardio-load/date", "cardio_load"),
        ("sleepwise/alertness/date", "sleepwise_alertness"),
        (
            "sleepwise/circadian-bedtime/date",
            "sleepwise_circadian_bedtime",
        ),
    ] {
        let from = if matches!(name, "daily_activity" | "activity_samples") {
            start_day.max(earliest_activity)
        } else {
            start_day
        };
        if from > end_day {
            continue;
        }
        let from = from.to_string();
        let mut request = client
            .get(format!("{origin}/users/{path}"))
            .bearer_auth(token)
            .query(&[("from", &from), ("to", &to)]);
        if name == "daily_activity" {
            request = request.query(&[("inactivity_stamps", true)]);
        }
        let Some(result) = optional(request).await? else {
            continue;
        };
        if path.starts_with("biosensing/") {
            polar_biosensing(&mut rows, &result, name, window)?;
            continue;
        }
        if name == "continuous_heart_rate" {
            let items = if result["date"].is_string() {
                std::slice::from_ref(&result)
            } else {
                super::records(&result["heart_rates"])?.as_slice()
            };
            for item in items {
                super::polar_series::continuous(&mut rows, item, window)?;
            }
            continue;
        }
        if matches!(name, "sleepwise_alertness" | "sleepwise_circadian_bedtime") {
            for item in super::records(&result)? {
                let (Some((start, _)), Some((end, _))) = (
                    point(&item["period_start_time"]),
                    point(&item["period_end_time"]),
                ) else {
                    continue;
                };
                if start < end && window.contains(start, end) {
                    rows.push(super::record(
                        "metric",
                        "Polar",
                        &format!("{name}:{start}"),
                        start,
                        end,
                        window.end,
                        json!({"metric":name,"time_precision":"instant","provider_data":item}),
                    ));
                }
            }
            continue;
        }
        append(
            &mut rows,
            &result,
            name,
            &[
                "measurement_time",
                "start_time",
                "sleep_date",
                "date",
                "test_time",
                "created",
            ],
            window,
        )?;
        if name == "activity_samples" {
            for item in super::records(&result)? {
                super::polar_series::activity_steps(&mut rows, item, window)?;
            }
        }
    }
    Ok(rows)
}

fn polar_biosensing(
    rows: &mut Vec<Value>,
    response: &Value,
    name: &str,
    window: &Window,
) -> Result<()> {
    for item in super::records(response)? {
        let (start, end, precision) = match name {
            "body_temperature" | "skin_contact" => {
                let Some((start, _)) = point(&item["start_time"]) else {
                    continue;
                };
                let Some((end, _)) = point(&item["end_time"]) else {
                    continue;
                };
                (start, end, "instant")
            }
            "sleep_skin_temperature" => {
                let Some((at, precision)) = point(&item["sleep_date"]) else {
                    continue;
                };
                (at, at, precision)
            }
            _ => {
                let Some(at) = item["test_time"]
                    .as_i64()
                    .and_then(|time| DateTime::from_timestamp(time, 0))
                else {
                    continue;
                };
                (at, at, "instant")
            }
        };
        if name == "body_temperature" {
            for sample in item["samples"].as_array().into_iter().flatten() {
                let Some(delta) = sample["recording_time_delta_milliseconds"]
                    .as_i64()
                    .filter(|delta| *delta >= 0)
                else {
                    continue;
                };
                let Some(at) = Duration::try_milliseconds(delta)
                    .and_then(|delta| start.checked_add_signed(delta))
                else {
                    continue;
                };
                if at > end
                    || !window.contains(at, at)
                    || !sample["temperature_celsius"].is_number()
                {
                    continue;
                }
                let id = polar_biosensing_id("body_temperature_sample", start, item);
                rows.push(super::record(
                    "metric", "Polar", &format!("{id}:{delta}"), at, at, window.end,
                    json!({"metric":"body_temperature_sample","time_precision":"instant","provider_data":{
                        "value":sample["temperature_celsius"],"unit":"Cel","measurement_type":item["measurement_type"],
                        "sensor_location":item["sensor_location"],"recording_time_delta_milliseconds":delta
                    }}),
                ));
            }
        }
        if window.contains(start, end) {
            let mut payload =
                json!({"metric":name,"time_precision":precision,"provider_data":item});
            if name == "ecg" {
                payload["signal_unit"] = json!("mV");
                payload["elapsed_time_unit"] = json!("ms");
            }
            rows.push(super::record(
                "metric",
                "Polar",
                &polar_biosensing_id(name, start, item),
                start,
                end,
                window.end,
                payload,
            ));
        }
    }
    Ok(())
}

fn polar_biosensing_id(name: &str, start: DateTime<Utc>, item: &Value) -> String {
    [
        name,
        &start.to_rfc3339(),
        item["source_device_id"].as_str().unwrap_or_default(),
        item["measurement_type"].as_str().unwrap_or_default(),
        item["sensor_location"].as_str().unwrap_or_default(),
    ]
    .join(":")
}
