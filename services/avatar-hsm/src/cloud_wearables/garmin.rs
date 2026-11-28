use super::*;
#[cfg(test)]
use chrono::NaiveDateTime;

#[cfg(test)]
#[path = "garmin_tests.rs"]
mod tests;

pub const BASE: &str = "https://personal.connectapi.garmin.com";

pub(crate) async fn access(client: &Client, key: &str) -> Result<String> {
    let mut auth = reqwest::header::HeaderValue::from_str(&format!("ApiKey {key}"))?;
    auth.set_sensitive(true);
    let response = client
        .post(format!(
            "{BASE}/personalapitoken-service/personal-api/personal-api-key/token"
        ))
        .header(reqwest::header::AUTHORIZATION, auth)
        .send()
        .await?;
    let token = check_provider_response(response)?.text().await?;
    let token = token.trim().to_owned();
    ensure!(
        !token.is_empty() && token.len() < 8192,
        "invalid Garmin token response"
    );
    Ok(token)
}

#[cfg(test)]
pub(super) async fn fetch(
    client: &Client,
    key: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    let token = access(client, key).await?;
    let window = Window { start, end };
    let first_day = (start - CalendarDuration::days(1)).date_naive();
    let last_day = (end + CalendarDuration::days(1)).date_naive();
    let from = first_day.to_string();
    let to = last_day.to_string();
    let get = |path: &str| client.get(format!("{BASE}/{path}")).bearer_auth(&token);
    let sleep = body(
        get("sleep-service/sleep/dailySleepSummariesByDate")
            .query(&[("startDate", &from), ("endDate", &to)])
            .send()
            .await?,
    )
    .await?;
    let mut rows = records(&sleep)?
        .iter()
        .map(|row| sleep_point(row, &window))
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>>>()?;
    rows.extend(super::vendor_metrics::garmin(client, &token, start, end).await?);
    rows.extend(cycle_days_at(client, BASE, &token, &window).await?);
    let daily_steps = body(
        get(&format!(
            "usersummary-service/stats/steps/daily/{from}/{to}"
        ))
        .send()
        .await?,
    )
    .await?;
    for item in records(&daily_steps)? {
        let day: chrono::NaiveDate = required_text(&item["calendarDate"])?.parse()?;
        let at = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
        if window.contains(at, at) {
            rows.push(record("metric","Garmin Connect",&format!("daily-steps:{day}"),at,at,end,
                json!({"metric":"daily_steps","time_precision":"provider_calendar_date","provider_data":item})));
        }
    }
    for page in 0..32 {
        let response = body(
            get("activitylist-service/activities/search/activities")
                .query(&[
                    ("startDate", &from),
                    ("endDate", &to),
                    ("start", &(page * 100).to_string()),
                    ("limit", &"100".to_owned()),
                ])
                .send()
                .await?,
        )
        .await?;
        let activities = records(&response)?;
        rows.extend(
            activities
                .iter()
                .map(|row| workout_point(row, &window))
                .filter_map(Result::transpose)
                .collect::<Result<Vec<_>>>()?,
        );
        if activities.len() < 100 {
            break;
        }
        ensure!(page < 31, "incomplete Garmin activity pagination");
    }

    let mut day = first_day;
    let mut hours = std::collections::BTreeMap::new();
    while day <= last_day {
        let response = get("wellness-service/wellness/dailyHeartRate")
            .query(&[("date", day.to_string())])
            .send()
            .await?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            heart_points(&body(response).await?, &window, &mut hours)?;
        }
        day = day.succ_opt().context("invalid Garmin date")?;
    }
    for (hour, points) in hours {
        let at = *points.keys().next().context("empty Garmin hour")?;
        let until = *points.keys().next_back().context("empty Garmin hour")?;
        let samples = points
            .into_iter()
            .map(|(at, bpm)| json!({"at":at,"bpm":bpm}))
            .collect::<Vec<_>>();
        ensure!(samples.len() <= 500, "Garmin hour exceeds sample limit");
        rows.push(record(
            "heart_rate",
            "Garmin Connect",
            &format!("hour:{hour}"),
            at,
            until,
            end,
            json!({"unit":"bpm","samples":samples}),
        ));
    }
    Ok(rows)
}

#[cfg(test)]
async fn cycle_days_at(
    client: &Client,
    origin: &str,
    token: &str,
    window: &Window,
) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    for day in window
        .start
        .date_naive()
        .iter_days()
        .take_while(|day| *day <= window.end.date_naive())
    {
        let at = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
        if !window.contains(at, at) {
            continue;
        }
        // Leave the optional todayDate to Garmin so predictions use its calendar context.
        let Some(view) = super::vendor_metrics::optional(
            client
                .get(format!(
                    "{origin}/periodichealth-service/menstrualcycle/dayview/{day}"
                ))
                .bearer_auth(token)
                .query(&[("includeAnalysis", true)]),
        )
        .await?
        else {
            continue;
        };
        if view.is_null() {
            continue;
        }
        ensure!(view.is_object(), "invalid Garmin cycle day response");
        if let Some(reported) = view["dayLog"]["calendarDate"]["localDate"].as_str() {
            ensure!(
                reported.parse::<chrono::NaiveDate>()? == day,
                "Garmin cycle log date differs from request"
            );
        }
        for (field, metric) in [
            ("dayLog", "menstrual_cycle_day"),
            ("daySummary", "menstrual_cycle"),
            ("weightGoals", "cycle_pregnancy_weight_goals"),
        ] {
            match view.get(field) {
                None | Some(Value::Null) => {}
                Some(Value::Object(data)) if data.is_empty() => {}
                Some(data @ Value::Object(_)) => rows.push(record(
                    "metric",
                    "Garmin Connect",
                    &format!("{metric}:{day}"),
                    at,
                    at,
                    window.end,
                    json!({"metric":metric,"time_precision":"provider_calendar_date",
                           "local_day":day,"provider_data":data}),
                )),
                _ => anyhow::bail!("invalid Garmin cycle day section"),
            }
        }
    }
    Ok(rows)
}

#[cfg(test)]
fn gmt(value: &Value) -> Result<DateTime<Utc>> {
    let input = required_text(value)?;
    if let Ok(time) = input.parse::<DateTime<Utc>>() {
        return Ok(time);
    }
    Ok(NaiveDateTime::parse_from_str(input, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S%.f"))?
        .and_utc())
}

#[cfg(test)]
fn sleep_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    if item["sleepStartTimestampGMT"].is_null() || item["sleepEndTimestampGMT"].is_null() {
        return Ok(None);
    }
    let start = gmt(&item["sleepStartTimestampGMT"])?;
    let end = gmt(&item["sleepEndTimestampGMT"])?;
    Ok((start < end && window.contains(start, end)).then_some(record(
        "sleep", "Garmin Connect", &integer(&item["id"])?.to_string(), start, end, window.end,
        json!({"stages":[],"provider_stage_summary":{"deep_seconds":item["deepSleepSeconds"], "light_seconds":item["lightSleepSeconds"],
            "rem_seconds":item["remSleepSeconds"],"awake_seconds":item["awakeSleepSeconds"]}}),
    )))
}

#[cfg(test)]
fn workout_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    let start = gmt(&item["startTimeGMT"])?;
    let elapsed = item["elapsedDuration"]
        .as_f64()
        .or_else(|| item["duration"].as_f64());
    let Some(duration) = elapsed.filter(|duration| (0.001..=604800.0).contains(duration)) else {
        return Ok(None);
    };
    let end = start + CalendarDuration::milliseconds((duration * 1000.0).round() as i64);
    Ok(window.contains(start, end).then_some(record(
        "workout",
        "Garmin Connect",
        &integer(&item["activityId"])?.to_string(),
        start,
        end,
        window.end,
        json!({"exercise_type":required_text(&item["activityType"]["typeKey"])?,
            "title":item["activityName"],"distance_meters":item["distance"]}),
    )))
}

#[cfg(test)]
type HeartHours = std::collections::BTreeMap<i64, std::collections::BTreeMap<DateTime<Utc>, u64>>;

#[cfg(test)]
fn heart_points(day: &Value, window: &Window, hours: &mut HeartHours) -> Result<()> {
    if day["heartRateValues"].is_null() {
        ensure!(
            day["calendarDate"].is_string(),
            "invalid Garmin heart rate response"
        );
        return Ok(());
    }
    for sample in records(&day["heartRateValues"])? {
        let Some(bpm) = sample[1].as_u64().filter(|bpm| (1..=350).contains(bpm)) else {
            continue;
        };
        let at = DateTime::from_timestamp_millis(
            sample[0]
                .as_i64()
                .context("missing Garmin sample timestamp")?,
        )
        .context("invalid Garmin sample timestamp")?;
        if window.contains(at, at) {
            hours
                .entry(at.timestamp().div_euclid(3600))
                .or_default()
                .insert(at, bpm);
        }
    }
    Ok(())
}
