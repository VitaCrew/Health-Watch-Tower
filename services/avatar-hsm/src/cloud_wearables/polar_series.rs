use super::*;
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};

#[cfg(test)]
#[path = "polar_series_tests.rs"]
mod tests;

#[derive(Clone, Copy)]
pub(super) struct Night {
    start: DateTime<FixedOffset>,
    end: DateTime<FixedOffset>,
}

impl Night {
    pub(super) fn parse(item: &Value) -> Result<Self> {
        Ok(Self {
            start: required_text(&item["sleep_start_time"])?.parse()?,
            end: required_text(&item["sleep_end_time"])?.parse()?,
        })
    }

    pub(super) fn utc_bounds(self) -> (DateTime<Utc>, DateTime<Utc>) {
        (self.start.to_utc(), self.end.to_utc())
    }

    fn entries(self, input: &Value) -> Vec<(DateTime<Utc>, &Value)> {
        let Some(input) = input.as_object() else {
            return Vec::new();
        };
        let duration = self.end - self.start;
        if self.start.offset() != self.end.offset()
            || duration <= CalendarDuration::zero()
            || duration >= CalendarDuration::days(1)
        {
            // Clock-only samples are ambiguous after a time zone change or a full day.
            return Vec::new();
        }
        let mut entries: Vec<_> = input
            .iter()
            .filter_map(|(clock, value)| {
                let time = clock_time(clock)?;
                let mut day = self.start.date_naive();
                let local_start = self.start.naive_local();
                if day.and_time(time) + CalendarDuration::minutes(1) <= local_start {
                    day = day.succ_opt()?;
                }
                let local = day.and_time(time);
                let at = local.and_local_timezone(*self.start.offset()).single()?;
                (at < self.end).then_some((at.to_utc(), value))
            })
            .collect();
        entries.sort_by_key(|(at, _)| *at);
        entries
    }

    pub(super) fn stages(self, input: &Value) -> Vec<Value> {
        let entries = self.entries(input);
        let mut intervals: Vec<(DateTime<Utc>, DateTime<Utc>, i64, i64)> = Vec::new();
        for (index, (start, value)) in entries.iter().enumerate() {
            let start = (*start).max(self.start.to_utc());
            let end = entries
                .get(index + 1)
                .map(|entry| entry.0)
                .unwrap_or(self.end.to_utc());
            let Some(provider_stage) = value.as_i64() else {
                continue;
            };
            let stage = match provider_stage {
                0 => 1,
                1 => 6,
                2 | 3 => 4,
                4 => 5,
                _ => continue,
            };
            if start >= end {
                continue;
            }
            match intervals.last_mut() {
                Some((_, previous_end, _, previous_code))
                    if *previous_end == start && *previous_code == provider_stage =>
                {
                    *previous_end = end;
                }
                _ => intervals.push((start, end, stage, provider_stage)),
            }
        }
        if intervals.len() > 500 {
            return Vec::new();
        }
        intervals.into_iter().map(|(start,end,stage,provider_stage)| {
            json!({"start":start,"end":end,"stage":stage,"provider_stage":provider_stage})
        }).collect()
    }

    pub(super) fn heart_rate(
        self,
        rows: &mut Vec<Value>,
        day: &str,
        input: &Value,
        window: &Window,
    ) {
        for (at, value) in self.entries(input) {
            if let Some(bpm) =
                valid_bpm(value).filter(|_| at >= self.start && window.contains(at, at))
            {
                rows.push(heart_rate_row(
                    "Polar Sleep",
                    &format!("sleep:{day}:{}", at.timestamp()),
                    at,
                    bpm,
                    window,
                ));
            }
        }
    }

    pub(super) fn recharge(self, rows: &mut Vec<Value>, item: &Value, window: &Window) {
        for (key, metric, unit) in [
            ("hrv_samples", "hrv_rmssd", "ms"),
            ("breathing_samples", "respiratory_rate", "breaths/min"),
        ] {
            for (at, value) in self.entries(&item[key]) {
                if value.as_f64().is_some_and(|value| value > 0.0)
                    && at >= self.start
                    && window.contains(at, at)
                {
                    rows.push(record(
                        "metric",
                        "Polar",
                        &format!("nightly-recharge:{metric}:{}", at.timestamp()),
                        at,
                        at,
                        window.end,
                        json!({"metric":metric,"time_precision":"instant",
                            "provider_data":{"value":value,"unit":unit,"sleep_date":item["date"]}
                        }),
                    ));
                }
            }
        }
    }
}

pub(crate) fn sleep_details(payload: &mut Value) {
    let raw = &payload["provider_data"];
    let Ok(night) = Night::parse(raw) else { return };
    let seconds = (night.end - night.start).num_milliseconds() as f64 / 1_000.0;
    if seconds <= 0.0 {
        return;
    }
    let duration = |field: &str| raw[field].as_u64().filter(|value| *value as f64 <= seconds);
    let score = |field: &str| {
        raw[field]
            .as_f64()
            .filter(|value| (1.0..=100.0).contains(value))
    };
    let grade = |field: &str| raw[field].as_u64().filter(|value| (1..=5).contains(value));
    let awake = duration("total_interruption_duration");
    let asleep = awake.map(|awake| seconds - awake as f64);
    let stages = [
        ("light_sleep", "light_seconds"),
        ("deep_sleep", "deep_seconds"),
        ("rem_sleep", "rem_seconds"),
        ("unrecognized_sleep_stage", "unrecognized_seconds"),
        ("total_interruption_duration", "awake_seconds"),
    ]
    .into_iter()
    .filter_map(|(source, key)| Some((key.to_owned(), json!(duration(source)?))))
    .collect::<serde_json::Map<_, _>>();
    let components = [
        ("group_duration_score", "duration"),
        ("group_solidity_score", "solidity"),
        ("group_regeneration_score", "regeneration"),
    ]
    .into_iter()
    .filter_map(|(source, key)| Some((key.to_owned(), json!(score(source)?))))
    .collect::<serde_json::Map<_, _>>();
    let metrics = json!({
        "sleep_window_seconds":seconds,
        "asleep_seconds":asleep,
        "sleep_efficiency_percent":asleep.map(|seconds_asleep| (seconds_asleep / seconds * 10_000.0).round() / 100.0),
        "short_interruption_seconds":duration("short_interruption_duration"),
        "long_interruption_seconds":duration("long_interruption_duration"),
        "sleep_goal_seconds":raw["sleep_goal"].as_u64(),
        "sleep_cycles":raw["sleep_cycles"].as_u64(),
        "continuity":raw["continuity"].as_f64().filter(|value| (1.0..=5.0).contains(value)),
        "continuity_class":grade("continuity_class"),
        "sleep_charge":grade("sleep_charge"),
        "sleep_rating":grade("sleep_rating")
    });
    let total_score = raw["sleep_score"]
        .as_u64()
        .filter(|value| (1..=100).contains(value));
    payload["provider_stage_summary"] = Value::Object(stages);
    payload["sleep_score_components"] = Value::Object(components);
    payload["sleep_metrics"] = metrics;
    payload["sleep_score"] = json!(total_score);
}

pub(super) fn continuous(rows: &mut Vec<Value>, item: &Value, window: &Window) -> Result<()> {
    let day: NaiveDate = required_text(&item["date"])?.parse()?;
    let civil_day = day.and_time(NaiveTime::MIN).and_utc();
    if window.contains(civil_day, civil_day) {
        rows.push(record(
            "metric", "Polar", &format!("continuous_heart_rate:{}", civil_day.to_rfc3339()),
            civil_day, civil_day, window.end,
            json!({"metric":"continuous_heart_rate","time_precision":"provider_calendar_date","provider_data":item}),
        ));
    }
    let Some(samples) = item["heart_rate_samples"].as_array() else {
        return Ok(());
    };
    let mut seen = HashSet::new();
    for sample in samples {
        let Some(clock) = sample["sample_time"].as_str() else {
            continue;
        };
        let Some(bpm) = valid_bpm(&sample["heart_rate"]) else {
            continue;
        };
        let instant = clock.parse::<DateTime<Utc>>().ok();
        let civil = clock_time(clock).map(|time| day.and_time(time).and_utc());
        let Some(at) = instant.or(civil) else {
            continue;
        };
        if !window.contains(at, at) || !seen.insert(at) {
            continue;
        }
        let id = format!("continuous:{}", at.to_rfc3339());
        if instant.is_some() {
            rows.push(heart_rate_row("Polar", &id, at, bpm, window));
        } else {
            rows.push(record(
                "metric", "Polar", &id, at, at, window.end,
                json!({"metric":"continuous_heart_rate_sample","time_precision":"provider_civil_time",
                    "provider_data":{"value":bpm,"unit":"bpm","date":day,"sample_time":clock}
                }),
            ));
        }
    }
    Ok(())
}

fn clock_time(value: &str) -> Option<NaiveTime> {
    NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M"))
        .ok()
}

fn valid_bpm(value: &Value) -> Option<u64> {
    value.as_u64().filter(|bpm| (1..=350).contains(bpm))
}

fn heart_rate_row(source: &str, id: &str, at: DateTime<Utc>, bpm: u64, window: &Window) -> Value {
    record(
        "heart_rate",
        source,
        id,
        at,
        at,
        window.end,
        json!({"unit":"bpm","samples":[{"at":at,"bpm":bpm}]}),
    )
}

pub(super) fn activity_steps(rows: &mut Vec<Value>, item: &Value, window: &Window) -> Result<()> {
    let day: NaiveDate = required_text(&item["date"])?.parse()?;
    let steps = &item["steps"];
    let Some(samples) = steps["samples"].as_array() else {
        return Ok(());
    };
    let interval = steps["interval_ms"]
        .as_i64()
        .filter(|ms| (1..=86_400_000).contains(ms));
    let mut seen = HashSet::new();
    for sample in samples {
        let Some(timestamp) = sample["timestamp"].as_str() else {
            continue;
        };
        let Some(count) = sample["steps"].as_u64().filter(|count| *count <= 1_000_000) else {
            continue;
        };
        let instant = timestamp.parse::<DateTime<Utc>>().ok();
        let civil = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M"))
            .ok();
        let Some(at) = instant.or_else(|| civil.map(|civil| civil.and_utc())) else {
            continue;
        };
        let end = interval.map_or(at, |ms| at + CalendarDuration::milliseconds(ms));
        if !window.contains(at, end) || !seen.insert((at, instant.is_some())) {
            continue;
        }
        let id = format!("activity_steps:{day}:{timestamp}");
        if instant.is_some() && interval.is_some() {
            rows.push(record(
                "steps",
                "Polar",
                &id,
                at,
                end,
                window.end,
                json!({
                    "count":count,"provider_day":day,"provider_timestamp":timestamp
                }),
            ));
        } else {
            rows.push(record(
                "metric",
                "Polar",
                &id,
                at,
                end,
                window.end,
                json!({
                    "metric":"activity_step_sample",
                    "time_precision":if instant.is_some() {"instant"} else {"provider_civil_time"},
                    "provider_data":{"value":count,"unit":"steps","date":day,
                        "timestamp":timestamp,"interval_ms":interval}
                }),
            ));
        }
    }
    Ok(())
}
