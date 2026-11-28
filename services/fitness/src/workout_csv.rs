use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Datelike, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use serde_json::json;
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;

use crate::store::{self, FitnessStore, ManualWorkout, WorkoutSetInput, WorkoutWrite};

const MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_ROWS: usize = 15_000;
const MAX_WORKOUTS: usize = 500;

#[derive(Clone, Debug)]
pub struct CsvWorkout {
    pub request: ManualWorkout,
    pub provider: &'static str,
    pub title_key: String,
}

#[derive(Clone, Debug)]
pub struct CsvBatch {
    pub provider: &'static str,
    pub digest: String,
    pub timezone: String,
    pub workouts: Vec<CsvWorkout>,
    pub skipped_rows: BTreeMap<String, usize>,
}

impl FitnessStore {
    pub async fn preview_workout_csv(
        &self,
        owner_id: &str,
        batch: &CsvBatch,
    ) -> Result<serde_json::Value> {
        let mut states = Vec::with_capacity(batch.workouts.len());
        let mut new_sets = 0usize;
        let mut counts = BTreeMap::<String, usize>::new();
        for workout in &batch.workouts {
            let state = self.classify_csv_workout(owner_id, workout).await?;
            if state == "new" {
                new_sets += workout.request.sets.len();
            }
            *counts.entry(state.clone()).or_default() += 1;
            states.push(state);
        }
        let samples:Vec<_>=batch.workouts.iter().zip(states.iter()).rev().take(8).map(|(workout,state)|json!({
            "performed_at":workout.request.performed_at,"state":state,"sets":workout.request.sets
        })).collect();
        Ok(
            json!({"provider":batch.provider,"digest":batch.digest,"new_workouts":counts.get("new").copied().unwrap_or(0),
            "new_sets":new_sets,"already_imported":counts.get("already_imported").copied().unwrap_or(0),
            "changed_workouts":counts.get("changed").copied().unwrap_or(0),
            "cross_source_matches":counts.get("cross_source_match").copied().unwrap_or(0),
            "skipped_rows":batch.skipped_rows,"samples":samples}),
        )
    }

    pub async fn import_workout_csv(
        &self,
        owner_id: &str,
        batch: &CsvBatch,
    ) -> Result<serde_json::Value> {
        let mut counts = BTreeMap::<String, usize>::new();
        let mut imported_sets = 0usize;
        for workout in &batch.workouts {
            let mut state = self.classify_csv_workout(owner_id, workout).await?;
            if state == "new" {
                let receipt = self
                    .record_imported_workout(owner_id, workout.provider, &workout.request)
                    .await?;
                if receipt.status == "cross_source_match" {
                    state = "cross_source_match".to_owned();
                } else {
                    imported_sets += workout.request.sets.len();
                }
            }
            *counts.entry(state).or_default() += 1;
        }
        Ok(
            json!({"imported_workouts":counts.get("new").copied().unwrap_or(0),
            "imported_sets":imported_sets,"already_imported":counts.get("already_imported").copied().unwrap_or(0),
            "changed_workouts":counts.get("changed").copied().unwrap_or(0),
            "cross_source_matches":counts.get("cross_source_match").copied().unwrap_or(0)}),
        )
    }

    async fn classify_csv_workout(&self, owner_id: &str, workout: &CsvWorkout) -> Result<String> {
        let request_key = format!("{}:{}", workout.provider, workout.request.request_key);
        let write = csv_write(owner_id, workout);
        let digest = store::import_fingerprint(owner_id, workout.provider, &workout.request)?;
        let client = self.pool.get().await?;
        if let Some(existing)=client.query_opt(
            "SELECT request_sha256,source FROM fitness.workout_sessions WHERE owner_id=$1 AND request_key=$2",
            &[&owner_id,&request_key],
        ).await?{
            let old_digest:String=existing.get(0);
            let source:String=existing.get(1);
            return Ok(if source==workout.provider&&old_digest==digest{"already_imported"}else{"changed"}.to_owned());
        }
        let other = if workout.provider == "strong" {
            "hevy"
        } else {
            "strong"
        };
        let at: DateTime<Utc> =
            DateTime::parse_from_rfc3339(&workout.request.performed_at)?.with_timezone(&Utc);
        let minute = at.timestamp().div_euclid(60) * 60;
        let from = DateTime::from_timestamp(minute, 0).context("invalid import time")?;
        let until = from + chrono::Duration::seconds(60);
        let rows=client.query(
            "SELECT request_sha256,performed_at FROM fitness.workout_sessions \
             WHERE owner_id=$1 AND source=$2 AND timezone=$3 AND performed_at >= $4 AND performed_at < $5",
            &[&owner_id,&other,&workout.request.timezone,&from,&until],
        ).await?;
        for row in rows {
            let old_digest: String = row.get(0);
            let old_at: DateTime<Utc> = row.get(1);
            if (old_at == at && old_digest == digest)
                || ((old_at.timestamp().rem_euclid(60) == 0 || at.timestamp().rem_euclid(60) == 0)
                    && store::fingerprint_at(&write, old_at)? == old_digest)
            {
                return Ok("cross_source_match".to_owned());
            }
        }
        Ok("new".to_owned())
    }
}

fn csv_write(owner_id: &str, workout: &CsvWorkout) -> WorkoutWrite {
    WorkoutWrite {
        owner_id: owner_id.to_owned(),
        request_key: format!("{}:{}", workout.provider, workout.request.request_key),
        workflow_run_id: None,
        performed_at: workout.request.performed_at.clone(),
        timezone: workout.request.timezone.clone(),
        sets: workout.request.sets.clone(),
        source: if workout.provider == "strong" {
            "strong"
        } else {
            "hevy"
        },
    }
}

pub fn parse(
    raw: &str,
    timezone: &str,
    strong_unit: Option<&str>,
    total_confirmed: bool,
) -> Result<CsvBatch> {
    if raw.is_empty() || raw.len() > MAX_BYTES {
        bail!("CSV must be non-empty and no larger than 4 MiB");
    }
    if raw.contains('\0') {
        bail!("CSV contains a NUL byte");
    }
    let zone: Tz = timezone.parse().context("invalid IANA timezone")?;
    if strong_unit.is_some_and(|unit| !matches!(unit, "kg" | "lb")) {
        bail!("Strong weight unit must be kg or lb");
    }
    let (provider, delimiter, headers, records) = read_rows(raw)?;
    let date_field = if provider == "hevy" {
        "start_time"
    } else {
        "date"
    };
    let title_field = if provider == "hevy" {
        "title"
    } else {
        "workout name"
    };
    let mut skipped = BTreeMap::new();
    let mut grouped: BTreeMap<(i64, String), (DateTime<Utc>, Vec<WorkoutSetInput>)> =
        BTreeMap::new();
    for (index, record) in records.iter().enumerate() {
        if index >= MAX_ROWS {
            bail!("CSV can contain at most 15,000 rows");
        }
        let row: BTreeMap<&str, &str> = headers
            .iter()
            .zip(record.iter())
            .map(|(h, v)| (h.as_str(), v.trim()))
            .collect();
        let set = match parse_set(&row, provider, delimiter, strong_unit, total_confirmed) {
            Ok(value) => value,
            Err(reason) => {
                *skipped.entry(reason.to_owned()).or_default() += 1;
                continue;
            }
        };
        let raw_time = row
            .get(date_field)
            .copied()
            .context("missing date column")?;
        let performed_at = match parse_clock(raw_time, zone) {
            Ok(value) => value,
            Err(reason) => {
                *skipped.entry(reason.to_owned()).or_default() += 1;
                continue;
            }
        };
        let title = normalize_word(row.get(title_field).copied().unwrap_or_default());
        let key = (performed_at.timestamp(), title);
        grouped
            .entry(key)
            .or_insert_with(|| (performed_at, Vec::new()))
            .1
            .push(set);
    }
    if grouped.len() > MAX_WORKOUTS {
        bail!("CSV can contain at most 500 workouts");
    }
    let mut workouts = Vec::new();
    for ((_, title), (at, sets)) in grouped {
        if sets.len() > 30 {
            *skipped.entry("too_many_sets".to_owned()).or_default() += sets.len();
            continue;
        }
        let local = at.with_timezone(&zone);
        let identity = serde_json::to_string(&json!([
            provider,
            local
                .naive_local()
                .format("%Y-%m-%dT%H:%M:%S%.f")
                .to_string(),
            0,
            title
        ]))?;
        let key = hex(&Sha256::digest(identity.as_bytes()))[..32].to_owned();
        workouts.push(CsvWorkout {
            request: ManualWorkout {
                request_key: key,
                performed_at: local.to_rfc3339(),
                timezone: timezone.to_owned(),
                sets,
            },
            provider,
            title_key: title,
        });
    }
    let options = serde_json::to_vec(&(timezone, strong_unit, total_confirmed))?;
    let raw_hash = Sha256::digest(raw.as_bytes());
    let mut digest = Sha256::new();
    digest.update(raw_hash);
    digest.update(options);
    Ok(CsvBatch {
        provider,
        digest: hex(&digest.finalize()),
        timezone: timezone.to_owned(),
        workouts,
        skipped_rows: skipped,
    })
}

fn read_rows(raw: &str) -> Result<(&'static str, u8, Vec<String>, Vec<csv::StringRecord>)> {
    for delimiter in [b',', b';'] {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .flexible(false)
            .from_reader(raw.as_bytes());
        let headers = match reader.headers() {
            Ok(value) => value.iter().map(normalize_word).collect::<Vec<_>>(),
            Err(_) => continue,
        };
        if headers.iter().collect::<BTreeSet<_>>().len() != headers.len() {
            continue;
        }
        let hevy = [
            "title",
            "start_time",
            "exercise_title",
            "set_index",
            "set_type",
            "reps",
        ]
        .iter()
        .all(|name| headers.iter().any(|header| header == name))
            && ["weight_kg", "weight_lbs", "weight_lb"]
                .iter()
                .any(|name| headers.iter().any(|h| h == name));
        let strong = [
            "date",
            "workout name",
            "exercise name",
            "set order",
            "weight",
            "reps",
        ]
        .iter()
        .all(|name| headers.iter().any(|header| header == name));
        if !hevy && !strong {
            continue;
        }
        let records = reader
            .records()
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("malformed CSV row or quoting")?;
        return Ok((
            if hevy { "hevy" } else { "strong" },
            delimiter,
            headers,
            records,
        ));
    }
    bail!("expected a supported Strong or Hevy CSV")
}

fn normalize_word(value: &str) -> String {
    value
        .nfkc()
        .collect::<String>()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_clock(raw: &str, zone: Tz) -> std::result::Result<DateTime<Utc>, &'static str> {
    let stamp: DateTime<chrono::FixedOffset> = if let Ok(stamp) = DateTime::parse_from_rfc3339(raw)
    {
        stamp
    } else {
        let layouts = [
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M",
            "%d %b %Y, %H:%M",
            "%d %b %Y, %H:%M:%S",
            "%d %B %Y, %H:%M",
            "%b %d, %Y, %I:%M %p",
            "%B %d, %Y, %I:%M %p",
            "%b %d, %Y, %I:%M:%S %p",
        ];
        let mut parsed = None;
        for layout in layouts {
            if let Ok(value) = NaiveDateTime::parse_from_str(raw, layout) {
                match zone.from_local_datetime(&value) {
                    chrono::LocalResult::Single(value) => parsed = Some(value.fixed_offset()),
                    chrono::LocalResult::Ambiguous(_, _) => return Err("ambiguous_local_time"),
                    chrono::LocalResult::None => return Err("unknown_date"),
                }
                break;
            }
        }
        parsed.ok_or("unknown_date")?
    };
    if !(1970..9999).contains(&stamp.year()) {
        return Err("unknown_date");
    }
    let offset = zone
        .offset_from_utc_datetime(&stamp.with_timezone(&Utc).naive_utc())
        .fix()
        .local_minus_utc();
    if offset != stamp.offset().local_minus_utc() {
        return Err("timezone_mismatch");
    }
    Ok(stamp.with_timezone(&Utc))
}

fn parse_set(
    row: &BTreeMap<&str, &str>,
    provider: &str,
    delimiter: u8,
    strong_unit: Option<&str>,
    total_confirmed: bool,
) -> std::result::Result<WorkoutSetInput, &'static str> {
    let exercise_name = if provider == "hevy" {
        row.get("exercise_title")
    } else {
        row.get("exercise name")
    }
    .copied()
    .unwrap_or_default();
    let (exercise, variant) =
        exercise_for(&normalize_word(exercise_name)).ok_or("unsupported_exercise")?;
    let raw_type = if provider == "hevy" {
        row.get("set_type")
    } else {
        row.get("set type")
    }
    .copied()
    .unwrap_or_default();
    let mut set_type = normalize_word(raw_type);
    if set_type.is_empty() && provider == "strong" {
        let order = normalize_word(row.get("set order").copied().unwrap_or_default());
        set_type = if order.chars().all(|c| c.is_ascii_digit()) && order.len() <= 4 {
            "normal".to_owned()
        } else {
            order
        };
    }
    let set_type = match set_type.as_str() {
        "normal" => "work",
        "warmup" | "w" => "warmup",
        "drop" | "dropset" | "drop set" | "d" => "drop",
        "failure" | "f" => "failure",
        _ => return Err("unknown_set_type"),
    };
    let reps = parse_number(
        row.get("reps").copied().unwrap_or_default(),
        delimiter == b';',
    )?;
    if reps.fract() != 0.0 {
        return Err("invalid_number");
    }
    if !(0.0..=1000.0).contains(&reps) {
        return Err("invalid_number");
    }
    let (weight, unit) = if provider == "hevy" {
        let fields = [
            ("weight_kg", "kg"),
            ("weight_lbs", "lb"),
            ("weight_lb", "lb"),
        ]
        .into_iter()
        .filter_map(|(column, unit)| {
            row.get(column)
                .filter(|v| !v.is_empty())
                .map(|value| (unit, *value))
        })
        .collect::<Vec<_>>();
        if fields.len() > 1 {
            return Err("ambiguous_weight_unit");
        }
        fields
            .first()
            .map(|(unit, value)| (*value, Some(*unit)))
            .unwrap_or(("", None))
    } else {
        let raw = row.get("weight").copied().unwrap_or_default();
        let hint = normalize_word(row.get("weight unit").copied().unwrap_or_default());
        let unit = if hint.is_empty() {
            strong_unit
        } else if ["kg", "kgs", "kilograms", "kilogram"].contains(&hint.as_str()) {
            Some("kg")
        } else if ["lb", "lbs", "pounds", "pound"].contains(&hint.as_str()) {
            Some("lb")
        } else {
            None
        };
        (raw, unit)
    };
    let mut load = if weight.is_empty() {
        0.0
    } else {
        parse_number(weight, delimiter == b';')?
    };
    if variant == "barbell" && load > 0.0 && unit.is_none() {
        return Err("missing_weight_unit");
    }
    if (variant == "bodyweight" && load != 0.0) || (variant == "barbell" && load <= 0.0) {
        return Err("unsupported_load");
    }
    if unit == Some("lb") {
        load = (load * 0.45359237 * 100.0).round() / 100.0;
    }
    let rpe = row
        .get("rpe")
        .filter(|v| !v.is_empty())
        .map(|v| parse_number(v, delimiter == b';'))
        .transpose()?;
    if load > 2000.0 || rpe.is_some_and(|value| !(1.0..=10.0).contains(&value)) {
        return Err("invalid_number");
    }
    Ok(WorkoutSetInput {
        exercise_id: exercise.to_owned(),
        variant_id: variant.to_owned(),
        reps: reps as i32,
        load_kg: if variant == "barbell" {
            Some(load)
        } else {
            None
        },
        load_mode: if variant == "barbell" && total_confirmed {
            Some("barbell_total".into())
        } else {
            None
        },
        rpe,
        set_type: set_type.to_owned(),
        plan_id: None,
        plan_revision: None,
        target_position: None,
    })
}

fn parse_number(raw: &str, decimal_comma: bool) -> std::result::Result<f64, &'static str> {
    let normalized = if decimal_comma && raw.bytes().filter(|b| *b == b',').count() == 1 {
        let (whole, fraction) = raw.split_once(',').ok_or("invalid_number")?;
        if whole.is_empty()
            || !whole.bytes().all(|b| b.is_ascii_digit())
            || !(1..=2).contains(&fraction.len())
            || !fraction.bytes().all(|b| b.is_ascii_digit())
        {
            return Err("invalid_number");
        }
        raw.replace(',', ".")
    } else {
        raw.to_owned()
    };
    if normalized.is_empty()
        || normalized.len() > 18
        || normalized.bytes().any(|b| !b.is_ascii_digit() && b != b'.')
        || normalized.matches('.').count() > 1
    {
        return Err("invalid_number");
    }
    let (whole, fraction) = normalized.split_once('.').unwrap_or((&normalized, ""));
    if whole.is_empty() || whole.len() > 8 || fraction.len() > 8 {
        return Err("invalid_number");
    }
    normalized
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .ok_or("invalid_number")
}

fn exercise_for(name: &str) -> Option<(&'static str, &'static str)> {
    Some(match name {
        "squat (barbell)"
        | "barbell squat"
        | "back squat (barbell)"
        | "스쿼트 (바벨)"
        | "바벨 스쿼트" => ("squat", "barbell"),
        "squat (bodyweight)" | "bodyweight squat" | "air squat" | "맨몸 스쿼트" => {
            ("squat", "bodyweight")
        }
        "bench press (barbell)" | "barbell bench press" | "바벨 벤치프레스" => {
            ("bench_press", "barbell")
        }
        "deadlift (barbell)" | "barbell deadlift" | "바벨 데드리프트" => {
            ("deadlift", "barbell")
        }
        "push up" | "push-up" | "pushup" | "푸시업" => ("push_up", "bodyweight"),
        _ => return None,
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
