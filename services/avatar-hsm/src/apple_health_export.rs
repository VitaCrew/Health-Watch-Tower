use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    ffi::OsString,
    fs::File,
    io::{BufRead, BufReader, Read},
    path::Path,
};

use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, Duration, FixedOffset, Utc};
use deadpool_postgres::Pool;
use quick_xml::{
    events::{BytesStart, Event},
    Reader,
};
use serde::Serialize;
use serde_json::json;

use crate::{
    apple_export_routes, store,
    wearables::{self, Record},
};

pub const PROVIDER: &str = "apple_health_export";
const MAX_XML_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const BATCH_SIZE: usize = 1000;

#[cfg(test)]
#[path = "apple_health_export_tests.rs"]
mod tests;

pub async fn command(arguments: &[OsString]) -> Result<()> {
    let (subject, path) = match arguments {
        [command, subject, path] if command == "import-apple-health" => {
            (subject, Some(Path::new(path)))
        }
        [command, subject] if command == "forget-apple-health" => (subject, None),
        _ => bail!(
            "usage: avatar-hsm import-apple-health SUBJECT FILE | forget-apple-health SUBJECT"
        ),
    };
    let subject = subject.to_str().context("invalid HSM subject")?;
    let database =
        std::env::var("AVATAR_HSM_DATABASE_URL").context("set AVATAR_HSM_DATABASE_URL")?;
    let pool = store::connect(&database).await?;
    ensure_subject(&pool, subject).await?;
    let result = if let Some(path) = path {
        serde_json::to_value(import_file(&pool, subject, path).await?)?
    } else {
        wearables::revoke_for(&pool, subject, PROVIDER).await?
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

async fn ensure_subject(pool: &Pool, subject: &str) -> Result<()> {
    let db = pool.get().await?;
    let exists: bool = db.query_one(
        "SELECT EXISTS(SELECT 1 FROM avatar_hsm.subject s WHERE s.id=$1
           AND (s.id=$2 OR EXISTS(SELECT 1 FROM avatar_hsm.wearable_profile p WHERE p.subject_id=s.id)))",
        &[&subject,&store::SUBJECT],
    ).await?.get(0);
    ensure!(exists, "HSM subject has no wearable profile");
    Ok(())
}

async fn import_file(pool: &Pool, subject: &str, path: &Path) -> Result<ImportSummary> {
    let mut file = BufReader::new(File::open(path).context("could not open Apple Health export")?);
    let is_zip = file.fill_buf()?.starts_with(b"PK");
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    prepare_import(&tx, subject).await?;
    let result = if is_zip {
        let mut archive = zip::ZipArchive::new(file).context("invalid Apple Health ZIP")?;
        let candidates: Vec<_> = (0..archive.len())
            .filter(|index| {
                archive.by_index_raw(*index).is_ok_and(|entry| {
                    entry.is_file()
                        && entry.enclosed_name().is_some_and(|path| {
                            path.file_name().is_some_and(|name| name == "export.xml")
                        })
                })
            })
            .collect();
        ensure!(
            candidates.len() == 1,
            "ZIP must contain exactly one export.xml"
        );
        let xml_name = archive.by_index_raw(candidates[0])?.name()?.into_owned();
        let (mut result, pending) = {
            let entry = archive.by_index(candidates[0])?;
            let size = entry.size();
            import_xml_rows(&tx, subject, entry, size).await?
        };
        for route in pending {
            if apple_export_routes::import(&tx, subject, &mut archive, &xml_name, route).await? {
                result.routes += 1;
            } else {
                result.routes_unavailable += 1;
            }
        }
        result
    } else {
        let size = file.get_ref().metadata()?.len();
        let (mut result, pending) = import_xml_rows(&tx, subject, file, size).await?;
        result.routes_unavailable = pending.len();
        result
    };
    tx.commit().await?;
    Ok(result)
}

#[derive(Default, Serialize)]
struct ImportSummary {
    accepted: usize,
    skipped: usize,
    routes: usize,
    routes_unavailable: usize,
}

struct ExportReader<R> {
    reader: Reader<BufReader<std::io::Take<R>>>,
    buffer: Vec<u8>,
    depth: usize,
    complete: bool,
    summary: ImportSummary,
    workout: Option<Record>,
    in_workout_route: bool,
}

impl<R: Read> ExportReader<R> {
    fn new(input: R) -> Self {
        let mut reader = Reader::from_reader(BufReader::new(input.take(MAX_XML_BYTES + 1)));
        reader.config_mut().trim_text(true);
        Self {
            reader,
            buffer: Vec::new(),
            depth: 0,
            complete: false,
            summary: ImportSummary::default(),
            workout: None,
            in_workout_route: false,
        }
    }

    fn batch(&mut self) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        let mut batch_ids = HashSet::new();
        while records.len() < BATCH_SIZE {
            self.buffer.clear();
            let event = self
                .reader
                .read_event_into(&mut self.buffer)
                .map_err(|_| anyhow::anyhow!("invalid Apple Health XML"))?;
            ensure!(
                self.reader.buffer_position() <= MAX_XML_BYTES,
                "Apple Health XML exceeds the size limit"
            );
            match &event {
                Event::Start(node) if self.depth == 0 => {
                    ensure!(
                        !self.complete && node.name().as_ref() == "HealthData",
                        "invalid Apple Health document root"
                    );
                }
                Event::Start(node) if self.depth == 1 && node.name().as_ref() == "Workout" => {
                    self.workout = parse_record(node)?;
                    if self.workout.is_none() {
                        self.summary.skipped += 1;
                    }
                }
                Event::Start(node) if self.depth == 2 && node.name().as_ref() == "WorkoutRoute" => {
                    self.in_workout_route = true;
                }
                Event::End(node) if self.depth == 3 && node.name().as_ref() == "WorkoutRoute" => {
                    self.in_workout_route = false;
                }
                Event::Start(node) | Event::Empty(node)
                    if self.depth >= 2 && self.workout.is_some() =>
                {
                    let workout = self.workout.as_mut().expect("workout initialized");
                    match (self.depth, node.name().as_ref()) {
                        (2, "WorkoutStatistics") => workout_stat(workout, node)?,
                        (2, "MetadataEntry") => workout_metadata(workout, node)?,
                        (3, "FileReference") if self.in_workout_route => {
                            if let Some(path) = attributes(node)?.remove("path") {
                                let entry = workout
                                    .payload
                                    .as_object_mut()
                                    .expect("workout payload")
                                    .entry("route_files")
                                    .or_insert_with(|| json!([]));
                                let routes = entry.as_array_mut().expect("route file list");
                                ensure!(
                                    routes.len() < 32,
                                    "too many Apple Health route files for one workout"
                                );
                                routes.push(json!(path));
                            }
                        }
                        _ => {}
                    }
                }
                Event::End(node) if self.depth == 2 && node.name().as_ref() == "Workout" => {
                    if let Some(mut record) = self.workout.take() {
                        finalize_workout(&mut record);
                        if batch_ids.insert(record.source_id.clone()) {
                            records.push(record);
                            self.summary.accepted += 1;
                        }
                    }
                }
                Event::Start(node) | Event::Empty(node)
                    if (matches!(self.depth, 1 | 2) && node.name().as_ref() == "Record")
                        || (self.depth == 1 && node.name().as_ref() == "Workout") =>
                {
                    if let Some(record) = parse_record(node)? {
                        if batch_ids.insert(record.source_id.clone()) {
                            records.push(record);
                            self.summary.accepted += 1;
                        }
                    } else {
                        self.summary.skipped += 1;
                    }
                }
                Event::Empty(_) if self.depth == 0 => bail!("invalid Apple Health document root"),
                Event::Eof => {
                    ensure!(
                        self.depth == 0 && self.complete,
                        "incomplete Apple Health XML"
                    );
                    break;
                }
                _ => {}
            }
            match event {
                Event::Start(_) => self.depth += 1,
                Event::End(_) => {
                    self.depth = self
                        .depth
                        .checked_sub(1)
                        .context("invalid Apple Health XML")?;
                    self.complete |= self.depth == 0;
                }
                _ => {}
            }
        }
        Ok(records)
    }
}

fn workout_stat(workout: &mut Record, node: &BytesStart<'_>) -> Result<()> {
    let fields = attributes(node)?;
    let get = |name: &str| fields.get(name).map_or("", String::as_str);
    let kind = get("type");
    if !kind.starts_with("HKQuantityTypeIdentifier") || kind.len() > 128 {
        return Ok(());
    }
    let unit = get("unit");
    if unit.len() > 64 {
        return Ok(());
    }
    let mut stats = serde_json::Map::new();
    for field in ["sum", "average", "minimum", "maximum"] {
        if let Some(value) = finite(get(field)) {
            stats.insert(field.into(), json!(value));
        }
    }
    if stats.is_empty() {
        return Ok(());
    }
    stats.insert("type".into(), json!(kind));
    stats.insert("unit".into(), json!(unit));
    let entries = workout
        .payload
        .as_object_mut()
        .expect("workout payload")
        .entry("statistics")
        .or_insert_with(|| json!([]));
    let entries = entries.as_array_mut().expect("workout statistics");
    ensure!(
        entries.len() < 128,
        "too many Apple Health workout statistics"
    );
    entries.push(serde_json::Value::Object(stats));
    Ok(())
}

fn workout_metadata(workout: &mut Record, node: &quick_xml::events::BytesStart<'_>) -> Result<()> {
    let values = attributes(node)?;
    let (Some(key), Some(raw)) = (values.get("key"), values.get("value")) else {
        return Ok(());
    };
    if key == "HKWeatherCondition" {
        if let Ok(code) = raw.parse::<u16>() {
            workout.payload["weather_condition_code"] = json!(code);
        }
        return Ok(());
    }
    let Some((amount, unit)) = raw.trim().split_once(char::is_whitespace) else {
        return Ok(());
    };
    let Some(amount) = finite(amount) else {
        return Ok(());
    };
    let unit = unit.trim();
    let (field, value) = match (key.as_str(), unit) {
        ("HKWeatherTemperature", "degC" | "°C") => ("weather_temperature_c", Some(amount)),
        ("HKWeatherTemperature", "degF" | "°F") => {
            ("weather_temperature_c", Some((amount - 32.0) / 1.8))
        }
        ("HKWeatherHumidity", "%") => {
            // Older Apple exports use whole percentages scaled by 100, e.g. 7900 %.
            let percent = if amount > 100.0 {
                amount / 100.0
            } else {
                amount
            };
            (
                "weather_humidity_percent",
                (0.0..=100.0).contains(&percent).then_some(percent),
            )
        }
        ("HKElevationAscended", unit) => ("elevation_ascent_m", distance_meters(amount, unit)),
        ("HKElevationDescended", unit) => ("elevation_descent_m", distance_meters(amount, unit)),
        _ => return Ok(()),
    };
    if let Some(value) = value.filter(|value| value.is_finite()) {
        workout.payload[field] = json!(value);
        if key == "HKWeatherHumidity" {
            workout.payload["weather_humidity_apple_export"] = json!(raw);
        }
    }
    Ok(())
}

fn finalize_workout(workout: &mut Record) {
    let Some(stats) = workout.payload["statistics"].as_array() else {
        return;
    };
    let mut distances = stats
        .iter()
        .filter(|row| {
            row["type"]
                .as_str()
                .is_some_and(|kind| kind.starts_with("HKQuantityTypeIdentifierDistance"))
        })
        .filter_map(|row| distance_meters(row["sum"].as_f64()?, row["unit"].as_str()?));
    let distance = distances.next().filter(|_| distances.next().is_none());
    let energy = stats
        .iter()
        .find(|row| row["type"] == "HKQuantityTypeIdentifierActiveEnergyBurned")
        .and_then(|row| energy_kcal(row["sum"].as_f64()?, row["unit"].as_str()?));
    if let Some(distance) = distance {
        workout.payload["distance_meters"] = json!(distance);
    }
    if let Some(energy) = energy {
        workout.payload["energy_kcal"] = json!(energy);
    }
}

fn finite(value: &str) -> Option<f64> {
    value.parse::<f64>().ok().filter(|value| value.is_finite())
}

fn workout_distance(value: &str, unit: &str) -> Option<f64> {
    distance_meters(finite(value)?, unit)
}

fn distance_meters(value: f64, unit: &str) -> Option<f64> {
    let factor = match unit {
        "m" => 1.0,
        "cm" => 0.01,
        "km" => 1000.0,
        "mi" => 1609.344,
        "yd" => 0.9144,
        "ft" => 0.3048,
        _ => return None,
    };
    let distance = value * factor;
    (distance.is_finite() && distance >= 0.0).then_some(distance)
}

fn workout_energy(value: &str, unit: &str) -> Option<f64> {
    energy_kcal(finite(value)?, unit)
}

fn energy_kcal(value: f64, unit: &str) -> Option<f64> {
    let kcal = match unit {
        "kcal" | "Cal" => value,
        "kJ" => value / 4.184,
        _ => return None,
    };
    (kcal.is_finite() && kcal >= 0.0).then_some(kcal)
}

async fn prepare_import(tx: &tokio_postgres::Transaction<'_>, subject: &str) -> Result<()> {
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext($2))",
        &[&subject, &PROVIDER],
    )
    .await?;
    tx.query_one("SELECT id FROM avatar_hsm.subject WHERE id=$1", &[&subject])
        .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,$2)
                ON CONFLICT DO NOTHING",
        &[&subject, &PROVIDER],
    )
    .await?;
    Ok(())
}

async fn import_xml_rows(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    input: impl Read,
    size: u64,
) -> Result<(ImportSummary, Vec<apple_export_routes::Pending>)> {
    ensure!(
        size <= MAX_XML_BYTES,
        "Apple Health XML exceeds the size limit"
    );
    let mut parser = ExportReader::new(input);
    let mut kinds = BTreeSet::new();
    let mut pending = Vec::new();
    loop {
        let mut records = parser.batch()?;
        if records.is_empty() {
            break;
        }
        kinds.extend(records.iter().map(|record| record.kind.clone()));
        for record in &mut records {
            if let Some(route) = apple_export_routes::Pending::take(record) {
                pending.push(route);
            }
        }
        wearables::upsert_records(tx, subject, PROVIDER, &records).await?;
    }
    ensure!(
        parser.summary.accepted > 0,
        "Apple Health export contains no supported records"
    );
    let kinds: Vec<_> = kinds.into_iter().collect();
    tx.execute("UPDATE avatar_hsm.device_connection SET state='synced',last_synced_at=now(),
        grants=ARRAY(SELECT DISTINCT unnest(grants || $3::text[])) WHERE subject_id=$1 AND provider=$2",
        &[&subject,&PROVIDER,&kinds]).await?;
    wearables::rebuild_daily(tx, subject, PROVIDER).await?;
    Ok((parser.summary, pending))
}

pub(crate) fn attributes(node: &BytesStart<'_>) -> Result<BTreeMap<String, String>> {
    let mut fields = BTreeMap::new();
    for attribute in node.attributes() {
        let attr = attribute.map_err(|_| anyhow::anyhow!("invalid Apple Health attribute"))?;
        let key = attr.key.as_ref().to_owned();
        let value = attr
            .normalized_value(quick_xml::XmlVersion::Implicit1_0)
            .map_err(|_| anyhow::anyhow!("invalid Apple Health XML encoding"))?;
        fields.insert(key, value.into_owned());
    }
    Ok(fields)
}

fn parse_record(node: &BytesStart<'_>) -> Result<Option<Record>> {
    let fields = attributes(node)?;
    let get = |key: &str| fields.get(key).map_or("", String::as_str);
    let Some((start, end)) = timestamp(get("startDate")).zip(timestamp(get("endDate"))) else {
        return Ok(None);
    };
    if start > end || end.to_utc() > Utc::now() + Duration::minutes(5) {
        return Ok(None);
    }
    let source = get("sourceName");
    if source.trim().is_empty() || source.len() > 256 {
        return Ok(None);
    }
    let source_id = ring::digest::digest(&ring::digest::SHA256, &serde_json::to_vec(&fields)?)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (started_at, ended_at) = (start.to_utc(), end.to_utc());
    let value = get("value");
    let unit = get("unit");
    let (kind, payload) = match (node.name().as_ref(), get("type")) {
        ("Workout", _) if start < end => {
            let activity = get("workoutActivityType");
            if activity.is_empty() || activity.len() > 128 {
                return Ok(None);
            }
            let mut workout = json!({"exercise_type":activity});
            let duration_factor = match get("durationUnit") {
                "s" => Some(1.0),
                "min" => Some(60.0),
                "hr" | "h" => Some(3600.0),
                _ => None,
            };
            if let Some(duration) = finite(get("duration"))
                .zip(duration_factor)
                .map(|(value, factor)| value * factor)
                .filter(|duration| duration.is_finite() && *duration >= 0.0)
            {
                workout["duration_seconds"] = json!(duration);
            }
            if let Some(distance) = workout_distance(get("totalDistance"), get("totalDistanceUnit"))
            {
                workout["distance_meters"] = json!(distance);
            }
            if let Some(energy) =
                workout_energy(get("totalEnergyBurned"), get("totalEnergyBurnedUnit"))
            {
                workout["energy_kcal"] = json!(energy);
            }
            ("workout", workout)
        }
        ("Record", "HKQuantityTypeIdentifierStepCount") if start < end && unit == "count" => {
            let Some(count) = value
                .parse::<u64>()
                .ok()
                .filter(|count| *count <= 1_000_000)
            else {
                return Ok(None);
            };
            ("steps", json!({"count":count,"unit":"steps"}))
        }
        ("Record", "HKQuantityTypeIdentifierHeartRate") if unit == "count/min" => {
            let Some(bpm) = value
                .parse::<f64>()
                .ok()
                .filter(|bpm| (1.0..=350.0).contains(bpm))
            else {
                return Ok(None);
            };
            (
                "heart_rate",
                json!({"unit":"bpm","samples":[{"at":started_at,"bpm":bpm}]}),
            )
        }
        ("Record", "HKCategoryTypeIdentifierSleepAnalysis") if start < end => {
            let Some(stage) = sleep_stage(value) else {
                return Ok(None);
            };
            let stages = stage
                .map(|stage| json!({"start":started_at,"end":ended_at,"stage":stage}))
                .into_iter()
                .collect::<Vec<_>>();
            (
                "sleep",
                json!({"segment_only":true,"in_bed":stage.is_none(),"stages":stages}),
            )
        }
        ("Record", "HKCategoryTypeIdentifierMindfulSession") if start < end => (
            "metric",
            json!({"metric":"mindfulness_session","provider_data":{
                "duration_seconds":(end-start).num_milliseconds() as f64 / 1_000.0,
                "utc_offset_seconds":start.offset().local_minus_utc()
            }}),
        ),
        ("Record", kind) => {
            let Some((metric, normalized, normalized_unit)) = metric(kind, value, unit) else {
                return Ok(None);
            };
            (
                "metric",
                json!({"metric":metric,"provider_data":{"value":normalized,"unit":normalized_unit,
                "original_value":value,"original_unit":unit,"utc_offset_seconds":start.offset().local_minus_utc()}}),
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(Record {
        kind: kind.into(),
        source: source.into(),
        source_id,
        started_at,
        ended_at,
        modified_at: ended_at,
        payload,
    }))
}

fn timestamp(value: &str) -> Option<DateTime<FixedOffset>> {
    DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S %z")
        .or_else(|_| DateTime::parse_from_rfc3339(value))
        .ok()
}

fn sleep_stage(value: &str) -> Option<Option<i32>> {
    match value.strip_prefix("HKCategoryValueSleepAnalysis")? {
        "InBed" => Some(None),
        "Asleep" | "AsleepUnspecified" => Some(Some(2)),
        "Awake" => Some(Some(1)),
        "AsleepCore" => Some(Some(4)),
        "AsleepDeep" => Some(Some(5)),
        "AsleepREM" => Some(Some(6)),
        _ => None,
    }
}

fn dietary_mass(kind: &str, value: f64, unit: &str) -> Option<(&'static str, f64, &'static str)> {
    const NUTRIENTS: &[(&str, &str, &str)] = &[
        ("Protein", "dietary_protein", "g"),
        ("Carbohydrates", "dietary_carbohydrate", "g"),
        ("FatTotal", "dietary_fat", "g"),
        ("FatMonounsaturated", "dietary_fat_monounsaturated", "g"),
        ("FatPolyunsaturated", "dietary_fat_polyunsaturated", "g"),
        ("FatSaturated", "dietary_fat_saturated", "g"),
        ("Fiber", "dietary_fiber", "g"),
        ("Sugar", "dietary_sugar", "g"),
        ("Caffeine", "dietary_caffeine", "mg"),
        ("Calcium", "dietary_calcium", "mg"),
        ("Chloride", "dietary_chloride", "mg"),
        ("Cholesterol", "dietary_cholesterol", "mg"),
        ("Copper", "dietary_copper", "mg"),
        ("Iron", "dietary_iron", "mg"),
        ("Magnesium", "dietary_magnesium", "mg"),
        ("Manganese", "dietary_manganese", "mg"),
        ("Niacin", "dietary_niacin", "mg"),
        ("PantothenicAcid", "dietary_pantothenic_acid", "mg"),
        ("Phosphorus", "dietary_phosphorus", "mg"),
        ("Potassium", "dietary_potassium", "mg"),
        ("Riboflavin", "dietary_riboflavin", "mg"),
        ("Sodium", "dietary_sodium", "mg"),
        ("Thiamin", "dietary_thiamin", "mg"),
        ("VitaminB6", "dietary_vitamin_b6", "mg"),
        ("VitaminC", "dietary_vitamin_c", "mg"),
        ("VitaminE", "dietary_vitamin_e", "mg"),
        ("Zinc", "dietary_zinc", "mg"),
        ("Biotin", "dietary_biotin", "mcg"),
        ("Chromium", "dietary_chromium", "mcg"),
        ("Folate", "dietary_folate", "mcg"),
        ("Iodine", "dietary_iodine", "mcg"),
        ("Molybdenum", "dietary_molybdenum", "mcg"),
        ("Selenium", "dietary_selenium", "mcg"),
        ("VitaminA", "dietary_vitamin_a", "mcg"),
        ("VitaminB12", "dietary_vitamin_b12", "mcg"),
        ("VitaminD", "dietary_vitamin_d", "mcg"),
        ("VitaminK", "dietary_vitamin_k", "mcg"),
    ];
    let suffix = kind.strip_prefix("Dietary")?;
    let &(_, metric, output) = NUTRIENTS.iter().find(|(name, _, _)| *name == suffix)?;
    let exponent = |unit: &str| match unit {
        "kg" => Some(3),
        "g" => Some(0),
        "mg" => Some(-3),
        "mcg" | "ug" | "µg" | "μg" => Some(-6),
        _ => None,
    };
    let normalized = value * 10_f64.powi(exponent(unit)? - exponent(output)?);
    (normalized.is_finite() && normalized >= 0.0).then_some((metric, normalized, output))
}

fn metric(kind: &str, value: &str, unit: &str) -> Option<(&'static str, f64, &'static str)> {
    let value = value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())?;
    let kind = kind.strip_prefix("HKQuantityTypeIdentifier")?;
    if let Some(nutrient) = dietary_mass(kind, value, unit) {
        return Some(nutrient);
    }
    let distance_metric = match kind {
        "WalkingStepLength" => Some("walking_step_length"),
        "DistanceWalkingRunning" => Some("walking_running_distance"),
        "DistanceCycling" => Some("cycling_distance"),
        "DistanceWheelchair" => Some("wheelchair_distance"),
        "DistanceSwimming" => Some("swimming_distance"),
        "DistanceDownhillSnowSports" => Some("downhill_snow_sports_distance"),
        "UnderwaterDepth" => Some("underwater_depth"),
        "SixMinuteWalkTestDistance" => Some("six_minute_walk_test_distance"),
        _ => None,
    };
    if let Some(name) = distance_metric {
        return Some((name, distance_meters(value, unit)?, "m"));
    }
    if kind == "WaistCircumference" {
        return Some((
            "waist_circumference",
            100.0 * distance_meters(value, unit)?,
            "cm",
        ));
    }
    if let Some(name) = match kind {
        "RunningStrideLength" => Some("running_stride_length"),
        "RunningVerticalOscillation" => Some("running_vertical_oscillation"),
        _ => None,
    } {
        return Some((name, 100.0 * distance_meters(value, unit)?, "cm"));
    }
    let energy_metric = match kind {
        "ActiveEnergyBurned" => Some("active_energy"),
        "BasalEnergyBurned" => Some("basal_energy"),
        _ => None,
    };
    if let Some(name) = energy_metric {
        return Some((name, energy_kcal(value, unit)?, "kcal"));
    }
    let (metric, output, factor, offset) = match (kind, unit) {
        ("PushCount", "count") if value >= 0.0 && value.fract() == 0.0 => {
            ("wheelchair_pushes", "pushes", 1.0, 0.0)
        }
        ("BodyMass", "kg") => ("weight", "kg", 1.0, 0.0),
        ("BodyMass", "lb") => ("weight", "kg", 0.45359237, 0.0),
        ("LeanBodyMass", "kg") => ("fat_free_mass", "kg", 1.0, 0.0),
        ("LeanBodyMass", "lb") => ("fat_free_mass", "kg", 0.45359237, 0.0),
        ("BodyFatPercentage", "%") => ("body_fat", "%", 100.0, 0.0),
        ("OxygenSaturation", "%") => ("oxygen_saturation", "%", 100.0, 0.0),
        ("AppleExerciseTime", "min") => ("exercise_time", "min", 1.0, 0.0),
        ("AppleExerciseTime", "s") => ("exercise_time", "min", 1.0 / 60.0, 0.0),
        ("AppleMoveTime", "min") => ("move_time", "min", 1.0, 0.0),
        ("AppleMoveTime", "s") => ("move_time", "min", 1.0 / 60.0, 0.0),
        ("AppleStandTime", "min") => ("stand_time", "min", 1.0, 0.0),
        ("AppleStandTime", "s") => ("stand_time", "min", 1.0 / 60.0, 0.0),
        ("FlightsClimbed", "count") => ("flights_climbed", "count", 1.0, 0.0),
        ("PhysicalEffort", "kcal/(kg*hr)" | "kcal/hr·kg" | "MET") => {
            ("physical_effort", "MET", 1.0, 0.0)
        }
        ("StairAscentSpeed", "m/s") => ("stair_ascent_speed", "m/s", 1.0, 0.0),
        ("StairDescentSpeed", "m/s") => ("stair_descent_speed", "m/s", 1.0, 0.0),
        ("AppleWalkingSteadiness", "%") => ("walking_steadiness", "%", 100.0, 0.0),
        ("SwimmingStrokeCount", "count") => ("swimming_stroke_count", "count", 1.0, 0.0),
        ("RunningGroundContactTime", "ms") => ("running_ground_contact_time", "ms", 1.0, 0.0),
        ("RunningGroundContactTime", "s") => ("running_ground_contact_time", "ms", 1_000.0, 0.0),
        ("RunningPower", "W") => ("running_power", "W", 1.0, 0.0),
        ("RunningPower", "kW") => ("running_power", "W", 1_000.0, 0.0),
        ("RunningSpeed", "m/s") => ("running_speed", "m/s", 1.0, 0.0),
        ("RunningSpeed", "km/hr" | "km/h") => ("running_speed", "m/s", 1.0 / 3.6, 0.0),
        ("RunningSpeed", "mi/hr" | "mi/h") => ("running_speed", "m/s", 0.44704, 0.0),
        ("EnvironmentalAudioExposure", "dBASPL") => {
            ("environmental_audio_exposure", "dBASPL", 1.0, 0.0)
        }
        ("EnvironmentalSoundReduction", "dBASPL") => {
            ("environmental_sound_reduction", "dBASPL", 1.0, 0.0)
        }
        ("HeadphoneAudioExposure", "dBASPL") => ("headphone_audio_exposure", "dBASPL", 1.0, 0.0),
        ("InhalerUsage", "count") => ("inhaler_usage", "count", 1.0, 0.0),
        ("TimeInDaylight", "min") => ("time_in_daylight", "min", 1.0, 0.0),
        ("TimeInDaylight", "s") => ("time_in_daylight", "min", 1.0 / 60.0, 0.0),
        ("UVExposure", "count") => ("uv_exposure", "count", 1.0, 0.0),
        ("PeripheralPerfusionIndex", "%") => ("peripheral_perfusion_index", "%", 100.0, 0.0),
        ("BloodAlcoholContent", "%") => ("blood_alcohol_content", "%", 100.0, 0.0),
        ("BodyMassIndex", "count" | "kg/m²") => ("body_mass_index", "kg/m²", 1.0, 0.0),
        ("WalkingHeartRateAverage", "count/min") => ("walking_heart_rate", "bpm", 1.0, 0.0),
        ("WorkoutEffortScore", "appleEffortScore") => ("workout_effort_score", "score", 1.0, 0.0),
        ("EstimatedWorkoutEffortScore", "appleEffortScore") => {
            ("estimated_workout_effort_score", "score", 1.0, 0.0)
        }
        ("ForcedExpiratoryVolume1", "L") => ("forced_expiratory_volume_1", "L", 1.0, 0.0),
        ("ForcedExpiratoryVolume1", "mL") => ("forced_expiratory_volume_1", "L", 0.001, 0.0),
        ("ForcedVitalCapacity", "L") => ("forced_vital_capacity", "L", 1.0, 0.0),
        ("ForcedVitalCapacity", "mL") => ("forced_vital_capacity", "L", 0.001, 0.0),
        ("PeakExpiratoryFlowRate", "L/min") => ("peak_expiratory_flow_rate", "L/min", 1.0, 0.0),
        ("PeakExpiratoryFlowRate", "L/s") => ("peak_expiratory_flow_rate", "L/min", 60.0, 0.0),
        ("AppleSleepingBreathingDisturbances", "count") => {
            ("sleeping_breathing_disturbances", "count", 1.0, 0.0)
        }
        ("WalkingAsymmetryPercentage", "%") => ("walking_asymmetry", "%", 100.0, 0.0),
        ("WalkingDoubleSupportPercentage", "%") => ("walking_double_support", "%", 100.0, 0.0),
        ("WalkingSpeed", "m/s") => ("walking_speed", "m/s", 1.0, 0.0),
        ("WalkingSpeed", "km/hr" | "km/h") => ("walking_speed", "m/s", 1.0 / 3.6, 0.0),
        ("WalkingSpeed", "mi/hr" | "mi/h") => ("walking_speed", "m/s", 0.44704, 0.0),
        ("HeartRateRecoveryOneMinute", "count/min") => {
            ("heart_rate_recovery_one_minute", "bpm", 1.0, 0.0)
        }
        ("RestingHeartRate", "count/min") => ("resting_heart_rate", "bpm", 1.0, 0.0),
        ("RespiratoryRate", "count/min") => ("respiratory_rate", "breaths/min", 1.0, 0.0),
        ("HeartRateVariabilitySDNN", "ms") => ("hrv_sdnn", "ms", 1.0, 0.0),
        ("VO2Max", "mL/min·kg" | "mL/(kg*min)" | "ml/(kg*min)") => {
            ("vo2_max", "mL/kg/min", 1.0, 0.0)
        }
        ("Height", "cm") => ("height", "cm", 1.0, 0.0),
        ("Height", "m") => ("height", "cm", 100.0, 0.0),
        ("Height", "in") => ("height", "cm", 2.54, 0.0),
        ("BloodGlucose", "mg/dL") => ("blood_glucose", "mg/dL", 1.0, 0.0),
        ("BloodGlucose", "mmol/L") => ("blood_glucose", "mg/dL", 18.015588, 0.0),
        ("BloodGlucose", unit) if unit.starts_with("mmol<180.15") && unit.ends_with(">/L") => {
            ("blood_glucose", "mg/dL", 18.015588, 0.0)
        }
        ("BloodPressureSystolic", "mmHg") => ("blood_pressure_systolic", "mmHg", 1.0, 0.0),
        ("BloodPressureDiastolic", "mmHg") => ("blood_pressure_diastolic", "mmHg", 1.0, 0.0),
        ("BodyTemperature", "degC" | "°C") => ("body_temperature", "°C", 1.0, 0.0),
        ("BodyTemperature", "degF" | "°F") => ("body_temperature", "°C", 5.0 / 9.0, -32.0),
        ("AppleSleepingWristTemperature", "degC" | "°C") => {
            ("sleep_wrist_temperature", "°C", 1.0, 0.0)
        }
        ("AppleSleepingWristTemperature", "degF" | "°F") => {
            ("sleep_wrist_temperature", "°C", 5.0 / 9.0, -32.0)
        }
        ("WaterTemperature", "degC" | "°C") => ("water_temperature", "°C", 1.0, 0.0),
        ("WaterTemperature", "degF" | "°F") => ("water_temperature", "°C", 5.0 / 9.0, -32.0),
        ("DietaryWater", "mL") => ("water_intake", "mL", 1.0, 0.0),
        ("DietaryWater", "L") => ("water_intake", "mL", 1000.0, 0.0),
        ("DietaryEnergyConsumed", "kcal") => ("dietary_energy", "kcal", 1.0, 0.0),
        _ => return None,
    };
    let normalized = (value + offset) * factor;
    (normalized.is_finite()
        && (matches!(output, "°C" | "dBASPL")
            || metric == "heart_rate_recovery_one_minute"
            || normalized >= 0.0)
        && (output != "%" || normalized <= 100.0))
        .then_some((metric, normalized, output))
}
