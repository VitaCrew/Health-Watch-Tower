use anyhow::{ensure, Result};
#[cfg(test)]
use chrono::{DateTime, Duration, Utc};
use deadpool_postgres::Pool;
#[cfg(test)]
use fitparser::{
    de::{DecodeOption, FitObject, FitStreamProcessor},
    profile::{
        field_types::{ExerciseCategory, Sport, SubSport},
        MesgNum,
    },
    FitDataRecord, Value as FitValue,
};
use serde::Deserialize;
#[cfg(test)]
use serde_json::Map;
use serde_json::{json, Value};

#[cfg(test)]
use crate::wearable_fit::validate_fit;
use crate::wearable_fit::{valid_id, MAX_PARTS};

#[cfg(test)]
#[path = "wearable_fit_details_tests.rs"]
mod tests;

const MAX_SAMPLES: usize = 100_000;
#[cfg(test)]
const MAX_LAPS: usize = 2_000;
#[cfg(test)]
const MAX_SESSIONS: usize = 128;
#[cfg(test)]
const MAX_LENGTHS: usize = 10_000;
#[cfg(test)]
const MAX_ZONE_REPORTS: usize = 2_000;
#[cfg(test)]
const MAX_EVENTS: usize = 10_000;
#[cfg(test)]
const MAX_DEVICE_MESSAGES: usize = 2_000;
#[cfg(test)]
const MAX_STRENGTH_MESSAGES: usize = 2_000;
#[cfg(test)]
const MAX_DEVELOPER_FIELD_BYTES: usize = 2 * 1024 * 1024;

#[cfg(test)]
#[derive(Default)]
struct CompressedHeartRate {
    anchor: Option<(DateTime<Utc>, u32)>,
    last_tick: u32,
    samples: Vec<Value>,
}

#[cfg(test)]
impl CompressedHeartRate {
    fn observe(&mut self, record: &FitDataRecord, packed: Option<FitValue>, truncated: &mut bool) {
        let get = |name| {
            record
                .fields()
                .iter()
                .find(|field| field.name() == name)
                .map(|field| field.value())
        };
        let event_ticks: Vec<_> = get("event_timestamp")
            .into_iter()
            .flat_map(fit_values)
            .map(|value| {
                TryInto::<f64>::try_into(value.clone())
                    .ok()
                    .map(|seconds| (seconds * 1024.0) as u32)
            })
            .collect();
        if let Some(FitValue::Timestamp(timestamp)) = get("timestamp") {
            let [Some(tick)] = event_ticks.as_slice() else {
                self.anchor = None;
                return;
            };
            let fraction = get("fractional_timestamp")
                .and_then(|value| TryInto::<f64>::try_into(value.clone()).ok())
                .unwrap_or_default();
            let utc = timestamp.with_timezone(&Utc);
            self.anchor = utc
                .checked_add_signed(Duration::nanoseconds((fraction * 1e9).round() as i64))
                .map(|utc| (utc, *tick));
            self.last_tick = *tick;
        }
        let Some((anchor, anchor_tick)) = self.anchor else {
            return;
        };
        let bpms = get("filtered_bpm").map_or(&[][..], fit_values);
        let mut ticks = event_ticks;
        if let Some(packed) = packed {
            let bytes: Option<Vec<_>> = fit_values(&packed)
                .iter()
                .map(|value| match value {
                    FitValue::Byte(byte) | FitValue::UInt8(byte) => Some(*byte),
                    _ => None,
                })
                .collect();
            let Some(bytes) = bytes else { return };
            let required = bpms.len().saturating_sub(ticks.len());
            if bytes.len() * 8 / 12 < required {
                return;
            }
            let mut tick = ticks.last().copied().flatten().unwrap_or(self.last_tick);
            for index in 0..required {
                let bit = index * 12;
                let pair = u32::from(u16::from_le_bytes([bytes[bit / 8], bytes[bit / 8 + 1]]));
                let low = (pair >> (bit % 8)) & 0xfff;
                // Resolve wraparound of the 12-bit clock relative to the previous event.
                tick = tick.wrapping_add(low.wrapping_sub(tick & 0xfff) & 0xfff);
                ticks.push(Some(tick));
            }
        }
        if ticks.len() != bpms.len() {
            return;
        }
        for (tick, bpm) in ticks.into_iter().zip(bpms) {
            let Some(tick) = tick else { continue };
            let delta = tick.wrapping_sub(anchor_tick);
            if delta > i32::MAX as u32 {
                continue;
            }
            self.last_tick = tick;
            let FitValue::UInt8(bpm @ 1..=254) = bpm else {
                continue;
            };
            if self.samples.len() == MAX_SAMPLES {
                *truncated = true;
                return;
            }
            let nanos = (i64::from(delta) * 1_000_000_000 + 512) / 1024;
            let Some(timestamp) = anchor.checked_add_signed(Duration::nanoseconds(nanos)) else {
                continue;
            };
            self.samples
                .push(json!({"timestamp":timestamp,"heart_rate_bpm":bpm}));
        }
    }
}

#[cfg(test)]
fn fit_values(value: &FitValue) -> &[FitValue] {
    match value {
        FitValue::Array(values) => values.as_slice(),
        value => std::slice::from_ref(value),
    }
}

#[cfg(test)]
fn read_fit(mut input: &[u8]) -> Result<Value> {
    validate_fit(input)?;
    let mut processor = FitStreamProcessor::new();
    // The original-file validator already covers the CRC, including the optional header checksum.
    processor.add_option(DecodeOption::SkipHeaderCrcValidation);
    processor.add_option(DecodeOption::SkipDataCrcValidation);
    let (mut sessions, mut laps, mut lengths, mut samples, mut zones, mut dives) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let mut events = Vec::new();
    let mut devices = Vec::new();
    let mut hrv = Vec::new();
    let mut sets = Vec::new();
    let mut exercise_titles = Vec::new();
    let mut splits = Vec::new();
    let mut split_summaries = Vec::new();
    let mut climb_events = Vec::new();
    let mut compressed_hr = CompressedHeartRate::default();
    let mut data_messages = 0usize;
    let mut truncated = false;
    let mut respiration_included = false;
    let mut developer_field_budget = MAX_DEVELOPER_FIELD_BYTES;
    while !input.is_empty() {
        let (remaining, entry) = processor
            .deserialize_next(input)
            .map_err(|_| anyhow::anyhow!("invalid FIT object"))?;
        ensure!(remaining.len() < input.len(), "FIT reader made no progress");
        input = remaining;
        if let FitObject::DataMessage(mut message) = entry {
            let message_index = data_messages;
            data_messages += 1;
            let has_developer_fields = !message.developer_fields().is_empty();
            // fitparser 0.11 expands field 10 to a fixed number of components and adds
            // arrays elementwise. Retain the packed bytes for sequential FIT clock expansion.
            let packed_hr = (message.global_message_number() == MesgNum::Hr.as_u16())
                .then(|| message.fields_mut().remove(&10))
                .flatten();
            let record = processor
                .decode_message(message)
                .map_err(|_| anyhow::anyhow!("unsupported FIT message"))?;
            if record.kind() == MesgNum::Hr {
                compressed_hr.observe(&record, packed_hr, &mut truncated);
                continue;
            }
            let (target, cap) = match record.kind() {
                MesgNum::Session => (&mut sessions, MAX_SESSIONS),
                MesgNum::Lap => (&mut laps, MAX_LAPS),
                MesgNum::Length => (&mut lengths, MAX_LENGTHS),
                MesgNum::TimeInZone => (&mut zones, MAX_ZONE_REPORTS),
                MesgNum::DiveSummary => (&mut dives, MAX_SESSIONS),
                MesgNum::Record => (&mut samples, MAX_SAMPLES),
                MesgNum::Event => (&mut events, MAX_EVENTS),
                MesgNum::DeviceInfo | MesgNum::FileId => (&mut devices, MAX_DEVICE_MESSAGES),
                MesgNum::Hrv => (&mut hrv, MAX_SAMPLES),
                MesgNum::Set => (&mut sets, MAX_STRENGTH_MESSAGES),
                MesgNum::ExerciseTitle => (&mut exercise_titles, MAX_STRENGTH_MESSAGES),
                MesgNum::Split => (&mut splits, MAX_LAPS),
                MesgNum::SplitSummary => (&mut split_summaries, MAX_LAPS),
                MesgNum::ClimbPro => (&mut climb_events, MAX_LAPS),
                _ => continue,
            };
            if target.len() >= cap {
                truncated = true;
                continue;
            }
            let (record, custom, clipped) = if has_developer_fields {
                split_developer_fields(record, &mut developer_field_budget)
            } else {
                (record, Vec::new(), false)
            };
            let mut fields = match record.kind() {
                MesgNum::Event => project_event(&record).unwrap_or_default(),
                MesgNum::DeviceInfo | MesgNum::FileId => project_device(&record),
                MesgNum::Hrv => project_hrv(&record, message_index).unwrap_or_default(),
                _ => project(&record),
            };
            let has_custom = !custom.is_empty();
            if has_custom
                && !matches!(record.kind(), MesgNum::DeviceInfo | MesgNum::FileId)
                && (record.kind() != MesgNum::Event || !fields.is_empty())
            {
                fields.insert("developer_fields".into(), Value::Array(custom));
            }
            truncated |= clipped;
            if record.kind() == MesgNum::Record
                && !has_custom
                && (!fields.contains_key("timestamp") || fields.len() < 2)
            {
                continue;
            }
            if !fields.is_empty() {
                respiration_included |= fields.contains_key("respiration_rate_breaths_per_minute")
                    || fields.contains_key("average_respiration_rate_breaths_per_minute")
                    || fields.contains_key("max_respiration_rate_breaths_per_minute")
                    || fields.contains_key("min_respiration_rate_breaths_per_minute");
                target.push(fields);
            }
        }
    }
    ensure!(
        !(sessions.is_empty()
            && laps.is_empty()
            && lengths.is_empty()
            && samples.is_empty()
            && zones.is_empty()
            && dives.is_empty()
            && events.is_empty()
            && hrv.is_empty()
            && sets.is_empty()
            && exercise_titles.is_empty()
            && splits.is_empty()
            && split_summaries.is_empty()
            && climb_events.is_empty()
            && compressed_hr.samples.is_empty()),
        "FIT contains no supported activity fields"
    );
    Ok(
        json!({"sessions":sessions,"laps":laps,"lengths":lengths,"samples":samples,"zones":zones,"dives":dives,
               "events":events,"devices":devices,"hrv":hrv,"compressed_heart_rate":compressed_hr.samples,
               "sets":sets,"exercise_titles":exercise_titles,
               "splits":splits,"split_summaries":split_summaries,"climb_events":climb_events,
               "truncated":truncated,"developer_fields_included":true,"cycling_dynamics_included":true,
               "respiration_included":respiration_included}),
    )
}

#[cfg(test)]
fn project_hrv(record: &FitDataRecord, message_index: usize) -> Option<Map<String, Value>> {
    let field = record
        .fields()
        .iter()
        .find(|field| field.name() == "time")?;
    let values = fit_values(field.value());
    // The FIT profile scales this field to seconds. Keep source order and missing slots.
    let intervals: Vec<_> = values
        .iter()
        .map(|value| {
            let seconds: f64 = value.clone().try_into().ok()?;
            let millis = (seconds * 1000.0).round();
            (1.0..f64::from(u16::MAX))
                .contains(&millis)
                .then_some(millis as u16)
        })
        .collect();
    intervals.iter().any(Option::is_some).then(|| {
        Map::from_iter([
            ("message_index".into(), json!(message_index)),
            ("rr_intervals_ms".into(), json!(intervals)),
        ])
    })
}

#[cfg(test)]
fn project_device(record: &FitDataRecord) -> Map<String, Value> {
    let mut fields = Map::new();
    for field in record.fields() {
        let name = match field.name() {
            "timestamp"
            | "time_created"
            | "manufacturer"
            | "product"
            | "garmin_product"
            | "favero_product"
            | "product_name"
            | "device_index"
            | "device_type"
            | "antplus_device_type"
            | "ant_device_type"
            | "source_type"
            | "software_version"
            | "hardware_version"
            | "battery_voltage"
            | "battery_status" => field.name(),
            _ => continue,
        };
        let value = match field.value() {
            FitValue::Timestamp(at) => json!(at.with_timezone(&Utc)),
            FitValue::String(text) if !text.is_empty() && text.len() <= 256 => json!(text),
            raw => {
                let Ok(number) = TryInto::<f64>::try_into(raw.clone()) else {
                    continue;
                };
                if !number.is_finite() {
                    continue;
                }
                json!(number)
            }
        };
        fields.insert(name.to_owned(), value);
    }
    if !fields.is_empty() {
        let message_type = if record.kind() == MesgNum::FileId {
            "file_id"
        } else {
            "device_info"
        };
        fields.insert("message_type".into(), json!(message_type));
    }
    fields
}

#[cfg(test)]
fn project(record: &FitDataRecord) -> Map<String, Value> {
    let mut values = Map::new();
    for field in record.fields() {
        if field.name() == "left_right_balance" {
            if let Some(right) = right_power_balance(record.kind(), field.value()) {
                values.insert("right_power_balance_percent".into(), json!(right));
                values.insert("left_power_balance_percent".into(), json!(100.0 - right));
            }
            continue;
        }
        let (name, priority) = match field.name() {
            "timestamp" => ("timestamp", true),
            "start_time" => ("started_at", true),
            "end_time" if record.kind() == MesgNum::Split => ("ended_at", true),
            "sport" => ("sport", true),
            "sub_sport" => ("sub_sport", true),
            "swim_stroke" => ("swim_stroke", true),
            "length_type" => ("length_type", true),
            "duration" if record.kind() == MesgNum::Set => ("duration_seconds", true),
            "repetitions" if record.kind() == MesgNum::Set => ("repetitions", true),
            "weight" if record.kind() == MesgNum::Set => ("weight_kg", true),
            "set_type" => ("set_type", true),
            "category" if record.kind() == MesgNum::Set => ("categories", true),
            "category_subtype" if record.kind() == MesgNum::Set => ("category_subtypes", true),
            "weight_display_unit" => ("weight_display_unit", true),
            "wkt_step_index" => ("workout_step_index", true),
            "exercise_category" => ("exercise_category", true),
            "exercise_name" => ("exercise_name_code", true),
            "wkt_step_name" => ("title", true),
            "split_type" => ("split_type", true),
            "num_splits" => ("split_count", true),
            "avg_vert_speed" => ("average_vertical_speed_mps", true),
            "max_speed" if matches!(record.kind(), MesgNum::Split | MesgNum::SplitSummary) => {
                ("maximum_speed_mps", true)
            }
            "start_position_lat" if record.kind() == MesgNum::Split => ("start_latitude_deg", true),
            "start_position_long" if record.kind() == MesgNum::Split => {
                ("start_longitude_deg", true)
            }
            "end_position_lat" if record.kind() == MesgNum::Split => ("end_latitude_deg", true),
            "end_position_long" if record.kind() == MesgNum::Split => ("end_longitude_deg", true),
            "start_elevation" => ("start_elevation_m", true),
            "active_time" => ("active_seconds", true),
            "total_moving_time" => ("moving_seconds", true),
            "climb_pro_event" => ("event", true),
            "climb_number" => ("climb_number", true),
            "climb_category" => ("climb_category", true),
            "current_dist" if record.kind() == MesgNum::ClimbPro => ("distance_m", true),
            "message_index" => ("message_index", true),
            "first_length_index" => ("first_length_index", true),
            "num_lengths" => ("length_count", true),
            "num_active_lengths" => ("active_length_count", true),
            "pool_length" => ("pool_length_m", true),
            "total_strokes" => ("stroke_count", true),
            "avg_swimming_cadence" => ("average_swim_cadence_strokes_per_minute", true),
            "time_in_hr_zone" => ("heart_rate_zone_seconds", true),
            "time_in_power_zone" => ("power_zone_seconds", true),
            "hr_zone_high_boundary" => ("heart_rate_zone_upper_bpm", true),
            "power_zone_high_boundary" => ("power_zone_upper_watts", true),
            "reference_mesg" => ("reference_message", true),
            "reference_index" => ("reference_index", true),
            "resting_heart_rate" => ("resting_heart_rate_bpm", true),
            "threshold_heart_rate" => ("threshold_heart_rate_bpm", true),
            "functional_threshold_power" => ("functional_threshold_power_watts", true),
            "total_timer_time" => ("timer_seconds", true),
            "total_elapsed_time" => ("elapsed_seconds", true),
            "total_distance" | "distance" => ("distance_m", true),
            "total_calories" => ("calories_kcal", true),
            "total_ascent" => ("ascent_m", true),
            "total_descent" => ("descent_m", true),
            "heart_rate" => ("heart_rate_bpm", true),
            "avg_heart_rate" => ("average_heart_rate_bpm", true),
            "max_heart_rate" => ("max_heart_rate_bpm", true),
            "enhanced_respiration_rate" => ("respiration_rate_breaths_per_minute", true),
            "enhanced_avg_respiration_rate" => {
                ("average_respiration_rate_breaths_per_minute", true)
            }
            "enhanced_max_respiration_rate" => ("max_respiration_rate_breaths_per_minute", true),
            "enhanced_min_respiration_rate" => ("min_respiration_rate_breaths_per_minute", true),
            "cadence" => ("cadence_rpm", true),
            "avg_cadence" => ("average_cadence_rpm", true),
            "power" => ("power_w", true),
            "avg_power" => ("average_power_w", true),
            "max_power" => ("max_power_w", true),
            "normalized_power" => ("normalized_power_w", true),
            "left_torque_effectiveness" => ("left_torque_effectiveness_percent", true),
            "right_torque_effectiveness" => ("right_torque_effectiveness_percent", true),
            "avg_left_torque_effectiveness" => ("average_left_torque_effectiveness_percent", true),
            "avg_right_torque_effectiveness" => {
                ("average_right_torque_effectiveness_percent", true)
            }
            "left_pedal_smoothness" => ("left_pedal_smoothness_percent", true),
            "right_pedal_smoothness" => ("right_pedal_smoothness_percent", true),
            "combined_pedal_smoothness" => ("combined_pedal_smoothness_percent", true),
            "avg_left_pedal_smoothness" => ("average_left_pedal_smoothness_percent", true),
            "avg_right_pedal_smoothness" => ("average_right_pedal_smoothness_percent", true),
            "avg_combined_pedal_smoothness" => ("average_combined_pedal_smoothness_percent", true),
            "left_pco" => ("left_platform_center_offset_mm", true),
            "right_pco" => ("right_platform_center_offset_mm", true),
            "avg_left_pco" => ("average_left_platform_center_offset_mm", true),
            "avg_right_pco" => ("average_right_platform_center_offset_mm", true),
            "left_power_phase" => ("left_power_phase_degrees", true),
            "right_power_phase" => ("right_power_phase_degrees", true),
            "left_power_phase_peak" => ("left_power_phase_peak_degrees", true),
            "right_power_phase_peak" => ("right_power_phase_peak_degrees", true),
            "avg_left_power_phase" => ("average_left_power_phase_degrees", true),
            "avg_right_power_phase" => ("average_right_power_phase_degrees", true),
            "avg_left_power_phase_peak" => ("average_left_power_phase_peak_degrees", true),
            "avg_right_power_phase_peak" => ("average_right_power_phase_peak_degrees", true),
            "step_length" => ("step_length_mm", true),
            "avg_step_length" => ("average_step_length_mm", true),
            "vertical_oscillation" => ("vertical_oscillation_mm", true),
            "avg_vertical_oscillation" => ("average_vertical_oscillation_mm", true),
            "vertical_ratio" => ("vertical_ratio_percent", true),
            "avg_vertical_ratio" => ("average_vertical_ratio_percent", true),
            "stance_time" => ("ground_contact_time_ms", true),
            "avg_stance_time" => ("average_ground_contact_time_ms", true),
            "stance_time_percent" => ("ground_contact_time_percent", true),
            "avg_stance_time_percent" => ("average_ground_contact_time_percent", true),
            "stance_time_balance" => ("ground_contact_balance_percent", true),
            "avg_stance_time_balance" => ("average_ground_contact_balance_percent", true),
            "altitude" => ("altitude_m", false),
            "enhanced_altitude" => ("altitude_m", true),
            "speed" => ("speed_mps", false),
            "enhanced_speed" => ("speed_mps", true),
            "avg_speed" => ("average_speed_mps", false),
            "enhanced_avg_speed" => ("average_speed_mps", true),
            "temperature" => ("temperature_c", true),
            "depth" => ("underwater_depth_m", true),
            "avg_depth" => ("average_depth_m", true),
            "max_depth" => ("max_depth_m", true),
            "surface_interval" => ("surface_interval_seconds", true),
            "bottom_time" => ("bottom_time_seconds", true),
            "dive_number" => ("dive_number", true),
            "dive_number_in_series" => ("dive_number", false),
            "dive_mode" => ("dive_mode", true),
            "end_cns" => ("end_cns_percent", true),
            "o2_toxicity" => ("oxygen_toxicity_percent", true),
            "position_lat" => ("latitude_deg", true),
            "position_long" => ("longitude_deg", true),
            _ => {
                if let Some((name, value)) = project_new_split_field(record.kind(), field) {
                    values.entry(name).or_insert(value);
                }
                continue;
            }
        };
        if !priority && values.contains_key(name) {
            continue;
        }
        let value = match field.value() {
            FitValue::Timestamp(at) => json!(at.with_timezone(&Utc)),
            FitValue::String(text)
                if !text.is_empty()
                    && text.len() <= 256
                    && matches!(
                        name,
                        "sport"
                            | "sub_sport"
                            | "swim_stroke"
                            | "length_type"
                            | "reference_message"
                            | "set_type"
                            | "weight_display_unit"
                            | "exercise_category"
                            | "title"
                            | "split_type"
                            | "event"
                    ) =>
            {
                json!(text)
            }
            raw if matches!(name, "categories" | "category_subtypes") => {
                // Indices pair the two fields, so invalid entries must not shift their neighbors.
                let entries: Vec<_> = fit_values(raw)
                    .iter()
                    .map(|entry| match entry {
                        FitValue::String(text) if !text.is_empty() && text.len() <= 256 => {
                            json!(text)
                        }
                        raw => TryInto::<i64>::try_into(raw.clone())
                            .ok()
                            .filter(|code| (0..i64::from(u16::MAX)).contains(code))
                            .map_or(Value::Null, |code| {
                                if name == "categories" && ExerciseCategory::is_named_variant(code)
                                {
                                    json!(ExerciseCategory::from(code).to_string())
                                } else {
                                    json!(code)
                                }
                            }),
                    })
                    .collect();
                if entries.iter().all(Value::is_null) {
                    continue;
                }
                json!(entries)
            }
            raw if matches!(
                name,
                "heart_rate_zone_seconds"
                    | "power_zone_seconds"
                    | "heart_rate_zone_upper_bpm"
                    | "power_zone_upper_watts"
            ) =>
            {
                let durations = match raw {
                    FitValue::Array(durations) => durations.as_slice(),
                    single => std::slice::from_ref(single),
                };
                // Missing values occupy their original zone position instead of shifting later zones.
                let zone_values: Vec<_> = durations
                    .iter()
                    .map(|duration| {
                        TryInto::<f64>::try_into(duration.clone())
                            .ok()
                            .filter(|num| num.is_finite() && *num >= 0.0)
                    })
                    .collect();
                if zone_values.iter().all(Option::is_none) {
                    continue;
                }
                json!(zone_values)
            }
            FitValue::Array(angles) if name.ends_with("_degrees") => {
                let degrees: Vec<_> = angles
                    .iter()
                    .take(4)
                    .map(|angle| {
                        TryInto::<f64>::try_into(angle.clone())
                            .ok()
                            .filter(|value| (0.0..=360.0).contains(value))
                    })
                    .collect();
                if degrees.iter().all(Option::is_none) {
                    continue;
                }
                json!(degrees)
            }
            _ if name.ends_with("_degrees") => continue,
            raw => {
                let Ok(mut num) = TryInto::<f64>::try_into(raw.clone()) else {
                    continue;
                };
                let latitude = name.ends_with("latitude_deg");
                let longitude = name.ends_with("longitude_deg");
                if latitude || longitude {
                    num *= 180.0 / 2147483648.0;
                }
                if !num.is_finite()
                    || (latitude && num.abs() > 90.0)
                    || (longitude && num.abs() > 180.0)
                    || (name.ends_with("_depth_m") && num < 0.0)
                    || (name.ends_with("heart_rate_bpm") && !(1.0..=350.0).contains(&num))
                {
                    continue;
                }
                json!(num)
            }
        };
        values.insert(name.to_owned(), value);
    }
    values
}

#[cfg(test)]
fn right_power_balance(kind: MesgNum, value: &FitValue) -> Option<f64> {
    // Record samples use whole percentages; lap and session summaries use hundredths.
    let (mask, flag, scale) = match kind {
        MesgNum::Record => (0x7f, 0x80, 1.0),
        MesgNum::Session | MesgNum::Lap => (0x3fff, 0x8000, 100.0),
        _ => return None,
    };
    let raw = match value {
        // fitparser renders named enum values as strings.
        FitValue::String(side) if side == "right" => flag,
        _ => {
            let raw: i64 = value.clone().try_into().ok()?;
            u16::try_from(raw).ok()?
        }
    };
    let percent = f64::from(raw & mask) / scale;
    if raw & !(mask | flag) != 0 || percent > 100.0 {
        return None;
    }
    Some(if raw & flag != 0 {
        percent
    } else {
        100.0 - percent
    })
}

#[cfg(test)]
fn project_new_split_field(
    kind: MesgNum,
    field: &fitparser::FitDataField,
) -> Option<(&'static str, Value)> {
    use MesgNum::{Split, SplitSummary};
    // These standard fields postdate the profile bundled with fitparser 0.11.
    // Only interpret unknown fields of these two message types, by their published field numbers.
    if !matches!(kind, Split | SplitSummary) || !field.name().starts_with("unknown_field_") {
        return None;
    }
    let raw: i64 = field.value().clone().try_into().ok()?;
    let number = field.number();
    match (kind, number) {
        (_, 253) | (SplitSummary, 79) => {
            let name = if number == 253 {
                "timestamp"
            } else {
                "started_at"
            };
            let utc = DateTime::<Utc>::from_timestamp(
                i64::from(u32::try_from(raw).ok()?) + 631_065_600,
                0,
            )?;
            return Some((name, json!(utc)));
        }
        (Split, 11) | (SplitSummary, 1) if Sport::is_named_variant(raw) => {
            return Some(("sport", json!(Sport::from(raw).to_string())));
        }
        (Split, 12) | (SplitSummary, 2) if SubSport::is_named_variant(raw) => {
            return Some(("sub_sport", json!(SubSport::from(raw).to_string())));
        }
        _ => {}
    }
    let (name, scale) = match (kind, number) {
        (Split, 11) | (SplitSummary, 1) => ("sport", 1.0),
        (Split, 12) | (SplitSummary, 2) => ("sub_sport", 1.0),
        (Split, 15) => ("average_heart_rate_bpm", 1.0),
        (Split, 16) => ("max_heart_rate_bpm", 1.0),
        (Split, 124) | (SplitSummary, 87) => ("min_heart_rate_bpm", 1.0),
        (Split, 29) | (SplitSummary, 14) => ("average_cadence_rpm", 128.0),
        (Split, 30) | (SplitSummary, 15) => ("max_cadence_rpm", 128.0),
        (Split, 35) | (SplitSummary, 20) => ("average_vertical_oscillation_mm", 10.0),
        (Split, 36) | (SplitSummary, 21) => ("average_vertical_ratio_percent", 100.0),
        (Split, 37) | (SplitSummary, 22) => ("average_ground_contact_time_ms", 10.0),
        (Split, 38) | (SplitSummary, 23) => ("average_ground_contact_balance_percent", 100.0),
        (Split, 39) | (SplitSummary, 24) => ("average_step_length_mm", 10.0),
        (Split, 40) | (SplitSummary, 25) => ("average_power_w", 1.0),
        (Split, 41) | (SplitSummary, 26) => ("max_power_w", 1.0),
        (Split, 42) | (SplitSummary, 27) => ("normalized_power_w", 1.0),
        (Split, 88) => ("average_grade_percent", 100.0),
        (Split, 89) => ("max_grade_percent", 100.0),
        (Split, 90) => ("min_cadence_rpm", 128.0),
        (Split, 93) | (SplitSummary, 71) => ("average_grade_adjusted_speed_mps", 1000.0),
        _ => return None,
    };
    if name.ends_with("heart_rate_bpm") && !(1..=350).contains(&raw) {
        return None;
    }
    Some((name, json!(raw as f64 / scale)))
}

#[cfg(test)]
fn split_developer_fields(
    record: FitDataRecord,
    budget: &mut usize,
) -> (FitDataRecord, Vec<Value>, bool) {
    // fitparser 0.11 exposes the developer index through its Serialize implementation.
    let mut standard = FitDataRecord::new(record.kind());
    let mut fields = Vec::new();
    for field in record.into_vec() {
        let Ok(value) = serde_json::to_value(&field) else {
            continue;
        };
        if let Some(index) = value["developer_data_index"].as_u64() {
            if !matches!(field.value(), FitValue::Invalid) {
                fields.push((index, field.number(), value));
            }
        } else {
            standard.push(field);
        }
    }
    fields.sort_by_key(|(index, number, _)| (*index, *number));
    let mut result = Vec::new();
    let mut clipped = false;
    for (_, _, field) in fields {
        // Include space for array separators and record keys in the storage budget.
        let size = field.to_string().len().saturating_add(24);
        if size > *budget {
            clipped = true;
            continue;
        }
        *budget -= size;
        result.push(field);
    }
    (standard, result, clipped)
}

#[cfg(test)]
fn project_event(record: &FitDataRecord) -> Option<Map<String, Value>> {
    let mut event = Map::new();
    let mut gear_change = None;
    for field in record.fields() {
        let name = field.name();
        let value = match (name, field.value()) {
            ("timestamp", FitValue::Timestamp(at)) => json!(at.with_timezone(&Utc)),
            (
                "event" | "event_type" | "timer_trigger" | "rider_position",
                FitValue::String(text),
            ) => {
                json!(text)
            }
            (
                "event" | "event_type" | "event_group" | "timer_trigger" | "rider_position"
                | "course_point_index" | "front_gear_num" | "front_gear" | "rear_gear_num"
                | "rear_gear" | "gear_change_data",
                raw,
            ) => {
                let Ok(value) = TryInto::<f64>::try_into(raw.clone()) else {
                    continue;
                };
                if !(0.0..=f64::from(u32::MAX)).contains(&value) || value.fract() != 0.0 {
                    continue;
                }
                if name == "gear_change_data" {
                    gear_change = Some(value as u32);
                    continue;
                }
                json!(value as u32)
            }
            _ => continue,
        };
        event.insert(name.to_owned(), value);
    }
    if let Some(data) = gear_change {
        // The decoder exposes the packed FIT subfield without always expanding its components.
        for (field, value) in ["front_gear", "front_gear_num", "rear_gear", "rear_gear_num"]
            .into_iter()
            .zip(data.to_le_bytes())
        {
            if value != 0 {
                event.entry(field).or_insert(json!(value));
            }
        }
    }
    (event.contains_key("timestamp")
        && event.contains_key("event")
        && event.contains_key("event_type"))
    .then_some(event)
}

#[cfg(test)]
async fn process_one(pool: &Pool) -> Result<bool> {
    let db = pool.get().await?;
    let Some(row)=db.query_opt("SELECT f.subject_id,f.provider,f.activity_id,f.part,f.sha256,f.data FROM avatar_hsm.wearable_fit f
        WHERE (NOT f.developer_fields_parsed OR NOT f.devices_parsed OR NOT f.hrv_parsed OR NOT f.compressed_heart_rate_parsed OR NOT f.strength_parsed OR NOT f.splits_parsed OR NOT f.cycling_dynamics_parsed OR NOT f.respiration_parsed) AND (f.provider<>'garmin' OR EXISTS (SELECT 1 FROM avatar_hsm.wearable_oauth_connection c
          WHERE c.subject_id=f.subject_id AND c.provider='garmin' AND (c.garmin_permissions & 1)<>0))
        ORDER BY f.captured_at LIMIT 1", &[]).await? else {return Ok(false);};
    let bytes: Vec<u8> = row.get(5);
    drop(db);
    let detail = tokio::task::spawn_blocking(move || read_fit(&bytes))
        .await
        .ok()
        .and_then(Result::ok);
    let (subject, provider, activity, part, hash): (String, String, String, i16, Vec<u8>) =
        (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
    let state = if detail.is_some() {
        "ready"
    } else {
        "unavailable"
    };
    pool.get().await?.execute("UPDATE avatar_hsm.wearable_fit
        SET detail_state=CASE WHEN $7::jsonb IS NULL AND detail IS NOT NULL THEN detail_state ELSE $6 END,
            detail=COALESCE($7::jsonb, detail),
            events_parsed=true, developer_fields_parsed=true, devices_parsed=true, hrv_parsed=true,
            compressed_heart_rate_parsed=true, strength_parsed=true, splits_parsed=true, cycling_dynamics_parsed=true,
            respiration_parsed=true
        WHERE subject_id=$1 AND provider=$2 AND activity_id=$3 AND part=$4 AND sha256=$5
          AND (NOT developer_fields_parsed OR NOT devices_parsed OR NOT hrv_parsed OR NOT compressed_heart_rate_parsed OR NOT strength_parsed OR NOT splits_parsed OR NOT cycling_dynamics_parsed OR NOT respiration_parsed)",
        &[&subject,&provider,&activity,&part,&hash,&state,&detail]).await?;
    Ok(true)
}

#[cfg(test)]
pub async fn worker(pool: Pool) {
    loop {
        let pause = if matches!(process_one(&pool).await, Ok(true)) {
            1
        } else {
            10
        };
        tokio::time::sleep(std::time::Duration::from_secs(pause)).await;
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    provider: String,
    source_id: String,
    part: Option<i16>,
    section: Option<String>,
    offset: Option<i64>,
    limit: Option<i64>,
}

pub async fn query(pool: &Pool, subject: &str, query: Query) -> Result<Value> {
    let section = query.section.as_deref().unwrap_or("sessions");
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(50);
    ensure!(
        valid_id(&query.provider)
            && valid_id(&query.source_id)
            && matches!(
                section,
                "sessions"
                    | "laps"
                    | "lengths"
                    | "samples"
                    | "zones"
                    | "dives"
                    | "events"
                    | "devices"
                    | "hrv"
                    | "compressed_heart_rate"
                    | "sets"
                    | "exercise_titles"
                    | "splits"
                    | "split_summaries"
                    | "climb_events"
            )
            && query
                .part
                .is_none_or(|part| (0..=MAX_PARTS as i16).contains(&part))
            && (0..=MAX_SAMPLES as i64).contains(&offset)
            && (1..=100).contains(&limit),
        "invalid FIT details query"
    );
    let db = pool.get().await?;
    let row=db.query_opt("WITH fit AS (SELECT part,detail_state,detail,events_parsed,developer_fields_parsed,devices_parsed,hrv_parsed,compressed_heart_rate_parsed,strength_parsed,splits_parsed,cycling_dynamics_parsed,respiration_parsed FROM avatar_hsm.wearable_fit
          WHERE subject_id=$1 AND provider=$2 AND activity_id=$3 AND ($4::smallint IS NULL OR part=$4)
          AND ($2<>'garmin' OR EXISTS (SELECT 1 FROM avatar_hsm.wearable_oauth_connection c WHERE c.subject_id=$1 AND c.provider=$2 AND (c.garmin_permissions & 1)<>0))
          AND ($2<>'intervals_icu' OR NOT EXISTS (SELECT 1 FROM avatar_hsm.wearable_oauth_connection c WHERE c.subject_id=$1 AND c.provider=$2 AND c.intervals_activity_granted IS FALSE))
          ORDER BY part LIMIT 1)
        SELECT part,CASE WHEN $5 = 'events' AND NOT events_parsed THEN 'pending'
                         WHEN $5 = 'events' AND NOT COALESCE(detail ? 'events',false) THEN 'unavailable'
                         WHEN $5 = 'devices' AND NOT devices_parsed THEN 'pending'
                         WHEN $5 = 'devices' AND NOT COALESCE(detail ? 'devices',false) THEN 'unavailable'
                         WHEN $5 = 'hrv' AND NOT hrv_parsed THEN 'pending'
                         WHEN $5 = 'hrv' AND NOT COALESCE(detail ? 'hrv',false) THEN 'unavailable'
                         WHEN $5 = 'compressed_heart_rate' AND NOT compressed_heart_rate_parsed THEN 'pending'
                         WHEN $5 = 'compressed_heart_rate' AND NOT COALESCE(detail ? 'compressed_heart_rate',false) THEN 'unavailable'
                         WHEN $5 IN ('sets','exercise_titles') AND NOT strength_parsed THEN 'pending'
                         WHEN $5 IN ('sets','exercise_titles') AND NOT COALESCE(detail ? $5,false) THEN 'unavailable'
                         WHEN $5 IN ('splits','split_summaries','climb_events') AND NOT splits_parsed THEN 'pending'
                         WHEN $5 IN ('splits','split_summaries','climb_events') AND NOT COALESCE(detail ? $5,false) THEN 'unavailable'
                         ELSE detail_state END,
          COALESCE((detail->>'truncated')::boolean,false),COALESCE(jsonb_array_length(detail->$5::text),0),
          COALESCE((SELECT jsonb_agg(value ORDER BY ordinal) FROM jsonb_array_elements(detail->$5::text) WITH ORDINALITY AS item(value,ordinal)
                     WHERE ordinal>$6::bigint AND ordinal<=$6::bigint+$7::bigint),'[]'::jsonb),
          CASE WHEN NOT developer_fields_parsed THEN 'pending'
               WHEN COALESCE(detail->>'developer_fields_included','false')='true' THEN 'ready'
               ELSE 'unavailable' END,
          CASE WHEN NOT cycling_dynamics_parsed THEN 'pending'
               WHEN COALESCE(detail->>'cycling_dynamics_included','false')='true' THEN 'ready'
               ELSE 'unavailable' END,
          CASE WHEN NOT respiration_parsed THEN 'pending'
               WHEN COALESCE(detail->>'respiration_included','false')='true' THEN 'ready'
               ELSE 'unavailable' END FROM fit",
        &[&subject,&query.provider,&query.source_id,&query.part,&section,&offset,&limit]).await?;
    let Some(row) = row else {
        return Ok(json!({"state":"not_found","records":[]}));
    };
    let count = i64::from(row.get::<_, i32>(3));
    Ok(
        json!({"state":row.get::<_,String>(1),"part":row.get::<_,i16>(0),"section":section,"total":count,"records":row.get::<_,Value>(4),
        "projection_truncated":row.get::<_,bool>(2),"developer_fields_state":row.get::<_,String>(5),
        "cycling_dynamics_state":row.get::<_,String>(6),
        "respiration_state":row.get::<_,String>(7),
        "next_offset":(offset+limit<count).then_some(offset+limit)}),
    )
}
