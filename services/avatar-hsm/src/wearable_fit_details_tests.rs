use super::*;
use anyhow::Context;
use std::io::Read;

fn fixture() -> Vec<u8> {
    fixture_with_sample_time(true)
}

fn fixture_with_sample_time(include_sample_time: bool) -> Vec<u8> {
    // Two developer fields with the same number and name in distinct developer namespaces.
    let mut data = vec![
        0x46, 0, 0, 206, 0, 6, 0, 1, 0x02, 1, 1, 0x02, 2, 1, 0x02, 3, 11, 0x07, 6, 1, 0x02, 8, 2,
        0x07,
    ];
    for index in [0, 1] {
        data.extend([6, index, 3, 0x84]);
        data.extend(b"heart_rate\0");
        data.extend([10, b'W', 0]);
    }
    data.extend([
        0x60, 0, 0, 20, 0, 14, 253, 4, 0x86, 3, 1, 0x02, 5, 4, 0x86, 6, 2, 0x84, 39, 2, 0x84, 41,
        2, 0x84, 83, 2, 0x84, 85, 2, 0x84, 92, 4, 0x86, 30, 1, 0x02, 43, 1, 0x02, 45, 1, 0x02, 69,
        2, 0x02, 108, 2, 0x84, 2, 3, 2, 1, 3, 2, 0,
    ]);
    for (second, heart_rate) in [(1_100_000_000u32, 73), (1_100_000_001, 81)] {
        data.push(0);
        let timestamp = if include_sample_time {
            second
        } else {
            u32::MAX
        };
        data.extend(timestamp.to_le_bytes());
        data.push(heart_rate);
        data.extend(12_345u32.to_le_bytes());
        data.extend(2_450u16.to_le_bytes());
        data.extend(825u16.to_le_bytes());
        data.extend(2_564u16.to_le_bytes());
        data.extend(750u16.to_le_bytes());
        data.extend(11_000u16.to_le_bytes());
        let depth = if heart_rate == 73 { 0u32 } else { 12_500u32 };
        data.extend(depth.to_le_bytes());
        let balance = if heart_rate == 73 { 0x80 | 51 } else { 51 };
        data.extend([balance, 160, 55, 3, 131]);
        let respiration = if heart_rate == 73 { 1890u16 } else { u16::MAX };
        data.extend(respiration.to_le_bytes());
        data.extend(1550u16.to_le_bytes());
        data.extend(825u16.to_le_bytes());
    }
    // Session (18): timestamp, zones, pool length, and respiration summary.
    data.extend([
        0x41, 0, 0, 18, 0, 7, 253, 4, 0x86, 65, 12, 0x86, 68, 4, 0x86, 44, 2, 0x84, 169, 2, 0x84,
        170, 2, 0x84, 180, 2, 0x84,
    ]);
    data.push(1);
    for value in [1_100_000_100u32, 12_500, u32::MAX, 27_000, 37_000] {
        data.extend(value.to_le_bytes());
    }
    data.extend(2_500u16.to_le_bytes());
    for value in [1977u16, 2743, 1150] {
        data.extend(value.to_le_bytes());
    }
    // Length (101): start, timer, strokes, stroke type, and active/idle marker.
    data.extend([
        0x42, 0, 0, 101, 0, 5, 2, 4, 0x86, 4, 4, 0x86, 5, 2, 0x84, 7, 1, 0x00, 12, 1, 0x00,
    ]);
    for (start, timer, strokes, stroke, kind) in [
        (1_100_000_000u32, 24_500u32, 18u16, 0, 1),
        (1_100_000_025, 12_000, 0, 0xff, 0),
    ] {
        data.push(2);
        data.extend(start.to_le_bytes());
        data.extend(timer.to_le_bytes());
        data.extend(strokes.to_le_bytes());
        data.extend([stroke, kind]);
    }
    // Time In Zone (216): session reference, zone durations, boundaries, and thresholds.
    data.extend([
        0x43, 0, 0, 216, 0, 8, 0, 2, 0x84, 1, 2, 0x84, 2, 12, 0x86, 5, 4, 0x86, 6, 2, 0x02, 9, 2,
        0x84, 13, 1, 0x02, 15, 2, 0x84,
    ]);
    data.push(3);
    data.extend(18u16.to_le_bytes());
    data.extend(0u16.to_le_bytes());
    for value in [12_500u32, u32::MAX, 27_000, 37_000] {
        data.extend(value.to_le_bytes());
    }
    data.extend([145, 170]);
    data.extend(265u16.to_le_bytes());
    data.push(165);
    data.extend(250u16.to_le_bytes());
    // Dive Summary (268): session reference, depths, surface interval, dive number, bottom time.
    data.extend([
        0x44, 0, 0, 12, 1, 7, 0, 2, 0x84, 1, 2, 0x84, 2, 4, 0x86, 3, 4, 0x86, 4, 4, 0x86, 10, 4,
        0x86, 11, 4, 0x86,
    ]);
    data.push(4);
    data.extend(18u16.to_le_bytes());
    data.extend(0u16.to_le_bytes());
    for value in [7_125u32, 12_500, 600, 2, 1_250_500] {
        data.extend(value.to_le_bytes());
    }
    // Event (21): UTC timestamp, event, event type, and event-specific data.
    data.extend([
        0x45, 0, 0, 21, 0, 4, 253, 4, 0x86, 0, 1, 0x00, 1, 1, 0x00, 3, 4, 0x86,
    ]);
    for (time, event, kind, detail) in [
        (1_100_000_000u32, 0, 0, 0u32),
        (1_100_000_001, 42, 3, u32::from_le_bytes([50, 2, 12, 3])),
        (1_100_000_002, 0, 4, 0),
    ] {
        data.push(5);
        data.extend(time.to_le_bytes());
        data.extend([event, kind]);
        data.extend(detail.to_le_bytes());
    }
    // HRV (78): arrays of uint16 RR intervals scaled to seconds by dividing by 1000.
    data.extend([0x47, 0, 0, 78, 0, 1, 0, 6, 0x84]);
    for intervals in [[725u16, u16::MAX, 812], [0, u16::MAX, 0], [900, 901, 902]] {
        data.push(7);
        for interval in intervals {
            data.extend(interval.to_le_bytes());
        }
    }
    // HR (132): UTC anchor and full 1/1024 s counter, followed by two packed 12-bit timestamps.
    data.extend([
        0x48, 0, 0, 132, 0, 4, 253, 4, 0x86, 0, 2, 0x84, 9, 4, 0x86, 6, 1, 0x02,
    ]);
    data.extend([0x49, 0, 0, 132, 0, 2, 10, 3, 0x0d, 6, 2, 0x02]);
    let delta = [9, 0xfa, 0xa0, 0x3f, 74, 76];
    data.extend(delta); // Without an anchor these samples have no absolute time.
    data.push(8);
    data.extend(1_100_000_003u32.to_le_bytes());
    data.extend(16_384u16.to_le_bytes());
    data.extend(4_090u32.to_le_bytes());
    data.push(72);
    data.extend(delta);
    // Set (225): FIT field 254 is its timestamp. "Pound" is a display preference only.
    data.extend([
        0x4a, 0, 0, 225, 0, 11, 254, 4, 0x86, 0, 4, 0x86, 3, 2, 0x84, 4, 2, 0x84, 5, 1, 0x02, 6, 4,
        0x86, 7, 6, 0x84, 8, 6, 0x84, 9, 2, 0x84, 10, 2, 0x84, 11, 2, 0x84,
    ]);
    for (index, start, reps, weight, set_type) in [
        (0u16, 1_100_000_040u32, 12u16, 920u16, 1),
        (1, 1_100_000_060, u16::MAX, u16::MAX, 0),
    ] {
        data.push(10);
        data.extend((start + 20).to_le_bytes());
        data.extend(19_500u32.to_le_bytes());
        data.extend(reps.to_le_bytes());
        data.extend(weight.to_le_bytes());
        data.push(set_type);
        data.extend(start.to_le_bytes());
        for code in [0u16, u16::MAX, 8, 2, u16::MAX, 4, 2, index, 9] {
            data.extend(code.to_le_bytes());
        }
    }
    // Exercise Title (264): exercise category and code, optional recorded name and index.
    data.extend([
        0x4b, 0, 0, 8, 1, 4, 0, 2, 0x84, 1, 2, 0x84, 2, 12, 0x07, 254, 2, 0x84,
    ]);
    data.extend([11, 0, 0, 2, 0]);
    data.extend(b"Bench press\0");
    data.extend([0, 0]);
    // Split (312): two records, including a type code outside the current profile.
    data.extend([
        0x4c, 0, 0, 56, 1, 16, 0, 1, 0x00, 2, 4, 0x86, 3, 4, 0x86, 4, 4, 0x86, 9, 4, 0x86, 21, 4,
        0x85, 22, 4, 0x85, 25, 4, 0x86, 26, 4, 0x85, 27, 4, 0x86, 74, 4, 0x86, 11, 1, 0x00, 12, 1,
        0x00, 15, 1, 0x02, 29, 2, 0x84, 40, 2, 0x84,
    ]);
    for (kind, latitude) in [(17, -1_073_741_824i32), (42, 1_100_000_000i32)] {
        data.extend([12, kind]);
        for raw in [60_500u32, 125_025, 3_125, 1_100_000_040] {
            data.extend(raw.to_le_bytes());
        }
        data.extend(latitude.to_le_bytes());
        data.extend(1_073_741_824i32.to_le_bytes());
        data.extend(4_750u32.to_le_bytes());
        data.extend((-300i32).to_le_bytes());
        data.extend(1_100_000_101u32.to_le_bytes());
        data.extend(3_125u32.to_le_bytes());
        data.extend([1, 3, 157]); // Running, trail, mean heart rate.
        data.extend(8_932u16.to_le_bytes()); // Cadence counter scaled by 128.
        data.extend(386u16.to_le_bytes());
    }
    // Split Summary (313): provider-reported totals and mean heart rate.
    data.extend([
        0x4d, 0, 0, 57, 1, 5, 0, 1, 0x00, 3, 2, 0x84, 4, 4, 0x86, 5, 4, 0x86, 10, 1, 0x02,
    ]);
    data.extend([13, 17]);
    data.extend(3u16.to_le_bytes());
    data.extend(180_025u32.to_le_bytes());
    data.extend(405_050u32.to_le_bytes());
    data.push(149);
    // ClimbPro (317): timestamp, its own position, phase, index, category and distance.
    data.extend([
        0x4e, 0, 0, 61, 1, 7, 253, 4, 0x86, 0, 4, 0x85, 1, 4, 0x85, 2, 1, 0x00, 3, 2, 0x84, 4, 1,
        0x02, 5, 4, 0x88,
    ]);
    data.push(14);
    data.extend(1_100_000_043u32.to_le_bytes());
    data.extend(536_870_912i32.to_le_bytes());
    data.extend((-1_073_741_824i32).to_le_bytes());
    data.push(1);
    data.extend(2u16.to_le_bytes());
    data.push(3);
    data.extend(52.25f32.to_le_bytes());
    let mut file = vec![12, 0x10, 0, 0];
    file.extend((data.len() as u32).to_le_bytes());
    file.extend(b".FIT");
    file.extend(data);
    let crc = file.iter().fold(0u16, |value, byte| {
        (0..8).fold(value ^ u16::from(*byte), |value, _| {
            if value & 1 == 0 {
                value >> 1
            } else {
                (value >> 1) ^ 0xa001
            }
        })
    });
    file.extend(crc.to_le_bytes());
    file
}

#[test]
fn fit_splits_and_climbs_preserve_scaled_fields_and_unrecognized_codes() -> Result<()> {
    let detail = read_fit(&fixture())?;
    let split = &detail["splits"][0];
    assert_eq!(split["split_type"], "rwd_run");
    assert_eq!(split["started_at"], "2024-11-08T11:34:00Z");
    assert_eq!(split["ended_at"], "2024-11-08T11:35:01Z");
    assert_eq!(split["distance_m"].as_f64(), Some(1250.25));
    assert_eq!(split["timer_seconds"].as_f64(), Some(60.5));
    assert_eq!(split["start_latitude_deg"].as_f64(), Some(-90.0));
    assert_eq!(split["start_longitude_deg"].as_f64(), Some(90.0));
    assert_eq!(split["start_elevation_m"].as_f64(), Some(125.0));
    assert_eq!(split["average_vertical_speed_mps"].as_f64(), Some(-0.3));
    assert_eq!(split["sport"], "running");
    assert_eq!(split["sub_sport"], "trail");
    assert_eq!(split["average_heart_rate_bpm"].as_f64(), Some(157.0));
    assert_eq!(split["average_cadence_rpm"].as_f64(), Some(69.78125));
    assert_eq!(split["average_power_w"].as_f64(), Some(386.0));
    assert_eq!(detail["splits"][1]["split_type"].as_f64(), Some(42.0));
    assert!(detail["splits"][1].get("start_latitude_deg").is_none());
    let summary = &detail["split_summaries"][0];
    assert_eq!(summary["split_count"].as_f64(), Some(3.0));
    assert_eq!(summary["distance_m"].as_f64(), Some(4050.5));
    assert_eq!(summary["average_heart_rate_bpm"].as_f64(), Some(149.0));
    let climb = &detail["climb_events"][0];
    assert_eq!(climb["event"], "start");
    assert_eq!(climb["climb_number"].as_f64(), Some(2.0));
    assert_eq!(climb["climb_category"].as_f64(), Some(3.0));
    assert_eq!(climb["distance_m"].as_f64(), Some(52.25));
    assert_eq!(climb["latitude_deg"].as_f64(), Some(45.0));
    Ok(())
}

#[test]
fn fit_splits_match_reference_files_when_available() -> Result<()> {
    for (env, counts, distance, climb, sport, cadence, power, normalized_power, started_at) in [
        (
            "AVATAR_REFERENCE_TRAIL_FIT",
            [218, 4, 5],
            4692.43,
            "complete",
            "running",
            69.78125,
            386.0,
            401.0,
            "2026-02-25T11:48:32Z",
        ),
        (
            "AVATAR_REFERENCE_CYCLING_FIT",
            [9, 2, 3],
            9326.6,
            "approach",
            "cycling",
            44.890625,
            49.0,
            68.0,
            "2026-02-17T22:05:18Z",
        ),
    ] {
        let Some(path) = std::env::var_os(env) else {
            continue;
        };
        let detail = read_fit(&std::fs::read(path)?)?;
        for (section, count) in ["splits", "split_summaries", "climb_events"]
            .into_iter()
            .zip(counts)
        {
            assert_eq!(detail[section].as_array().map(Vec::len), Some(count));
        }
        let first = &detail["splits"][0];
        assert_eq!(first["distance_m"].as_f64(), Some(distance));
        assert_eq!(first["sport"], sport);
        assert_eq!(first["average_cadence_rpm"].as_f64(), Some(cadence));
        assert_eq!(first["average_power_w"].as_f64(), Some(power));
        assert_eq!(detail["climb_events"][0]["event"], climb);
        let summary = &detail["split_summaries"][0];
        assert_eq!(summary["started_at"], started_at);
        assert_eq!(
            summary["normalized_power_w"].as_f64(),
            Some(normalized_power)
        );
        if sport == "cycling" {
            let first_respiration = detail["samples"]
                .as_array()
                .context("missing samples")?
                .iter()
                .find_map(|row| row["respiration_rate_breaths_per_minute"].as_f64());
            assert_eq!(first_respiration, Some(18.9));
            assert_eq!(
                detail["laps"][0]["average_respiration_rate_breaths_per_minute"].as_f64(),
                Some(21.33)
            );
            assert_eq!(
                detail["sessions"][0]["max_respiration_rate_breaths_per_minute"].as_f64(),
                Some(27.43)
            );
            let sample = detail["samples"]
                .as_array()
                .context("missing samples")?
                .iter()
                .find(|sample| sample["right_power_balance_percent"].is_number())
                .context("missing first cycling balance")?;
            assert_eq!(sample["right_power_balance_percent"].as_f64(), Some(51.0));
            assert_eq!(
                sample["left_torque_effectiveness_percent"].as_f64(),
                Some(80.0)
            );
            assert_eq!(sample["left_platform_center_offset_mm"].as_f64(), Some(8.0));
            let phase = sample["left_power_phase_degrees"][1]
                .as_f64()
                .context("end angle")?;
            assert!((phase - 184.218752878418).abs() < 0.0001);
            assert_eq!(
                detail["laps"][0]["right_power_balance_percent"].as_f64(),
                Some(44.57)
            );
            assert_eq!(
                detail["sessions"][0]["right_power_balance_percent"].as_f64(),
                Some(43.86)
            );
            assert_eq!(
                detail["sessions"][0]["normalized_power_w"].as_f64(),
                Some(68.0)
            );
            assert_eq!(
                detail["sessions"][0]["average_left_power_phase_degrees"]
                    .as_array()
                    .map(Vec::len),
                Some(4)
            );
        }
    }
    Ok(())
}

#[test]
fn fit_strength_projects_sets_and_titles_in_file_order() -> Result<()> {
    let detail = read_fit(&fixture())?;
    let sets = detail["sets"].as_array().context("missing sets")?;
    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0]["timestamp"], "2024-11-08T11:34:20Z");
    assert_eq!(sets[0]["started_at"], "2024-11-08T11:34:00Z");
    assert_eq!(sets[0]["set_type"], "active");
    assert_eq!(sets[0]["duration_seconds"].as_f64(), Some(19.5));
    assert_eq!(sets[0]["repetitions"].as_f64(), Some(12.0));
    assert_eq!(sets[0]["weight_kg"].as_f64(), Some(57.5));
    assert_eq!(sets[0]["weight_display_unit"], "pound");
    assert_eq!(sets[0]["workout_step_index"].as_f64(), Some(9.0));
    assert_eq!(
        sets[0]["categories"],
        json!(["bench_press", null, "deadlift"])
    );
    assert_eq!(sets[0]["category_subtypes"], json!([2, null, 4]));
    assert_eq!(sets[1]["set_type"], "rest");
    assert!(sets[1].get("repetitions").is_none());
    assert!(sets[1].get("weight_kg").is_none());
    let title = &detail["exercise_titles"][0];
    assert_eq!(title["title"], "Bench press");
    assert_eq!(title["exercise_category"], "bench_press");
    assert_eq!(title["exercise_name_code"].as_f64(), Some(2.0));
    Ok(())
}

#[test]
fn fit_strength_matches_reference_file_when_available() -> Result<()> {
    let Some(path) = std::env::var_os("AVATAR_REFERENCE_STRENGTH_FIT") else {
        return Ok(());
    };
    let detail = read_fit(&std::fs::read(path)?)?;
    let sets = detail["sets"].as_array().context("missing sets")?;
    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0]["set_type"], "active");
    assert_eq!(sets[0]["repetitions"].as_f64(), Some(12.0));
    assert_eq!(sets[0]["weight_kg"].as_f64(), Some(0.0));
    assert_eq!(sets[0]["duration_seconds"].as_f64(), Some(120.925));
    assert_eq!(sets[1]["set_type"], "rest");
    assert_eq!(detail["exercise_titles"], json!([]));
    Ok(())
}

#[test]
fn fit_record_has_utc_time_and_scaled_si_fields() -> Result<()> {
    let original = fixture();
    let fit = read_fit(&original)?;
    assert_eq!(fit["samples"].as_array().map(Vec::len), Some(2));
    assert_eq!(fit["samples"][0]["timestamp"], "2024-11-08T11:33:20Z");
    assert_eq!(fit["samples"][0]["heart_rate_bpm"].as_f64(), Some(73.0));
    assert_eq!(fit["samples"][0]["distance_m"].as_f64(), Some(123.45));
    assert_eq!(fit["samples"][0]["speed_mps"].as_f64(), Some(2.45));
    let sample = &fit["samples"][0];
    assert_eq!(sample["vertical_oscillation_mm"].as_f64(), Some(82.5));
    assert_eq!(sample["ground_contact_time_ms"].as_f64(), Some(256.4));
    assert_eq!(sample["vertical_ratio_percent"].as_f64(), Some(7.5));
    assert_eq!(sample["step_length_mm"].as_f64(), Some(1_100.0));
    assert_eq!(sample["underwater_depth_m"], json!(0.0));
    assert_eq!(
        sample["developer_fields"],
        json!([
            {"developer_data_index":0,"number":3,"name":"heart_rate","value":82.5,"units":"W"},
            {"developer_data_index":1,"number":3,"name":"heart_rate","value":155.0,"units":"W"}
        ])
    );
    assert_eq!(fit["samples"][1]["underwater_depth_m"], json!(12.5));
    let events = fit["events"].as_array().context("missing FIT events")?;
    assert_eq!(events.len(), 3);
    assert_eq!(events[0]["event"], "timer");
    assert_eq!(events[0]["event_type"], "start");
    assert_eq!(events[0]["timer_trigger"], "manual");
    assert_eq!(events[1]["event"], "front_gear_change");
    assert_eq!(events[1]["front_gear"], 50);
    assert_eq!(events[1]["rear_gear"], 12);
    assert_eq!(events[2]["event_type"], "stop_all");
    assert_eq!(
        fit["compressed_heart_rate"],
        json!([
            {"timestamp":"2024-11-08T11:33:23.500Z","heart_rate_bpm":72},
            {"timestamp":"2024-11-08T11:33:23.750Z","heart_rate_bpm":74},
            {"timestamp":"2024-11-08T11:33:24.500Z","heart_rate_bpm":76}
        ])
    );
    assert_eq!(
        fit["hrv"],
        json!([
            {"message_index":12,"rr_intervals_ms":[725,null,812]},
            {"message_index":14,"rr_intervals_ms":[900,901,902]}
        ])
    );
    let dive = &fit["dives"][0];
    assert_eq!(dive["reference_message"], "session");
    assert_eq!(dive["reference_index"], json!(0.0));
    assert_eq!(dive["average_depth_m"], json!(7.125));
    assert_eq!(dive["max_depth_m"], json!(12.5));
    assert_eq!(dive["surface_interval_seconds"], json!(600.0));
    assert_eq!(dive["dive_number"], json!(2.0));
    assert_eq!(dive["bottom_time_seconds"], json!(1250.5));
    let session = &fit["sessions"][0];
    assert_eq!(
        session["heart_rate_zone_seconds"],
        json!([12.5, null, 27.0])
    );
    assert_eq!(session["power_zone_seconds"], json!([37.0]));
    assert_eq!(session["pool_length_m"], json!(25.0));
    let active = &fit["lengths"][0];
    assert_eq!(active["started_at"], "2024-11-08T11:33:20Z");
    assert_eq!(active["timer_seconds"], json!(24.5));
    assert_eq!(active["stroke_count"].as_f64(), Some(18.0));
    assert_eq!(active["swim_stroke"], "freestyle");
    assert_eq!(active["length_type"], "active");
    assert_eq!(fit["lengths"][1]["length_type"], "idle");
    let zones = &fit["zones"][0];
    assert_eq!(zones["reference_message"], "session");
    assert_eq!(zones["reference_index"].as_f64(), Some(0.0));
    assert_eq!(zones["heart_rate_zone_seconds"], json!([12.5, null, 27.0]));
    assert_eq!(zones["heart_rate_zone_upper_bpm"], json!([145.0, 170.0]));
    assert_eq!(zones["power_zone_seconds"], json!([37.0]));
    assert_eq!(zones["power_zone_upper_watts"], json!([265.0]));
    assert_eq!(zones["threshold_heart_rate_bpm"], json!(165.0));
    assert_eq!(zones["functional_threshold_power_watts"], json!(250.0));
    let mut corrupt = original;
    corrupt[21] ^= 1;
    assert!(read_fit(&corrupt).is_err());
    let untimed = read_fit(&fixture_with_sample_time(false))?;
    assert!(untimed["samples"][0].get("timestamp").is_none());
    assert_eq!(
        untimed["samples"][0]["developer_fields"][0]["value"],
        json!(82.5)
    );
    assert_eq!(untimed["samples"][1]["heart_rate_bpm"], json!(81.0));
    if let Some(path) = std::env::var_os("AVATAR_REFERENCE_DIVE_FIT") {
        let fit = read_fit(&std::fs::read(path)?)?;
        let samples = fit["samples"]
            .as_array()
            .context("missing FIT samples")?
            .iter()
            .filter(|sample| sample["underwater_depth_m"].as_f64().is_some())
            .count();
        let dives = fit["dives"].as_array().context("missing summaries")?;
        ensure!(
            samples > 0
                && dives
                    .iter()
                    .any(|dive| dive["max_depth_m"].as_f64().is_some())
        );
        eprintln!(
            "parsed public dive FIT with {samples} depth samples and {} summaries",
            dives.len()
        );
    }
    if let Some(path) = std::env::var_os("AVATAR_SUUNTO_FIT_SAMPLE_ZIP") {
        let mut archive = zip::ZipArchive::new(std::fs::File::open(path)?)?;
        let mut files = 0;
        let mut laps = 0;
        let mut heart = 0;
        for index in 0..archive.len() {
            let item = archive.by_index(index)?;
            let name = item.name()?.into_owned();
            if !name.to_ascii_lowercase().ends_with(".fit") {
                continue;
            }
            let mut bytes = Vec::new();
            item.take(12 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
            let fit = read_fit(&bytes)
                .map_err(|error| anyhow::anyhow!("Suunto fixture {name}: {error}"))?;
            files += 1;
            laps += fit["laps"].as_array().map_or(0, Vec::len);
            heart += fit["samples"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|sample| sample.get("heart_rate_bpm").is_some())
                .count();
        }
        ensure!(
            files > 0 && laps > 0 && heart > 0,
            "official FIT archive did not exercise laps and heart-rate data"
        );
        eprintln!("parsed {files} public FIT files, {laps} laps, and {heart} heart-rate samples");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn stored_fit_is_projected_and_paged_without_exposing_other_accounts() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let owner = "fit-details-fixture";
    let provider = "coros";
    let activity = "detail-fixture";
    let bytes = fixture();
    let hash = crate::wearable_webhooks::hash(&bytes);
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&owner],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.device_connection(subject_id,provider,state) VALUES($1,$2,'synced')", &[&owner,&provider]).await?;
    db.execute("INSERT INTO watch_tower.wearable_fit_job(subject_id,provider,activity_id,status) VALUES($1,$2,$3,'stored')", &[&owner,&provider,&activity]).await?;
    db.execute("INSERT INTO avatar_hsm.wearable_fit(subject_id,provider,activity_id,part,data,sha256) VALUES($1,$2,$3,2,$4,$5)", &[&owner,&provider,&activity,&bytes,&hash]).await?;
    let args = |offset| Query {
        provider: provider.into(),
        source_id: activity.into(),
        part: Some(2),
        section: Some("samples".into()),
        offset: Some(offset),
        limit: Some(1),
    };
    assert_eq!(query(&pool, owner, args(0)).await?["state"], "pending");
    let previous_details = json!({"samples":[{"heart_rate_bpm":73},{"heart_rate_bpm":81}]});
    db.execute(
        "UPDATE avatar_hsm.wearable_fit SET detail_state='ready',detail=$4
         WHERE subject_id=$1 AND provider=$2 AND activity_id=$3 AND part=2",
        &[&owner, &provider, &activity, &previous_details],
    )
    .await?;
    let before = query(&pool, owner, args(0)).await?;
    assert_eq!(before["developer_fields_state"], "pending");
    assert_eq!(before["cycling_dynamics_state"], "pending");
    assert_eq!(before["respiration_state"], "pending");
    assert_eq!(before["state"], "ready");
    assert_eq!(before["records"][0]["heart_rate_bpm"], 73);
    let mut event_query = args(0);
    event_query.section = Some("events".into());
    let events = query(&pool, owner, event_query).await?;
    assert_eq!(events["state"], "pending");
    assert_eq!(events["records"], json!([]));
    let strength_args = |section: &str, offset| Query {
        section: Some(section.into()),
        ..args(offset)
    };
    assert_eq!(
        query(&pool, owner, strength_args("sets", 0)).await?["state"],
        "pending"
    );
    assert_eq!(
        query(&pool, owner, strength_args("splits", 0)).await?["state"],
        "pending"
    );
    assert!(process_one(&pool).await?);
    let first = query(&pool, owner, args(0)).await?;
    assert_eq!(first["cycling_dynamics_state"], "ready");
    assert_eq!(
        first["records"][0]["right_power_balance_percent"].as_f64(),
        Some(51.0)
    );
    assert_eq!(first["developer_fields_state"], "ready");
    assert_eq!(first["state"], "ready");
    assert_eq!(first["total"], 2);
    assert_eq!(first["next_offset"], 1);
    let page = query(&pool, owner, strength_args("sets", 0)).await?;
    assert_eq!(page["state"], "ready");
    assert_eq!(page["next_offset"], 1);
    assert_eq!(page["records"][0]["weight_kg"].as_f64(), Some(57.5));
    let page = query(&pool, owner, strength_args("sets", 1)).await?;
    assert!(page["next_offset"].is_null());
    assert_eq!(page["records"][0]["set_type"], "rest");
    let page = query(&pool, owner, strength_args("splits", 0)).await?;
    assert_eq!(page["state"], "ready");
    assert_eq!(page["total"], 2);
    assert_eq!(page["next_offset"], 1);
    assert_eq!(page["records"][0]["split_type"], "rwd_run");
    db.execute(
        "UPDATE avatar_hsm.wearable_fit SET devices_parsed=false,detail=detail-'devices'
        WHERE subject_id=$1 AND provider=$2 AND activity_id=$3 AND part=2",
        &[&owner, &provider, &activity],
    )
    .await?;
    let mut legacy_devices = args(0);
    legacy_devices.section = Some("devices".into());
    assert_eq!(
        query(&pool, owner, legacy_devices).await?["state"],
        "pending"
    );
    assert_eq!(query(&pool, owner, args(0)).await?["state"], "ready");
    assert!(process_one(&pool).await?);
    let mut reparsed_devices = args(0);
    reparsed_devices.section = Some("devices".into());
    let devices = query(&pool, owner, reparsed_devices).await?;
    assert_eq!(devices["state"], "ready");
    assert_eq!(devices["records"], json!([]));
    db.execute(
        "UPDATE avatar_hsm.wearable_fit SET hrv_parsed=false,detail=detail-'hrv'
        WHERE subject_id=$1 AND provider=$2 AND activity_id=$3 AND part=2",
        &[&owner, &provider, &activity],
    )
    .await?;
    let mut hrv_query = args(0);
    hrv_query.section = Some("hrv".into());
    assert_eq!(query(&pool, owner, hrv_query).await?["state"], "pending");
    assert_eq!(query(&pool, owner, args(0)).await?["state"], "ready");
    assert!(process_one(&pool).await?);
    let mut hrv_query = args(1);
    hrv_query.section = Some("hrv".into());
    let page = query(&pool, owner, hrv_query).await?;
    assert_eq!(page["state"], "ready");
    assert_eq!(page["total"], 2);
    assert_eq!(
        page["records"][0]["rr_intervals_ms"],
        json!([900, 901, 902])
    );
    let mut length_query = args(1);
    length_query.section = Some("lengths".into());
    let lengths = query(&pool, owner, length_query).await?;
    assert_eq!(lengths["total"], 2);
    assert_eq!(lengths["records"][0]["length_type"], "idle");
    assert_eq!(
        query(&pool, owner, args(1)).await?["records"][0]["heart_rate_bpm"].as_f64(),
        Some(81.0)
    );
    verify_mcp(&pool, owner, provider, activity).await?;
    assert_eq!(
        query(&pool, "other-owner", args(0)).await?["state"],
        "not_found"
    );
    let unchanged: Vec<u8> = db
        .query_one(
            "SELECT data FROM avatar_hsm.wearable_fit WHERE subject_id=$1",
            &[&owner],
        )
        .await?
        .get(0);
    assert_eq!(unchanged, bytes);
    // An upgrade also revisits files whose standard events were already processed.
    db.execute(
        "UPDATE avatar_hsm.wearable_fit SET developer_fields_parsed=false,detail=detail-'developer_fields_included'
         WHERE subject_id=$1 AND provider=$2 AND activity_id=$3 AND part=2",
        &[&owner, &provider, &activity],
    )
    .await?;
    let mut event_query = args(0);
    event_query.section = Some("events".into());
    let retained = query(&pool, owner, event_query).await?;
    assert_eq!(retained["state"], "ready");
    assert_eq!(retained["developer_fields_state"], "pending");
    assert!(process_one(&pool).await?);
    assert_eq!(
        query(&pool, owner, args(0)).await?["developer_fields_state"],
        "ready"
    );
    // A failed reparse must finish the events job without erasing an older projection.
    let invalid_fit = vec![0u8; 16];
    let invalid_hash = crate::wearable_webhooks::hash(&invalid_fit);
    db.execute("INSERT INTO avatar_hsm.wearable_fit(subject_id,provider,activity_id,part,data,sha256,detail_state,detail)
        VALUES($1,$2,$3,3,$4,$5,'ready',$6)", &[&owner,&provider,&activity,&invalid_fit,&invalid_hash,&previous_details]).await?;
    assert!(process_one(&pool).await?);
    assert!(!process_one(&pool).await?);
    let mut failed = args(0);
    failed.part = Some(3);
    failed.section = Some("events".into());
    assert_eq!(query(&pool, owner, failed).await?["state"], "unavailable");
    let mut preserved = args(0);
    preserved.part = Some(3);
    let preserved = query(&pool, owner, preserved).await?;
    assert_eq!(preserved["developer_fields_state"], "unavailable");
    assert_eq!(preserved["cycling_dynamics_state"], "unavailable");
    assert_eq!(preserved["respiration_state"], "unavailable");
    assert_eq!(preserved["state"], "ready");
    assert_eq!(preserved["records"][0]["heart_rate_bpm"], 73);
    crate::wearables::revoke_for(&pool, owner, provider).await?;
    assert_eq!(query(&pool, owner, args(0)).await?["state"], "not_found");
    Ok(())
}

async fn verify_mcp(pool: &Pool, owner: &str, provider: &str, activity: &str) -> Result<()> {
    let db = pool.get().await?;
    let read_token = "fit-test-read-token-012345678901234567890123456789";
    db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))", &[&owner,&read_token]).await?;
    let local = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", local.local_addr()?);
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: read_token.to_owned(),
        control_token: "fixture-control-token-01234567890123456789".into(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(local, crate::router(app)).await });
    let client = crate::cloud_wearables::http_client()?;
    let query = json!({"provider":provider,"source_id":activity,"section":"samples","limit":1});
    let response: Value=client.post(format!("{origin}/mcp/wearable-read")).bearer_auth(read_token)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wearable_fit_details","arguments":query}}))
        .send().await?.error_for_status()?.json().await?;
    assert_eq!(
        response["result"]["structuredContent"]["developer_fields_state"],
        "ready"
    );
    assert_eq!(
        response["result"]["structuredContent"]["records"][0]["developer_fields"][0]["value"],
        json!(82.5)
    );
    assert_eq!(
        response["result"]["structuredContent"]["records"][0]["heart_rate_bpm"].as_f64(),
        Some(73.0)
    );
    assert_eq!(
        response["result"]["structuredContent"]["records"][0]["step_length_mm"].as_f64(),
        Some(1_100.0)
    );
    let cycling = &response["result"]["structuredContent"];
    assert_eq!(cycling["cycling_dynamics_state"], "ready");
    assert_eq!(cycling["respiration_state"], "ready");
    assert_eq!(
        cycling["records"][0]["respiration_rate_breaths_per_minute"].as_f64(),
        Some(18.9)
    );
    assert_eq!(
        cycling["records"][0]["left_power_balance_percent"].as_f64(),
        Some(49.0)
    );
    assert_eq!(
        cycling["records"][0]["left_torque_effectiveness_percent"].as_f64(),
        Some(80.0)
    );
    assert_eq!(
        cycling["records"][0]["left_pedal_smoothness_percent"].as_f64(),
        Some(27.5)
    );
    let lengths: Value = client.post(format!("{origin}/mcp/wearable-read")).bearer_auth(read_token)
        .json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"wearable_fit_details","arguments":{
            "provider":provider,"source_id":activity,"section":"lengths","limit":1
        }}})).send().await?.error_for_status()?.json().await?;
    assert_eq!(
        lengths["result"]["structuredContent"]["records"][0]["swim_stroke"],
        "freestyle"
    );
    let dives: Value = client.post(format!("{origin}/mcp/wearable-read")).bearer_auth(read_token)
        .json(&json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"wearable_fit_details","arguments":{
            "provider":provider,"source_id":activity,"section":"dives","limit":1
        }}})).send().await?.error_for_status()?.json().await?;
    assert_eq!(
        dives["result"]["structuredContent"]["records"][0]["max_depth_m"],
        json!(12.5)
    );
    let events: Value = client.post(format!("{origin}/mcp/wearable-read")).bearer_auth(read_token)
        .json(&json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"wearable_fit_details","arguments":{
            "provider":provider,"source_id":activity,"section":"events","offset":1,"limit":1
        }}})).send().await?.error_for_status()?.json().await?;
    let page = &events["result"]["structuredContent"];
    assert_eq!(page["records"][0]["front_gear"], 50);
    assert_eq!(page["total"], 3);
    assert_eq!(page["next_offset"], 2);
    let hrv: Value = client.post(format!("{origin}/mcp/wearable-read")).bearer_auth(read_token)
        .json(&json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"wearable_fit_details","arguments":{
            "provider":provider,"source_id":activity,"section":"hrv","limit":1
        }}})).send().await?.error_for_status()?.json().await?;
    let page = &hrv["result"]["structuredContent"];
    assert_eq!(
        page["records"][0]["rr_intervals_ms"],
        json!([725, null, 812])
    );
    assert!(page["records"][0].get("timestamp").is_none());
    assert_eq!(page["next_offset"], 1);
    let heart_rate: Value = client.post(format!("{origin}/mcp/wearable-read")).bearer_auth(read_token)
        .json(&json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"wearable_fit_details","arguments":{
            "provider":provider,"source_id":activity,"section":"compressed_heart_rate","offset":1,"limit":1
        }}})).send().await?.error_for_status()?.json().await?;
    let page = &heart_rate["result"]["structuredContent"];
    assert_eq!(
        page["records"][0],
        json!({"timestamp":"2024-11-08T11:33:23.750Z","heart_rate_bpm":74})
    );
    assert_eq!(page["total"], 3);
    assert_eq!(page["next_offset"], 2);
    let strength: Value = client.post(format!("{origin}/mcp/wearable-read")).bearer_auth(read_token)
        .json(&json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"wearable_fit_details","arguments":{
            "provider":provider,"source_id":activity,"section":"exercise_titles"
        }}})).send().await?.error_for_status()?.json().await?;
    assert_eq!(
        strength["result"]["structuredContent"]["records"][0]["title"],
        "Bench press"
    );
    // The MVP host's service token reads the synthetic operator, so mirror the fixture for that path.
    if let Some(path) = std::env::var_os("AVATAR_FIT_HOST_TEST_EXE") {
        db.execute("INSERT INTO avatar_hsm.device_connection(subject_id,provider,state) VALUES('synthetic-mvp',$1,'synced')", &[&provider]).await?;
        db.execute("INSERT INTO watch_tower.wearable_fit_job(subject_id,provider,activity_id,status) VALUES('synthetic-mvp',$1,$2,'stored')", &[&provider,&activity]).await?;
        db.execute("INSERT INTO avatar_hsm.wearable_fit(subject_id,provider,activity_id,part,data,sha256,detail_state,detail,events_parsed,developer_fields_parsed,devices_parsed,hrv_parsed,compressed_heart_rate_parsed,strength_parsed,splits_parsed,cycling_dynamics_parsed,respiration_parsed)
            SELECT 'synthetic-mvp',provider,activity_id,part,data,sha256,detail_state,detail,events_parsed,developer_fields_parsed,devices_parsed,hrv_parsed,compressed_heart_rate_parsed,strength_parsed,splits_parsed,cycling_dynamics_parsed,respiration_parsed
            FROM avatar_hsm.wearable_fit WHERE subject_id=$1 AND provider=$2 AND activity_id=$3", &[&owner,&provider,&activity]).await?;
        let lease = crate::wearables::issue_lease(pool, crate::wearables::SAMSUNG_HEALTH).await?;
        let end = chrono::Utc::now();
        crate::wearables::commit(pool, crate::wearables::SAMSUNG_HEALTH, serde_json::from_value(json!({
            "lease":lease["lease"],"start":end-chrono::Duration::days(1),"end":end,"grants":[],"records":[],
            "samsung_goals":{"timezone":"Asia/Seoul","targets":[{"kind":"steps","steps":8000}]}
        }))?).await?;
        let verified = verify_host(path, &origin, read_token, &query).await;
        let rows = db
            .query_one(
                "SELECT count(*) FILTER (WHERE provider='healthkit'),
                        count(*) FILTER (WHERE provider='samsung_health')
                 FROM avatar_hsm.wearable_record
                 WHERE subject_id='synthetic-mvp' AND source='fixture.batch'",
                &[],
            )
            .await?;
        crate::wearables::revoke_for(pool, "synthetic-mvp", provider).await?;
        crate::wearables::revoke(pool, crate::wearables::SAMSUNG_HEALTH).await?;
        crate::wearables::revoke(pool, crate::wearables::HEALTHKIT).await?;
        crate::wearables::revoke(pool, crate::wearables::HEALTH_CONNECT).await?;
        verified?;
        assert_eq!(rows.get::<_, i64>(0), 4501);
        assert_eq!(rows.get::<_, i64>(1), 4502);
    }
    server.abort();
    Ok(())
}

async fn verify_host(
    path: std::ffi::OsString,
    origin: &str,
    read_token: &str,
    query: &Value,
) -> Result<()> {
    use std::process::{Child, Command, Stdio};
    struct TestHost(Child);
    impl Drop for TestHost {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let socket = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    let port = socket.local_addr()?.port();
    drop(socket);
    let workspace = std::env::current_dir()?
        .join("target")
        .join(format!("fit-host-smoke-{}-{port}", std::process::id()));
    let core_token = "fit-host-test-core-token-01234567890123456789";
    let mut command = Command::new(path);
    command
        .env("OPENHUMAN_WORKSPACE", workspace)
        .env("OPENHUMAN_MVP_PORT", port.to_string())
        .env("OPENHUMAN_CORE_TOKEN", core_token)
        .env("OPENHUMAN_KEYRING_MASTER_KEY", "a1".repeat(32))
        .env("AVATAR_HSM_BASE_URL", origin)
        .env("AVATAR_HSM_READ_TOKEN", read_token)
        .env(
            "AVATAR_HSM_CONTROL_TOKEN",
            "fixture-control-token-01234567890123456789",
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut host = TestHost(command.spawn()?);
    let client = crate::cloud_wearables::http_client()?;
    for _ in 0..75 {
        if host.0.try_wait()?.is_some() {
            anyhow::bail!("OpenHuman FIT smoke host exited early");
        }
        let response=client.post(format!("http://127.0.0.1:{port}/rpc")).bearer_auth(core_token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"openhuman.avatar_hsm_wearable_fit_details","params":{"query":query}})).send().await;
        if let Ok(response) = response {
            let envelope: Value = response.error_for_status()?.json().await?;
            ensure!(
                envelope.get("error").is_none(),
                "OpenHuman FIT RPC returned an error: {}",
                envelope["error"]
            );
            let mut fit = &envelope["result"];
            if fit.get("logs").is_some() && fit.get("result").is_some() {
                fit = &fit["result"];
            }
            fit = fit.get("data").unwrap_or(fit);
            assert_eq!(fit["state"], "ready");
            assert_eq!(fit["developer_fields_state"], "ready");
            assert_eq!(fit["cycling_dynamics_state"], "ready");
            assert_eq!(fit["respiration_state"], "ready");
            assert_eq!(
                fit["records"][0]["respiration_rate_breaths_per_minute"].as_f64(),
                Some(18.9)
            );
            assert_eq!(
                fit["records"][0]["right_power_balance_percent"].as_f64(),
                Some(51.0)
            );
            assert_eq!(
                fit["records"][0]["developer_fields"][0]["value"],
                json!(82.5)
            );
            assert_eq!(fit["records"][0]["heart_rate_bpm"].as_f64(), Some(73.0));
            for (section, field, expected) in [
                ("lengths", "length_type", json!("active")),
                (
                    "sessions",
                    "heart_rate_zone_seconds",
                    json!([12.5, null, 27.0]),
                ),
                ("zones", "heart_rate_zone_upper_bpm", json!([145.0, 170.0])),
                ("dives", "max_depth_m", json!(12.5)),
                ("events", "event_type", json!("start")),
                ("hrv", "rr_intervals_ms", json!([725, null, 812])),
                ("compressed_heart_rate", "heart_rate_bpm", json!(72)),
                ("sets", "weight_kg", json!(57.5)),
                ("exercise_titles", "title", json!("Bench press")),
                ("splits", "distance_m", json!(1250.25)),
                ("split_summaries", "distance_m", json!(4050.5)),
                ("climb_events", "event", json!("start")),
            ] {
                let mut detail_query = query.clone();
                detail_query["section"] = json!(section);
                let envelope: Value = client.post(format!("http://127.0.0.1:{port}/rpc"))
                    .bearer_auth(core_token)
                    .json(&json!({"jsonrpc":"2.0","id":1,"method":"openhuman.avatar_hsm_wearable_fit_details","params":{"query":detail_query}}))
                    .send().await?.error_for_status()?.json().await?;
                let data = &envelope["result"];
                let data = if data.get("logs").is_some() {
                    &data["result"]
                } else {
                    data
                };
                assert_eq!(
                    data.get("data").unwrap_or(data)["records"][0][field],
                    expected
                );
            }
            eprintln!("OpenHuman FIT RPC returned PostgreSQL-backed samples, HRV intervals, zone times, swim lengths, dive summaries, and events");
            let envelope: Value = client.post(format!("http://127.0.0.1:{port}/rpc"))
                .bearer_auth(core_token)
                .json(&json!({"jsonrpc":"2.0","id":2,"method":"openhuman.avatar_hsm_wearable_goals","params":{}}))
                .send().await?.error_for_status()?.json().await?;
            ensure!(
                envelope.get("error").is_none(),
                "OpenHuman goals RPC returned an error: {}",
                envelope["error"]
            );
            let mut goals = &envelope["result"];
            if goals.get("logs").is_some() && goals.get("result").is_some() {
                goals = &goals["result"];
            }
            let goals = goals.get("data").unwrap_or(goals);
            assert_eq!(goals["targets"][0]["steps"], 8000);
            eprintln!("OpenHuman goals RPC returned a PostgreSQL-backed 8000 step target");
            verify_bulk_device_upload(&client, port, core_token).await?;
            verify_samsung_batches(&client, port, core_token).await?;
            verify_reconnect_cancels_upload(&client, port, core_token).await?;
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    }
    anyhow::bail!("OpenHuman FIT smoke listener timed out")
}

async fn verify_reconnect_cancels_upload(
    client: &reqwest::Client,
    port: u16,
    token: &str,
) -> Result<()> {
    let url = format!("http://127.0.0.1:{port}/rpc");
    let start = "2026-04-26T03:00:00Z";
    let end = "2026-04-26T04:00:00Z";
    let begin = "openhuman.avatar_hsm_device_begin";
    let commit = "openhuman.avatar_hsm_device_commit";
    let snapshot = |lease: &Value| {
        json!({"snapshot":{"provider":"health_connect","lease":lease["lease"],
        "start":start,"end":end,"grants":[],"records":[]}})
    };
    let old = call_device_rpc(
        client,
        &url,
        token,
        begin,
        json!({"provider":"health_connect"}),
    )
    .await?;
    call_device_rpc(client, &url, token, commit, snapshot(&old)).await?;
    call_device_rpc(
        client,
        &url,
        token,
        "openhuman.avatar_hsm_device_disconnect",
        json!({"provider":"health_connect"}),
    )
    .await?;
    let new = call_device_rpc(
        client,
        &url,
        token,
        begin,
        json!({"provider":"health_connect"}),
    )
    .await?;
    assert_ne!(old["generation"], new["generation"]);
    call_device_rpc(client, &url, token, commit, snapshot(&new)).await?;
    let stale = call_device_rpc(
        client,
        &url,
        token,
        begin,
        json!({"provider":"health_connect",
        "require_active":true,"expected_generation":old["generation"]}),
    )
    .await;
    let rejection = stale
        .err()
        .context("stale generation received an upload lease")?;
    ensure!(
        rejection
            .to_string()
            .contains("Avatar HSM rejected the operation"),
        "unexpected response for stale generation: {rejection}"
    );
    let current = call_device_rpc(
        client,
        &url,
        token,
        begin,
        json!({"provider":"health_connect",
        "require_active":true,"expected_generation":new["generation"]}),
    )
    .await?;
    assert_eq!(current["generation"], new["generation"]);
    call_device_rpc(client, &url, token, commit, snapshot(&current)).await?;
    eprintln!("OpenHuman denied the stale continuation after disconnect and reconnect");
    Ok(())
}

async fn verify_samsung_batches(client: &reqwest::Client, port: u16, token: &str) -> Result<()> {
    let url = format!("http://127.0.0.1:{port}/rpc");
    let source = "fixture.batch";
    let start = "2026-04-25T15:00:00Z";
    let end = "2026-04-27T15:00:00Z";
    let workout_end = "2026-04-25T15:30:00Z";
    for (index, batch) in [(0, 4000), (4000, 4501), (0, 4000)].into_iter().enumerate() {
        let lease = call_device_rpc(
            client,
            &url,
            token,
            "openhuman.avatar_hsm_device_begin",
            json!({"provider":"samsung_health","require_active":index>0}),
        )
        .await?;
        let mut records: Vec<_> = (batch.0..batch.1)
            .map(|id| {
                json!({
                    "kind":"metric","source":source,"source_id":format!("samsung:{id}"),
                    "started_at":start,"ended_at":start,"modified_at":start,
                    "payload":{"metric":"weight","provider_data":{"value":73.0,"unit":"kg"}}
                })
            })
            .collect();
        if index == 1 {
            records.push(
                json!({"kind":"workout","source":source,"source_id":"samsung:workout",
                "started_at":start,"ended_at":workout_end,"modified_at":workout_end,
                "payload":{"exercise_type":"RUNNING"}}),
            );
        }
        let mut snapshot = json!({"provider":"samsung_health","lease":lease["lease"],
            "start":start,"end":end,"historical":true,"grants":["metric","workout"],
            "metric_grants_version":2,"metric_grants":["weight"],"records":records});
        if index == 0 {
            snapshot["samsung_activity"] = json!({"permission":"granted","timezone":"Asia/Seoul",
                "days":[{"day":"2026-04-26","stats":{"distance_meters":2500}}]});
        }
        if index == 1 {
            snapshot["samsung_routes"] = json!({"permission":"granted","routes":[{
                "source":source,"source_id":"samsung:workout","source_point_count":1,
                "points":[[start.parse::<chrono::DateTime<chrono::Utc>>()?.timestamp_millis(),37.5,127.0,null,null]]
            }]});
        }
        let ack = call_device_rpc(
            client,
            &url,
            token,
            "openhuman.avatar_hsm_device_commit",
            json!({"snapshot":snapshot}),
        )
        .await?;
        assert_eq!(
            ack["accepted"].as_u64(),
            Some((batch.1 - batch.0) as u64 + u64::from(index == 1))
        );
    }
    let route = call_device_rpc(
        client,
        &url,
        token,
        "openhuman.avatar_hsm_wearable_sessions",
        json!({"query":{"route":{"source":source,"source_id":"samsung:workout"}}}),
    )
    .await?;
    assert_eq!(route["records"][0]["latitude"].as_f64(), Some(37.5));
    let activity = call_device_rpc(
        client,
        &url,
        token,
        "openhuman.avatar_hsm_wearable_daily",
        json!({"query":{"kind":"activity","provider":"samsung_health",
            "start_day":"2026-04-26","end_day":"2026-04-26"}}),
    )
    .await?;
    assert_eq!(
        activity["records"][0]["stats"]["distance_meters"].as_f64(),
        Some(2500.0)
    );
    eprintln!("Samsung batches retained 4,502 rows, activity and GPS across a retry");
    Ok(())
}

async fn verify_bulk_device_upload(client: &reqwest::Client, port: u16, token: &str) -> Result<()> {
    let url = format!("http://127.0.0.1:{port}/rpc");
    let start = "2026-04-25T08:00:00Z";
    let end = "2026-04-26T08:00:00Z";
    for (index, uploads) in [(0, 4000), (4000, 4501), (0, 4000)].into_iter().enumerate() {
        let lease = call_device_rpc(
            client,
            &url,
            token,
            "openhuman.avatar_hsm_device_begin",
            json!({"provider":"healthkit","require_active":index>0}),
        )
        .await?;
        let records: Vec<_> = (uploads.0..uploads.1)
            .map(|id| {
                json!({
                    "kind":"metric","source":"fixture.batch","source_id":format!("bulk:{id}"),
                    "started_at":start,"ended_at":start,"modified_at":start,
                    "payload":{"metric":"weight","provider_data":{"value":73.0,"unit":"kg"}}
                })
            })
            .collect();
        let ack = call_device_rpc(
            client, &url, token, "openhuman.avatar_hsm_device_commit",
            json!({"snapshot":{"provider":"healthkit","lease":lease["lease"],"start":start,"end":end,
                "historical":true,"grants":["metric"],"records":records}}),
        ).await?;
        assert_eq!(
            ack["accepted"].as_u64(),
            Some((uploads.1 - uploads.0) as u64)
        );
    }
    eprintln!("OpenHuman RPC accepted 4,501 samples in two batches and retried the first batch");
    Ok(())
}

async fn call_device_rpc(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    method: &str,
    params: Value,
) -> Result<Value> {
    let envelope: Value = client
        .post(url)
        .bearer_auth(token)
        .json(&json!({"jsonrpc":"2.0","id":3,"method":method,"params":params}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        envelope.get("error").is_none(),
        "OpenHuman device RPC {method} returned an error: {}",
        envelope["error"]
    );
    let mut result = &envelope["result"];
    if result.get("logs").is_some() && result.get("result").is_some() {
        result = &result["result"];
    }
    Ok(result.get("data").unwrap_or(result).clone())
}
