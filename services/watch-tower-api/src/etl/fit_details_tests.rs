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
