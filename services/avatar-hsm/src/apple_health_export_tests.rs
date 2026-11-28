use super::*;
use serde_json::Value;
use std::io::Write;

const XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE HealthData [
<!ELEMENT HealthData ANY>
]>
<HealthData locale="ko_KR">
  <Record type="HKQuantityTypeIdentifierStepCount" sourceName="Watch &amp; Phone" unit="count" value="420"
    startDate="2026-02-09 01:00:00 +0900" endDate="2026-02-09 01:05:00 +0900"/>
  <Record type="HKQuantityTypeIdentifierHeartRate" sourceName="Watch &amp; Phone" unit="count/min" value="72"
    startDate="2026-02-09 01:01:00 +0900" endDate="2026-02-09 01:01:00 +0900"/>
  <Record type="HKCategoryTypeIdentifierSleepAnalysis" sourceName="Watch &amp; Phone" value="HKCategoryValueSleepAnalysisAsleepDeep"
    startDate="2026-02-09 01:10:00 +0900" endDate="2026-02-09 01:30:00 +0900"/>
  <Workout workoutActivityType="HKWorkoutActivityTypeWalking" sourceName="Watch &amp; Phone" duration="29.5" durationUnit="min"
    startDate="2026-02-09 07:00:00 +0900" endDate="2026-02-09 07:30:00 +0900">
    <WorkoutStatistics type="HKQuantityTypeIdentifierDistanceWalkingRunning" unit="km" sum="2.5"/>
    <WorkoutStatistics type="HKQuantityTypeIdentifierActiveEnergyBurned" unit="kJ" sum="418.4"/>
    <WorkoutStatistics type="HKQuantityTypeIdentifierHeartRate" unit="count/min" average="110" minimum="90" maximum="140"/>
    <MetadataEntry key="HKWeatherTemperature" value="50 degF"/>
    <MetadataEntry key="HKWeatherHumidity" value="7900 %"/>
    <MetadataEntry key="HKWeatherCondition" value="7"/>
    <MetadataEntry key="HKElevationAscended" value="5348 cm"/>
    <MetadataEntry key="HKElevationDescended" value="0 m"/>
    <WorkoutRoute><FileReference path="/workout-routes/route.gpx"/><FileReference path="/workout-routes/../another.gpx"/></WorkoutRoute>
  </Workout>
  <Record type="HKQuantityTypeIdentifierBodyMass" sourceName="Scale" unit="lb" value="160"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:00:00 +0900"/>
  <Correlation type="HKCorrelationTypeIdentifierBloodPressure">
    <Record type="HKQuantityTypeIdentifierBloodPressureSystolic" sourceName="Cuff" unit="mmHg" value="115"
      startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:00:00 +0900"/>
  </Correlation>
  <Record type="HKQuantityTypeIdentifierBloodGlucose" sourceName="Glucose" unit="mmol&lt;180.1558800000541&gt;/L" value="6"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:00:00 +0900"/>
  <Record type="HKQuantityTypeIdentifierBodyTemperature" sourceName="Thermometer" unit="degF" value="98.6"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:00:00 +0900"/>
  <Record type="HKQuantityTypeIdentifierOxygenSaturation" sourceName="Watch" unit="%" value="0.98"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:00:00 +0900"/>
  <Record type="HKQuantityTypeIdentifierWalkingSpeed" sourceName="Watch" unit="km/hr" value="7.2"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:01:00 +0900"/>
  <Record type="HKQuantityTypeIdentifierUnderwaterDepth" sourceName="Watch" unit="ft" value="25"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:00:00 +0900"/>
  <Record type="HKQuantityTypeIdentifierWaterTemperature" sourceName="Watch" unit="degF" value="30.2"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:00:00 +0900"/>
  <Record type="HKQuantityTypeIdentifierRunningGroundContactTime" sourceName="Watch" unit="s" value="0.244"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:01:00 +0900"/>
  <Record type="HKQuantityTypeIdentifierTimeInDaylight" sourceName="Watch" unit="s" value="630"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:10:30 +0900"/>
  <Record type="HKQuantityTypeIdentifierDietaryVitaminB12" sourceName="Nutrition" unit="mg" value="0.0025"
    startDate="2026-02-09 11:30:00 +0900" endDate="2026-02-09 11:30:00 +0900"/>
  <Record type="HKQuantityTypeIdentifierWorkoutEffortScore" sourceName="Watch" unit="appleEffortScore" value="7"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:30:00 +0900"/>
  <Record type="HKQuantityTypeIdentifierEstimatedWorkoutEffortScore" sourceName="Watch" unit="appleEffortScore" value="5"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:30:00 +0900"/>
  <Record type="HKQuantityTypeIdentifierUnknown" sourceName="Watch" unit="count" value="1"
    startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:00:00 +0900"/>
</HealthData>"#;
const GPX: &str = r#"<gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1" creator="Synthetic watch"><trk>
<trkseg>
<trkpt lat="37.5" lon="127.0"><ele>14.5</ele><time>2026-02-08T22:02:00Z</time></trkpt>
<trkpt lat="37.7" lon="127.1"><time>2026-02-08T21:55:00Z</time></trkpt>
</trkseg><trkseg><trkpt lat="37.6" lon="127.1"><time>2026-02-08T22:05:00Z</time></trkpt></trkseg>
</trk></gpx>"#;

#[test]
fn apple_export_reads_dtd_nested_pressure_and_localized_units() -> Result<()> {
    let mut reader = ExportReader::new(XML.as_bytes());
    let rows = reader.batch()?;
    assert_eq!(reader.summary.accepted, 17);
    assert_eq!(reader.summary.skipped, 1);
    assert_eq!(rows[0].source, "Watch & Phone");
    assert_eq!(rows[0].started_at.to_rfc3339(), "2026-02-08T16:00:00+00:00");
    assert_eq!(rows[2].payload["stages"][0]["stage"], 5);
    assert_eq!(
        rows[3].payload["exercise_type"],
        "HKWorkoutActivityTypeWalking"
    );
    assert_eq!(rows[5].payload["provider_data"]["value"], 115.0);
    assert_eq!(rows[3].payload["distance_meters"].as_f64(), Some(2500.0));
    assert_eq!(
        rows[3].payload["weather_temperature_c"].as_f64(),
        Some(10.0)
    );
    assert_eq!(
        rows[3].payload["weather_humidity_percent"].as_f64(),
        Some(79.0)
    );
    assert_eq!(rows[3].payload["weather_humidity_apple_export"], "7900 %");
    assert_eq!(rows[3].payload["weather_condition_code"], 7);
    assert!((rows[3].payload["elevation_ascent_m"].as_f64().unwrap() - 53.48).abs() < 1e-9);
    assert_eq!(rows[3].payload["elevation_descent_m"].as_f64(), Some(0.0));
    assert_eq!(rows[3].payload["duration_seconds"].as_f64(), Some(1770.0));
    assert!((rows[3].payload["energy_kcal"].as_f64().unwrap() - 100.0).abs() < 0.001);
    assert_eq!(rows[3].payload["statistics"][2]["average"], 110.0);
    assert_eq!(rows[6].payload["provider_data"]["unit"], "mg/dL");
    assert!(
        (rows[6].payload["provider_data"]["value"].as_f64().unwrap() - 108.093528).abs() < 0.00001
    );
    assert!((rows[7].payload["provider_data"]["value"].as_f64().unwrap() - 37.0).abs() < 0.00001);
    assert_eq!(rows[8].payload["provider_data"]["value"], 98.0);
    assert_eq!(rows[9].payload["metric"], "walking_speed");
    assert_eq!(rows[9].payload["provider_data"]["value"], 2.0);
    assert_eq!(
        metric("HKQuantityTypeIdentifierWalkingStepLength", "75", "cm"),
        Some(("walking_step_length", 0.75, "m"))
    );
    assert_eq!(
        metric("HKQuantityTypeIdentifierPushCount", "124", "count"),
        Some(("wheelchair_pushes", 124.0, "pushes"))
    );
    assert!(metric("HKQuantityTypeIdentifierPushCount", "0.5", "count").is_none());
    assert_eq!(
        metric("HKQuantityTypeIdentifierDistanceWheelchair", "1.25", "km"),
        Some(("wheelchair_distance", 1_250.0, "m"))
    );
    assert!((rows[10].payload["provider_data"]["value"].as_f64().unwrap() - 7.62).abs() < 1e-9);
    assert!((rows[11].payload["provider_data"]["value"].as_f64().unwrap() + 1.0).abs() < 1e-9);
    assert_eq!(
        metric(
            "HKQuantityTypeIdentifierWalkingAsymmetryPercentage",
            "0.12",
            "%"
        ),
        Some(("walking_asymmetry", 12.0, "%"))
    );
    assert_eq!(
        metric(
            "HKQuantityTypeIdentifierHeartRateRecoveryOneMinute",
            "-3",
            "count/min"
        ),
        Some(("heart_rate_recovery_one_minute", -3.0, "bpm"))
    );
    Ok(())
}

#[test]
fn apple_export_normalizes_additional_vitals() {
    for (kind, value, unit, name, expected, output) in [
        (
            "ForcedExpiratoryVolume1",
            "2500",
            "mL",
            "forced_expiratory_volume_1",
            2.5,
            "L",
        ),
        (
            "ForcedVitalCapacity",
            "3.2",
            "L",
            "forced_vital_capacity",
            3.2,
            "L",
        ),
        (
            "PeakExpiratoryFlowRate",
            "6",
            "L/s",
            "peak_expiratory_flow_rate",
            360.0,
            "L/min",
        ),
        (
            "PeripheralPerfusionIndex",
            "0.125",
            "%",
            "peripheral_perfusion_index",
            12.5,
            "%",
        ),
        (
            "BloodAlcoholContent",
            "0.0005",
            "%",
            "blood_alcohol_content",
            0.05,
            "%",
        ),
        (
            "BodyMassIndex",
            "22",
            "count",
            "body_mass_index",
            22.0,
            "kg/m²",
        ),
        (
            "WalkingHeartRateAverage",
            "85",
            "count/min",
            "walking_heart_rate",
            85.0,
            "bpm",
        ),
        (
            "AppleSleepingBreathingDisturbances",
            "2",
            "count",
            "sleeping_breathing_disturbances",
            2.0,
            "count",
        ),
    ] {
        let id = format!("HKQuantityTypeIdentifier{kind}");
        assert_eq!(metric(&id, value, unit), Some((name, expected, output)));
    }
    assert!(metric(
        "HKQuantityTypeIdentifierForcedVitalCapacity",
        "3.2",
        "count"
    )
    .is_none());
}

#[test]
fn apple_export_normalizes_activity_measurements() {
    for (kind, value, unit, name, expected, output) in [
        ("AppleExerciseTime", "120", "s", "exercise_time", 2.0, "min"),
        ("AppleMoveTime", "40", "min", "move_time", 40.0, "min"),
        ("AppleStandTime", "10", "min", "stand_time", 10.0, "min"),
        (
            "FlightsClimbed",
            "4",
            "count",
            "flights_climbed",
            4.0,
            "count",
        ),
        (
            "PhysicalEffort",
            "4.5",
            "kcal/(kg*hr)",
            "physical_effort",
            4.5,
            "MET",
        ),
        (
            "StairAscentSpeed",
            "0.44",
            "m/s",
            "stair_ascent_speed",
            0.44,
            "m/s",
        ),
        (
            "StairDescentSpeed",
            "0.49",
            "m/s",
            "stair_descent_speed",
            0.49,
            "m/s",
        ),
        (
            "SixMinuteWalkTestDistance",
            "0.352",
            "km",
            "six_minute_walk_test_distance",
            352.0,
            "m",
        ),
        (
            "WaistCircumference",
            "0.8125",
            "m",
            "waist_circumference",
            81.25,
            "cm",
        ),
        (
            "AppleWalkingSteadiness",
            "0.875",
            "%",
            "walking_steadiness",
            87.5,
            "%",
        ),
        (
            "DistanceDownhillSnowSports",
            "1.25",
            "km",
            "downhill_snow_sports_distance",
            1250.0,
            "m",
        ),
        (
            "SwimmingStrokeCount",
            "27",
            "count",
            "swimming_stroke_count",
            27.0,
            "count",
        ),
        (
            "RunningGroundContactTime",
            "0.244",
            "s",
            "running_ground_contact_time",
            244.0,
            "ms",
        ),
        ("RunningPower", "243", "W", "running_power", 243.0, "W"),
        ("RunningSpeed", "12.6", "km/hr", "running_speed", 3.5, "m/s"),
        (
            "RunningStrideLength",
            "1.125",
            "m",
            "running_stride_length",
            112.5,
            "cm",
        ),
        (
            "RunningVerticalOscillation",
            "8.5",
            "cm",
            "running_vertical_oscillation",
            8.5,
            "cm",
        ),
    ] {
        assert_eq!(
            metric(&format!("HKQuantityTypeIdentifier{kind}"), value, unit),
            Some((name, expected, output))
        );
    }
}

#[test]
fn apple_export_preserves_exposure_quantities_and_units() {
    for (kind, value, unit, name, expected, output) in [
        (
            "EnvironmentalAudioExposure",
            "65.5",
            "dBASPL",
            "environmental_audio_exposure",
            65.5,
            "dBASPL",
        ),
        (
            "EnvironmentalSoundReduction",
            "-2.5",
            "dBASPL",
            "environmental_sound_reduction",
            -2.5,
            "dBASPL",
        ),
        (
            "HeadphoneAudioExposure",
            "73",
            "dBASPL",
            "headphone_audio_exposure",
            73.0,
            "dBASPL",
        ),
        ("InhalerUsage", "2", "count", "inhaler_usage", 2.0, "count"),
        (
            "TimeInDaylight",
            "630",
            "s",
            "time_in_daylight",
            10.5,
            "min",
        ),
        ("UVExposure", "4", "count", "uv_exposure", 4.0, "count"),
    ] {
        assert_eq!(
            metric(&format!("HKQuantityTypeIdentifier{kind}"), value, unit),
            Some((name, expected, output))
        );
    }
    assert!(metric("HKQuantityTypeIdentifierUVExposure", "-1", "count").is_none());
}

#[test]
fn apple_export_normalizes_micro_and_macronutrient_masses() {
    for (kind, value, unit, expected, output) in [
        ("Protein", "12000", "mg", 12.0, "g"),
        ("FatMonounsaturated", "1.125", "g", 1.125, "g"),
        ("Fiber", "3.5", "g", 3.5, "g"),
        ("Sodium", "0", "mg", 0.0, "mg"),
        ("Calcium", "0.125", "g", 125.0, "mg"),
        ("Folate", "24", "µg", 24.0, "mcg"),
        ("VitaminB12", "0.0025", "mg", 2.5, "mcg"),
        ("VitaminD", "4000", "ug", 4000.0, "mcg"),
    ] {
        let (_, actual, actual_unit) = metric(
            &format!("HKQuantityTypeIdentifierDietary{kind}"),
            value,
            unit,
        )
        .unwrap();
        assert_eq!((actual, actual_unit), (expected, output));
    }
    assert!(metric("HKQuantityTypeIdentifierDietaryVitaminA", "80", "IU").is_none());
    assert!(metric("HKQuantityTypeIdentifierDietarySodium", "-1", "g").is_none());
}

#[test]
fn apple_export_preserves_a_mindfulness_interval() -> Result<()> {
    let xml = r#"<HealthData><Record type="HKCategoryTypeIdentifierMindfulSession"
        sourceName="Watch" value="HKCategoryValueNotApplicable" startDate="2026-02-09 06:30:00 +0900"
        endDate="2026-02-09 06:38:30 +0900"/></HealthData>"#;
    let mut reader = ExportReader::new(xml.as_bytes());
    let rows = reader.batch()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].payload["metric"], "mindfulness_session");
    assert_eq!(rows[0].payload["provider_data"]["duration_seconds"], 510.0);
    assert_eq!(
        rows[0].payload["provider_data"]["utc_offset_seconds"],
        32_400
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn apple_export_zip_is_replayable_queryable_and_atomic() -> Result<()> {
    let pool = store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "wearable-apple-export-fixture";
    let token = "apple-export-fixture-read-token-12345678901234";
    let db = pool.get().await?;
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
        &[&subject,&token]).await?;
    ensure_subject(&pool, subject).await?;
    assert!(
        ensure_subject(&pool, "wearable-unknown-apple-export-fixture")
            .await
            .is_err()
    );
    let path = std::env::temp_dir().join(format!(
        "avatar-apple-export-fixture-{}-{}.zip",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    ));
    struct RemoveZip(std::path::PathBuf);
    impl Drop for RemoveZip {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let file = File::create_new(&path)?;
    let path = RemoveZip(path);
    let mut archive = zip::ZipWriter::new(file);
    archive.start_file(
        "apple_health_export/export.xml",
        zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated),
    )?;
    archive.write_all(XML.as_bytes())?;
    archive.start_file(
        "apple_health_export/workout-routes/route.gpx",
        zip::write::SimpleFileOptions::default(),
    )?;
    archive.write_all(GPX.as_bytes())?;
    drop(archive.finish()?);
    for _ in 0..2 {
        let stats = import_file(&pool, subject, &path.0).await?;
        assert_eq!(stats.accepted, 17);
        assert_eq!(stats.routes, 1);
    }
    let count = || async {
        Ok::<_, anyhow::Error>(db.query_one("SELECT count(*) FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider=$2",
            &[&subject,&PROVIDER]).await?.get::<_,i64>(0))
    };
    assert_eq!(count().await?, 17);
    let mut invalid = String::from("<HealthData>");
    for n in 0..BATCH_SIZE {
        invalid.push_str(&format!(r#"<Record type="HKQuantityTypeIdentifierHeartRate" sourceName="Broken file" unit="count/min" value="80" device="{n}" startDate="2026-02-09 11:00:00 +0900" endDate="2026-02-09 11:00:00 +0900"/>"#));
    }
    let mut invalid_db = pool.get().await?;
    let invalid_tx = invalid_db.transaction().await?;
    prepare_import(&invalid_tx, subject).await?;
    assert!(import_xml_rows(
        &invalid_tx,
        subject,
        invalid.as_bytes(),
        invalid.len() as u64
    )
    .await
    .is_err());
    invalid_tx.rollback().await?;
    assert_eq!(count().await?, 17);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    let app = std::sync::Arc::new(crate::mcp::App {
        db: pool.clone(),
        read_token: "apple-export-operator-fixture-read-token-12345".into(),
        control_token: "apple-export-operator-fixture-control-token-12345".into(),
        rules: crate::acute::rules()?,
    });
    let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
    let client = crate::cloud_wearables::http_client()?;
    for (tool, args, expected) in [
        (
            "wearable_samples",
            json!({"provider":PROVIDER,"kind":"metric","metric":"underwater_depth","start":"2026-02-08T15:00:00Z","end":"2026-02-09T15:00:00Z"}),
            "metric",
        ),
        (
            "wearable_samples",
            json!({"provider":PROVIDER,"kind":"metric","metric":"running_ground_contact_time","start":"2026-02-08T15:00:00Z","end":"2026-02-09T15:00:00Z"}),
            "metric",
        ),
        (
            "wearable_samples",
            json!({"provider":PROVIDER,"kind":"metric","metric":"time_in_daylight","start":"2026-02-08T15:00:00Z","end":"2026-02-09T15:00:00Z"}),
            "metric",
        ),
        (
            "wearable_samples",
            json!({"provider":PROVIDER,"kind":"metric","metric":"dietary_vitamin_b12","start":"2026-02-08T15:00:00Z","end":"2026-02-09T15:00:00Z"}),
            "metric",
        ),
        (
            "wearable_samples",
            json!({"provider":PROVIDER,"kind":"metric","metric":"weather_humidity","start":"2026-02-08T15:00:00Z","end":"2026-02-09T15:00:00Z"}),
            "metric",
        ),
        (
            "wearable_samples",
            json!({"provider":PROVIDER,"kind":"metric","metric":"weather_temperature","start":"2026-02-08T15:00:00Z","end":"2026-02-09T15:00:00Z"}),
            "metric",
        ),
        (
            "wearable_samples",
            json!({"provider":PROVIDER,"kind":"metric","metric":"workout_effort_score","start":"2026-02-08T15:00:00Z","end":"2026-02-09T15:00:00Z"}),
            "metric",
        ),
        (
            "wearable_samples",
            json!({"provider":PROVIDER,"kind":"metric","metric":"estimated_workout_effort_score","start":"2026-02-08T15:00:00Z","end":"2026-02-09T15:00:00Z"}),
            "metric",
        ),
        (
            "wearable_daily",
            json!({"provider":PROVIDER,"start_day":"2026-02-09","end_day":"2026-02-09"}),
            "steps",
        ),
        (
            "wearable_samples",
            json!({"provider":PROVIDER,"kind":"heart_rate","start":"2026-02-08T15:00:00Z","end":"2026-02-09T15:00:00Z"}),
            "heart_rate",
        ),
        (
            "wearable_sessions",
            json!({"provider":PROVIDER,"kind":"sleep","start":"2026-02-08T15:00:00Z","end":"2026-02-09T15:00:00Z"}),
            "sleep",
        ),
        (
            "wearable_sessions",
            json!({"provider":PROVIDER,"kind":"workout","start":"2026-02-08T15:00:00Z","end":"2026-02-09T15:00:00Z"}),
            "workout",
        ),
    ] {
        let envelope = client.post(format!("http://{address}/mcp/wearable-read")).bearer_auth(token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":tool,"arguments":args}}))
            .send().await?.error_for_status()?.json::<Value>().await?;
        let records = &envelope["result"]["structuredContent"]["records"];
        assert_eq!(records.as_array().map(Vec::len), Some(1), "{envelope}");
        assert_eq!(records[0]["kind"], expected);
        if expected == "steps" {
            assert_eq!(records[0]["stats"]["total"], 420);
        } else if args["metric"] == "running_ground_contact_time" {
            assert_eq!(records[0]["payload"]["provider_data"]["unit"], "ms");
            assert_eq!(records[0]["payload"]["provider_data"]["value"], 244.0);
            assert_eq!(
                timestamp(records[0]["started_at"].as_str().unwrap()),
                timestamp("2026-02-09T02:00:00Z")
            );
        } else if args["metric"] == "time_in_daylight" {
            assert_eq!(records[0]["payload"]["provider_data"]["unit"], "min");
            assert_eq!(records[0]["payload"]["provider_data"]["value"], 10.5);
            assert_eq!(
                timestamp(records[0]["ended_at"].as_str().unwrap()),
                timestamp("2026-02-09T02:10:30Z")
            );
        } else if args["metric"] == "dietary_vitamin_b12" {
            assert_eq!(records[0]["payload"]["provider_data"]["unit"], "mcg");
            assert_eq!(records[0]["payload"]["provider_data"]["value"], 2.5);
            assert_eq!(records[0]["source"], "Nutrition");
        } else if matches!(
            args["metric"].as_str(),
            Some("workout_effort_score" | "estimated_workout_effort_score")
        ) {
            let value = if args["metric"] == "workout_effort_score" {
                7.0
            } else {
                5.0
            };
            assert_eq!(records[0]["payload"]["provider_data"]["unit"], "score");
            assert_eq!(
                records[0]["payload"]["provider_data"]["value"].as_f64(),
                Some(value)
            );
        } else if matches!(
            args["metric"].as_str(),
            Some("weather_humidity" | "weather_temperature")
        ) {
            let (unit, value) = if args["metric"] == "weather_humidity" {
                ("%", 79.0)
            } else {
                ("°C", 10.0)
            };
            assert_eq!(records[0]["payload"]["provider_data"]["unit"], unit);
            assert_eq!(
                records[0]["payload"]["provider_data"]["value"].as_f64(),
                Some(value)
            );
            assert!(records[0]["payload"]["provider_data"]["workout_id"].is_string());
        } else if expected == "metric" {
            assert_eq!(records[0]["payload"]["provider_data"]["unit"], "m");
            assert!(
                (records[0]["payload"]["provider_data"]["value"]
                    .as_f64()
                    .unwrap()
                    - 7.62)
                    .abs()
                    < 1e-9
            );
        } else if expected == "workout" {
            assert_eq!(records[0]["route_point_count"], 2);
            assert_eq!(
                records[0]["payload"]["weather_temperature_c"].as_f64(),
                Some(10.0)
            );
            assert_eq!(
                records[0]["payload"]["distance_meters"].as_f64(),
                Some(2500.0)
            );
            assert!(records[0]["payload"].get("route_files").is_none());
            let route = |offset| {
                json!({"apple_route":{
                    "source":records[0]["source"],"source_id":records[0]["source_id"],"limit":1,"offset":offset
                }})
            };
            let request = serde_json::from_value(route(0)["apple_route"].clone())?;
            assert_eq!(
                apple_export_routes::query(&pool, "different-wearable-owner", request).await?
                    ["state"],
                "not_found"
            );
            for (offset, latitude) in [(0, 37.5), (1, 37.6)] {
                let envelope: Value = client.post(format!("http://{address}/mcp/wearable-read")).bearer_auth(token)
                    .json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"wearable_sessions","arguments":route(offset)}}))
                    .send().await?.error_for_status()?.json().await?;
                let data = &envelope["result"]["structuredContent"];
                assert_eq!(data["stored_point_count"], 2, "{envelope}");
                assert_eq!(data["records"][0]["latitude"].as_f64(), Some(latitude));
                assert_eq!(data["records"][0]["segment"], offset + 1);
                assert_eq!(data["next_offset"].as_u64(), (offset == 0).then_some(1));
            }
        }
    }
    server.abort();
    wearables::revoke_for(&pool, subject, PROVIDER).await?;
    assert_eq!(count().await?, 0);
    let routes: i64 = db
        .query_one(
            "SELECT count(*) FROM avatar_hsm.apple_export_workout_route WHERE subject_id=$1",
            &[&subject],
        )
        .await?
        .get(0);
    assert_eq!(routes, 0);
    Ok(())
}
