use super::*;

fn sample() -> Value {
    json!({"activities":[
        {"userId":"garmin-bound","userAccessToken":"must-not-be-stored","activityId":123,
            "startTimeInSeconds":1777012200,"durationInSeconds":1500,"activityType":"RUNNING"},
        {"userId":"unconnected","activityId":987,
            "startTimeInSeconds":1777012200,"durationInSeconds":1900,"activityType":"WALKING"}
    ],"dailies":[{"userId":"garmin-bound","summaryId":"day-1","startTimeInSeconds":1777012200,
       "calendarDate":"2026-04-24","durationInSeconds":86400,"steps":850,"activeKilocalories":120}]})
}

#[test]
fn garmin_sleep_stage_intervals_are_ordered_and_normalized() -> Result<()> {
    let start = DateTime::from_timestamp(1777012200, 0).context("fixture start")?;
    let end = start + Duration::minutes(30);
    let stages = json!({
        "rem":[{"startTimeInSeconds":1777012800,"endTimeInSeconds":1777014000}],
        "light":[{"startTimeInSeconds":1777012200,"endTimeInSeconds":1777012800}]
    });
    let result = sleep_stages(&stages, start, end);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0]["stage"], 4);
    assert_eq!(result[1]["stage"], 6);
    let mut invalid = stages;
    invalid["light"][0]["endTimeInSeconds"] = json!(1777012801);
    assert!(sleep_stages(&invalid, start, end).is_empty());
    Ok(())
}

#[test]
fn ping_urls_enqueue_pull_instead_of_decoding_push_fields() -> Result<()> {
    assert!(decode(&json!({"activities":[{"userId":"garmin-bound","callbackURL":"https://local.invalid/private"}]}), Utc::now()).is_err());
    let ping = json!({"activities":[{"userId":"garmin-bound","callbackURL":"https://apis.garmin.com/wellness-api/rest/activities?uploadStartTimeInSeconds=1&uploadEndTimeInSeconds=2"}]});
    let queued = decode(&ping, Utc::now())?;
    let owner = &queued[&hash(b"garmin-bound")];
    assert_eq!(owner.pulls.len(), 1);
    assert!(owner.records.is_empty());
    let groups = decode(&sample(), Utc::now())?;
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[&hash(b"garmin-bound")].records.len(), 2);
    let valid = "https://apis.garmin.com/wellness-api/rest/activityFile?id=123&token=fixture";
    let file = |url| json!({"activityFiles":[{"userId":"garmin-bound","activityId":123,"fileType":"FIT","callbackURL":url}]});
    assert_eq!(
        decode(&file(valid), Utc::now())?[&hash(b"garmin-bound")]
            .fit_files
            .len(),
        1
    );
    assert!(decode(
        &file("https://apis.garmin.com.other.invalid/wellness-api/rest/activityFile"),
        Utc::now()
    )
    .is_err());
    assert!(decode(
        &file("https://apis.garmin.com/wellness-api/rest/user/id"),
        Utc::now()
    )
    .is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
async fn garmin_push_is_atomic_owner_scoped_and_revocable() -> Result<()> {
    let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let subject = "garmin-push-fixture";
    db.execute(
        "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
        &[&subject],
    )
    .await?;
    db.execute(
        "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'garmin')",
        &[&subject],
    )
    .await?;
    db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))", &[&subject]).await?;
    db.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash,garmin_permissions) VALUES($1,'garmin',''::bytea,$2,3)", &[&subject,&hash(b"garmin-bound")]).await?;

    persist(&pool, b"first", decode(&sample(), Utc::now())?).await?;
    persist(&pool, b"first", decode(&sample(), Utc::now())?).await?;
    let records = db.query("SELECT source_id,payload FROM avatar_hsm.wearable_record WHERE subject_id=$1 ORDER BY kind", &[&subject]).await?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].get::<_, String>(0), "123");
    let workout: Value = records[1].get(1);
    assert_eq!(workout["provider_data"]["userAccessToken"], Value::Null);
    assert_eq!(workout["provider_data"]["userId"], Value::Null);
    let receipt = db
        .query_one(
            "SELECT count(*) FROM watch_tower.wearable_webhook_receipt WHERE provider='garmin' AND event_hash=$1",
            &[&&b"first"[..]],
        )
        .await?;
    assert_eq!(receipt.get::<_, i64>(0), 1);
    let query = wearables::RecordQuery {
        kind: "workout".into(),
        start: DateTime::from_timestamp(1777010000, 0).context("fixture start")?,
        end: DateTime::from_timestamp(1777020000, 0).context("fixture end")?,
        offset: 0,
        source: None,
        provider: Some(PROVIDER.into()),
        metric: None,
    };
    let sessions = wearables::records_for(&pool, subject, query, true).await?;
    assert_eq!(sessions["records"].as_array().map(Vec::len), Some(1));

    if env::var_os("AVATAR_WEARABLE_OAUTH_KEY").is_some() {
        let url = "https://apis.garmin.com/wellness-api/rest/activityFile?id=123&token=fixture";
        let file = json!({"activityFiles":[{"userId":"garmin-bound","activityId":123,"fileType":"FIT","callbackURL":url}]});
        persist(&pool, b"fit", decode(&file, Utc::now())?).await?;
        let job = db.query_one("SELECT sealed_source FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider='garmin' AND activity_id='123'", &[&subject]).await?;
        let sealed: Vec<u8> = job.get(0);
        assert_ne!(sealed, url.as_bytes());
        assert!(
            wearable_oauth::open_garmin_fit_url("another-subject", "123", sealed.clone()).is_err()
        );
        assert_eq!(
            wearable_oauth::open_garmin_fit_url(subject, "123", sealed)?,
            url
        );
    }

    let mut changed = sample();
    changed["userPermissionsChange"] =
        json!([{"userId":"garmin-bound","permissions":["HEALTH_EXPORT"]}]);
    let generation: String = db.query_one("SELECT generation::text FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1 AND provider='garmin'", &[&subject]).await?.get(0);
    let current = Utc::now();
    let mut confirmed = decode(&changed, current)?;
    confirmed
        .get_mut(&hash(b"garmin-bound"))
        .context("fixture owner")?
        .permission_check = Some((generation.clone(), current));
    persist(&pool, b"permissions", confirmed).await?;
    let broad = json!({"userPermissionsChange":[{"userId":"garmin-bound","permissions":["ACTIVITY_EXPORT","HEALTH_EXPORT"]}]});
    let mut stale = decode(&broad, current)?;
    stale
        .get_mut(&hash(b"garmin-bound"))
        .context("fixture owner")?
        .permission_check = Some((generation, current - Duration::seconds(1)));
    persist(&pool, b"stale-permissions", stale).await?;
    persist(&pool, b"second-push", decode(&sample(), Utc::now())?).await?;
    let records = db
        .query(
            "SELECT kind FROM avatar_hsm.wearable_record WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].get::<_, String>(0), "metric");
    let pending_files = db
        .query_one(
            "SELECT count(*) FROM watch_tower.wearable_fit_job WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
    assert_eq!(pending_files.get::<_, i64>(0), 0);

    let daily = || wearables::DailyQuery {
        start_day: chrono::NaiveDate::from_ymd_opt(2026, 4, 24).unwrap(),
        end_day: chrono::NaiveDate::from_ymd_opt(2026, 4, 24).unwrap(),
        source: None,
        provider: Some(PROVIDER.into()),
        kind: Some("activity".into()),
    };
    let activity = wearables::daily_for(&pool, subject, daily()).await?;
    assert_eq!(activity["records"][0]["stats"]["active_energy_kcal"], 120);
    assert_eq!(
        activity["records"][0]["stats"]["total_energy_kcal"],
        Value::Null
    );
    let mut db_write = pool.get().await?;
    let tx = db_write.transaction().await?;
    apply_permissions(&tx, subject, ACTIVITY_EXPORT, Utc::now()).await?;
    tx.commit().await?;
    persist(
        &pool,
        b"after-health-withdrawal",
        decode(&sample(), Utc::now())?,
    )
    .await?;
    let activity = wearables::daily_for(&pool, subject, daily()).await?;
    assert_eq!(activity["state"], "permission_missing");
    assert_eq!(activity["records"], json!([]));

    let revoke = decode(
        &json!({"deregistrations":[{"userId":"garmin-bound"}]}),
        Utc::now(),
    )?;
    persist(&pool, b"revoke", revoke).await?;
    persist(&pool, b"other", decode(&sample(), Utc::now())?).await?;
    let status = wearables::status_for(&pool, subject, PROVIDER).await?;
    assert_eq!(status["state"], "revoked");
    let records = db
        .query_one(
            "SELECT count(*) FROM avatar_hsm.wearable_record WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
    assert_eq!(records.get::<_, i64>(0), 0);
    let jobs = db
        .query_one(
            "SELECT count(*) FROM watch_tower.wearable_fit_job WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
    assert_eq!(jobs.get::<_, i64>(0), 0);
    Ok(())
}
