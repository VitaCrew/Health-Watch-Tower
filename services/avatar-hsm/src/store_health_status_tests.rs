use super::*;

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database"]
async fn health_status_is_read_only_and_user_response_is_due_idempotent_and_terminal() -> Result<()>
{
    let pool = connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let now = Utc::now();
    let id = format!(
        "health-status-v1-{}",
        now.timestamp_nanos_opt().unwrap_or_default()
    );
    let scheduled = now - chrono::Duration::minutes(10);
    let eligible = now - chrono::Duration::minutes(5);
    let expires = now + chrono::Duration::hours(1);
    db.execute(
        "INSERT INTO avatar_hsm.dose
           (id,subject_id,item_seq,scheduled_at,eligible_at,expires_at)
         VALUES($1,$2,'SYNTH-A10',$3,$4,$5)",
        &[&id, &SUBJECT, &scheduled, &eligible, &expires],
    )
    .await?;

    let before = db
        .query_one(
            "SELECT requested_at IS NULL FROM avatar_hsm.dose WHERE id=$1",
            &[&id],
        )
        .await?
        .get::<_, bool>(0);
    let facts = health_status_v1(&db, now).await?;
    let after_read = db
        .query_one(
            "SELECT requested_at IS NULL FROM avatar_hsm.dose WHERE id=$1",
            &[&id],
        )
        .await?
        .get::<_, bool>(0);
    assert!(before && after_read);
    assert!(facts["medication_doses"]
        .as_array()
        .unwrap()
        .iter()
        .any(|dose| dose["dose_id"] == id));

    let taken = respond_v1(&pool, &id, "taken", now).await?;
    assert_eq!(taken["response"], "taken");
    assert_eq!(
        respond_v1(&pool, &id, "taken", now + chrono::Duration::minutes(1)).await?["response"],
        "taken"
    );
    assert!(respond_v1(&pool, &id, "skipped", now).await.is_err());
    assert!(respond_v1(&pool, &id, "not_yet", now).await.is_err());

    db.execute("DELETE FROM avatar_hsm.dose WHERE id=$1", &[&id])
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database"]
async fn request_scoped_hsm_records_do_not_cross_subjects() -> Result<()> {
    let pool = connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let client = pool.get().await?;
    let suffix = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let subject_a = format!("scope-a-{suffix}");
    let subject_b = format!("scope-b-{suffix}");
    let shared_record_id = format!("scope-dose-{suffix}");
    let now = Utc::now();
    let scheduled = now - chrono::Duration::minutes(10);
    let eligible = now - chrono::Duration::minutes(5);
    let expires = now + chrono::Duration::hours(1);

    client
        .execute(
            "INSERT INTO avatar_hsm.subject(id,timezone) VALUES($1,'UTC'),($2,'UTC')",
            &[&subject_a, &subject_b],
        )
        .await?;
    client
        .execute(
            "INSERT INTO avatar_hsm.dose(id,subject_id,item_seq,scheduled_at,eligible_at,expires_at)
             VALUES($1,$2,'SYNTH-A10',$3,$4,$5),($1,$6,'SYNTH-A10',$3,$4,$5)",
            &[&shared_record_id, &subject_a, &scheduled, &eligible, &expires, &subject_b],
        )
        .await?;

    crate::mcp::scoped_call(
        &pool,
        crate::mcp::Surface::Control,
        &subject_a,
        "record_medication_response_v1",
        &json!({"dose_id":shared_record_id,"response":"taken"}),
    )
    .await?;
    let state_a = crate::mcp::scoped_call(
        &pool,
        crate::mcp::Surface::Read,
        &subject_a,
        "health_status_v1",
        &json!({}),
    )
    .await?;
    let state_b = crate::mcp::scoped_call(
        &pool,
        crate::mcp::Surface::Read,
        &subject_b,
        "health_status_v1",
        &json!({}),
    )
    .await?;
    let dose_is_taken = |state: &Value| {
        state["medication_doses"]
            .as_array()
            .unwrap()
            .iter()
            .any(|dose| dose["dose_id"] == shared_record_id && dose["response"] == "taken")
    };
    let dose_is_unanswered = |state: &Value| {
        state["medication_doses"]
            .as_array()
            .unwrap()
            .iter()
            .any(|dose| dose["dose_id"] == shared_record_id && dose["response"].is_null())
    };
    assert!(dose_is_taken(&state_a));
    assert!(dose_is_unanswered(&state_b));

    crate::mcp::scoped_call(
        &pool,
        crate::mcp::Surface::Control,
        &subject_a,
        "confirm_diary_entry",
        &json!({"id":shared_record_id,"entry":{"kind":"water","ml":300}}),
    )
    .await?;
    crate::mcp::scoped_call(
        &pool,
        crate::mcp::Surface::Control,
        &subject_b,
        "confirm_diary_entry",
        &json!({"id":shared_record_id,"entry":{"kind":"water","ml":500}}),
    )
    .await?;
    let diary_query = json!({
        "start": (now - chrono::Duration::days(1)),
        "end": (now + chrono::Duration::days(1)),
    });
    let diary_a = crate::mcp::scoped_call(
        &pool,
        crate::mcp::Surface::Read,
        &subject_a,
        "diary_entries",
        &diary_query,
    )
    .await?;
    let diary_b = crate::mcp::scoped_call(
        &pool,
        crate::mcp::Surface::Read,
        &subject_b,
        "diary_entries",
        &diary_query,
    )
    .await?;
    let water_amount = |entries: &Value| {
        entries["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == shared_record_id)
            .and_then(|entry| entry["entry"]["ml"].as_u64())
    };
    assert_eq!(water_amount(&diary_a), Some(300));
    assert_eq!(water_amount(&diary_b), Some(500));
    assert_eq!(diary_a["synthetic"], false);
    assert_eq!(diary_b["synthetic"], false);

    let start_a = now - chrono::Duration::hours(8);
    let end_a = now - chrono::Duration::hours(1);
    let start_b = start_a - chrono::Duration::hours(2);
    let end_b = end_a - chrono::Duration::hours(2);
    crate::store::save_sleep_for(&pool, &subject_a, &shared_record_id, start_a, end_a).await?;
    crate::store::save_sleep_for(&pool, &subject_b, &shared_record_id, start_b, end_b).await?;
    let sleep_a = crate::store::sleep_sessions_for(&client, &subject_a).await?;
    let sleep_b = crate::store::sleep_sessions_for(&client, &subject_b).await?;
    let stored_start_a = chrono::DateTime::parse_from_rfc3339(
        sleep_a["completed_sleep_sessions"][0]["started_at"]
            .as_str()
            .unwrap(),
    )?
    .with_timezone(&Utc);
    let stored_start_b = chrono::DateTime::parse_from_rfc3339(
        sleep_b["completed_sleep_sessions"][0]["started_at"]
            .as_str()
            .unwrap(),
    )?
    .with_timezone(&Utc);
    assert_eq!(
        stored_start_a.timestamp_micros(),
        start_a.timestamp_micros()
    );
    assert_eq!(
        stored_start_b.timestamp_micros(),
        start_b.timestamp_micros()
    );

    client
        .execute(
            "DELETE FROM avatar_hsm.dose WHERE subject_id = ANY($1)",
            &[&vec![subject_a.clone(), subject_b.clone()]],
        )
        .await?;
    client
        .execute(
            "DELETE FROM avatar_hsm.sleep_session WHERE subject_id = ANY($1)",
            &[&vec![subject_a.clone(), subject_b.clone()]],
        )
        .await?;
    client
        .execute(
            "DELETE FROM avatar_hsm.diary_entry WHERE subject_id = ANY($1)",
            &[&vec![subject_a.clone(), subject_b.clone()]],
        )
        .await?;
    client
        .execute(
            "DELETE FROM avatar_hsm.subject WHERE id = ANY($1)",
            &[&vec![subject_a, subject_b]],
        )
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database"]
async fn public_catalog_cache_upserts_and_reads_without_person_scope() -> Result<()> {
    let pool = connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let source_id = format!(
        "verification-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let retrieved_at = Utc::now();
    let item = json!({
        "source": "verification_catalog",
        "source_id": source_id,
        "name": "Test food",
        "food_nutrients": [{"nutrientName":"Protein","value":12.5,"unitName":"G"}],
        "license_id": "CC0-1.0",
        "license_uri": "https://creativecommons.org/publicdomain/zero/1.0/",
        "source_uri": "https://example.test/item",
        "retrieved_at": retrieved_at.to_rfc3339(),
    });
    crate::catalog_store::save(&pool, std::slice::from_ref(&item)).await?;
    let fresh_after = retrieved_at - chrono::Duration::hours(1);
    let fresh = crate::catalog_store::find(
        &pool,
        "verification_catalog",
        item["source_id"].as_str().unwrap(),
        Some(fresh_after),
    )
    .await?;
    assert_eq!(fresh.len(), 1);
    assert_eq!(fresh[0]["food_nutrients"][0]["value"], 12.5);
    assert_eq!(fresh[0]["license_id"], "CC0-1.0");
    let name_match =
        crate::catalog_store::find(&pool, "verification_catalog", "Test", Some(fresh_after))
            .await?;
    assert_eq!(name_match.len(), 1);

    let too_recent = retrieved_at + chrono::Duration::minutes(1);
    assert!(crate::catalog_store::find(
        &pool,
        "verification_catalog",
        item["source_id"].as_str().unwrap(),
        Some(too_recent),
    )
    .await?
    .is_empty());
    assert_eq!(
        crate::catalog_store::find(
            &pool,
            "verification_catalog",
            item["source_id"].as_str().unwrap(),
            None,
        )
        .await?
        .len(),
        1
    );

    let updated_at = retrieved_at + chrono::Duration::minutes(2);
    let updated = json!({
        "source": "verification_catalog",
        "source_id": item["source_id"],
        "name": "Updated test food",
        "food_nutrients": [{"nutrientName":"Protein","value":13.0,"unitName":"G"}],
        "license_id": "CC0-1.0",
        "license_uri": "https://creativecommons.org/publicdomain/zero/1.0/",
        "source_uri": "https://example.test/item",
        "retrieved_at": updated_at.to_rfc3339(),
    });
    crate::catalog_store::save(&pool, std::slice::from_ref(&updated)).await?;
    let after_upsert = crate::catalog_store::find(
        &pool,
        "verification_catalog",
        updated["source_id"].as_str().unwrap(),
        Some(retrieved_at + chrono::Duration::minutes(1)),
    )
    .await?;
    assert_eq!(after_upsert.len(), 1);
    assert_eq!(after_upsert[0]["name"], "Updated test food");

    let db = pool.get().await?;
    let has_subject_id: bool = db
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='avatar_catalog' AND table_name='reference_item'
                  AND column_name='subject_id'
            )",
            &[],
        )
        .await?
        .get(0);
    assert!(!has_subject_id);
    db.execute(
        "DELETE FROM avatar_catalog.reference_item WHERE source='verification_catalog' AND source_id=$1",
        &[&item["source_id"].as_str().unwrap()],
    )
    .await?;
    Ok(())
}
