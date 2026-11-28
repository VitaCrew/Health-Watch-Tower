use super::*;

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database in WATCH_TOWER_TEST_DATABASE_URL"]
async fn confirmed_fhir_import_persists_provenance_and_idempotent_hsm_outbox() -> Result<()> {
    let url = std::env::var("WATCH_TOWER_TEST_DATABASE_URL")?;
    let pool = crate::store::connect(&url).await?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let subject = format!("clinical-test-{suffix}");
    let client_id = format!("clinical-client-{suffix}");
    let actor_id = format!("clinical-actor-{suffix}");
    let source = "https://hospital.example.test/fhir";
    let client = pool.get().await?;
    client
        .execute(
            "INSERT INTO watch_tower.consent_grant
         (grant_id,subject_id,client_id,scope,version,granted_by_identity)
         VALUES($1,$2,$3,'records.write',1,'owner:test')",
            &[&format!("grant-{suffix}"), &subject, &client_id],
        )
        .await?;
    bind_patient(
        &pool,
        &subject,
        &actor_id,
        &client_id,
        1,
        &json!({"source_system":source,"patient_reference":"Patient/p-1"}),
    )
    .await?;
    let bundle = json!({
        "resourceType":"Bundle","type":"collection",
        "entry":[
            {"fullUrl":"Patient/p-1","resource":{"resourceType":"Patient","id":"p-1"}},
            {"fullUrl":"Condition/c-1","resource":{
                "resourceType":"Condition","id":"c-1","clinicalStatus":{"coding":[{"system":"http://terminology.hl7.org/CodeSystem/condition-clinical","code":"active"}]},
                "verificationStatus":{"coding":[{"system":"http://terminology.hl7.org/CodeSystem/condition-ver-status","code":"confirmed"}]},
                "code":{"coding":[{"system":"http://snomed.info/sct","code":"123456","display":"sample"}]},
                "subject":{"reference":"Patient/p-1"},"recordedDate":"2025-01-01T12:00:00Z"
            }}
        ]
    });
    let args = json!({
        "idempotency_key":format!("batch-{suffix}"),"source_system":source,
        "patient_reference":"Patient/p-1","bundle":bundle
    });
    let first = import_bundle(&pool, &subject, &actor_id, &client_id, 1, &args).await?;
    assert_eq!(first["imported"], 1);
    assert_eq!(first["hsm_state"], "queued");
    let replay = import_bundle(&pool, &subject, &actor_id, &client_id, 1, &args).await?;
    assert_eq!(replay["imported"], 0);
    assert_eq!(replay["unchanged"], 1);
    let stored = list_records(&pool, &subject, 10).await?;
    assert_eq!(stored["count"], 1);
    assert_eq!(stored["records"][0]["resource_type"], "Condition");
    let outbox: i64 = client
        .query_one(
            "SELECT count(*) FROM watch_tower.clinical_hsm_outbox WHERE subject_id=$1",
            &[&subject],
        )
        .await?
        .get(0);
    assert_eq!(outbox, 1);

    let mut revised = args.clone();
    revised["bundle"]["entry"][1]["resource"]["meta"] = json!({"versionId":"2"});
    revised["bundle"]["entry"][1]["resource"]["clinicalStatus"]["coding"][0]["code"] =
        json!("resolved");
    let correction = import_bundle(&pool, &subject, &actor_id, &client_id, 1, &revised).await?;
    assert_eq!(correction["imported"], 1);
    let versions = list_records(&pool, &subject, 10).await?;
    assert_eq!(versions["count"], 2);
    assert!(versions["records"]
        .as_array()
        .unwrap()
        .iter()
        .any(|record| !record["supersedes_record_id"].is_null()));

    client
        .execute(
            "UPDATE watch_tower.consent_grant SET revoked_at=now()
             WHERE subject_id=$1 AND client_id=$2 AND scope='records.write'",
            &[&subject, &client_id],
        )
        .await?;
    let mut after_revocation = revised.clone();
    after_revocation["bundle"]["entry"][1]["resource"]["meta"]["versionId"] = json!("3");
    assert!(
        import_bundle(&pool, &subject, &actor_id, &client_id, 1, &after_revocation)
            .await
            .is_err()
    );

    client
        .execute(
            "DELETE FROM watch_tower.clinical_record WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
    client
        .execute(
            "DELETE FROM watch_tower.clinical_patient_binding WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
    client
        .execute(
            "DELETE FROM watch_tower.consent_grant WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
    Ok(())
}

#[test]
fn kr_core_profiles_are_bound_to_their_fhir_resource_types() {
    assert!(valid_canonical(
        "http://www.hl7korea.or.kr/fhir/krcore/StructureDefinition/krcore-patient"
    ));
    assert!(kr_core_profile_matches(
        "Patient",
        "http://www.hl7korea.or.kr/fhir/krcore/StructureDefinition/krcore-patient"
    ));
    assert!(kr_core_profile_matches(
        "Condition",
        "http://www.hl7korea.or.kr/fhir/krcore/StructureDefinition/krcore-condition-encounter-diagnosis"
    ));
    assert!(kr_core_profile_matches(
        "Observation",
        "http://www.hl7korea.or.kr/fhir/krcore/StructureDefinition/krcore-observation-laboratory-result"
    ));
    assert!(kr_core_profile_matches(
        "Observation",
        "http://www.hl7korea.or.kr/fhir/krcore/StructureDefinition/krcore-bloodpressure"
    ));
    assert!(!kr_core_profile_matches(
        "Observation",
        "http://www.hl7korea.or.kr/fhir/krcore/StructureDefinition/krcore-observation-unknown"
    ));
    assert!(!kr_core_profile_matches(
        "Patient",
        "http://www.hl7korea.or.kr/fhir/krcore/StructureDefinition/krcore-medicationrequest"
    ));
    assert!(!kr_core_profile_matches(
        "Organization",
        "http://www.hl7korea.or.kr/fhir/krcore/StructureDefinition/krcore-observation-vitalsigns"
    ));
}
