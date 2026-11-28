use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use fhir::r4::{resources::Resource, validate::Validate};
use ring::digest::{digest, SHA256};
use serde_json::{json, Value};
use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
};

pub fn write_tool() -> Value {
    json!({
        "name":"import_fhir_bundle",
        "description":"Import an owner-authorized FHIR R4 collection Bundle fetched by an OpenHuman hospital connector. Watch Tower verifies resource structure, the mapped patient reference, provenance, replay and revision conflicts, stores the clinical records in its PostgreSQL database, then queues a compact HSM state update.",
        "inputSchema":{
            "type":"object",
            "properties":{
                "idempotency_key":{"type":"string","minLength":8,"maxLength":160},
                "source_system":{"type":"string","format":"uri","maxLength":300},
                "patient_reference":{"type":"string","pattern":"^Patient/[A-Za-z0-9.-]{1,64}$"},
                "bundle":{"type":"object","description":"FHIR R4 collection Bundle returned by the user's authorized hospital connector"}
            },
            "required":["idempotency_key","source_system","patient_reference","bundle"],
            "additionalProperties":false
        }
    })
}

pub fn bind_tool() -> Value {
    json!({
        "name":"bind_fhir_patient",
        "description":"Record an owner's explicit confirmation that this authenticated Watch Tower subject corresponds to a Patient reference returned by the connected hospital source. This binding is required before importing that source's records.",
        "inputSchema":{"type":"object","properties":{"source_system":{"type":"string","format":"uri","maxLength":300},"patient_reference":{"type":"string","pattern":"^Patient/[A-Za-z0-9.-]{1,64}$"}},"required":["source_system","patient_reference"],"additionalProperties":false}
    })
}

pub async fn bind_patient(
    pool: &Pool,
    subject_id: &str,
    actor_id: &str,
    client_id: &str,
    consent_version: i64,
    args: &Value,
) -> Result<Value> {
    let source_system = required_string(args, "source_system", 300)?;
    ensure!(
        source_system.starts_with("https://"),
        "source_system must be an HTTPS canonical URI"
    );
    let patient_reference = required_string(args, "patient_reference", 200)?;
    ensure!(
        patient_reference
            .strip_prefix("Patient/")
            .is_some_and(|id| !id.is_empty()
                && id.len() <= 64
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-'))),
        "patient_reference must be a valid FHIR Patient reference"
    );
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    lock_active_write_consent(&tx, subject_id, client_id, consent_version).await?;
    tx.execute(
        "INSERT INTO watch_tower.clinical_patient_binding
         (subject_id,source_system,patient_reference,confirmed_by,consent_version)
         VALUES($1,$2,$3,$4,$5)
         ON CONFLICT(subject_id,source_system) DO UPDATE SET
           patient_reference=EXCLUDED.patient_reference, confirmed_by=EXCLUDED.confirmed_by,
           consent_version=EXCLUDED.consent_version, confirmed_at=now(), revoked_at=NULL",
        &[
            &subject_id,
            &source_system,
            &patient_reference,
            &actor_id,
            &consent_version,
        ],
    )
    .await?;
    tx.commit().await?;
    Ok(
        json!({"status":"bound","source_system":source_system,"patient_reference":patient_reference}),
    )
}

pub fn read_tool() -> Value {
    json!({
        "name":"get_clinical_records",
        "description":"Read a subject-scoped list of normalized clinical record facts and provenance from Watch Tower.",
        "inputSchema":{"type":"object","properties":{"limit":{"type":"integer","minimum":1,"maximum":100}},"additionalProperties":false},
        "annotations":{"readOnlyHint":true}
    })
}

const MAX_RESOURCES: usize = 500;
const SUPPORTED_TYPES: [&str; 4] = [
    "Condition",
    "AllergyIntolerance",
    "Observation",
    "MedicationRequest",
];
const KR_CORE_PACKAGE_URL: &str = "https://www.hl7korea.or.kr/fhir/krcore/package.tgz";
const KR_CORE_PREFIX: &str = "http://www.hl7korea.or.kr/fhir/krcore/StructureDefinition/";

struct ClinicalResource {
    resource_type: String,
    source_record_id: String,
    source_version: String,
    profile_uri: String,
    occurred_at: Option<DateTime<Utc>>,
    resource: Value,
    fact: Value,
    event_id: String,
    hash: String,
}

pub async fn import_bundle(
    pool: &Pool,
    subject_id: &str,
    actor_id: &str,
    client_id: &str,
    consent_version: i64,
    args: &Value,
) -> Result<Value> {
    let source_system = required_string(args, "source_system", 300)?;
    ensure!(
        source_system.starts_with("https://"),
        "source_system must be an HTTPS canonical URI"
    );
    let patient_reference = required_string(args, "patient_reference", 200)?;
    ensure!(
        patient_reference.starts_with("Patient/"),
        "patient_reference must identify a FHIR Patient resource"
    );
    let bundle = &args["bundle"];
    ensure!(
        bundle["resourceType"] == "Bundle",
        "FHIR input must be an R4 Bundle"
    );
    ensure!(
        bundle["type"] == "collection",
        "only FHIR collection Bundles are accepted"
    );
    let entries = bundle["entry"]
        .as_array()
        .context("Bundle.entry is required")?;
    ensure!(
        !entries.is_empty() && entries.len() <= MAX_RESOURCES + 1,
        "FHIR Bundle entry count is out of range"
    );
    let typed_bundle: fhir::r4::resources::Bundle =
        serde_json::from_value(bundle.clone()).context("invalid FHIR R4 Bundle structure")?;
    let bundle_issues = typed_bundle.validate();
    ensure!(
        bundle_issues.is_empty(),
        "FHIR R4 Bundle validation failed: {}",
        bundle_issues
            .iter()
            .take(5)
            .map(|issue| format!("{}: {}", issue.path, issue.message))
            .collect::<Vec<_>>()
            .join("; ")
    );
    validate_provider_profiles(bundle).await?;

    let mut patient_matches = 0usize;
    let mut resources = Vec::new();
    for entry in entries {
        let resource = entry
            .get("resource")
            .context("Bundle entry is missing resource")?;
        ensure!(resource.is_object(), "FHIR resource must be an object");
        let resource_type = resource["resourceType"]
            .as_str()
            .context("FHIR resourceType is required")?;
        let typed: Resource = serde_json::from_value(resource.clone())
            .with_context(|| format!("invalid FHIR R4 {resource_type} structure"))?;
        let issues = typed.validate();
        ensure!(
            issues.is_empty(),
            "FHIR R4 structural validation failed: {}",
            issues
                .iter()
                .take(5)
                .map(|issue| format!("{}: {}", issue.path, issue.message))
                .collect::<Vec<_>>()
                .join("; ")
        );

        if resource_type == "Patient" {
            let id = resource["id"]
                .as_str()
                .context("Patient.id is required for patient binding")?;
            ensure!(
                format!("Patient/{id}") == patient_reference,
                "FHIR Patient does not match the authenticated patient binding"
            );
            patient_matches += 1;
            continue;
        }
        ensure!(
            SUPPORTED_TYPES.contains(&resource_type),
            "unsupported clinical resource type: {resource_type}"
        );
        ensure!(
            resource_subject_reference(resource, resource_type) == Some(patient_reference.as_str()),
            "clinical resource subject does not match the authenticated patient binding"
        );
        let id = resource["id"]
            .as_str()
            .context("clinical resource id is required")?;
        ensure!(
            !id.trim().is_empty() && id.len() <= 128,
            "invalid clinical resource id"
        );
        let bytes = serde_json::to_vec(resource)?;
        let hash = sha256_hex(&bytes);
        let version = resource["meta"]["versionId"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("content:{hash}"));
        let profiles = resource["meta"]["profile"].as_array();
        if let Some(profiles) = profiles {
            ensure!(
                !profiles.is_empty(),
                "resource meta.profile cannot be empty"
            );
            for profile in profiles.iter().filter_map(Value::as_str) {
                ensure!(
                    valid_canonical(profile),
                    "resource meta.profile must contain valid FHIR canonical URIs"
                );
            }
        }
        let profile_uri = profiles
            .and_then(|values| values.iter().filter_map(Value::as_str).next())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("http://hl7.org/fhir/StructureDefinition/{resource_type}"));
        let occurred_at = resource_date(resource);
        let code = resource_code(resource, resource_type);
        let fact = json!({
            "record_type": resource_type,
            "clinical_status": clinical_status(resource, resource_type),
            "code": code.as_ref().and_then(|value| value["code"].as_str()),
            "display": code.as_ref().and_then(|value| value["display"].as_str()),
            "occurred_at": occurred_at,
            "normalized": normalize_resource(resource, resource_type, code.as_ref(), occurred_at),
        });
        let event_id = sha256_hex(
            format!("clinical-v1\0{subject_id}\0{source_system}\0{resource_type}\0{id}\0{version}")
                .as_bytes(),
        );
        resources.push(ClinicalResource {
            resource_type: resource_type.into(),
            source_record_id: id.into(),
            source_version: version,
            profile_uri,
            occurred_at,
            resource: resource.clone(),
            fact,
            event_id,
            hash,
        });
    }
    ensure!(
        patient_matches == 1,
        "FHIR Bundle must contain exactly one matching Patient resource"
    );
    ensure!(
        !resources.is_empty() && resources.len() <= MAX_RESOURCES,
        "FHIR Bundle contains no supported clinical records"
    );

    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    // Serialize the final write against consent revocation. The policy check
    // above authorizes the request; this row lock makes the database commit
    // the write's consent linearization point.
    lock_active_write_consent(&tx, subject_id, client_id, consent_version).await?;
    let binding = tx
        .query_opt(
            "SELECT patient_reference FROM watch_tower.clinical_patient_binding
         WHERE subject_id=$1 AND source_system=$2 AND revoked_at IS NULL FOR SHARE",
            &[&subject_id, &source_system],
        )
        .await?;
    ensure!(
        binding.is_some_and(|row| row.get::<_, String>(0) == patient_reference),
        "hospital Patient reference has no matching owner-confirmed binding"
    );
    let mut inserted = 0usize;
    let mut unchanged = 0usize;
    for item in &resources {
        let candidate_supersedes_record_id: Option<String> = tx
            .query_opt(
                "SELECT record_id::text FROM watch_tower.clinical_record
             WHERE subject_id=$1 AND source_system=$2 AND resource_type=$3 AND source_record_id=$4
             ORDER BY imported_at DESC LIMIT 1 FOR UPDATE",
                &[
                    &subject_id,
                    &source_system,
                    &item.resource_type,
                    &item.source_record_id,
                ],
            )
            .await?
            .map(|row| row.get(0));
        let row = tx.query_opt(
            "INSERT INTO watch_tower.clinical_record
                (subject_id,source_system,patient_reference,resource_type,source_record_id,source_version,
                 fhir_version,profile_uri,resource_sha256,occurred_at,normalized_fact,resource,actor_id,consent_version,supersedes_record_id)
             VALUES($1,$2,$3,$4,$5,$6,'4.0.1',$7,$8,$9,$10,$11,$12,$13,$14::text::uuid)
             ON CONFLICT(subject_id,source_system,resource_type,source_record_id,source_version) DO NOTHING
             RETURNING record_id::text",
            &[&subject_id, &source_system, &patient_reference, &item.resource_type, &item.source_record_id,
              &item.source_version, &item.profile_uri, &item.hash, &item.occurred_at, &item.fact, &item.resource,
              &actor_id, &consent_version, &candidate_supersedes_record_id],
        ).await?;
        let (record_id, supersedes_record_id) = if let Some(row) = row {
            inserted += 1;
            (row.get::<_, String>(0), candidate_supersedes_record_id)
        } else {
            let existing = tx.query_one(
                "SELECT record_id::text,resource_sha256,supersedes_record_id::text FROM watch_tower.clinical_record
                 WHERE subject_id=$1 AND source_system=$2 AND resource_type=$3 AND source_record_id=$4 AND source_version=$5
                 FOR UPDATE",
                &[&subject_id, &source_system, &item.resource_type, &item.source_record_id, &item.source_version],
            ).await?;
            ensure!(
                existing.get::<_, String>(1) == item.hash,
                "FHIR source version was reused with different content"
            );
            unchanged += 1;
            (existing.get(0), existing.get(2))
        };
        let happened_at = item.occurred_at.unwrap_or_else(Utc::now);
        let fact = json!({
            "record_ref": record_id,
            "record_type": item.resource_type,
            "clinical_status": item.fact["clinical_status"],
            "code": item.fact["code"],
            "display": item.fact["display"],
            "occurred_at": happened_at,
            "supersedes_record_ref": supersedes_record_id,
        });
        tx.execute(
            "INSERT INTO watch_tower.clinical_hsm_outbox(event_id,record_id,subject_id,occurred_at,consent_version,fact)
             VALUES($1,$2::text::uuid,$3,$4,$5,$6) ON CONFLICT(event_id) DO NOTHING",
            &[&item.event_id, &record_id, &subject_id, &happened_at, &consent_version, &fact],
        ).await?;
    }
    tx.commit().await?;
    Ok(json!({
        "status":"accepted", "fhir_version":"4.0.1",
        "imported":inserted, "unchanged":unchanged, "hsm_state":"queued"
    }))
}

async fn lock_active_write_consent(
    tx: &tokio_postgres::Transaction<'_>,
    subject_id: &str,
    client_id: &str,
    consent_version: i64,
) -> Result<()> {
    let active = tx
        .query_opt(
            "SELECT version FROM watch_tower.consent_grant
         WHERE subject_id=$1 AND client_id=$2 AND scope='records.write'
           AND version=$3 AND revoked_at IS NULL
           AND (expires_at IS NULL OR expires_at > now()) FOR UPDATE",
            &[&subject_id, &client_id, &consent_version],
        )
        .await?;
    ensure!(
        active.is_some(),
        "records.write consent was revoked or expired before clinical persistence"
    );
    Ok(())
}

async fn validate_provider_profiles(bundle: &Value) -> Result<()> {
    let mut kr_core_profiles = std::collections::BTreeMap::<String, Vec<Value>>::new();
    let entries = bundle["entry"]
        .as_array()
        .context("Bundle.entry is required")?;
    for entry in entries {
        let Some(resource) = entry.get("resource") else {
            continue;
        };
        let resource_type = resource["resourceType"].as_str().unwrap_or_default();
        let profiles = resource["meta"]["profile"].as_array();
        let Some(profiles) = profiles else { continue };
        for value in profiles {
            let profile = value
                .as_str()
                .context("meta.profile values must be canonical URLs")?;
            let canonical = profile.split('|').next().unwrap_or(profile);
            if canonical.starts_with("http://hl7.org/fhir/StructureDefinition/") {
                ensure!(
                    canonical == format!("http://hl7.org/fhir/StructureDefinition/{resource_type}"),
                    "FHIR base profile does not match resource type"
                );
                continue;
            }
            ensure!(
                canonical.starts_with(KR_CORE_PREFIX)
                    && kr_core_profile_matches(resource_type, canonical),
                "provider profile is not enabled for this resource type"
            );
            ensure!(
                profile
                    .split_once('|')
                    .is_none_or(|(_, version)| version == "2.0.0"),
                "unsupported KR Core profile version"
            );
            kr_core_profiles
                .entry(canonical.to_owned())
                .or_default()
                .push(resource.clone());
        }
    }
    if kr_core_profiles.is_empty() {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || {
        let validator = std::env::var("WATCH_TOWER_FHIR_VALIDATOR")
            .unwrap_or_else(|_| "gofhir-validator".to_owned());
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let workdir =
            std::env::temp_dir().join(format!("watch-tower-fhir-{}-{nonce}", std::process::id()));
        fs::create_dir(&workdir).context("cannot create private FHIR validation workspace")?;
        let cleanup = TempValidationDir(workdir.clone());
        for (profile_index, (_profile, resources)) in kr_core_profiles.into_iter().enumerate() {
            let mut files = Vec::with_capacity(resources.len());
            for (resource_index, resource) in resources.into_iter().enumerate() {
                let path = workdir.join(format!("resource-{profile_index}-{resource_index}.json"));
                let mut file = fs::File::create(&path)?;
                file.write_all(&serde_json::to_vec(&resource)?)?;
                files.push(path);
            }
            let mut command = Command::new(&validator);
            command.args([
                "-version",
                "4.0.1",
                "-package-url",
                KR_CORE_PACKAGE_URL,
                "-output",
                "json",
            ]);
            command.args(&files);
            let report_path = workdir.join(format!("report-{profile_index}.json"));
            command
                .stdout(Stdio::from(fs::File::create(&report_path)?))
                .stderr(Stdio::null());
            let mut child = command
                .spawn()
                .context("FHIR implementation-guide validator is unavailable")?;
            let started = std::time::Instant::now();
            let status = loop {
                if let Some(status) = child.try_wait()? {
                    break status;
                }
                if started.elapsed() > std::time::Duration::from_secs(45) {
                    let _ = child.kill();
                    let _ = child.wait();
                    anyhow::bail!("FHIR R4 KR Core profile validation timed out");
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            };
            ensure!(
                status.success(),
                "FHIR R4 KR Core profile validation failed"
            );
            let reports: Vec<Value> = serde_json::from_slice(&fs::read(report_path)?)
                .context("FHIR validator returned an invalid report")?;
            ensure!(
                reports.len() == files.len()
                    && reports.iter().all(|report| report["valid"] == true)
                    && !reports
                        .iter()
                        .any(
                            |report| report["issues"].as_array().is_some_and(|issues| issues
                                .iter()
                                .any(|issue| {
                                    issue["code"] == "not-found"
                                        || issue["diagnostics"].as_str().is_some_and(|message| {
                                            message.contains("was not checked")
                                        })
                                }))
                        ),
                "FHIR R4 KR Core profile validation failed or the pinned profile was unavailable"
            );
        }
        drop(cleanup);
        Ok(())
    })
    .await
    .context("FHIR profile validator task failed")??;
    Ok(())
}

struct TempValidationDir(std::path::PathBuf);

impl Drop for TempValidationDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn kr_core_profile_matches(resource_type: &str, canonical: &str) -> bool {
    let name = canonical.strip_prefix(KR_CORE_PREFIX).unwrap_or_default();
    match resource_type {
        "Patient" => name == "krcore-patient",
        "AllergyIntolerance" => name == "krcore-allergyintolerance",
        "Condition" => matches!(
            name,
            "krcore-condition-chief-complaint" | "krcore-condition-encounter-diagnosis"
        ),
        "MedicationRequest" => name == "krcore-medicationrequest",
        "Observation" => matches!(
            name,
            "krcore-observation-function-test"
                | "krcore-observation-laboratory-result"
                | "krcore-vitalsigns"
                | "krcore-bloodpressure"
                | "krcore-bodyheight"
                | "krcore-bodytemperature"
                | "krcore-bodyweight"
                | "krcore-heartrate"
                | "krcore-pulseoximetry"
                | "krcore-respiratoryrate"
        ),
        _ => false,
    }
}

pub async fn list_records(pool: &Pool, subject_id: &str, limit: i64) -> Result<Value> {
    let client = pool.get().await?;
    let rows = client.query(
        "SELECT record_id::text,source_system,resource_type,source_record_id,source_version,profile_uri,
                occurred_at,normalized_fact,imported_at,supersedes_record_id::text
         FROM watch_tower.clinical_record WHERE subject_id=$1
         ORDER BY occurred_at DESC NULLS LAST, imported_at DESC LIMIT $2",
        &[&subject_id, &limit.clamp(1,100)],
    ).await?;
    let records = rows
        .into_iter()
        .map(|row| {
            json!({
                "record_id":row.get::<_,String>(0), "source_system":row.get::<_,String>(1),
                "resource_type":row.get::<_,String>(2), "source_record_id":row.get::<_,String>(3),
                "source_version":row.get::<_,String>(4), "profile_uri":row.get::<_,String>(5),
                "occurred_at":row.get::<_,Option<DateTime<Utc>>>(6), "fact":row.get::<_,Value>(7),
                "imported_at":row.get::<_,DateTime<Utc>>(8),
                "supersedes_record_id":row.get::<_,Option<String>>(9)
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"records":records,"count":records.len()}))
}

pub fn start_hsm_outbox_worker(hsm_pool: Pool, watch_tower_pool: Pool) {
    tokio::spawn(async move {
        let started = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let worker = format!("clinical-hsm-{}-{started}", std::process::id());
        loop {
            match dispatch_hsm_outbox(&hsm_pool, &watch_tower_pool, &worker).await {
                Ok(0) => tokio::time::sleep(Duration::from_secs(2)).await,
                Ok(_) => {}
                Err(_) => {
                    eprintln!("clinical HSM outbox dispatch failed; retry scheduled");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    });
}

async fn dispatch_hsm_outbox(hsm_pool: &Pool, pool: &Pool, worker: &str) -> Result<usize> {
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    let row = tx.query_opt(
        "WITH candidate AS (
            SELECT event_id FROM watch_tower.clinical_hsm_outbox
            WHERE applied_at IS NULL AND next_attempt_at <= now()
              AND (lease_until IS NULL OR lease_until < now())
            ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1
         )
         UPDATE watch_tower.clinical_hsm_outbox o SET lease_owner=$1,
             lease_until=now()+interval '90 seconds',attempts=o.attempts+1
         FROM candidate c WHERE o.event_id=c.event_id
         RETURNING o.event_id,o.record_id::text,o.subject_id,o.occurred_at,o.consent_version,o.fact",
        &[&worker],
    ).await?;
    tx.commit().await?;
    let Some(row) = row else {
        return Ok(0);
    };
    let event_id: String = row.get(0);
    let record_id: String = row.get(1);
    let subject_id: String = row.get(2);
    let occurred_at: DateTime<Utc> = row.get(3);
    let consent_version: i64 = row.get(4);
    let fact: Value = row.get(5);
    match avatar_hsm::state_owner::apply_clinical_record(
        hsm_pool,
        &subject_id,
        &record_id,
        &event_id,
        occurred_at,
        consent_version,
        fact,
    )
    .await
    {
        Ok(_) => {
            pool.get().await?.execute(
                "UPDATE watch_tower.clinical_hsm_outbox SET applied_at=now(),lease_owner=NULL,lease_until=NULL,last_failure_code=NULL
                 WHERE event_id=$1 AND lease_owner=$2 AND applied_at IS NULL",
                &[&event_id, &worker],
            ).await?;
        }
        Err(error) => {
            let diagnostic = format!("{error:#}");
            let code = if diagnostic.contains("sequence conflict") {
                "hsm_sequence_conflict"
            } else {
                "hsm_write_failed"
            };
            pool.get().await?.execute(
                "UPDATE watch_tower.clinical_hsm_outbox SET lease_owner=NULL,lease_until=NULL,
                    next_attempt_at=now()+LEAST(interval '1 hour', interval '2 seconds' * power(2,LEAST(attempts,10))),
                    last_failure_code=$3
                 WHERE event_id=$1 AND lease_owner=$2 AND applied_at IS NULL",
                &[&event_id, &worker, &code],
            ).await?;
            bail!("clinical outbox event {event_id} could not update HSM: {code}");
        }
    }
    Ok(1)
}

fn resource_subject_reference<'a>(resource: &'a Value, resource_type: &str) -> Option<&'a str> {
    if resource_type == "AllergyIntolerance" {
        resource["patient"]["reference"].as_str()
    } else {
        resource["subject"]["reference"].as_str()
    }
}

fn resource_date(resource: &Value) -> Option<DateTime<Utc>> {
    [
        "recordedDate",
        "onsetDateTime",
        "effectiveDateTime",
        "authoredOn",
        "date",
    ]
    .iter()
    .filter_map(|key| resource[*key].as_str())
    .find_map(|value| {
        DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|time| time.with_timezone(&Utc))
    })
    .or_else(|| {
        resource["onsetPeriod"]["start"]
            .as_str()
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|time| time.with_timezone(&Utc))
    })
}

fn resource_code(resource: &Value, resource_type: &str) -> Option<Value> {
    let concept = match resource_type {
        "Condition" | "Observation" | "AllergyIntolerance" => &resource["code"],
        "MedicationRequest" => resource.get("medicationCodeableConcept")?,
        _ => return None,
    };
    let coding = concept["coding"].as_array()?.first()?;
    Some(json!({"system":coding["system"],"code":coding["code"],"display":coding["display"]}))
}

fn clinical_status<'a>(resource: &'a Value, resource_type: &str) -> Option<&'a str> {
    match resource_type {
        "Condition" | "AllergyIntolerance" => {
            resource["clinicalStatus"]["coding"][0]["code"].as_str()
        }
        "Observation" | "MedicationRequest" => resource["status"].as_str(),
        _ => None,
    }
}

fn normalize_resource(
    resource: &Value,
    resource_type: &str,
    code: Option<&Value>,
    occurred_at: Option<DateTime<Utc>>,
) -> Value {
    let value = resource.as_object().and_then(|object| {
        object
            .iter()
            .find(|(key, _)| key.starts_with("value"))
            .map(|(_, value)| value.clone())
    });
    json!({
        "resource_type":resource_type,
        "status":clinical_status(resource, resource_type),
        "code":code,
        "occurred_at":occurred_at,
        "value":value,
        "unit":resource["valueQuantity"]["unit"],
        "condition_onset":resource.get("onsetDateTime").or_else(|| resource.get("onsetPeriod")),
        "condition_abatement":resource.get("abatementDateTime").or_else(|| resource.get("abatementPeriod")),
        "medication_intent":resource["intent"],
        "medication_dosage":resource["dosageInstruction"],
    })
}

fn required_string(args: &Value, key: &str, max: usize) -> Result<String> {
    let value = args[key]
        .as_str()
        .context("required string argument is missing")?
        .trim();
    ensure!(
        !value.is_empty() && value.len() <= max,
        "argument is empty or too long"
    );
    Ok(value.to_owned())
}

fn valid_canonical(value: &str) -> bool {
    (value.starts_with("https://")
        || value.starts_with("http://hl7.org/fhir/")
        || value.starts_with(KR_CORE_PREFIX))
        && value.len() <= 512
        && !value.chars().any(char::is_whitespace)
}

fn sha256_hex(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
#[path = "clinical_tests.rs"]
mod tests;
