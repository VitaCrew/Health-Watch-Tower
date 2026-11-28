use super::*;
use ring::{
    rand::SystemRandom,
    signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_ASN1_SIGNING},
};
use serde_json::json;

#[test]
fn validates_tink_prefix_and_ecdsa_der_for_exact_bytes() -> Result<()> {
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng)
        .map_err(|_| anyhow::anyhow!("cannot generate fixture key"))?;
    let signer = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8.as_ref(), &rng)
        .map_err(|_| anyhow::anyhow!("cannot read fixture key"))?;
    let point = signer.public_key().as_ref();
    let mut proto = vec![0x12, 6, 8, 3, 0x10, 2, 0x18, 2, 0x1a, 33, 0];
    proto.extend_from_slice(&point[1..33]);
    proto.extend_from_slice(&[0x22, 32]);
    proto.extend_from_slice(&point[33..65]);
    let id: u32 = 12934;
    let keys = decode_keys(
        &json!({"key":[{"keyId":id,"status":"ENABLED","outputPrefixType":"TINK","keyData":{
            "typeUrl":"type.googleapis.com/google.crypto.tink.EcdsaPublicKey","keyMaterialType":"ASYMMETRIC_PUBLIC",
            "value":STANDARD.encode(proto)
        }}]}),
    )?;
    let body = br#"[{"data":{"healthUserId":"test"}}]"#;
    let result = signer
        .sign(&rng, body)
        .map_err(|_| anyhow::anyhow!("fixture signing failed"))?;
    let mut tin = vec![1];
    tin.extend_from_slice(&id.to_be_bytes());
    tin.extend_from_slice(result.as_ref());
    let (parsed, der) = signature_parts(&tin)?;
    assert_eq!(parsed, id);
    assert!(verify(&keys[&parsed], body, der));
    assert!(!verify(&keys[&parsed], b"[]", der));
    assert!(signature_parts(&tin[1..]).is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL in AVATAR_HSM_TEST_DATABASE_URL"]
async fn handshake_is_bearer_only_and_signed_notice_queues_only_affected_days() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let app = App {
        db: pool.clone(),
        read_token: String::new(),
        control_token: String::new(),
        rules: crate::acute::rules()?,
    };
    let owner = "3298401232";
    let subject = "google-target-webhook-fixture";
    let other = "google-other-webhook-fixture";
    let db = pool.get().await?;
    for (id, account) in [(subject, owner), (other, "3019845239")] {
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&id],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES
            ($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))",
            &[&id],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'google_health')",
            &[&id],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_oauth_connection
            (subject_id,provider,sealed_tokens,provider_account_hash)
            VALUES($1,'google_health',''::bytea,sha256(convert_to($2,'UTF8')))",
            &[&id, &account],
        )
        .await?;
        db.execute(
            "INSERT INTO watch_tower.wearable_sync_schedule
            (subject_id,provider,next_sync,backfill_from,backfill_to)
            VALUES($1,'google_health',now()+interval '1 day',now(),now())",
            &[&id],
        )
        .await?;
    }
    let key = "fixture-webhook-secret-unique-for-test";
    let challenge = br#"{"type":"verification"}"#;
    let mut headers = HeaderMap::new();
    assert_eq!(
        receive_with_secret(&app, &headers, challenge, key).await,
        StatusCode::UNAUTHORIZED
    );
    headers.insert("authorization", format!("Bearer {key}").parse()?);
    assert_eq!(
        receive_with_secret(&app, &headers, challenge, key).await,
        StatusCode::OK
    );
    let body = serde_json::to_vec(&json!([{"data":{
        "healthUserId":owner,"dataType":"sleep","operation":"UPSERT",
        "intervals":[
            {"physicalTimeInterval":{"startTime":"2026-04-12T22:00:00Z","endTime":"2026-04-13T06:00:00Z"}},
            {"civilIso8601TimeInterval":{"startTime":"2026-04-18T00:00:00","endTime":"2026-04-19T00:00:00"}},
            {"civilDateTimeInterval":{"startDateTime":{"date":{"year":2026,"month":4,"day":22}},
                "endDateTime":{"date":{"year":2026,"month":4,"day":23}}}}
        ]
    }}]))?;
    assert_eq!(
        receive_with_secret(&app, &headers, &body, key).await,
        StatusCode::UNAUTHORIZED
    );
    let rng = SystemRandom::new();
    let private = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng)
        .map_err(|_| anyhow::anyhow!("cannot generate fixture key"))?;
    let signer = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, private.as_ref(), &rng)
        .map_err(|_| anyhow::anyhow!("cannot read fixture key"))?;
    let id = 17_319u32;
    let cache = KEYS.get_or_init(|| Mutex::new(CachedKeys::default()));
    *cache.lock().await = CachedKeys {
        fetched: Some(Instant::now()),
        keys: HashMap::from([(id, signer.public_key().as_ref().try_into()?)]),
    };
    let mut signature = vec![1];
    signature.extend(id.to_be_bytes());
    signature.extend(
        signer
            .sign(&rng, &body)
            .map_err(|_| anyhow::anyhow!("cannot sign fixture notification"))?
            .as_ref(),
    );
    headers.insert(
        "google-health-api-signature",
        STANDARD.encode(signature).parse()?,
    );
    let mut tampered = body.clone();
    tampered.push(b' ');
    assert_eq!(
        receive_with_secret(&app, &headers, &tampered, key).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        receive_with_secret(&app, &headers, &body, key).await,
        StatusCode::OK
    );
    let targets = db
        .query(
            "SELECT subject_id,day,revision::text FROM watch_tower.wearable_target_day
        WHERE subject_id=ANY($1) ORDER BY day",
            &[&vec![subject, other]],
        )
        .await?;
    assert!(targets.iter().all(|row| row.get::<_, String>(0) == subject));
    let days: Vec<_> = targets
        .iter()
        .map(|row| row.get::<_, NaiveDate>(1).to_string())
        .collect();
    assert_eq!(
        days,
        ["2026-04-12", "2026-04-13", "2026-04-18", "2026-04-22"]
    );
    assert_eq!(
        receive_with_secret(&app, &headers, &body, key).await,
        StatusCode::OK
    );
    let retried = db.query("SELECT revision::text FROM watch_tower.wearable_target_day WHERE subject_id=$1 ORDER BY day", &[&subject]).await?;
    assert_eq!(
        targets
            .iter()
            .map(|row| row.get::<_, String>(2))
            .collect::<Vec<_>>(),
        retried
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>(),
    );
    let deletion = serde_json::to_vec(&json!([{"data":{
        "healthUserId":owner,"dataType":"steps","operation":"DELETE",
        "intervals":[{"physicalTimeInterval":{
            "startTime":"2026-04-02T03:00:00Z","endTime":"2026-04-02T03:20:00Z"
        }}]
    }}]))?;
    let mut signature = vec![1];
    signature.extend(id.to_be_bytes());
    signature.extend(
        signer
            .sign(&rng, &deletion)
            .map_err(|_| anyhow::anyhow!("cannot sign fixture deletion"))?
            .as_ref(),
    );
    headers.insert(
        "google-health-api-signature",
        STANDARD.encode(signature).parse()?,
    );
    assert_eq!(
        receive_with_secret(&app, &headers, &deletion, key).await,
        StatusCode::OK
    );
    let row = db
        .query_one(
            "SELECT subject_id FROM watch_tower.wearable_target_day
         WHERE subject_id=ANY($1) AND day='2026-04-02'",
            &[&vec![subject, other]],
        )
        .await?;
    assert_eq!(row.get::<_, String>(0), subject);
    db.execute(
        "INSERT INTO avatar_hsm.wearable_record
         (subject_id,provider,kind,source,source_id,started_at,ended_at,modified_at,payload)
         VALUES ($1,'google_health','sleep','Google Health','9875412','2026-03-09 22:00Z','2026-03-10 06:00Z',now(),'{}'),
                ($1,'google_health','workout','Google Health','9875412','2026-03-06 01:00Z','2026-03-06 02:00Z',now(),'{}'),
                ($1,'google_health','metric','Google Health','weight:9875412','2026-03-12 08:00Z','2026-03-12 08:00Z',now(),'{}'),
                ($2,'google_health','sleep','Google Health','9875412','2026-03-07 22:00Z','2026-03-08 06:00Z',now(),'{}')",
        &[&subject, &other],
    ).await?;
    let records_only = serde_json::to_vec(&json!([
        {"data":{"healthUserId":owner,"dataType":"sleep","operation":"DELETE","recordId":"9875412"}},
        {"data":{"healthUserId":owner,"dataType":"weight","operation":"DELETE","recordId":"9875412"}}
    ]))?;
    let mut signature = vec![1];
    signature.extend(id.to_be_bytes());
    signature.extend(
        signer
            .sign(&rng, &records_only)
            .map_err(|_| anyhow::anyhow!("cannot sign fixture record IDs"))?
            .as_ref(),
    );
    headers.insert(
        "google-health-api-signature",
        STANDARD.encode(signature).parse()?,
    );
    assert_eq!(
        receive_with_secret(&app, &headers, &records_only, key).await,
        StatusCode::OK
    );
    let inferred_days = db
        .query(
            "SELECT subject_id, day::text FROM watch_tower.wearable_target_day
         WHERE subject_id=ANY($1) AND day<'2026-04-01' ORDER BY day",
            &[&vec![subject, other]],
        )
        .await?;
    assert!(inferred_days
        .iter()
        .all(|row| row.get::<_, String>(0) == subject));
    assert_eq!(
        inferred_days
            .iter()
            .map(|row| row.get::<_, String>(1))
            .collect::<Vec<_>>(),
        ["2026-03-09", "2026-03-10", "2026-03-12"]
    );
    let live_wake: bool = db
        .query_one(
            "SELECT wake_requested FROM watch_tower.wearable_sync_schedule
         WHERE subject_id=$1 AND provider='google_health'",
            &[&subject],
        )
        .await?
        .get(0);
    assert!(!live_wake);
    *cache.lock().await = CachedKeys::default();
    db.execute(
        "DELETE FROM avatar_hsm.wearable_record WHERE provider='google_health' AND subject_id=ANY($1)",
        &[&vec![subject, other]],
    ).await?;
    db.execute(
        "DELETE FROM avatar_hsm.device_connection WHERE provider='google_health' AND subject_id=ANY($1)",
        &[&vec![subject, other]],
    )
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL in AVATAR_HSM_TEST_DATABASE_URL"]
async fn google_target_reconciles_only_present_grants_and_complete_intervals() -> Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let db = pool.get().await?;
    let subject = "google-target-deletion-fixture";
    let other = "google-other-deletion-fixture";
    let provider = "google_health";
    let start = "2026-04-02T00:00:00Z";
    let end = "2026-04-03T00:00:00Z";
    let sample = |kind: &str, id: &str, from: &str, until: &str, payload: Value| {
        json!({"kind":kind,"source":"Google Health","source_id":id,
            "started_at":from,"ended_at":until,"modified_at":end,"payload":payload})
    };
    let deleted = sample(
        "steps",
        "deleted",
        "2026-04-02T03:00:00Z",
        "2026-04-02T03:20:00Z",
        json!({"count":55,"unit":"steps"}),
    );
    let kept = sample(
        "steps",
        "kept",
        "2026-04-02T04:00:00Z",
        "2026-04-02T04:20:00Z",
        json!({"count":42,"unit":"steps"}),
    );
    let boundary = sample(
        "sleep",
        "boundary",
        "2026-04-02T18:00:00Z",
        end,
        json!({"stages":[]}),
    );
    let workout = sample(
        "workout",
        "out-of-scope",
        "2026-04-02T08:00:00Z",
        "2026-04-02T09:00:00Z",
        json!({"exercise_type":"walking"}),
    );
    let weight = sample(
        "metric",
        "weight:deleted",
        "2026-04-02T09:00:00Z",
        "2026-04-02T09:00:00Z",
        json!({"metric":"weight","provider_data":{"weightGrams":70000}}),
    );
    let ecg = sample(
        "metric",
        "electrocardiogram:out-of-scope",
        "2026-04-02T10:00:00Z",
        "2026-04-02T10:01:00Z",
        json!({"metric":"electrocardiogram","provider_data":{}}),
    );
    let calories = sample(
        "metric",
        "total_calories:2026-04-02",
        start,
        start,
        json!({"metric":"total_calories","time_precision":"provider_calendar_date","provider_data":{"rollup":{"kcalSum":1550}}}),
    );
    for owner in [subject, other] {
        db.execute("INSERT INTO avatar_hsm.subject VALUES($1,'UTC')", &[&owner])
            .await?;
        let lease = crate::wearables::issue_lease_for(&pool, owner, provider).await?;
        let snapshot = serde_json::from_value(
            json!({"lease":lease["lease"],"start":start,"end":end,
            "grants":["steps","sleep","workout","metric"],"records":[deleted,kept,boundary,workout,weight,ecg,calories]}),
        )?;
        crate::wearables::store_snapshot(&pool, owner, provider, snapshot, false).await?;
    }
    let lease = crate::wearables::issue_lease_for(&pool, subject, provider).await?;
    let mut snapshot: crate::wearables::Snapshot = serde_json::from_value(json!({
        "lease":lease["lease"],"start":start,"end":end,"grants":["steps","sleep","metric"],"records":[kept]}))?;
    snapshot.google_metric_coverage = vec!["weight".into(), "total_calories".into()];
    crate::wearables::store_snapshot(&pool, subject, provider, snapshot, true).await?;
    let ids = db
        .query(
            "SELECT source_id FROM avatar_hsm.wearable_record
         WHERE subject_id=$1 AND provider='google_health' ORDER BY source_id",
            &[&subject],
        )
        .await?;
    assert_eq!(
        ids.iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>(),
        [
            "boundary",
            "electrocardiogram:out-of-scope",
            "kept",
            "out-of-scope"
        ]
    );
    let totals = db.query("SELECT subject_id,(stats->>'total')::bigint FROM avatar_hsm.wearable_daily
         WHERE subject_id=ANY($1) AND provider='google_health' AND kind='steps' ORDER BY subject_id",
         &[&vec![subject,other]]).await?;
    assert_eq!(
        totals
            .iter()
            .map(|row| row.get::<_, i64>(1))
            .collect::<Vec<_>>(),
        [97, 42]
    );
    let remaining_rollups = db
        .query(
            "SELECT subject_id FROM avatar_hsm.wearable_daily WHERE subject_id=ANY($1)
         AND provider='google_health' AND calculated_by='provider' AND kind='activity'",
            &[&vec![subject, other]],
        )
        .await?;
    assert_eq!(remaining_rollups.len(), 1);
    assert_eq!(remaining_rollups[0].get::<_, String>(0), other);
    Ok(())
}
