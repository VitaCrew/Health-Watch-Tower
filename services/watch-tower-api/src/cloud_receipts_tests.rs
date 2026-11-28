use anyhow::Context;

use super::*;

#[test]
fn cloud_receipt_envelope_is_payload_free_and_replay_stable() {
    let request_sha256 = "a".repeat(64);
    let build = || RecordEnvelope {
        subject_id: "person-1".into(),
        actor_id: "service:hsm-cloud-sync".into(),
        adapter_id: "hsm".into(),
        source_id: "cloud-wearable-sync:oura".into(),
        record_id: format!("snapshot:{request_sha256}"),
        revision: 1,
        run_id: Some("f8f6d129-bc72-478f-898f-d66c29abfdbc".into()),
        observed_at_unix: 1_800_000_000,
        idempotency_key: format!("cloud-wearable-sync:oura:{request_sha256}"),
    };
    let first = build();
    let replay = build();
    assert!(first.is_valid());
    assert_eq!(first, replay);
    assert!(first.record_id.starts_with("snapshot:"));
    let mut invalid_run = first.clone();
    invalid_run.run_id = Some("not-a-uuid".into());
    assert!(!invalid_run.is_valid());
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL databases in WATCH_TOWER_TEST_DATABASE_URL and AVATAR_HSM_TEST_DATABASE_URL"]
async fn dispatches_hsm_outbox_into_common_record_ledger_and_acknowledges() -> anyhow::Result<()> {
    let watch_tower_url = std::env::var("WATCH_TOWER_TEST_DATABASE_URL")?;
    let hsm_url = std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?;
    let policy_pool = store::connect(&watch_tower_url).await?;
    let hsm_pool = avatar_hsm::store::connect(&hsm_url).await?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let subject_id = format!("cloud-receipt-test-{suffix}");
    let request_sha256 = "b".repeat(64);
    let run_id = "2d412635-2384-48ac-8ccb-521242a878d6";
    let hsm_db = hsm_pool.get().await?;
    let outbox_id: i64 = hsm_db
        .query_one(
            "INSERT INTO avatar_hsm.watch_tower_record_outbox
                (subject_id,provider,request_sha256,observed_at,run_id)
             VALUES($1,'oura',$2,now(),$3::text::uuid) RETURNING id",
            &[&subject_id, &request_sha256, &run_id],
        )
        .await?
        .get(0);

    drain(&hsm_pool, &policy_pool).await?;

    let receipt_db = policy_pool.get().await?;
    let receipt = receipt_db
        .query_opt(
            "SELECT request_sha256, actor_id, last_run_id::text FROM watch_tower.record_receipt
             WHERE subject_id=$1 AND adapter_id='hsm'
               AND idempotency_key=$2",
            &[
                &subject_id,
                &format!("cloud-wearable-sync:oura:{request_sha256}"),
            ],
        )
        .await?;
    let receipt = receipt.context("common Watch Tower record receipt was not created")?;
    assert_eq!(receipt.get::<_, String>(0), request_sha256);
    assert_eq!(receipt.get::<_, String>(1), "service:hsm-cloud-sync");
    assert_eq!(receipt.get::<_, String>(2), run_id);
    let etl_run = receipt_db
        .query_one(
            "SELECT source_id,status,receipt_count,latest_request_sha256
             FROM watch_tower.etl_run WHERE subject_id=$1 AND run_id=$2::text::uuid",
            &[&subject_id, &run_id],
        )
        .await?;
    assert_eq!(etl_run.get::<_, String>(0), "cloud-wearable-sync:oura");
    assert_eq!(etl_run.get::<_, String>(1), "succeeded");
    assert_eq!(etl_run.get::<_, i64>(2), 1);
    assert_eq!(etl_run.get::<_, String>(3), request_sha256);

    hsm_db
        .execute(
            "UPDATE avatar_hsm.watch_tower_record_outbox
             SET delivered_at=NULL WHERE id=$1",
            &[&outbox_id],
        )
        .await?;
    drain(&hsm_pool, &policy_pool).await?;
    let same_run_count: i64 = receipt_db
        .query_one(
            "SELECT receipt_count FROM watch_tower.etl_run
             WHERE subject_id=$1 AND run_id=$2::text::uuid",
            &[&subject_id, &run_id],
        )
        .await?
        .get(0);
    assert_eq!(same_run_count, 1);

    let next_run_id = "60defde5-196f-4b1f-ae6d-c64497519890";
    hsm_db
        .execute(
            "UPDATE avatar_hsm.watch_tower_record_outbox
             SET run_id=$2::text::uuid,delivered_at=NULL WHERE id=$1",
            &[&outbox_id, &next_run_id],
        )
        .await?;
    drain(&hsm_pool, &policy_pool).await?;
    let refreshed = receipt_db
        .query_one(
            "SELECT count(*),(array_agg(last_run_id::text))[1]
             FROM watch_tower.record_receipt
             WHERE subject_id=$1 AND adapter_id='hsm'",
            &[&subject_id],
        )
        .await?;
    assert_eq!(refreshed.get::<_, i64>(0), 1);
    assert_eq!(refreshed.get::<_, String>(1), next_run_id);
    let run_count: i64 = receipt_db
        .query_one(
            "SELECT count(*) FROM watch_tower.etl_run WHERE subject_id=$1",
            &[&subject_id],
        )
        .await?
        .get(0);
    assert_eq!(run_count, 2);

    let outbox = hsm_db
        .query_one(
            "SELECT delivered_at IS NOT NULL, attempts FROM avatar_hsm.watch_tower_record_outbox
             WHERE id=$1",
            &[&outbox_id],
        )
        .await?;
    assert!(outbox.get::<_, bool>(0));
    assert_eq!(outbox.get::<_, i32>(1), 3);

    receipt_db
        .execute(
            "DELETE FROM watch_tower.etl_run WHERE subject_id=$1",
            &[&subject_id],
        )
        .await?;
    receipt_db
        .execute(
            "DELETE FROM watch_tower.record_receipt WHERE subject_id=$1",
            &[&subject_id],
        )
        .await?;
    hsm_db
        .execute(
            "DELETE FROM avatar_hsm.watch_tower_record_outbox WHERE id=$1",
            &[&outbox_id],
        )
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL databases in WATCH_TOWER_TEST_DATABASE_URL and AVATAR_HSM_TEST_DATABASE_URL"]
async fn failed_receipt_does_not_block_later_receipts_in_the_claimed_batch() -> anyhow::Result<()> {
    let watch_tower_url = std::env::var("WATCH_TOWER_TEST_DATABASE_URL")?;
    let hsm_url = std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?;
    let policy_pool = store::connect(&watch_tower_url).await?;
    let hsm_pool = avatar_hsm::store::connect(&hsm_url).await?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let subject_id = format!("cloud-receipt-batch-test-{suffix}");
    let rejected_hash = "c".repeat(64);
    let accepted_hash = "e".repeat(64);
    let rejected_key = format!("cloud-wearable-sync:oura:{rejected_hash}");
    let rejected_run_id = "72ca1803-8d05-45a5-93b9-c9bb450e0eb8";
    let accepted_run_id = "d9c14337-0487-4f16-ae5d-f4ae6a177eef";
    let policy_db = policy_pool.get().await?;
    policy_db
        .execute(
            "INSERT INTO watch_tower.record_receipt
                (subject_id,adapter_id,idempotency_key,request_sha256,source_id,record_id,
                 revision,actor_id,observed_at)
             VALUES($1,'hsm',$2,$3,'cloud-wearable-sync:oura','snapshot:conflict',1,
                    'service:hsm-cloud-sync',now())",
            &[&subject_id, &rejected_key, &"d".repeat(64)],
        )
        .await?;
    let hsm_db = hsm_pool.get().await?;
    let rejected_id: i64 = hsm_db
        .query_one(
            "INSERT INTO avatar_hsm.watch_tower_record_outbox
                (subject_id,provider,request_sha256,observed_at,run_id)
             VALUES($1,'oura',$2,now(),$3::text::uuid) RETURNING id",
            &[&subject_id, &rejected_hash, &rejected_run_id],
        )
        .await?
        .get(0);
    let accepted_id: i64 = hsm_db
        .query_one(
            "INSERT INTO avatar_hsm.watch_tower_record_outbox
                (subject_id,provider,request_sha256,observed_at,run_id)
             VALUES($1,'garmin',$2,now(),$3::text::uuid) RETURNING id",
            &[&subject_id, &accepted_hash, &accepted_run_id],
        )
        .await?
        .get(0);

    assert!(drain(&hsm_pool, &policy_pool).await.is_err());

    let outbox = hsm_db
        .query(
            "SELECT id,delivered_at IS NOT NULL FROM avatar_hsm.watch_tower_record_outbox
             WHERE subject_id=$1 ORDER BY id",
            &[&subject_id],
        )
        .await?;
    assert_eq!(outbox.len(), 2);
    assert_eq!(outbox[0].get::<_, i64>(0), rejected_id);
    assert!(!outbox[0].get::<_, bool>(1));
    assert_eq!(outbox[1].get::<_, i64>(0), accepted_id);
    assert!(outbox[1].get::<_, bool>(1));
    let accepted_receipt: i64 = policy_db
        .query_one(
            "SELECT count(*) FROM watch_tower.record_receipt
             WHERE subject_id=$1 AND request_sha256=$2",
            &[&subject_id, &accepted_hash],
        )
        .await?
        .get(0);
    assert_eq!(accepted_receipt, 1);
    let accepted_run: i64 = policy_db
        .query_one(
            "SELECT count(*) FROM watch_tower.etl_run WHERE subject_id=$1 AND run_id=$2::text::uuid",
            &[&subject_id, &accepted_run_id],
        )
        .await?
        .get(0);
    assert_eq!(accepted_run, 1);
    let rejected_run = policy_db
        .query_one(
            "SELECT status,failure_count,last_failure_code,receipt_count
             FROM watch_tower.etl_run WHERE subject_id=$1 AND run_id=$2::text::uuid",
            &[&subject_id, &rejected_run_id],
        )
        .await?;
    assert_eq!(rejected_run.get::<_, String>(0), "failed");
    assert_eq!(rejected_run.get::<_, i64>(1), 1);
    assert_eq!(rejected_run.get::<_, String>(2), "record_dispatch_failed");
    assert_eq!(rejected_run.get::<_, i64>(3), 0);

    policy_db
        .execute(
            "DELETE FROM watch_tower.record_receipt WHERE subject_id=$1 AND idempotency_key=$2",
            &[&subject_id, &rejected_key],
        )
        .await?;
    hsm_db
        .execute(
            "UPDATE avatar_hsm.watch_tower_record_outbox
             SET lease_until=now()-interval '1 second' WHERE id=$1",
            &[&rejected_id],
        )
        .await?;
    drain(&hsm_pool, &policy_pool).await?;
    let recovered_run = policy_db
        .query_one(
            "SELECT status,failure_count,last_failure_code,receipt_count
             FROM watch_tower.etl_run WHERE subject_id=$1 AND run_id=$2::text::uuid",
            &[&subject_id, &rejected_run_id],
        )
        .await?;
    assert_eq!(recovered_run.get::<_, String>(0), "succeeded");
    assert_eq!(recovered_run.get::<_, i64>(1), 1);
    assert_eq!(recovered_run.get::<_, Option<String>>(2), None);
    assert_eq!(recovered_run.get::<_, i64>(3), 1);

    policy_db
        .execute(
            "DELETE FROM watch_tower.etl_run WHERE subject_id=$1",
            &[&subject_id],
        )
        .await?;
    policy_db
        .execute(
            "DELETE FROM watch_tower.record_receipt WHERE subject_id=$1",
            &[&subject_id],
        )
        .await?;
    hsm_db
        .execute(
            "DELETE FROM avatar_hsm.watch_tower_record_outbox WHERE subject_id=$1",
            &[&subject_id],
        )
        .await?;
    Ok(())
}
