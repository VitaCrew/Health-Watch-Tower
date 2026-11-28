use super::{list, Surface};

fn contains_tool(tools: &[serde_json::Value], name: &str) -> bool {
    tools.iter().any(|tool| tool["name"] == name)
}

#[test]
fn hsm_exposes_health_facts_and_user_responses_without_alert_queues() {
    let read = list(Surface::Read);
    let control = list(Surface::Control);

    assert!(contains_tool(&read, "health_status_v1"));
    assert!(contains_tool(&read, "medication_catalog"));
    assert!(!contains_tool(&read, "medication_reminders"));
    assert!(contains_tool(&control, "record_medication_response_v1"));
    assert!(contains_tool(&control, "request_oura_due_sync"));
    assert!(contains_tool(&control, "issue_health_connect_lease"));
    assert!(contains_tool(&control, "commit_health_connect_snapshot"));
    assert!(!contains_tool(&control, "request_due_medications"));
    assert!(!contains_tool(&control, "record_medication_response"));
    assert!(!contains_tool(&control, "record_acute_signal"));
    assert!(!contains_tool(&control, "queue_guardian_push"));
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database"]
async fn mobile_device_controls_are_scoped_to_the_authenticated_subject() -> anyhow::Result<()> {
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let suffix = chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_default()
        .to_string();
    let subject_a = format!("scope-a-{suffix}");
    let subject_b = format!("scope-b-{suffix}");
    let db = pool.get().await?;
    for subject in [&subject_a, &subject_b] {
        db.execute(
            "INSERT INTO avatar_hsm.subject(id,timezone) VALUES($1,'Asia/Seoul')",
            &[subject],
        )
        .await?;
    }
    drop(db);

    let mut lease_a = String::new();
    for subject in [&subject_a, &subject_b] {
        let result = super::scoped_call(
            &pool,
            super::Surface::Control,
            subject,
            "issue_health_connect_lease",
            &serde_json::json!({"provider":"health_connect"}),
        )
        .await?;
        if subject == &subject_a {
            lease_a = result["lease"].as_str().unwrap_or_default().to_owned();
        }
    }
    let now = chrono::Utc::now();
    let cross_subject_commit = super::scoped_call(
        &pool,
        super::Surface::Control,
        &subject_b,
        "commit_health_connect_snapshot",
        &serde_json::json!({
            "provider":"health_connect",
            "lease":lease_a,
            "start":now - chrono::Duration::minutes(10),
            "end":now,
            "grants":[],
            "records":[]
        }),
    )
    .await;
    assert!(cross_subject_commit.is_err());
    let status_a = super::scoped_call(
        &pool,
        super::Surface::Read,
        &subject_a,
        "device_status",
        &serde_json::json!({"provider":"health_connect"}),
    )
    .await?;
    let status_b = super::scoped_call(
        &pool,
        super::Surface::Read,
        &subject_b,
        "device_status",
        &serde_json::json!({"provider":"health_connect"}),
    )
    .await?;
    assert_eq!(status_a["state"], "pending");
    assert_eq!(status_b["state"], "pending");

    super::scoped_call(
        &pool,
        super::Surface::Control,
        &subject_a,
        "revoke_health_connect",
        &serde_json::json!({"provider":"health_connect"}),
    )
    .await?;
    let status_a_after_revoke = super::scoped_call(
        &pool,
        super::Surface::Read,
        &subject_a,
        "device_status",
        &serde_json::json!({"provider":"health_connect"}),
    )
    .await?;
    let status_b_after_revoke = super::scoped_call(
        &pool,
        super::Surface::Read,
        &subject_b,
        "device_status",
        &serde_json::json!({"provider":"health_connect"}),
    )
    .await?;
    assert_eq!(status_a_after_revoke["state"], "revoked");
    assert_eq!(status_b_after_revoke["state"], "pending");

    Ok(())
}
