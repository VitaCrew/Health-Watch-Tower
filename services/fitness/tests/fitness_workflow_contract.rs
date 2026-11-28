use serde_json::Value;

fn workflow() -> Value {
    serde_json::from_str(include_str!(
        "../../../docs/workflows/fitness-workout-confirmation.json"
    ))
    .expect("Fitness confirmation Workflow is valid JSON")
}

#[test]
fn approval_edit_is_the_only_set_list_saved_to_fitness() {
    let workflow = workflow();
    let confirm = &workflow["nodes"][1]["config"];
    assert_eq!(confirm["subject"]["target_sets"], 3);
    assert_eq!(confirm["subject"]["sets"].as_array().unwrap().len(), 3);

    let record = &workflow["nodes"][2]["config"]["args"]["arguments"];
    assert_eq!(record["sets"], "=nodes.confirm.item.json.subject.sets");
    assert_eq!(
        record["performed_at"],
        "=nodes.confirm.item.json.subject.scheduled_at"
    );
}

#[test]
fn hsm_receives_committed_count_and_retries_the_same_workflow_step() {
    let workflow = workflow();
    let hsm = &workflow["nodes"][3]["config"];
    assert_eq!(
        hsm["args"]["fitness_workout_id"],
        "=nodes.fitness_record.item.json.workout_id"
    );
    assert_eq!(
        hsm["args"]["entry"]["sets"],
        "=nodes.fitness_record.item.json.set_count"
    );
    assert_eq!(
        hsm["args"]["entry"]["target_sets"],
        "=nodes.confirm.item.json.subject.target_sets"
    );
    assert_eq!(hsm["retry"]["max_attempts"], 4);
    assert_eq!(hsm["retry"]["backoff"], "exponential");
}

#[test]
fn unanswered_confirmation_has_no_fitness_or_hsm_write_edge() {
    let workflow = workflow();
    let confirm = &workflow["nodes"][1]["config"];
    assert_eq!(confirm["wait_mode"], "suspend");
    assert_eq!(
        confirm["metadata"]["no_response_policy"],
        "expire_workflow_run_without_fitness_or_hsm_write"
    );
    assert_eq!(workflow["edges"].as_array().unwrap().len(), 4);
    assert_eq!(workflow["edges"][1]["from_port"], "approved");
    assert_eq!(workflow["edges"][2]["from_port"], "rejected");
    assert_eq!(workflow["edges"][3]["to_node"], "hsm_confirm");
    assert!(!workflow["edges"]
        .as_array()
        .unwrap()
        .iter()
        .any(|edge| { edge["from_node"] == "confirm" && edge["from_port"] == "timeout" }));
}
