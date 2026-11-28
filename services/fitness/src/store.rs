use anyhow::{bail, Context, Result};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio_postgres::NoTls;
use uuid::Uuid;
use watch_tower_core::{
    followups::WorkflowExecutionRef,
    plans::{ActualReference, PlanActualLink, PlannedReference},
    records::RecordEnvelope,
};

const WORKOUT_MIGRATION: &str = include_str!("../migrations/001-workouts.sql");
const REQUEST_KEY_MIGRATION: &str = include_str!("../migrations/002-workout-request-keys.sql");
const IMPORT_SOURCE_MIGRATION: &str = include_str!("../migrations/003-workout-import-sources.sql");

#[derive(Clone)]
pub struct FitnessStore {
    pub(crate) pool: Pool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkoutSetInput {
    #[serde(default = "default_exercise_id")]
    pub exercise_id: String,
    pub variant_id: String,
    pub reps: i32,
    pub load_kg: Option<f64>,
    pub load_mode: Option<String>,
    pub rpe: Option<f64>,
    #[serde(default = "default_set_type")]
    pub set_type: String,
    #[serde(default)]
    pub plan_id: Option<String>,
    #[serde(default)]
    pub plan_revision: Option<i64>,
    #[serde(default)]
    pub target_position: Option<i32>,
}

fn default_exercise_id() -> String {
    "squat".to_owned()
}

fn default_set_type() -> String {
    "work".to_owned()
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConfirmedWorkout {
    pub workflow_run_id: String,
    pub performed_at: String,
    pub timezone: String,
    pub sets: Vec<WorkoutSetInput>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct WorkoutWrite {
    pub(crate) owner_id: String,
    pub(crate) request_key: String,
    pub(crate) workflow_run_id: Option<String>,
    pub(crate) performed_at: String,
    pub(crate) timezone: String,
    pub(crate) sets: Vec<WorkoutSetInput>,
    pub(crate) source: &'static str,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ManualWorkout {
    pub request_key: String,
    pub performed_at: String,
    pub timezone: String,
    pub sets: Vec<WorkoutSetInput>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct WorkoutReceipt {
    pub workout_id: String,
    pub owner_id: String,
    pub workflow_run_id: Option<String>,
    pub request_key: String,
    pub performed_at: DateTime<Utc>,
    pub timezone: String,
    pub set_count: usize,
    pub status: &'static str,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PlanTargetInput {
    #[serde(default = "default_exercise_id")]
    pub exercise_id: String,
    pub variant_id: String,
    pub target_reps: i32,
    #[serde(default = "default_rest_seconds")]
    pub rest_seconds: i32,
    #[serde(default)]
    pub target_load_kg: Option<f64>,
}

fn default_rest_seconds() -> i32 {
    60
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TrainingPlanInput {
    pub request_key: String,
    pub title: String,
    pub day: String,
    pub timezone: String,
    pub targets: Vec<PlanTargetInput>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TrainingPlanDraft {
    pub title: String,
    pub day: String,
    pub timezone: String,
    pub targets: Vec<PlanTargetInput>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProfileSettings {
    pub goal: String,
    pub experience: String,
    pub equipment: Vec<String>,
    pub days_per_week: i32,
}

impl FitnessStore {
    pub async fn connect(url: &str) -> Result<Self> {
        let config: tokio_postgres::Config = url.parse().context("invalid Fitness database URL")?;
        let manager = Manager::from_config(
            config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Verified,
            },
        );
        let pool = Pool::builder(manager).max_size(12).build()?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }

    async fn migrate(&self) -> Result<()> {
        let mut client = self
            .pool
            .get()
            .await
            .context("connecting to Fitness PostgreSQL")?;
        let tx = client.transaction().await?;
        tx.query_one("SELECT pg_advisory_xact_lock(17841, 1)", &[])
            .await?;
        tx.batch_execute(WORKOUT_MIGRATION)
            .await
            .context("applying Fitness schema migration 001")?;
        tx.batch_execute(REQUEST_KEY_MIGRATION)
            .await
            .context("applying Fitness schema migration 002")?;
        tx.batch_execute(IMPORT_SOURCE_MIGRATION)
            .await
            .context("applying Fitness schema migration 003")?;
        tx.commit().await?;
        Ok(())
    }

    /// Verify an adapter-owned record reference without returning its payload.
    pub async fn record_exists(&self, owner_id: &str, record_id: &str) -> Result<bool> {
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM fitness.workout_sessions
                     WHERE owner_id=$1 AND id=$2
                    UNION ALL
                    SELECT 1 FROM fitness.workout_sets s
                     JOIN fitness.workout_sessions w ON w.id=s.workout_id
                     WHERE w.owner_id=$1 AND s.id=$2 AND s.deleted_at IS NULL
                    UNION ALL
                    SELECT 1 FROM fitness.training_plans
                     WHERE owner_id=$1 AND id=$2 AND deleted_at IS NULL
                )",
                &[&owner_id, &record_id],
            )
            .await?;
        Ok(row.get(0))
    }

    pub async fn record_confirmed_workout(
        &self,
        owner_id: &str,
        request: &ConfirmedWorkout,
    ) -> Result<WorkoutReceipt> {
        let workflow_ref = WorkflowExecutionRef {
            workflow_id: None,
            run_id: request.workflow_run_id.clone(),
            task_key: "confirmed-record:fitness-workout".to_owned(),
            assignee_id: None,
        };
        if !workflow_ref.is_valid() {
            bail!("confirmed workout requires a valid OpenHuman Workflow execution reference");
        }
        let normalized = WorkoutWrite {
            owner_id: owner_id.to_owned(),
            request_key: format!("workflow:{}", request.workflow_run_id),
            workflow_run_id: Some(request.workflow_run_id.clone()),
            performed_at: request.performed_at.clone(),
            timezone: request.timezone.clone(),
            sets: request.sets.clone(),
            source: "workflow",
        };
        self.record_workout(&normalized).await
    }

    pub async fn record_manual_workout(
        &self,
        owner_id: &str,
        request: &ManualWorkout,
    ) -> Result<WorkoutReceipt> {
        let normalized = WorkoutWrite {
            owner_id: owner_id.to_owned(),
            request_key: format!("manual:{}", request.request_key),
            workflow_run_id: None,
            performed_at: request.performed_at.clone(),
            timezone: request.timezone.clone(),
            sets: request.sets.clone(),
            source: "manual",
        };
        self.record_workout(&normalized).await
    }

    pub async fn record_imported_workout(
        &self,
        owner_id: &str,
        provider: &str,
        request: &ManualWorkout,
    ) -> Result<WorkoutReceipt> {
        if !matches!(provider, "strong" | "hevy") {
            bail!("unsupported workout import source");
        }
        let normalized = WorkoutWrite {
            owner_id: owner_id.to_owned(),
            request_key: format!("{provider}:{}", request.request_key),
            workflow_run_id: None,
            performed_at: request.performed_at.clone(),
            timezone: request.timezone.clone(),
            sets: request.sets.clone(),
            source: if provider == "strong" {
                "strong"
            } else {
                "hevy"
            },
        };
        self.record_workout(&normalized).await
    }

    async fn record_workout(&self, request: &WorkoutWrite) -> Result<WorkoutReceipt> {
        let (performed_at, timezone) = validate_workout(request)?;
        let envelope = RecordEnvelope {
            subject_id: request.owner_id.clone(),
            actor_id: request
                .workflow_run_id
                .as_deref()
                .map(|run_id| format!("workflow:{run_id}"))
                .unwrap_or_else(|| format!("adapter:{}", request.source)),
            adapter_id: "fitness".to_owned(),
            source_id: request.source.to_owned(),
            record_id: request.request_key.clone(),
            revision: 1,
            run_id: None,
            observed_at_unix: performed_at.timestamp(),
            idempotency_key: request.request_key.clone(),
        };
        if !envelope.is_valid() {
            bail!("workout record is missing common provenance fields");
        }
        let request_sha256 = fingerprint(request)?;
        let mut client = self
            .pool
            .get()
            .await
            .context("getting Fitness database connection")?;
        let tx = client.transaction().await?;
        if matches!(request.source, "strong" | "hevy") {
            let digest = Sha256::digest(request.owner_id.as_bytes());
            let lock_key = i64::from_be_bytes(digest[..8].try_into()?);
            tx.query_one("SELECT pg_advisory_xact_lock($1)", &[&lock_key])
                .await?;
        }
        if let Some(existing) = tx
            .query_opt(
                "SELECT id, request_sha256, performed_at, timezone, \
                        (SELECT count(*)::bigint FROM fitness.workout_sets s WHERE s.workout_id = w.id AND s.deleted_at IS NULL) \
                 FROM fitness.workout_sessions w WHERE owner_id = $1 AND request_key = $2 FOR UPDATE",
                &[&request.owner_id, &request.request_key],
            )
            .await?
        {
            let old_fingerprint: String = existing.get(1);
            if old_fingerprint != request_sha256 {
                bail!("workflow_run_id was already used for different workout data");
            }
            let receipt = WorkoutReceipt {
                workout_id: existing.get(0),
                owner_id: request.owner_id.clone(),
                workflow_run_id: request.workflow_run_id.clone(),
                request_key: request.request_key.clone(),
                performed_at: existing.get(2),
                timezone: existing.get(3),
                set_count: existing.get::<_, i64>(4) as usize,
                status: "recorded",
            };
            tx.commit().await?;
            return Ok(receipt);
        }

        if matches!(request.source, "strong" | "hevy") {
            let other = if request.source == "strong" {
                "hevy"
            } else {
                "strong"
            };
            let minute = performed_at.timestamp().div_euclid(60) * 60;
            let start = DateTime::from_timestamp(minute, 0).context("invalid workout time")?;
            let stop = start + chrono::Duration::seconds(60);
            let candidates = tx.query(
                "SELECT id,request_sha256,performed_at FROM fitness.workout_sessions \
                 WHERE owner_id=$1 AND source=$2 AND timezone=$3 AND performed_at >= $4 AND performed_at < $5 FOR UPDATE",
                &[&request.owner_id,&other,&timezone,&start,&stop],
            ).await?;
            for candidate in candidates {
                let old_at: DateTime<Utc> = candidate.get(2);
                let old_digest: String = candidate.get(1);
                let matches = (old_at == performed_at && old_digest == request_sha256)
                    || ((old_at.timestamp().rem_euclid(60) == 0
                        || performed_at.timestamp().rem_euclid(60) == 0)
                        && fingerprint_at(request, old_at)? == old_digest);
                if matches {
                    let receipt = WorkoutReceipt {
                        workout_id: candidate.get(0),
                        owner_id: request.owner_id.clone(),
                        workflow_run_id: None,
                        request_key: request.request_key.clone(),
                        performed_at,
                        timezone: timezone.clone(),
                        set_count: 0,
                        status: "cross_source_match",
                    };
                    tx.commit().await?;
                    return Ok(receipt);
                }
            }
        }

        let workout_id = Uuid::new_v4().simple().to_string();
        let inserted = tx
            .query_opt(
                "INSERT INTO fitness.workout_sessions \
                (id, owner_id, request_key, workflow_run_id, request_sha256, performed_at, timezone, source) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (owner_id, request_key) DO NOTHING RETURNING id",
                &[
                    &workout_id,
                    &request.owner_id,
                    &request.request_key,
                    &request.workflow_run_id,
                    &request_sha256,
                    &performed_at,
                    &timezone,
                    &request.source,
                ],
            )
            .await
            .with_context(|| {
                format!(
                    "saving Fitness workout request {}",
                    request.request_key
                )
            })?;

        if inserted.is_none() {
            let existing = tx
                .query_one(
                    "SELECT id, request_sha256, performed_at, timezone, \
                            (SELECT count(*)::bigint FROM fitness.workout_sets s WHERE s.workout_id = w.id AND s.deleted_at IS NULL) \
                     FROM fitness.workout_sessions w WHERE owner_id = $1 AND request_key = $2 FOR UPDATE",
                    &[&request.owner_id, &request.request_key],
                )
                .await?;
            let old_fingerprint: String = existing.get(1);
            if old_fingerprint != request_sha256 {
                bail!("workflow_run_id was already used for different workout data");
            }
            let receipt = WorkoutReceipt {
                workout_id: existing.get(0),
                owner_id: request.owner_id.clone(),
                workflow_run_id: request.workflow_run_id.clone(),
                request_key: request.request_key.clone(),
                performed_at: existing.get(2),
                timezone: existing.get(3),
                set_count: existing.get::<_, i64>(4) as usize,
                status: "recorded",
            };
            tx.commit().await?;
            return Ok(receipt);
        }

        for (index, set) in request.sets.iter().enumerate() {
            let set_id = Uuid::new_v4().simple().to_string();
            let position = (index + 1) as i32;
            tx.execute(
                "INSERT INTO fitness.workout_sets \
                    (id, workout_id, position, exercise_id, variant_id, reps, load_kg, load_mode, rpe, set_type) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                    &[&set_id, &workout_id, &position, &set.exercise_id, &set.variant_id, &set.reps,
                  &set.load_kg, &set.load_mode, &set.rpe, &set.set_type],
            )
            .await?;
            if let (Some(plan_id), Some(plan_revision), Some(target_position)) =
                (&set.plan_id, set.plan_revision, set.target_position)
            {
                let plan = tx.query_opt(
                    "SELECT r.definition FROM fitness.training_plans p \
                     JOIN fitness.training_plan_revisions r ON r.plan_id=p.id AND r.revision=p.revision \
                     WHERE p.id=$1 AND p.owner_id=$2 AND p.revision=$3 AND p.deleted_at IS NULL FOR UPDATE OF p",
                    &[plan_id,&request.owner_id,&plan_revision],
                ).await?.ok_or_else(|| anyhow::anyhow!("training plan revision is unavailable"))?;
                let definition: Value = plan.get(0);
                let targets = definition["targets"]
                    .as_array()
                    .context("invalid plan targets")?;
                let target = targets
                    .get(usize::try_from(target_position - 1)?)
                    .context("training target is unavailable")?;
                let plan_day: NaiveDate = definition["day"]
                    .as_str()
                    .context("invalid plan day")?
                    .parse()?;
                let plan_zone: Tz = definition["timezone"]
                    .as_str()
                    .context("invalid plan timezone")?
                    .parse()?;
                if target["exercise_id"] != set.exercise_id
                    || target["variant_id"] != set.variant_id
                    || set.reps <= 0
                    || performed_at.with_timezone(&plan_zone).date_naive() != plan_day
                {
                    bail!("workout set does not complete its training plan target");
                }
                let occupied = tx.query_opt(
                    "SELECT set_id FROM fitness.training_plan_executions WHERE plan_id=$1 AND plan_revision=$2 AND target_position=$3",
                    &[plan_id,&plan_revision,&target_position],
                ).await?;
                if occupied.is_some() {
                    bail!("training plan target already has a linked workout set");
                }
                let plan_actual = PlanActualLink {
                    planned: PlannedReference {
                        subject_id: request.owner_id.clone(),
                        adapter_id: "fitness".to_owned(),
                        plan_id: plan_id.clone(),
                        revision: plan_revision,
                        planned_at_unix: None,
                    },
                    actual: ActualReference {
                        subject_id: request.owner_id.clone(),
                        adapter_id: "fitness".to_owned(),
                        record_id: set_id.clone(),
                        revision: 1,
                        occurred_at_unix: performed_at.timestamp(),
                    },
                    target_key: Some(target_position.to_string()),
                };
                if !plan_actual.is_valid() {
                    bail!("plan and actual record references do not identify the same subject");
                }
                tx.execute(
                    "INSERT INTO fitness.training_plan_executions (set_id,plan_id,plan_revision,target_position) VALUES ($1,$2,$3,$4)",
                    &[&set_id,plan_id,&plan_revision,&target_position],
                ).await?;
            }
        }
        tx.commit().await?;

        Ok(WorkoutReceipt {
            workout_id,
            owner_id: request.owner_id.clone(),
            workflow_run_id: request.workflow_run_id.clone(),
            request_key: request.request_key.clone(),
            performed_at,
            timezone,
            set_count: request.sets.len(),
            status: "recorded",
        })
    }

    pub async fn workout_day(
        &self,
        owner_id: &str,
        day: NaiveDate,
        timezone: &str,
        offset: i64,
    ) -> Result<Value> {
        if !(0..=1_000_000).contains(&offset) {
            bail!("invalid workout page offset");
        }
        let zone: Tz = timezone.parse().context("invalid IANA workout timezone")?;
        let start = zone
            .from_local_datetime(&day.and_hms_opt(0, 0, 0).context("invalid workout day")?)
            .single()
            .context("ambiguous or invalid local day boundary")?
            .with_timezone(&Utc);
        let next_day = day.succ_opt().context("unsupported workout day")?;
        let end = zone
            .from_local_datetime(
                &next_day
                    .and_hms_opt(0, 0, 0)
                    .context("invalid workout day")?,
            )
            .single()
            .context("ambiguous or invalid local day boundary")?
            .with_timezone(&Utc);
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT w.id, w.performed_at, w.timezone, w.source, s.id, s.position, s.revision, \
                        s.exercise_id, s.variant_id, s.reps, s.load_kg, s.load_mode, s.rpe, s.set_type \
                 FROM fitness.workout_sessions w JOIN fitness.workout_sets s ON s.workout_id = w.id \
                 WHERE w.owner_id = $1 AND w.performed_at >= $2 AND w.performed_at < $3 AND s.deleted_at IS NULL \
                 ORDER BY w.performed_at, w.id, s.position LIMIT 31 OFFSET $4",
                &[&owner_id, &start, &end, &offset],
            )
            .await?;
        let sets: Vec<Value> = rows
            .iter()
            .take(30)
            .map(|row| {
                json!({
                    "workout_id": row.get::<_, String>(0),
                    "performed_at": row.get::<_, DateTime<Utc>>(1),
                    "timezone": row.get::<_, String>(2),
                    "source": row.get::<_, String>(3),
                    "set_id": row.get::<_, String>(4),
                    "position": row.get::<_, i32>(5),
                    "revision": row.get::<_, i32>(6),
                    "exercise_id": row.get::<_, String>(7),
                    "variant_id": row.get::<_, String>(8),
                    "reps": row.get::<_, i32>(9),
                    "load_kg": row.get::<_, Option<f64>>(10),
                    "load_mode": row.get::<_, Option<String>>(11),
                    "rpe": row.get::<_, Option<f64>>(12),
                    "set_type": row.get::<_, String>(13)
                })
            })
            .collect();
        Ok(json!({
            "date": day,
            "timezone": timezone,
            "sets": sets,
            "next_offset": if rows.len() > 30 { Some(offset + 30) } else { None }
        }))
    }

    pub async fn change_workout_set(
        &self,
        owner_id: &str,
        set_id: &str,
        expected_revision: i32,
        replacement: Option<&WorkoutSetInput>,
    ) -> Result<Value> {
        if set_id.len() != 32 || !set_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("invalid set id");
        }
        if expected_revision < 1 {
            bail!("invalid set revision");
        }
        if let Some(set) = replacement {
            validate_set(set)?;
        }
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let row = tx
            .query_opt(
                "SELECT s.revision, s.deleted_at, s.exercise_id, s.variant_id, s.reps, \
                        s.load_kg, s.load_mode, s.rpe, s.set_type \
                 FROM fitness.workout_sets s \
             JOIN fitness.workout_sessions w ON w.id = s.workout_id \
             WHERE s.id = $1 AND w.owner_id = $2 FOR UPDATE OF s",
                &[&set_id, &owner_id],
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("workout set not found"))?;
        let revision: i32 = row.get(0);
        let deleted_at: Option<DateTime<Utc>> = row.get(1);
        if revision != expected_revision || deleted_at.is_some() {
            if expected_revision.checked_add(1) == Some(revision) {
                let retry_matches = match replacement {
                    None => deleted_at.is_some(),
                    Some(set) => {
                        deleted_at.is_none()
                            && row.get::<_, String>(2) == set.exercise_id
                            && row.get::<_, String>(3) == set.variant_id
                            && row.get::<_, i32>(4) == set.reps
                            && row.get::<_, Option<f64>>(5) == set.load_kg
                            && row.get::<_, Option<String>>(6) == set.load_mode
                            && row.get::<_, Option<f64>>(7) == set.rpe
                            && row.get::<_, String>(8) == set.set_type
                    }
                };
                if retry_matches {
                    tx.commit().await?;
                    return Ok(json!({
                        "set_id": set_id,
                        "revision": revision,
                        "deleted": deleted_at.is_some()
                    }));
                }
            }
            bail!("workout set revision conflict");
        }
        let new_revision = revision + 1;
        match replacement {
            Some(set) => {
                tx.execute(
                    "UPDATE fitness.workout_sets SET exercise_id=$3, variant_id=$4, reps=$5, load_kg=$6, \
                     load_mode=$7, rpe=$8, set_type=$9, revision=$10 WHERE id=$1 AND revision=$2",
                    &[&set_id, &expected_revision, &set.exercise_id, &set.variant_id, &set.reps,
                      &set.load_kg, &set.load_mode, &set.rpe, &set.set_type, &new_revision],
                ).await?;
            }
            None => {
                tx.execute(
                    "UPDATE fitness.workout_sets SET deleted_at=now(), revision=$3 WHERE id=$1 AND revision=$2",
                    &[&set_id, &expected_revision, &new_revision],
                ).await?;
            }
        }
        tx.commit().await?;
        Ok(json!({"set_id": set_id, "revision": new_revision, "deleted": replacement.is_none()}))
    }

    pub async fn create_training_plan(
        &self,
        owner_id: &str,
        input: &TrainingPlanInput,
    ) -> Result<Value> {
        validate_plan(input)?;
        let plan_json =
            plan_definition_json(&input.title, &input.day, &input.timezone, &input.targets)?;
        let fingerprint = fingerprint_value(&plan_json)?;
        let plan_id = Uuid::new_v4().simple().to_string();
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let inserted = tx.query_opt(
            "INSERT INTO fitness.training_plans (id, owner_id, request_key, request_sha256, revision) \
             VALUES ($1, $2, $3, $4, 1) ON CONFLICT (owner_id, request_key) DO NOTHING RETURNING id",
            &[&plan_id, &owner_id, &input.request_key, &fingerprint],
        ).await?;
        let resolved_id = if inserted.is_some() {
            tx.execute(
                "INSERT INTO fitness.training_plan_revisions (plan_id, revision, definition) VALUES ($1, 1, $2)",
                &[&plan_id, &plan_json],
            ).await?;
            plan_id
        } else {
            let existing = tx.query_one(
                "SELECT p.id, p.request_sha256, p.revision, r.definition \
                 FROM fitness.training_plans p JOIN fitness.training_plan_revisions r \
                   ON r.plan_id = p.id AND r.revision = p.revision \
                 WHERE p.owner_id = $1 AND p.request_key = $2 AND p.deleted_at IS NULL FOR UPDATE OF p",
                &[&owner_id, &input.request_key],
            ).await?;
            let old_fingerprint: String = existing.get(1);
            if old_fingerprint != fingerprint {
                bail!("request_key was already used for different training plan data");
            }
            let value: Value = existing.get(3);
            tx.commit().await?;
            return Ok(
                json!({"id": existing.get::<_, String>(0), "revision": existing.get::<_, i64>(2), "plan": value, "status": "recorded"}),
            );
        };
        tx.commit().await?;
        Ok(json!({"id": resolved_id, "revision": 1, "plan": plan_json, "status": "recorded"}))
    }

    pub async fn revise_training_plan(
        &self,
        owner_id: &str,
        plan_id: &str,
        expected_revision: i64,
        draft: &TrainingPlanDraft,
    ) -> Result<Value> {
        validate_plan_definition(&draft.title, &draft.day, &draft.timezone, &draft.targets)?;
        let definition =
            plan_definition_json(&draft.title, &draft.day, &draft.timezone, &draft.targets)?;
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let row = tx
            .query_opt(
                "SELECT p.revision, r.definition FROM fitness.training_plans p \
             JOIN fitness.training_plan_revisions r ON r.plan_id=p.id AND r.revision=p.revision \
             WHERE p.id=$1 AND p.owner_id=$2 AND p.deleted_at IS NULL FOR UPDATE OF p",
                &[&plan_id, &owner_id],
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("training plan not found"))?;
        let current_revision: i64 = row.get(0);
        if current_revision != expected_revision {
            let previous: Value = row.get(1);
            if expected_revision.checked_add(1) == Some(current_revision) && previous == definition
            {
                tx.commit().await?;
                return Ok(json!({
                    "id": plan_id,
                    "revision": current_revision,
                    "targets": definition
                }));
            }
            bail!("training plan revision conflict");
        }
        let previous: Value = row.get(1);
        let next_revision = current_revision + 1;
        tx.execute(
            "INSERT INTO fitness.training_plan_revisions (plan_id, revision, definition) VALUES ($1,$2,$3)",
            &[&plan_id, &next_revision, &definition],
        ).await?;
        if previous["day"] == definition["day"]
            && previous["timezone"] == definition["timezone"]
            && previous["targets"] == definition["targets"]
        {
            tx.execute("UPDATE fitness.training_plan_executions SET plan_revision=$2 WHERE plan_id=$1 AND plan_revision=$3",
                &[&plan_id,&next_revision,&current_revision]).await?;
        } else {
            tx.execute(
                "DELETE FROM fitness.training_plan_executions WHERE plan_id=$1",
                &[&plan_id],
            )
            .await?;
        }
        tx.execute(
            "UPDATE fitness.training_plans SET revision=$2 WHERE id=$1 AND owner_id=$3",
            &[&plan_id, &next_revision, &owner_id],
        )
        .await?;
        tx.commit().await?;
        Ok(json!({"id":plan_id,"revision":next_revision,"targets":definition}))
    }

    pub async fn delete_training_plan(
        &self,
        owner_id: &str,
        plan_id: &str,
        expected_revision: i64,
    ) -> Result<Value> {
        let client = self.pool.get().await?;
        let deleted = client
            .execute(
                "DELETE FROM fitness.training_plans WHERE id=$1 AND owner_id=$2 AND revision=$3",
                &[&plan_id, &owner_id, &expected_revision],
            )
            .await?;
        if deleted == 0 {
            bail!("training plan is missing or revision changed");
        }
        Ok(json!({"id":plan_id,"deleted":true}))
    }

    pub async fn save_profile(
        &self,
        owner_id: &str,
        revision: i64,
        settings: Option<&ProfileSettings>,
    ) -> Result<Value> {
        if let Some(value) = settings {
            validate_profile(value)?;
        }
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let current = tx.query_opt("SELECT revision, settings FROM fitness.training_profiles WHERE owner_id=$1 FOR UPDATE", &[&owner_id]).await?;
        let current_revision = current
            .as_ref()
            .map(|row| row.get::<_, i64>(0))
            .unwrap_or(0);
        let active = current.as_ref().is_some_and(|row| {
            !row.get::<_, Value>(1)
                .as_object()
                .is_none_or(|value| value.is_empty())
        });
        if current_revision != revision
            || (settings.is_none() && !active)
            || (settings.is_some() && active && revision == 0)
        {
            bail!("training profile revision conflict");
        }
        let next = current_revision + 1;
        let value = settings
            .map(serde_json::to_value)
            .transpose()?
            .unwrap_or_else(|| json!({}));
        if current.is_some() {
            tx.execute("UPDATE fitness.training_profiles SET revision=$2, settings=$3, updated_at=now() WHERE owner_id=$1",
                &[&owner_id,&next,&value]).await?;
        } else if settings.is_some() {
            tx.execute("INSERT INTO fitness.training_profiles (owner_id, revision, settings) VALUES ($1,1,$2)",
                &[&owner_id,&value]).await?;
        } else {
            bail!("training profile does not exist");
        }
        tx.commit().await?;
        Ok(json!({"revision":next,"settings":value}))
    }

    pub async fn link_training_execution(
        &self,
        owner_id: &str,
        plan_id: &str,
        revision: i64,
        position: i32,
        set_id: Option<&str>,
    ) -> Result<Value> {
        if !(1..=30).contains(&position) {
            bail!("invalid plan target position");
        }
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let plan = tx
            .query_opt(
                "SELECT p.revision, r.definition FROM fitness.training_plans p \
             JOIN fitness.training_plan_revisions r ON r.plan_id=p.id AND r.revision=p.revision \
             WHERE p.id=$1 AND p.owner_id=$2 AND p.deleted_at IS NULL FOR UPDATE OF p",
                &[&plan_id, &owner_id],
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("training plan not found"))?;
        let current_revision: i64 = plan.get(0);
        if current_revision != revision {
            bail!("training plan revision conflict");
        }
        let definition: Value = plan.get(1);
        let targets = definition["targets"]
            .as_array()
            .context("invalid saved training plan")?;
        let target = targets
            .get((position - 1) as usize)
            .context("target position not found")?;
        if let Some(set_id) = set_id {
            let actual = tx.query_opt(
                "SELECT s.exercise_id,s.variant_id,s.reps,s.deleted_at,w.performed_at,w.timezone \
                 FROM fitness.workout_sets s JOIN fitness.workout_sessions w ON w.id=s.workout_id \
                 WHERE s.id=$1 AND w.owner_id=$2 FOR UPDATE OF s",
                &[&set_id,&owner_id],
            ).await?.ok_or_else(|| anyhow::anyhow!("workout set not found"))?;
            let deleted: Option<DateTime<Utc>> = actual.get(3);
            let performed_at: DateTime<Utc> = actual.get(4);
            let workout_timezone: String = actual.get(5);
            let plan_day: NaiveDate = definition["day"]
                .as_str()
                .context("invalid plan day")?
                .parse()?;
            let plan_zone: Tz = definition["timezone"]
                .as_str()
                .context("invalid plan timezone")?
                .parse()?;
            if deleted.is_some()
                || actual.get::<_, String>(0) != target["exercise_id"]
                || actual.get::<_, String>(1) != target["variant_id"]
                || actual.get::<_, i32>(2) <= 0
                || performed_at.with_timezone(&plan_zone).date_naive() != plan_day
            {
                bail!("workout set does not complete this target");
            }
            let _: Tz = workout_timezone
                .parse()
                .context("invalid workout timezone")?;
            tx.execute("DELETE FROM fitness.training_plan_executions WHERE plan_id=$1 AND plan_revision=$2 AND target_position=$3",
                &[&plan_id,&revision,&position]).await?;
            tx.execute("INSERT INTO fitness.training_plan_executions (set_id,plan_id,plan_revision,target_position) VALUES ($1,$2,$3,$4)",
                &[&set_id,&plan_id,&revision,&position]).await?;
        } else {
            tx.execute("DELETE FROM fitness.training_plan_executions WHERE plan_id=$1 AND plan_revision=$2 AND target_position=$3",
                &[&plan_id,&revision,&position]).await?;
        }
        tx.commit().await?;
        Ok(
            json!({"plan_id":plan_id,"revision":revision,"target_position":position,"set_id":set_id}),
        )
    }

    pub async fn training_context(&self, owner_id: &str, plan_id: Option<&str>) -> Result<Value> {
        let client = self.pool.get().await?;
        let profile = client
            .query_opt(
                "SELECT revision, settings FROM fitness.training_profiles WHERE owner_id = $1",
                &[&owner_id],
            )
            .await?;
        let rows = client.query(
            "SELECT p.id, p.revision, r.definition, p.created_at FROM fitness.training_plans p \
             JOIN fitness.training_plan_revisions r ON r.plan_id = p.id AND r.revision = p.revision \
             WHERE p.owner_id = $1 AND p.deleted_at IS NULL AND ($2::text IS NULL OR p.id=$2) \
             ORDER BY p.created_at DESC, p.id LIMIT 30",
            &[&owner_id, &plan_id],
        ).await?;
        let profile = profile.and_then(|row| {
            let revision: i64 = row.get(0);
            let settings: Value = row.get(1);
            if settings.as_object().is_none_or(|fields| fields.is_empty()) {
                None
            } else {
                Some(json!({"revision":revision,"settings":settings}))
            }
        });
        let mut plans = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.get(0);
            let revision: i64 = row.get(1);
            let definition: Value = row.get(2);
            let day: NaiveDate = definition["day"]
                .as_str()
                .context("invalid plan day")?
                .parse()?;
            let timezone = definition["timezone"]
                .as_str()
                .context("invalid plan timezone")?;
            let zone: Tz = timezone.parse().context("invalid plan timezone")?;
            let executions = client.query(
                "SELECT e.target_position,s.id,s.reps,s.load_kg,s.exercise_id,s.variant_id,s.deleted_at,w.performed_at \
                 FROM fitness.training_plan_executions e JOIN fitness.workout_sets s ON s.id=e.set_id \
                 JOIN fitness.workout_sessions w ON w.id=s.workout_id \
                 WHERE e.plan_id=$1 AND e.plan_revision=$2 AND w.owner_id=$3",
                &[&id,&revision,&owner_id],
            ).await?;
            let targets = definition["targets"]
                .as_array()
                .context("invalid plan targets")?;
            let mut completed = Vec::new();
            for execution in executions {
                let position: i32 = execution.get(0);
                let index = (position - 1) as usize;
                if index >= targets.len() {
                    continue;
                }
                let deleted: Option<DateTime<Utc>> = execution.get(6);
                let performed_at: DateTime<Utc> = execution.get(7);
                if deleted.is_none()
                    && execution.get::<_, String>(4) == targets[index]["exercise_id"]
                    && execution.get::<_, String>(5) == targets[index]["variant_id"]
                    && execution.get::<_, i32>(2) > 0
                    && performed_at.with_timezone(&zone).date_naive() == day
                {
                    completed.push(json!({"target_position":position,"set_id":execution.get::<_,String>(1),
                        "recorded_reps":execution.get::<_,i32>(2),"recorded_load_kg":execution.get::<_,Option<f64>>(3)}));
                }
            }
            plans.push(json!({"id":id,"revision":revision,"targets":definition,
                "completed":completed,"planned_sets":targets.len()}));
        }
        if plan_id.is_some() && plans.is_empty() {
            bail!("training plan not found");
        }
        Ok(json!({"profile": profile, "plans": plans}))
    }

    /// Return reference-only links for the common Watch Tower timeline. The
    /// Fitness schema remains the source of plans, target rules, and actuals.
    pub async fn plan_actual_links(
        &self,
        owner_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Value> {
        if start >= end || end - start > chrono::Duration::days(366) {
            bail!("select a range of up to 366 days");
        }
        let client = self.pool.get().await?;
        let start_day = start.date_naive();
        let end_day = end.date_naive();
        let plans = client
            .query(
                "SELECT p.id,p.revision,r.definition::text FROM fitness.training_plans p
             JOIN fitness.training_plan_revisions r ON r.plan_id=p.id AND r.revision=p.revision
             WHERE p.owner_id=$1 AND p.deleted_at IS NULL
               AND (r.definition->>'day')::date >= $2 AND (r.definition->>'day')::date <= $3
             ORDER BY (r.definition->>'day')::date,p.id",
                &[&owner_id, &start_day, &end_day],
            )
            .await?;
        let mut links = Vec::new();
        for plan in plans {
            let plan_id: String = plan.get(0);
            let plan_revision: i64 = plan.get(1);
            let definition: Value = serde_json::from_str(&plan.get::<_, String>(2))
                .context("invalid stored training plan definition")?;
            let day: NaiveDate = definition["day"]
                .as_str()
                .context("invalid plan day")?
                .parse()?;
            let timezone: Tz = definition["timezone"]
                .as_str()
                .context("invalid plan timezone")?
                .parse()?;
            let planned_at = day
                .and_hms_opt(0, 0, 0)
                .context("invalid plan date")?
                .and_local_timezone(timezone)
                .earliest()
                .context("ambiguous local plan start")?
                .timestamp();
            let targets = definition["targets"]
                .as_array()
                .context("invalid plan targets")?;
            let executions = client.query(
                "SELECT e.target_position,s.id,s.revision,s.reps,s.exercise_id,s.variant_id,s.deleted_at,w.performed_at
                 FROM fitness.training_plan_executions e
                 JOIN fitness.workout_sets s ON s.id=e.set_id
                 JOIN fitness.workout_sessions w ON w.id=s.workout_id
                 WHERE e.plan_id=$1 AND e.plan_revision=$2 AND w.owner_id=$3",
                &[&plan_id,&plan_revision,&owner_id],
            ).await?;
            for (index, target) in targets.iter().enumerate() {
                let position = i32::try_from(index + 1)?;
                let execution = executions
                    .iter()
                    .find(|row| row.get::<_, i32>(0) == position);
                let actual = execution.and_then(|row| {
                    let deleted: Option<DateTime<Utc>> = row.get(6);
                    let performed_at: DateTime<Utc> = row.get(7);
                    let target_day = performed_at.with_timezone(&timezone).date_naive();
                    (deleted.is_none()
                        && row.get::<_, String>(4) == target["exercise_id"]
                        && row.get::<_, String>(5) == target["variant_id"]
                        && row.get::<_, i32>(3) > 0
                        && target_day == day)
                        .then(|| watch_tower_core::plans::ActualReference {
                            subject_id: owner_id.to_owned(),
                            adapter_id: "fitness".to_owned(),
                            record_id: row.get(1),
                            revision: i64::from(row.get::<_, i32>(2)),
                            occurred_at_unix: performed_at.timestamp(),
                        })
                });
                let state = if actual.is_some() {
                    "completed"
                } else {
                    "planned"
                };
                let observation = watch_tower_core::plans::PlanActualObservation {
                    planned: watch_tower_core::plans::PlannedReference {
                        subject_id: owner_id.to_owned(),
                        adapter_id: "fitness".to_owned(),
                        plan_id: plan_id.clone(),
                        revision: plan_revision,
                        planned_at_unix: Some(planned_at),
                    },
                    actual,
                    target_key: Some(position.to_string()),
                    state: state.to_owned(),
                };
                anyhow::ensure!(
                    observation.is_valid(),
                    "invalid Fitness plan/actual reference"
                );
                links.push(json!({
                    "planned": {
                        "subject_id": observation.planned.subject_id,
                        "adapter_id": observation.planned.adapter_id,
                        "plan_id": observation.planned.plan_id,
                        "revision": observation.planned.revision,
                        "planned_at_unix": observation.planned.planned_at_unix
                    },
                    "actual": observation.actual.map(|actual| json!({
                        "subject_id": actual.subject_id,
                        "adapter_id": actual.adapter_id,
                        "record_id": actual.record_id,
                        "revision": actual.revision,
                        "occurred_at_unix": actual.occurred_at_unix
                    })),
                    "target_key": observation.target_key,
                    "state": observation.state
                }));
            }
        }
        Ok(json!({"links":links}))
    }
}

fn validate_workout(request: &WorkoutWrite) -> Result<(DateTime<Utc>, String)> {
    if request.owner_id.trim().is_empty() || request.owner_id.len() > 64 {
        bail!("invalid Fitness owner id");
    }
    if request.request_key.trim().is_empty() || request.request_key.len() > 160 {
        bail!("invalid workout request key");
    }
    if request.source == "workflow"
        && request
            .workflow_run_id
            .as_deref()
            .is_none_or(|id| id.trim().is_empty() || id.len() > 128)
    {
        bail!("invalid Workflow run id");
    }
    if matches!(request.source, "manual" | "strong" | "hevy") {
        let prefix = format!("{}:", request.source);
        let key = request
            .request_key
            .strip_prefix(&prefix)
            .unwrap_or_default();
        if key.len() != 32 || !key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("manual request_key must be a 32-character hexadecimal id");
        }
    }
    if !matches!(request.source, "manual" | "workflow" | "strong" | "hevy") {
        bail!("unsupported workout source");
    }
    if request.sets.is_empty() || request.sets.len() > 30 {
        bail!("workout must contain between 1 and 30 sets");
    }
    let zone: Tz = request
        .timezone
        .parse()
        .context("invalid IANA workout timezone")?;
    let parsed: DateTime<FixedOffset> = DateTime::parse_from_rfc3339(&request.performed_at)
        .context("performed_at must be RFC3339 with an explicit UTC offset")?;
    if !(1970..9999).contains(&parsed.year()) {
        bail!("unsupported performed_at year");
    }
    let performed_at = parsed.with_timezone(&Utc);
    if zone
        .offset_from_utc_datetime(&performed_at.naive_utc())
        .fix()
        .local_minus_utc()
        != parsed.offset().local_minus_utc()
    {
        bail!("performed_at offset must match timezone");
    }
    for set in &request.sets {
        validate_set(set)?;
        if set.plan_id.is_some() != set.plan_revision.is_some()
            || set.plan_id.is_some() != set.target_position.is_some()
        {
            bail!("plan_id, plan_revision, and target_position must be supplied together");
        }
        if set
            .target_position
            .is_some_and(|position| !(1..=30).contains(&position))
        {
            bail!("target_position must be between 1 and 30");
        }
    }
    Ok((performed_at, request.timezone.clone()))
}

fn validate_set(set: &WorkoutSetInput) -> Result<()> {
    let variant_ok = match set.exercise_id.as_str() {
        "squat" => matches!(set.variant_id.as_str(), "bodyweight" | "barbell"),
        "bench_press" | "deadlift" => set.variant_id == "barbell",
        "push_up" => set.variant_id == "bodyweight",
        _ => false,
    };
    if !variant_ok {
        bail!("unsupported exercise and variant pair");
    }
    if !(0..=1000).contains(&set.reps) {
        bail!("reps must be between 0 and 1000");
    }
    let barbell = set.variant_id == "barbell";
    if barbell != set.load_kg.is_some() {
        bail!("load_kg is required for barbell sets only");
    }
    if let Some(load) = set.load_kg {
        if !load.is_finite() || load <= 0.0 || load > 2000.0 {
            bail!("load_kg must be finite and between 0 and 2000");
        }
    }
    if set.load_mode.is_some() && (!barbell || set.load_mode.as_deref() != Some("barbell_total")) {
        bail!("load_mode is only supported as barbell_total for barbell sets");
    }
    if let Some(rpe) = set.rpe {
        if !rpe.is_finite() || !(1.0..=10.0).contains(&rpe) {
            bail!("rpe must be finite and between 1 and 10");
        }
    }
    if !matches!(
        set.set_type.as_str(),
        "work" | "warmup" | "drop" | "failure"
    ) {
        bail!("unsupported set_type");
    }
    Ok(())
}

pub(crate) fn import_fingerprint(
    owner_id: &str,
    provider: &str,
    request: &ManualWorkout,
) -> Result<String> {
    let source = if provider == "strong" {
        "strong"
    } else if provider == "hevy" {
        "hevy"
    } else {
        bail!("unsupported provider")
    };
    let write = WorkoutWrite {
        owner_id: owner_id.to_owned(),
        request_key: format!("{source}:{}", request.request_key),
        workflow_run_id: None,
        performed_at: request.performed_at.clone(),
        timezone: request.timezone.clone(),
        sets: request.sets.clone(),
        source,
    };
    fingerprint(&write)
}

pub(crate) fn fingerprint(request: &WorkoutWrite) -> Result<String> {
    let at = DateTime::parse_from_rfc3339(&request.performed_at)?.with_timezone(&Utc);
    fingerprint_at(request, at)
}

pub(crate) fn fingerprint_at(request: &WorkoutWrite, at: DateTime<Utc>) -> Result<String> {
    let zone: Tz = request
        .timezone
        .parse()
        .context("invalid workout timezone")?;
    let mut sets = serde_json::to_value(&request.sets)?;
    if let Some(items) = sets.as_array_mut() {
        for item in items {
            if let Some(fields) = item.as_object_mut() {
                if fields.get("set_type").and_then(Value::as_str) == Some("work") {
                    fields.remove("set_type");
                }
                if fields.get("load_mode").is_some_and(Value::is_null) {
                    fields.remove("load_mode");
                }
                fields.remove("plan_id");
                fields.remove("plan_revision");
                fields.remove("target_position");
            }
        }
    }
    fingerprint_value(&json!({
        "performed_at":at.with_timezone(&zone).to_rfc3339(),
        "timezone":request.timezone,
        "sets":sets
    }))
}

fn fingerprint_value(value: &Value) -> Result<String> {
    let canonical = serde_json::to_vec(value)?;
    let mut digest = Sha256::new();
    digest.update(canonical);
    Ok(format!("{:x}", digest.finalize()))
}

fn plan_definition_json(
    title: &str,
    day: &str,
    timezone: &str,
    targets: &[PlanTargetInput],
) -> Result<Value> {
    let mut target_values = serde_json::to_value(targets)?;
    if let Some(items) = target_values.as_array_mut() {
        for item in items {
            if let Some(fields) = item.as_object_mut() {
                if fields.get("target_load_kg").is_some_and(Value::is_null) {
                    fields.remove("target_load_kg");
                }
            }
        }
    }
    Ok(json!({"title":title,"day":day,"timezone":timezone,"targets":target_values}))
}

fn validate_plan(input: &TrainingPlanInput) -> Result<()> {
    if input.request_key.len() != 32 || !input.request_key.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("request_key must be a 32-character hexadecimal id");
    }
    validate_plan_definition(&input.title, &input.day, &input.timezone, &input.targets)
}

fn validate_plan_definition(
    title: &str,
    day: &str,
    timezone: &str,
    targets: &[PlanTargetInput],
) -> Result<()> {
    if title.trim().is_empty() || title.trim().len() > 120 {
        bail!("title must contain 1 to 120 characters");
    }
    let day: NaiveDate = day.parse().context("day must use YYYY-MM-DD")?;
    if !(1970..9999).contains(&day.year()) {
        bail!("unsupported plan date");
    }
    let _: Tz = timezone.parse().context("invalid IANA plan timezone")?;
    if targets.is_empty() || targets.len() > 30 {
        bail!("plan must contain between 1 and 30 targets");
    }
    for target in targets {
        let variant_ok = match target.exercise_id.as_str() {
            "squat" => matches!(target.variant_id.as_str(), "bodyweight" | "barbell"),
            "bench_press" | "deadlift" => target.variant_id == "barbell",
            "push_up" => target.variant_id == "bodyweight",
            _ => false,
        };
        if !variant_ok {
            bail!("unsupported exercise and variant pair in training plan");
        }
        if target.target_reps == 0
            || target.target_reps > 1000
            || !(0..=1800).contains(&target.rest_seconds)
        {
            bail!("target reps must be positive and rest_seconds must be between 0 and 1800");
        }
        if target.target_load_kg.is_some() && target.variant_id != "barbell" {
            bail!("target load is only supported for barbell sets");
        }
        if target
            .target_load_kg
            .is_some_and(|load| !load.is_finite() || load <= 0.0 || load > 2000.0)
        {
            bail!("target load must be finite and between 0 and 2000 kg");
        }
    }
    Ok(())
}

fn validate_profile(profile: &ProfileSettings) -> Result<()> {
    if profile.goal.trim().is_empty() || profile.goal.trim().len() > 120 {
        bail!("profile goal must contain 1 to 120 characters");
    }
    if !matches!(
        profile.experience.as_str(),
        "beginner" | "intermediate" | "advanced"
    ) {
        bail!("invalid training experience");
    }
    if profile.equipment.len() > 2
        || profile
            .equipment
            .iter()
            .any(|e| !matches!(e.as_str(), "bodyweight" | "barbell"))
    {
        bail!("invalid training equipment");
    }
    if !(1..=7).contains(&profile.days_per_week) {
        bail!("days_per_week must be between 1 and 7");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> WorkoutWrite {
        WorkoutWrite {
            owner_id: "owner-1".into(),
            request_key: "workflow:run-1".into(),
            workflow_run_id: Some("run-1".into()),
            performed_at: "2026-11-27T19:00:00+09:00".into(),
            timezone: "Asia/Seoul".into(),
            source: "workflow",
            sets: vec![WorkoutSetInput {
                exercise_id: "bench_press".into(),
                variant_id: "barbell".into(),
                reps: 10,
                load_kg: Some(40.0),
                load_mode: Some("barbell_total".into()),
                rpe: None,
                set_type: "work".into(),
                plan_id: None,
                plan_revision: None,
                target_position: None,
            }],
        }
    }

    #[test]
    fn accepts_a_confirmed_workout_with_matching_local_offset() {
        let (at, zone) = validate_workout(&request()).unwrap();
        assert_eq!(at.to_rfc3339(), "2026-11-27T10:00:00+00:00");
        assert_eq!(zone, "Asia/Seoul");
    }

    #[test]
    fn rejects_workflow_retries_with_a_nonexistent_or_ambiguous_timestamp() {
        let mut body = request();
        body.performed_at = "2026-11-27T19:00:00Z".into();
        assert!(validate_workout(&body).is_err());
        body.performed_at = "2026-11-27T19:00:00+09:00".into();
        body.timezone = "Not/AZone".into();
        assert!(validate_workout(&body).is_err());
    }

    #[test]
    fn exercise_catalog_enforces_load_semantics() {
        let mut set = request().sets.remove(0);
        assert!(validate_set(&set).is_ok());
        set.load_kg = None;
        assert!(validate_set(&set).is_err());
        set.exercise_id = "push_up".into();
        set.variant_id = "bodyweight".into();
        set.load_mode = None;
        assert!(validate_set(&set).is_ok());
    }

    #[test]
    fn workflow_run_identity_is_not_the_approval_response_store() {
        let request = request();
        assert_eq!(request.workflow_run_id.as_deref(), Some("run-1"));
        assert_eq!(request.sets.len(), 1);
        assert_eq!(fingerprint(&request).unwrap().len(), 64);
    }

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL database in FITNESS_TEST_DATABASE_URL"]
    async fn exact_set_and_plan_revision_retries_return_the_committed_result() -> Result<()> {
        let db_url = std::env::var("FITNESS_TEST_DATABASE_URL")?;
        let store = FitnessStore::connect(&db_url).await?;
        let owner = format!("replay-test-{}", Uuid::new_v4().simple());
        let request_key = Uuid::new_v4().simple().to_string();
        let workout = ManualWorkout {
            request_key,
            performed_at: Utc::now()
                .with_timezone(&chrono_tz::Asia::Seoul)
                .to_rfc3339(),
            timezone: "Asia/Seoul".into(),
            sets: vec![WorkoutSetInput {
                exercise_id: "bench_press".into(),
                variant_id: "barbell".into(),
                reps: 10,
                load_kg: Some(40.0),
                load_mode: Some("barbell_total".into()),
                rpe: Some(7.0),
                set_type: "work".into(),
                plan_id: None,
                plan_revision: None,
                target_position: None,
            }],
        };
        let receipt = store.record_manual_workout(&owner, &workout).await?;
        let db = store.pool.get().await?;
        let set_id: String = db
            .query_one(
                "SELECT id FROM fitness.workout_sets WHERE workout_id=$1",
                &[&receipt.workout_id],
            )
            .await?
            .get(0);
        let replacement = WorkoutSetInput {
            reps: 9,
            ..workout.sets[0].clone()
        };
        let set_result = store
            .change_workout_set(&owner, &set_id, 1, Some(&replacement))
            .await?;
        assert_eq!(set_result["revision"], 2);
        assert_eq!(
            store
                .change_workout_set(&owner, &set_id, 1, Some(&replacement))
                .await?,
            set_result
        );

        let plan_request_key = Uuid::new_v4().simple().to_string();
        let plan = store
            .create_training_plan(
                &owner,
                &TrainingPlanInput {
                    request_key: plan_request_key,
                    title: "Replay plan".into(),
                    day: "2026-11-28".into(),
                    timezone: "Asia/Seoul".into(),
                    targets: vec![PlanTargetInput {
                        exercise_id: "bench_press".into(),
                        variant_id: "barbell".into(),
                        target_reps: 10,
                        rest_seconds: 60,
                        target_load_kg: Some(40.0),
                    }],
                },
            )
            .await?;
        let plan_id = plan["id"].as_str().unwrap();
        let draft = TrainingPlanDraft {
            title: "Replay plan v2".into(),
            day: "2026-11-28".into(),
            timezone: "Asia/Seoul".into(),
            targets: vec![PlanTargetInput {
                exercise_id: "bench_press".into(),
                variant_id: "barbell".into(),
                target_reps: 8,
                rest_seconds: 75,
                target_load_kg: Some(42.5),
            }],
        };
        let plan_result = store
            .revise_training_plan(&owner, plan_id, 1, &draft)
            .await?;
        assert_eq!(plan_result["revision"], 2);
        assert_eq!(
            store
                .revise_training_plan(&owner, plan_id, 1, &draft)
                .await?,
            plan_result
        );
        Ok(())
    }
}
