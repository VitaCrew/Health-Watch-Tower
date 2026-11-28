//! Domain-neutral contracts shared by Watch Tower adapters and the MCP host.
//! Domain payloads and validation rules remain in their owning adapters.

pub mod followups {
    /// A cross-domain review intent. The referenced record stays in its
    /// adapter; review responses and deadlines stay in OpenHuman Workflow.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FollowupRequest {
        pub idempotency_key: String,
        pub subject_id: String,
        pub adapter_id: String,
        pub record_id: String,
        pub record_revision: i64,
        pub reason_code: String,
        pub role: String,
        pub urgency: String,
    }

    impl FollowupRequest {
        pub fn is_valid(&self) -> bool {
            (1..=160).contains(&self.idempotency_key.len())
                && !self.idempotency_key.trim().is_empty()
                && (1..=160).contains(&self.subject_id.len())
                && !self.subject_id.trim().is_empty()
                && (1..=64).contains(&self.adapter_id.len())
                && self.adapter_id.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'_' | b'-')
                })
                && (1..=160).contains(&self.record_id.len())
                && !self.record_id.trim().is_empty()
                && self.record_revision > 0
                && valid_code(&self.reason_code)
                && matches!(
                    self.role.as_str(),
                    "caregiver" | "clinician" | "trainer" | "nutritionist"
                )
                && matches!(self.urgency.as_str(), "routine" | "soon" | "urgent")
        }
    }

    fn valid_code(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    }

    /// A link from a cross-domain follow-up request to its OpenHuman Workflow.
    /// Response, timeout, and retry state stay in that Workflow execution.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct WorkflowExecutionRef {
        pub workflow_id: Option<String>,
        pub run_id: String,
        pub task_key: String,
        pub assignee_id: Option<String>,
    }

    impl WorkflowExecutionRef {
        pub fn is_valid(&self) -> bool {
            self.workflow_id
                .as_deref()
                .is_none_or(|id| !id.trim().is_empty())
                && !self.run_id.trim().is_empty()
                && !self.task_key.trim().is_empty()
        }
    }
}

pub mod plans {
    /// References an adapter-owned plan revision without copying its payload.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PlannedReference {
        pub subject_id: String,
        pub adapter_id: String,
        pub plan_id: String,
        pub revision: i64,
        pub planned_at_unix: Option<i64>,
    }

    /// References an adapter-owned actual record without copying its payload.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ActualReference {
        pub subject_id: String,
        pub adapter_id: String,
        pub record_id: String,
        pub revision: i64,
        pub occurred_at_unix: i64,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PlanActualLink {
        pub planned: PlannedReference,
        pub actual: ActualReference,
        pub target_key: Option<String>,
    }

    impl PlanActualLink {
        pub fn is_valid(&self) -> bool {
            !self.planned.subject_id.trim().is_empty()
                && self.planned.subject_id == self.actual.subject_id
                && !self.planned.adapter_id.trim().is_empty()
                && !self.actual.adapter_id.trim().is_empty()
                && !self.planned.plan_id.trim().is_empty()
                && !self.actual.record_id.trim().is_empty()
                && self.planned.revision > 0
                && self.actual.revision > 0
        }
    }

    /// Common projection for a planned target, whether it has an actual record
    /// yet or remains pending. Adapters emit references and normalized state;
    /// their plan and record payloads stay in their owner schemas.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PlanActualObservation {
        pub planned: PlannedReference,
        pub actual: Option<ActualReference>,
        pub target_key: Option<String>,
        pub state: String,
    }

    impl PlanActualObservation {
        pub fn is_valid(&self) -> bool {
            !self.planned.subject_id.trim().is_empty()
                && !self.planned.adapter_id.trim().is_empty()
                && !self.planned.plan_id.trim().is_empty()
                && self.planned.revision > 0
                && !self.state.trim().is_empty()
                && self.state.len() <= 64
                && self.actual.as_ref().is_none_or(|actual| {
                    !actual.subject_id.trim().is_empty()
                        && actual.subject_id == self.planned.subject_id
                        && !actual.adapter_id.trim().is_empty()
                        && !actual.record_id.trim().is_empty()
                        && actual.revision > 0
                })
        }
    }
}

pub mod policy {
    /// Stable identity asserted by a configured identity provider.
    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    pub struct ExternalIdentity {
        pub issuer: String,
        pub subject: String,
        pub client_id: String,
    }

    /// Watch Tower-owned mapping to person and adapter-owned account handles.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SubjectMapping {
        pub subject_id: String,
        pub fitness_owner_id: String,
    }

    /// A consent grant is evaluated at request time; revocation or expiry
    /// immediately makes it inactive for subsequent reads and writes.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ConsentGrant {
        pub grant_id: String,
        pub subject_id: String,
        pub client_id: String,
        pub scope: String,
        pub version: i64,
        pub expires_at_unix: Option<i64>,
        pub revoked_at_unix: Option<i64>,
    }

    impl ConsentGrant {
        pub fn is_active_at(&self, now_unix: i64) -> bool {
            self.version > 0
                && self.revoked_at_unix.is_none()
                && self
                    .expires_at_unix
                    .is_none_or(|expires| expires > now_unix)
        }
    }

    /// Minimal audit metadata. Never put health payloads, credentials, or tool
    /// arguments in this event.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct AuditEvent {
        pub actor_id: String,
        pub subject_id: Option<String>,
        pub operation: String,
        pub resource_id: String,
        pub allowed: bool,
        pub consent_version: Option<i64>,
    }
}

pub mod records {
    /// Provenance envelope for validation and replay. The adapter owns the
    /// record schema and decides which state transition is valid.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RecordEnvelope {
        pub subject_id: String,
        pub actor_id: String,
        pub adapter_id: String,
        pub source_id: String,
        pub record_id: String,
        pub revision: i64,
        /// ETL execution that produced this payload-free provenance receipt.
        pub run_id: Option<String>,
        pub observed_at_unix: i64,
        pub idempotency_key: String,
    }

    impl RecordEnvelope {
        pub fn is_valid(&self) -> bool {
            !self.subject_id.trim().is_empty()
                && !self.actor_id.trim().is_empty()
                && !self.adapter_id.trim().is_empty()
                && !self.source_id.trim().is_empty()
                && !self.record_id.trim().is_empty()
                && self.revision > 0
                && !self.idempotency_key.trim().is_empty()
                && self.run_id.as_ref().is_none_or(|id| {
                    id.len() == 36
                        && id.bytes().enumerate().all(|(index, byte)| {
                            if matches!(index, 8 | 13 | 18 | 23) {
                                byte == b'-'
                            } else {
                                byte.is_ascii_hexdigit()
                            }
                        })
                })
        }
    }
}
