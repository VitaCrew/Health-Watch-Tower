# Watch Tower common contracts

This small Rust crate defines the four cross-domain contracts used by the
integrated Watch Tower runtime:

- `plans`: references link a plan revision to an adapter-owned actual record;
- `records`: source, actor, timestamp, revision, and idempotency provenance;
- `followups`: a deduplicated task link to an OpenHuman Workflow execution;
- `policy`: external identity, revocable consent, and payload-free audit data.

These contracts do not own food, medication, fitness, or wearable payloads.
The owning adapters keep their schemas and validation rules. OpenHuman remains
the owner of Workflow responses, deadlines, and no-response outcomes; HSM
remains the owner of personal state transitions.
