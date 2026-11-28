# Avatar Fitness (Rust)

This service is the Rust Fitness module in the integrated Health Watch Tower
runtime. It owns workout plans, strength workout records, and performance
records in PostgreSQL schema `fitness`. It does not own schedules, confirmation
responses, personal health state, or posture/video analysis.

## Boundaries

- OpenHuman Cron starts the workout Workflow. The existing Workflow approval
  step is the confirmation record; rejection and timeout stay in that run.
- After approval, the Workflow calls `record_confirmed_workout`. Its
  `workflow_run_id` is the idempotency key, so retries cannot create duplicate
  workouts and there is no Fitness confirmation table.
- Fitness returns `workout_id`; the Workflow passes it to the OpenHuman HSM
  controller. HSM derives a stable event ID and updates only personal state.
- `get_workout_day`, training progress/context, and `preview_workout_csv` are
  exposed on the authenticated read surface. Workout, plan, profile, and
  `import_workout_csv` writes use the authenticated write surface.
- Set `FITNESS_DATABASE_URL` to the Watch Tower PostgreSQL URL. If omitted,
  the service uses `AVATAR_HSM_DATABASE_URL` so both service schemas can share
  the same PostgreSQL database. The services still access only their own
  schemas through their own stores.

## Shared MCP host

The standalone `watch-tower-api` process hosts the combined MCP at
`http://127.0.0.1:17842/mcp` and delegates Fitness tools to this crate's
`FitnessStore`; HSM tools call the HSM-owned store API. Fitness remains the
owner of its PostgreSQL schema. The HSM's `/mcp/read` and `/mcp/control` paths
are private adapter routes. Gemini and ChatGPT still need a remotely reachable
deployment plus OAuth and person-level permission checks.

The current bearer pair is still service-level authentication. The new
`services/watch-tower-core` crate defines four domain-neutral contracts, and
the combined endpoint writes payload-free calls to shared audit storage.
Client-specific OAuth identities, relationship/consent enforcement, and
generic event-to-role Workflow dispatch remain unimplemented.
The standalone Fitness listener below is retained for adapter development;
the shared runtime uses the HSM-hosted MCP.

## Run the Fitness adapter locally

Provide `FITNESS_OWNER_ID`, separate `FITNESS_MCP_READ_TOKEN` and
`FITNESS_MCP_WRITE_TOKEN` values (each at least 32 characters), and the shared
database URL. Then run:

```powershell
$env:RUSTUP_TOOLCHAIN = '1.99.1-x86_64-pc-windows-msvc'
cargo run --manifest-path services/fitness/Cargo.toml
```

The service binds to loopback on port `17841` by default. Register its read and
write MCP endpoints in OpenHuman with their matching credentials, then set the
Workflow's `fitness_mcp_server` input to that registered server ID.

## Migration status

Implemented in Rust: idempotent Workflow-confirmed and manual workout writes,
revision-checked set replacement and deletion, local-day workout paging,
training progress, plan creation/revision/deletion, profile revision writes,
plan target execution links, and Strong/Hevy CSV preview/import with source
idempotency and cross-source duplicate detection. Historical Python records
are out of scope for the current migration goal. Strong/Hevy exports can be
imported later with an explicit Fitness owner ID; the GenUI database is not
connected to Fitness. The existing Python app remains a separate regression
baseline.

## Integrated runtime verification

On 2026-11-26, a local OpenHuman host, Rust Fitness, and HSM ran against an
isolated PostgreSQL 17 database. An OpenHuman Cron job started a saved Workflow;
the existing approval node captured confirmation, Fitness persisted three
sets with the Workflow run ID as its idempotency key, and the Workflow passed
the returned workout ID to the HSM controller. The run completed with all
nodes successful. The Fitness row and HSM diary event shared the same workout
identity (`fitness-<workout_id>`); the isolated HSM fixture subject was
`synthetic-mvp`, so this verifies service integration without using a real
person's health record. The test-only Cron was disabled and its services and
database were removed after verification.

No historical data migration was performed, per the migration goal's scope.
