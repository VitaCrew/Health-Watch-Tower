# OpenFGA model for Watch Tower

Import `model.fga` into OpenFGA and set its store and model IDs in
`OPENFGA_STORE_ID` and `OPENFGA_AUTHORIZATION_MODEL_ID`.

The resource object key is `person:<subject_id>`. The user object key used by
the Rust gateway is `user:oidc_<base64url-sha256(issuer + NUL + sub)>`. This
keeps provider and subject namespaces distinct. Add tuples such as `owner`
for the person or `caregiver`, `trainer`, or `nutritionist` for assigned staff.
The gateway checks `can_read` or `can_write` for each MCP tool call; the
person/client consent grant is a separate, mandatory check.

The first model is person-wide. More restrictive role-by-data-class relations
can be added without moving source data or response state out of its owning
module.

Use the Watch Tower operator CLI's `grant-role` and `revoke-role` commands to
write the supported `owner`, `caregiver`, `clinician`, `trainer`, and
`nutritionist` tuples. The CLI resolves the external identity through an active
Watch Tower identity link, derives the same opaque OpenFGA user key used by the
gateway, and audits the relationship change in `watch_tower.mcp_audit`. A role
tuple does not grant consent; each MCP request still needs the matching
per-client consent scope.
