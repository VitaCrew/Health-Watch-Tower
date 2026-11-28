# Health Watch Tower runtime

The gateway, personal health state (HSM), Fitness, and their shared contracts live under `services/`. The gateway owns the ETL worker lifecycle and exposes the unified `/mcp` endpoint. Each service applies its own PostgreSQL migrations.

## Local startup

Copy `.env.example` to `.env`. Set a URL-safe database password, distinct bearer tokens of at least 32 characters, and the mapped Fitness owner ID.

```sh
docker compose up --build --wait
curl --fail http://127.0.0.1:17842/healthz
docker compose down
```

The port is published on loopback. Authenticated personal-record operations require the corresponding identity link, consent, and OpenFGA authorization. Configure OIDC and OpenFGA before connecting external clients. Leave `WATCH_TOWER_REQUIRE_OPENHUMAN_IDENTITY=1` for normal deployments.

```sh
cargo test --locked --manifest-path services/watch-tower-api/Cargo.toml
cargo test --locked --manifest-path services/avatar-hsm/Cargo.toml
cargo test --locked --manifest-path services/fitness/Cargo.toml
cargo test --locked --manifest-path services/watch-tower-core/Cargo.toml
```

Vita connects over HTTP/MCP without a Watch Tower database connection. For integration configuration, see [vita-monorepo](https://github.com/VitaCrew/vita-monorepo).

The runtime snapshot originates from [Avatar Agent at the migration baseline](https://github.com/munsunouk/avatar-agent/tree/7a7ff53554766cdbf4e8ef7202f049b76cc744b6/openhuman-runtime/services). The runtime's source distribution includes its inherited GPLv3 license file.
