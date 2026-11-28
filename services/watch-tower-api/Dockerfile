FROM golang:1.25-bookworm AS fhir-validator
RUN GOBIN=/out go install github.com/gofhir/validator/v2/cmd/gofhir-validator@v2.2.0

FROM rust:1.99.1-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --locked --release --manifest-path services/watch-tower-api/Cargo.toml

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10002 watchtower \
    && useradd --uid 10002 --gid 10002 --no-create-home --shell /usr/sbin/nologin watchtower
COPY --from=build /src/services/watch-tower-api/target/release/watch-tower-api /usr/local/bin/watch-tower-api
COPY --from=fhir-validator /out/gofhir-validator /usr/local/bin/gofhir-validator
ENV HOME=/tmp FHIR_PACKAGE_PATH=/tmp/fhir-packages
USER watchtower:watchtower
EXPOSE 17842
ENTRYPOINT ["/usr/local/bin/watch-tower-api"]
