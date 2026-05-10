# Build:  docker build -t roda-control -f control.Dockerfile .
# Run:    docker run --rm -p 50051:50051 roda-control

FROM rust:1.95-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    pkg-config \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY config.toml ./config.toml

RUN cargo build --release \
    --package cluster --bin roda-server \
    --package control --bin control

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Sibling layout: control's resolve_server_bin finds roda-server next
# to it without an env var.
COPY --from=builder /src/target/release/control     /app/bin/control
COPY --from=builder /src/target/release/roda-server /app/bin/roda-server

EXPOSE 50051

# Override at runtime: RODA_CONTROL_ADDR, INITIAL_NODE_COUNT.
ENTRYPOINT ["/usr/bin/tini", "--", "/app/bin/control"]
