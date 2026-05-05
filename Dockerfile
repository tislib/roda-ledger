# Stage 1: Builder
FROM rust:latest AS builder

# Install protobuf compiler for tonic-build
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/roda-ledger

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY config.toml ./config.toml

# Build the project with cluster feature in release mode
RUN cargo build --release --package cluster --bin roda-server

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary and the default config from the builder stage
COPY --from=builder /usr/src/roda-ledger/target/release/roda-server /app/roda-server
COPY --from=builder /usr/src/roda-ledger/config.toml /app/config.toml

# Expose the gRPC port
EXPOSE 50051

# Config file path (override by bind-mounting /app/config.toml or setting RODA_CONFIG)
ENV RODA_CONFIG=/app/config.toml

# Create data directory and set volume
RUN mkdir -p /app/data
VOLUME /app/data

# Run the binary against the configured config.toml
ENTRYPOINT ["/app/roda-server"]
