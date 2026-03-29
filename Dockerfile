# Stage 1: Builder
FROM rust:latest AS builder

# Install protobuf compiler for tonic-build
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/roda-ledger

# Copy all files
COPY . .

# Build the project with grpc feature in release mode
RUN cargo build --release --features grpc

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /usr/src/roda-ledger/target/release/roda-ledger /app/roda-ledger

# Expose the gRPC port
EXPOSE 50051

# Set default environment variables
ENV RODA_GRPC_ADDR=0.0.0.0:50051
ENV RODA_DATA_DIR=/data
ENV RODA_MAX_ACCOUNTS=1000000
ENV RODA_SNAPSHOT_INTERVAL=600
ENV RODA_IN_MEMORY=false

# Create data directory and set volume
RUN mkdir -p /data
VOLUME /data

# Run the binary
ENTRYPOINT ["/app/roda-ledger"]
