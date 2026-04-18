# Roda-Ledger

Roda-Ledger is a high-performance, crash-safe financial ledger engine. It handles the hard parts of financial infrastructure — strict transaction ordering, atomicity, durability, and crash recovery — so you can focus on your business logic.

It is built for engineers who need a ledger they can trust. Not a general-purpose database stretched to fit financial use cases. Not a message queue masquerading as a transaction log. A purpose-built ledger engine, designed from the ground up for correctness and throughput.

It is also designed to get out of your way. Common operations — deposits, withdrawals, transfers — work out of the box. Custom multi-account logic is expressed through registered WASM functions without touching the engine. How much durability you need is your call, per submission: fire and forget for maximum throughput, or wait for disk confirmation when it matters. Run it as a Docker service in minutes, or embed it as a library. The ledger adapts to your use case, not the other way around.

**2.49 million transactions per second. Sub-microsecond latency. WAL-backed durability. Zero torn writes.**

---

## Quick Start

Get a ledger running in under a minute.

### 1. Start the server

```bash
docker run -p 50051:50051 -v $(pwd)/data:/app/data tislib/roda-ledger:latest
```

That's it. The ledger is running, persisting data to `./data`, and listening for gRPC connections on port `50051`.

### 2. Submit a deposit

```bash
grpcurl -plaintext -d '{"deposit": {"account": 1, "amount": "1000", "user_ref": "123"}}' \
  localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```

### 3. Check the balance

```bash
grpcurl -plaintext -d '{"account_id": 1}' \
  localhost:50051 roda.ledger.v1.Ledger/GetBalance
```

You should see a balance of `1000` for account `1`.

Stop the container and start it again — the balance is still there. That is the durability guarantee in action.

---

## Where to Go Next

Roda-Ledger's documentation is organized into four levels. Start at the top and go as deep as your use case requires.

| Level | Document | What it covers |
|---|---|---|
| 1 | [Concepts](./01-concepts.md) | Mental model, guarantees, invariants. Read this first. |
| 2 | [API](./02-api.md) | Operations, wait levels, gRPC reference. |
| 3 | [Architecture](./03-architecture.md) | Pipeline design, stage internals, recovery. |
| 4 | [Contributing](./05-contributing.md) | How to set up, run benchmarks, and extend the system. |

If you want to understand what roda-ledger guarantees and why, start with **[Concepts](./01-concepts.md)**.

If you want to integrate it into your system, start with **[API](./02-api.md)**.