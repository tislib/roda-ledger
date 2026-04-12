# roda-ledger

**A financial ledger engine that treats correctness guarantees as a choice, not a constraint — and performance as a consequence of good design, not a tradeoff.**

roda-ledger is built around a single idea: the ledger should adapt to you. Choose your consistency level per call. Define your own transaction logic via composite operations, with WASM custom logic on the roadmap. Get 2.49 million transactions per second out of the box.

---

## Why roda-ledger?

Most ledger systems make you pick two out of three:

- **Fast** — but fixed operations, no programmability (TigerBeetle)
- **Programmable** — but slow, closed-source, expensive (Thought Machine Vault)
- **Correct** — but not a ledger, no financial primitives (general-purpose databases)

roda-ledger pursues all three. It does this through a staged pipeline where each stage runs on a dedicated thread, adds a specific guarantee, and exposes its progress as an observable index.

---

## Performance

Benchmarks on bare metal, WAL persistence enabled:

> Load test (50s, 1M accounts): **2.49M tx/s** avg · P50 70ns · P99 170ns · P999 12.4µs — [full report](docs/load.md)

---

## Quick start

```bash
docker run -p 50051:50051 -v $(pwd)/data:/app/data tislib/roda-ledger:latest
```

Deposit to an account:

```bash
grpcurl -plaintext -d '{"deposit": {"account": 1, "amount": "1000", "user_ref": "1"}}' \
  localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```

Check the balance:

```bash
grpcurl -plaintext -d '{"account_id": 1}' \
  localhost:50051 roda.ledger.v1.Ledger/GetBalance
```

Stop the container, restart it — the balance is still there. That is the durability guarantee.

---

## How it works

roda-ledger is a four-stage pipeline. Each stage runs on its own thread, communicates through lock-free queues, and advances a monotonic index as it processes transactions:

```
submit() → [Sequencer] → [Transactor] → [WAL] → [Snapshotter] → get_balance()
               ↓               ↓            ↓            ↓
           PENDING         COMPUTED     COMMITTED    ON_SNAPSHOT
```

The caller chooses which stage to wait for:

```bash
# Fire and forget — maximum throughput
SubmitOperation(deposit)

# Wait for execution result — know immediately if it failed
SubmitAndWait(deposit, WAIT_LEVEL_PROCESSED)

# Wait for durability — transaction survives a crash
SubmitAndWait(deposit, WAIT_LEVEL_COMMITTED)

# Wait for readable balance — linearizable reads
SubmitAndWait(deposit, WAIT_LEVEL_SNAPSHOT)
```

The three pipeline indexes — `compute_index`, `commit_index`, `snapshot_index` — are observable via `GetPipelineIndex`. This is the pattern at the heart of roda-ledger: **Observable Staged Commitment**. The gaps between indexes tell you exactly where your system is under load.

---

## Documentation

Full documentation: [tislib.net/roda-ledger](https://tislib.net/roda-ledger/)

## Operations

Four built-in operation types cover common financial workflows:

| Operation | Description |
|---|---|
| `Deposit` | Credit an account from the system source |
| `Withdrawal` | Debit an account to the system sink |
| `Transfer` | Move funds between accounts atomically |
| `Composite` | Arbitrary sequence of credits and debits — your logic, ledger guarantees |

`Composite` is the escape hatch. Multi-leg settlements, fee deductions, split payments — anything that doesn't fit the named types. The zero-sum invariant is always enforced. WASM-sandboxed custom operations are on the roadmap.

---

## Guarantees

- **Zero-sum invariant** — every transaction nets to zero. Money is never created or destroyed.
- **No torn writes** — each balance update is a single atomic store.
- **Crash safety** — committed transactions survive crashes and power loss via WAL + snapshot recovery.
- **Serializability** — by default, from the single-writer Transactor.
- **Linearizable reads** — opt-in, via `submit_wait(snapshot)`.

---

## Tradeoffs

roda-ledger is a specialized engine. It is deliberately not general-purpose.

- **Single node** — no replication today. Raft-based multi-node is planned.
- **Fixed account space** — `max_accounts` is pre-allocated at startup. O(1) balance access always, memory committed upfront.
- **No query language** — balance lookup by account ID only. No ad-hoc reads.
- **No auth** — no authentication or authorization today. mTLS is planned. roda-ledger trusts its caller; access control belongs in the layer above it.

---

## Documentation

| Level | Document | What it covers |
|---|---|---|
| 1 | [Concepts](docs/01-concepts.md) | Mental model, guarantees, invariants |
| 2 | [API](docs/02-api.md) | Operations, wait levels, gRPC reference |
| 3 | [Architecture](docs/03-architecture.md) | Pipeline design, stage internals, recovery |
| 4 | [Internals](docs/04-internals.md) | Data structures, WAL format, lock mechanics |

---

## Roadmap

- Raft-based multi-node replication
- WASM-sandboxed custom transaction logic
- mTLS authentication
- Account hierarchies and sub-account structures

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
