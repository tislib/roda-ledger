# roda-ledger

**A programmable financial ledger engine that treats correctness guarantees as a choice, not a constraint — and performance as a consequence of good design, not a tradeoff.**

roda-ledger is built around a single idea: the ledger should adapt to you. Choose your consistency level per call. Define your own transaction logic via composite operations, or upload sandboxed WebAssembly functions that execute atomically inside the ledger as first-class operations. Get 2.49 million transactions per second out of the box.

---

## Why roda-ledger?

Most ledger systems make you pick two out of three:

- **Fast** — but fixed operations, no programmability (TigerBeetle)
- **Programmable** — but slow, closed-source, expensive (Thought Machine Vault)
- **Correct** — but not a ledger, no financial primitives (general-purpose databases)

roda-ledger pursues all three. It does this through a staged pipeline where each stage runs on a dedicated thread, adds a specific guarantee, and exposes its progress as an observable index — and a sandboxed WebAssembly runtime that lets you extend the ledger with custom operations at runtime, without recompiling the engine.

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

## Operations

Five operation types cover both common financial workflows and arbitrary user-defined logic:

| Operation | Description |
|---|---|
| `Deposit` | Credit an account from the system source |
| `Withdrawal` | Debit an account to the system sink |
| `Transfer` | Move funds between accounts atomically |
| `Composite` | Arbitrary sequence of credits and debits — your logic, ledger guarantees |
| `Function` | Invoke a registered WebAssembly function as a first-class atomic transaction |

`Composite` is the escape hatch for one-off multi-leg flows. `Function` is the **programmable ledger** surface: upload a compiled WebAssembly module via `RegisterFunction`, then invoke it by name. The function runs inside the Transactor — single-threaded, deterministic, sandboxed — and produces normal WAL entries alongside every other transaction. The zero-sum invariant is always enforced; non-zero return codes trigger full rollback. See [WASM Runtime](docs/wasm-runtime.md) for the ABI, host API, and durability guarantees.

---

## Programmable ledger

A registered WASM function becomes a first-class `Operation::Function` — sequenced, executed atomically by the Transactor, durably logged, and recoverable across restarts. The runtime exposes a deliberately narrow host API (`credit`, `debit`, `get_balance`) so every execution is deterministic: no clocks, no I/O, no randomness, no threads. This is the property that makes the runtime fit for future Raft replication — followers apply the leader's WAL entries directly, with no re-execution.

```bash
# Register a compiled WASM module
grpcurl -d "$(jq -n --arg bin "$(base64 -w0 fee_transfer.wasm)" \
  '{name:"fee_transfer", binary:$bin, override_existing:false}')" \
  -plaintext localhost:50051 roda.ledger.v1.Ledger/RegisterFunction

# Invoke it like any other operation
grpcurl -plaintext -d '{"function": {"name": "fee_transfer", "params": [101, 202, 999, 10000, 50], "user_ref": 1}}' \
  localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```

Functions can be written in Rust, AssemblyScript, or hand-written WAT. Registration is durable before the call returns; the binary, a `FunctionRegistered` WAL record, and a paired function snapshot together let recovery rebuild the live registry deterministically.

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
Full documentation: [tislib.net/roda-ledger](https://tislib.net/roda-ledger/)

| Level | Document | What it covers |
|---|---|---|
| 1 | [Concepts](docs/01-concepts.md) | Mental model, guarantees, invariants |
| 2 | [API](docs/02-api.md) | Operations, wait levels, gRPC reference |
| 3 | [Architecture](docs/03-architecture.md) | Pipeline design, stage internals, recovery |
| 4 | [Internals](docs/04-internals.md) | Data structures, WAL format, lock mechanics |
| + | [WASM Runtime](docs/wasm-runtime.md) | Programmable ledger — ABI, registration, recovery |

---

## Roadmap

- Raft-based multi-node replication
- mTLS authentication
- Account hierarchies and sub-account structures
- Per-function CPU / memory metering and quotas

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
