# gRPC API Examples

This document provides concrete examples of how to interact with the Roda-Ledger gRPC API using `grpcurl`.

## Prerequisites

- [grpcurl](https://github.com/fullstorydev/grpcurl) installed.
- Roda-Ledger server running (e.g., via Docker on `localhost:50051`).

## Operations

### Submit a Deposit

Deposits credit an account from the system source.

```bash
grpcurl -plaintext -d '{
  "deposit": {
    "account": 1,
    "amount": "1000",
    "user_ref": "100"
  }
}' localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```

### Submit a Withdrawal

Withdrawals debit an account to the system sink. Fails if balance is insufficient.

```bash
grpcurl -plaintext -d '{
  "withdrawal": {
    "account": 1,
    "amount": "500",
    "user_ref": "101"
  }
}' localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```

### Submit a Transfer

Atomic movement of funds between two accounts.

```bash
grpcurl -plaintext -d '{
  "transfer": {
    "from": 1,
    "to": 2,
    "amount": "250",
    "user_ref": "102"
  }
}' localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```

### Submit a Composite Operation

A multi-step atomic operation. The following example performs a transfer with a fee, ensuring the sender doesn't go negative.

```bash
grpcurl -plaintext -d '{
  "composite": {
    "steps": [
      { "credit": { "account_id": 1, "amount": "105" } },
      { "debit":  { "account_id": 2, "amount": "100" } },
      { "debit":  { "account_id": 0, "amount": "5" } }
    ],
    "flags": "1",
    "user_ref": "103"
  }
}' localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```
*Note: `flags: "1"` corresponds to `CHECK_NEGATIVE_BALANCE`.*

### Submit a Batch of Operations

```bash
grpcurl -plaintext -d '{
  "operations": [
    { "deposit": { "account": 3, "amount": "1000" } },
    { "deposit": { "account": 4, "amount": "1000" } }
  ]
}' localhost:50051 roda.ledger.v1.Ledger/SubmitBatch
```

## Queries

### Get Account Balance

```bash
grpcurl -plaintext -d '{"account_id": 1}' localhost:50051 roda.ledger.v1.Ledger/GetBalance
```

### Get Multiple Balances

```bash
grpcurl -plaintext -d '{"account_ids": [1, 2, 3]}' localhost:50051 roda.ledger.v1.Ledger/GetBalances
```

### Get Transaction Status

Check the current stage of a transaction (PENDING, COMPUTED, COMMITTED, ON_SNAPSHOT, or ERROR).

```bash
grpcurl -plaintext -d '{"transaction_id": "1"}' localhost:50051 roda.ledger.v1.Ledger/GetTransactionStatus
```

### Get Pipeline Indexes

Monitor the progress of the entire pipeline.

```bash
grpcurl -plaintext -d '{}' localhost:50051 roda.ledger.v1.Ledger/GetPipelineIndex
```

## Transaction Statuses

- `PENDING` (0): Sequenced but not yet processed.
- `COMPUTED` (1): Logic executed, balance updated in memory.
- `COMMITTED` (2): Flushed to WAL, durable.
- `ON_SNAPSHOT` (3): Applied to balance cache, queryable via `GetBalance`.
- `ERROR` (4): Rejected (check `fail_reason`).
