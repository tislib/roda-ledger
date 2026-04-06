# Server Mode (Docker)

The fastest way to deploy Roda-Ledger is using the official Docker image.

## 1. Run the Server

```bash
docker run -p 50051:50051 -v $(pwd)/data:/data tislib/roda-ledger:latest
```

## 2. Interact via gRPC

Use `grpcurl` to submit operations.

### Deposit Funds

```bash
grpcurl -plaintext -d '{"deposit": {"account": 1, "amount": "1000", "user_ref": "123"}}' \
  localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```

### Get Balance

```bash
grpcurl -plaintext -d '{"account_id": 1}' \
  localhost:50051 roda.ledger.v1.Ledger/GetBalance
```

For a full list of gRPC operations and examples, see the [gRPC API Reference](../guides/grpc-api.md).
