#!/bin/bash

WAIT_LEVEL=${1:-"processed"}
BATCH_SIZE=${2:-1}

if [ "$BATCH_SIZE" -gt 1 ]; then
    case "$WAIT_LEVEL" in
        "processed") WL="WAIT_LEVEL_PROCESSED" ;;
        "committed") WL="WAIT_LEVEL_COMMITTED" ;;
        *) echo "Usage: $0 [processed|committed] [batch_size]"; exit 1 ;;
    esac

    # Generate a JSON payload with BATCH_SIZE operations
    # Each operation in SubmitBatchAndWaitRequest is a SubmitAndWaitRequest
    OP='{"deposit":{"account":1,"amount":100}}'
    if [ "$BATCH_SIZE" -gt 1 ]; then
        OPS=$(printf "$OP,%.0s" $(seq 1 $((BATCH_SIZE-1))))
        OPS="$OPS$OP"
    else
        OPS="$OP"
    fi
    DATA="{\"operations\": [$OPS], \"waitLevel\": \"$WL\"}"

    ghz --insecure \
        --proto proto/ledger.proto \
        --call roda.ledger.v1.Ledger/SubmitBatchAndWait \
        --concurrency 100 \
        --total $((10000000 / BATCH_SIZE)) \
        --data "$DATA" \
        localhost:50051
elif [ "$WAIT_LEVEL" = "processed" ]; then
    ghz --insecure \
        --proto proto/ledger.proto \
        --call roda.ledger.v1.Ledger/SubmitAndWait \
        --concurrency 100 \
        --total 1000000 \
        --data '{"deposit":{"account":1,"amount":100},"waitLevel":"WAIT_LEVEL_PROCESSED"}' \
        localhost:50051
elif [ "$WAIT_LEVEL" = "committed" ]; then
    ghz --insecure \
        --proto proto/ledger.proto \
        --call roda.ledger.v1.Ledger/SubmitAndWait \
        --concurrency 100 \
        --total 1000000 \
        --data '{"deposit":{"account":1,"amount":100},"waitLevel":"WAIT_LEVEL_COMMITTED"}' \
        localhost:50051
else
    echo "Usage: $0 [processed|committed] [batch_size]"
    exit 1
fi