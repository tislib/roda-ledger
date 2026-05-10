#!/usr/bin/env bash
# Supervisor for the demo container: nginx (UI) + control (gRPC).
# Propagates SIGTERM/SIGINT, exits with the first non-zero child status.

set -e

CONTROL_PLANE_URL="${CONTROL_PLANE_URL:-http://localhost:50051}"
INITIAL_NODE_COUNT="${INITIAL_NODE_COUNT:-3}"

cat > /app/ui/config.json <<EOF
{"controlPlaneUrl":"${CONTROL_PLANE_URL}"}
EOF

nginx -g 'daemon off;' &
NGINX_PID=$!

/app/bin/control --addr 0.0.0.0:50051 --initial-node-count "${INITIAL_NODE_COUNT}" &
CONTROL_PID=$!

shutdown() {
    kill -TERM "$NGINX_PID" "$CONTROL_PID" 2>/dev/null || true
}
trap shutdown TERM INT

wait -n
EXIT_CODE=$?
shutdown
wait || true
exit "$EXIT_CODE"
