#!/bin/sh
# Drop-in for nginx's /docker-entrypoint.d/. Runs before nginx starts
# (the official image's entrypoint executes everything in this dir
# alphabetically, then exec's nginx).
#
# Templates config.json from CONTROL_PLANE_URL so the browser knows
# which gRPC endpoint to hit. Default works when both containers
# publish their ports to the host.

set -e

: "${CONTROL_PLANE_URL:=http://localhost:50051}"

cat > /usr/share/nginx/html/config.json <<EOF
{"controlPlaneUrl":"${CONTROL_PLANE_URL}"}
EOF
