#!/usr/bin/env bash
# Runs ON a fresh Ubuntu server (streamed in via util/run-remote.sh). Installs Docker
# and brings up the control+ui demo stack.
# Required env: REPO, BRANCH, SERVER_IP, CONTROL_IMAGE, UI_IMAGE.
set -euo pipefail

apt-get update -qq
curl -fsSL https://get.docker.com | sh

mkdir -p /root/roda-demo && cd /root/roda-demo

# Fetch the same compose file end-users would download themselves.
curl -fsSL "https://raw.githubusercontent.com/$REPO/$BRANCH/docker-compose.control.yml" \
  -o docker-compose.yml

# Swap :latest for the demo-tagged images we just pushed.
# Anchor on `image:` so the comments referencing :latest stay intact.
sed -i "s|image: tislib/roda-ledger:control-latest|image: $CONTROL_IMAGE|" docker-compose.yml
sed -i "s|image: tislib/roda-ledger:ui-latest|image: $UI_IMAGE|" docker-compose.yml

# Override CONTROL_PLANE_URL so the UI calls the public IP, not localhost.
cat > docker-compose.override.yml <<OVERRIDE
services:
  ui:
    environment:
      CONTROL_PLANE_URL: "http://$SERVER_IP:50051"
OVERRIDE

docker compose pull
docker compose up -d
docker compose ps
