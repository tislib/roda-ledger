# Build (from repo root):
#   docker build -t roda-ui -f ui/Dockerfile .
# Run:
#   docker run --rm -p 8080:8080 -e CONTROL_PLANE_URL=http://localhost:50051 roda-ui

FROM node:22-bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
# buf.gen.yaml reads `../crates/proto/proto` relative to ui/.
COPY crates/proto/proto ./crates/proto/proto
COPY ui ./ui

WORKDIR /src/ui
RUN npm install --no-audit --no-fund
RUN npm run build

FROM nginx:1.27-alpine AS runtime

# SPA fallback so client-side routes work.
RUN printf '%s\n' \
    'server {' \
    '  listen 8080 default_server;' \
    '  listen [::]:8080 default_server;' \
    '  root /usr/share/nginx/html;' \
    '  index index.html;' \
    '  location / { try_files $uri $uri/ /index.html; }' \
    '}' > /etc/nginx/conf.d/default.conf

COPY --from=builder /src/ui/dist /usr/share/nginx/html

# nginx's official image runs everything in /docker-entrypoint.d/
# before exec'ing nginx, so we hook config.json templating in there
# without overriding ENTRYPOINT/CMD.
COPY ui/docker-entrypoint.sh /docker-entrypoint.d/40-roda-config.sh
RUN chmod +x /docker-entrypoint.d/40-roda-config.sh

EXPOSE 8080
