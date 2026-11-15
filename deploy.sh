#!/usr/bin/env bash

set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${APP_DIR}"

COMPOSE_CMD=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)

echo "Starting PSP Relay production deployment in ${APP_DIR}..."

# Ensure untracked .env exists before resolving Compose configuration.
if [[ ! -f ".env" ]]; then
    echo "ERROR: Missing .env file. Copy .env.example to .env and configure secrets first." >&2
    exit 1
fi

# Resolve the effective host config bind-mount source from Compose (honoring .env and shell overrides).
CONFIG_SOURCE="$("${COMPOSE_CMD[@]}" config | awk '
    /^[[:space:]]*source:[[:space:]]*/ {
        line = $0
        sub(/^[[:space:]]*source:[[:space:]]*/, "", line)
        gsub(/^["'\'']|["'\'']$/, "", line)
        src = line
    }
    /^[[:space:]]*target:[[:space:]]*\/app\/config\.yaml([[:space:]]|$)/ {
        print src
        exit
    }
')"

if [[ -z "${CONFIG_SOURCE}" || ! -f "${CONFIG_SOURCE}" ]]; then
    echo "ERROR: Configured routing file does not exist: ${CONFIG_SOURCE:-<unresolved>}" >&2
    echo "Set CONFIG_HOST_PATH in .env (e.g. ./config.prod.yaml) and create the file before deploying." >&2
    exit 1
fi

echo "Pulling latest changes from origin/main..."
git pull --ff-only origin main

echo "Building and starting postgres and psprelay containers (excluding local kafka profile)..."
"${COMPOSE_CMD[@]}" up -d --build

# Derive the published host endpoint directly from Compose so bound IPs and env overrides are respected.
PUBLISHED_ENDPOINT="$("${COMPOSE_CMD[@]}" port psprelay 8083 | head -n 1 || true)"
if [[ -z "${PUBLISHED_ENDPOINT}" ]]; then
    echo "ERROR: Could not determine published host endpoint for psprelay port 8083." >&2
    "${COMPOSE_CMD[@]}" logs --tail 50 psprelay >&2 || true
    exit 1
fi

PROBE_ENDPOINT="$(printf '%s\n' "${PUBLISHED_ENDPOINT}" | sed -E 's/^0\.0\.0\.0:/127.0.0.1:/; s/^\[::\]:/127.0.0.1:/')"
HEALTH_URL="http://${PROBE_ENDPOINT}/v1/healthz"

CONTAINER_ID="$("${COMPOSE_CMD[@]}" ps -q psprelay)"

echo "Waiting for health check at ${HEALTH_URL}..."
for attempt in $(seq 1 45); do
    HEALTH_STATUS="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${CONTAINER_ID}" 2>/dev/null || true)"
    if [[ "${HEALTH_STATUS}" == "healthy" ]] && curl -fsS "${HEALTH_URL}" >/dev/null 2>&1; then
        echo "PSP Relay is healthy at ${HEALTH_URL}."
        "${COMPOSE_CMD[@]}" ps
        exit 0
    fi
    sleep 1
done

echo "ERROR: PSP Relay failed health check within 45s (last container state: ${HEALTH_STATUS:-unknown})." >&2
"${COMPOSE_CMD[@]}" logs --tail 50 psprelay >&2 || true
exit 1
