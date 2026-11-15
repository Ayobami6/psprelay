#!/usr/bin/env bash

set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${APP_DIR}"

echo "Starting PSP Relay production deployment in ${APP_DIR}..."

# Ensure untracked production configuration files exist before proceeding.
if [[ ! -f ".env" ]]; then
    echo "ERROR: Missing .env file. Copy .env.example to .env and configure secrets first." >&2
    exit 1
fi

if [[ ! -f "config.prod.yaml" ]]; then
    echo "ERROR: Missing config.prod.yaml file. Create config.prod.yaml with product routing rules first." >&2
    exit 1
fi

echo "Pulling latest changes from origin/main..."
git pull --ff-only origin main

echo "Building and starting postgres and psprelay containers (excluding local kafka profile)..."
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build

# Parse APP_PORT safely without sourcing arbitrary secret values into shell variables.
APP_PORT="$(grep -E '^APP_PORT=' .env | tail -n 1 | cut -d= -f2- | tr -d '\r"'\''' || true)"
APP_PORT="${APP_PORT:-8083}"
HEALTH_URL="http://127.0.0.1:${APP_PORT}/v1/healthz"

echo "Waiting for health check at ${HEALTH_URL}..."
for attempt in $(seq 1 30); do
    if curl -fsS "${HEALTH_URL}" >/dev/null 2>&1; then
        echo "PSP Relay is healthy at ${HEALTH_URL}."
        docker compose -f docker-compose.yml -f docker-compose.prod.yml ps
        exit 0
    fi
    sleep 1
done

echo "ERROR: PSP Relay failed health check within 30s." >&2
docker compose -f docker-compose.yml -f docker-compose.prod.yml logs --tail 50 psprelay >&2 || true
exit 1
