#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/home/ec2-user/crm_mvp_release}"
BRANCH="${BRANCH:-main}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"

cd "$APP_DIR"

git fetch origin "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

docker compose -f "$COMPOSE_FILE" build web worker

# Remove the partially-created source-build database if an earlier deploy attempt
# started it; production continues to use the existing crm_db container.
docker rm -f crm_mvp_release-db-1 >/dev/null 2>&1 || true

# Replace the legacy image-based web container with the freshly built source image.
docker rm -f crm_web >/dev/null 2>&1 || true

docker compose -f "$COMPOSE_FILE" up -d --remove-orphans web worker adminer
docker compose -f "$COMPOSE_FILE" exec -T web python manage.py migrate
docker compose -f "$COMPOSE_FILE" ps
