#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/home/ec2-user/crm_mvp_release}"
BRANCH="${BRANCH:-main}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"

cd "$APP_DIR"

git fetch origin "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

docker compose -f "$COMPOSE_FILE" up -d db
docker compose -f "$COMPOSE_FILE" build web worker
docker compose -f "$COMPOSE_FILE" run --rm web python manage.py migrate
docker compose -f "$COMPOSE_FILE" up -d --remove-orphans web worker adminer
docker compose -f "$COMPOSE_FILE" ps
