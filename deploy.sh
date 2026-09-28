#!/usr/bin/env bash
# Runs on the EC2 server (called by Jenkins over SSH): pull the latest code, rebuild, health check.
set -euo pipefail
cd "$(dirname "$0")"

git pull --ff-only

# Secrets added by newer versions of the app are generated once and kept in .env.
if ! grep -q '^JWT_SECRET=' .env; then
  echo "JWT_SECRET=$(openssl rand -hex 32)" >> .env
fi
docker compose -f docker-compose.prod.yml up -d --build
docker image prune -f

for attempt in $(seq 1 30); do
  if curl -fs http://localhost/health; then
    echo " - deploy succeeded"
    exit 0
  fi
  sleep 2
done

echo "Health check failed after deploy"
docker compose -f docker-compose.prod.yml ps
exit 1
