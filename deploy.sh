#!/usr/bin/env bash
# Deploys one exact commit on the EC2 server and rolls back if it isn't healthy.
#
#   ./deploy.sh <commit>    Jenkins passes the commit it tested
#   ./deploy.sh             the latest commit on origin/main
#
# 1. Check out the commit (detached), so the compose files and migrations match the code.
# 2. Use the image Jenkins built for that commit from ECR (ledgerflow:<commit>), or build it here
#    if there's no registry.
# 3. Start it and wait until every container is healthy: the API answers /health and every
#    background process reports a heartbeat.
# 4. If that fails, deploy the previous commit again and exit with an error, so Jenkins fails.
#
# Migrations must stay backward compatible (add columns/tables; remove them only in a later
# release), so the previous version still works after a rollback.
set -euo pipefail
cd "$(dirname "$0")"

COMPOSE=(docker compose -f docker-compose.prod.yml)
if [[ -n "${DEPLOY_COMPOSE_OVERRIDE:-}" ]]; then COMPOSE+=(-f "$DEPLOY_COMPOSE_OVERRIDE"); fi
SERVICES=(api relay worker notifier webhooks reconciler caddy postgres redis kafka)
HEALTH_TIMEOUT=${DEPLOY_HEALTH_TIMEOUT:-300}  # seconds to wait for everything to be healthy
LAST_GOOD=.last-good-deploy  # commit of the last deploy that passed its health checks

ensure_setting() {  # adds KEY=value to .env once; existing values are kept
  grep -q "^$1=" .env || echo "$1=$2" >> .env
}

current_ip() {
  local token
  token=$(curl -fs --max-time 2 -X PUT http://169.254.169.254/latest/api/token \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60") &&
    curl -fs --max-time 2 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/public-ipv4
}

configure() {
  # Settings added by newer versions of the app, generated once.
  ensure_setting JWT_SECRET "$(openssl rand -hex 32)"
  if ! grep -q '^SITE_ADDRESS=' .env; then
    if ip=$(current_ip); then
      ensure_setting SITE_ADDRESS "${ip//./-}.sslip.io"  # free hostname for the IP, so HTTPS works
      ensure_setting COOKIE_SECURE true
    else
      ensure_setting SITE_ADDRESS ":80"
    fi
  fi
  set -a; source .env; set +a
}

prepare_image() {  # $1 = full commit hash
  export APP_IMAGE="ledgerflow:$1"
  if docker image inspect "$APP_IMAGE" >/dev/null 2>&1; then return; fi  # e.g. rolling back
  if [[ -n "${ECR_REPOSITORY:-}" ]]; then
    aws ecr get-login-password --region "$AWS_REGION" |
      docker login --username AWS --password-stdin "${ECR_REPOSITORY%%/*}" >/dev/null
    if docker pull --quiet "$ECR_REPOSITORY:$1"; then
      docker tag "$ECR_REPOSITORY:$1" "$APP_IMAGE"
      return
    fi
    echo "Image $ECR_REPOSITORY:$1 not found; building it here"
  fi
  docker build --quiet -t "$APP_IMAGE" . >/dev/null
}

healthy() {  # every long-running service is running and (if it has a check) healthy
  local status
  status=$("${COMPOSE[@]}" ps --format '{{.Service}} {{.State}} {{.Health}}' "${SERVICES[@]}")
  for service in "${SERVICES[@]}"; do
    grep -Eq "^$service running( healthy)? *$" <<<"$status" || return 1
  done
}

deploy() {  # $1 = commit
  git checkout --quiet --detach "$1"
  configure
  prepare_image "$(git rev-parse HEAD)"
  "${COMPOSE[@]}" up -d --remove-orphans --no-build
  for _ in $(seq 1 $((HEALTH_TIMEOUT / 5))); do
    if healthy; then return 0; fi
    sleep 5
  done
  "${COMPOSE[@]}" ps
  return 1
}

# Everything runs from main(), which bash reads completely before starting: the checkout below
# can replace this file while it runs.
main() {
  git fetch --quiet origin
  local target previous
  target=$(git rev-parse --verify "${1:-origin/main}^{commit}")
  # Roll back to the last deploy that was healthy, not just whatever is checked out: after a
  # failed deploy the checkout is the broken commit.
  previous=$(cat "$LAST_GOOD" 2>/dev/null || git rev-parse HEAD)
  echo "Deploying $target (last good deploy: $previous)"

  if deploy "$target"; then
    echo "$target" > "$LAST_GOOD"
    docker image prune -f >/dev/null
    echo "Deploy of $target succeeded"
    exit 0
  fi

  echo "Deploy of $target is unhealthy; rolling back to $previous"
  if [[ "$previous" != "$target" ]] && deploy "$previous"; then
    echo "Rolled back to $previous"
  else
    echo "Rollback failed too; check: ${COMPOSE[*]} ps / logs"
  fi
  exit 1
}

main "$@"
