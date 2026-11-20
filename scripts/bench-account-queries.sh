#!/usr/bin/env bash
# GET /accounts/{id} p95 under concurrent write load, in three configurations:
#   baseline        schema V2 (foreign-key indexes only), Redis cache off
#   indexed         V3 composite covering index, cache off
#   indexed-cached  V3 index + Redis cache
# Needs a stack started with FLYWAY_TARGET=2 and loaded with history (bench seed + a long
# transfers run). V3 cannot be rolled back, so this runs once per fresh database.
set -euo pipefail
cd "$(dirname "$0")/.."
WRITE_RATE=${WRITE_RATE:-1000}
READ_RATE=${READ_RATE:-1000}
DURATION=${DURATION:-5m}

phase() {
  local name=$1 target=$2 cache=$3
  echo "== $name (flyway target $target, cache $cache)"
  FLYWAY_TARGET=$target ACCOUNT_CACHE_ENABLED=$cache docker compose up -d --wait ledger
  scripts/k6.sh account-queries.js WRITE_RATE="$WRITE_RATE" READ_RATE="$READ_RATE" DURATION="$DURATION" \
    RUN="aq-$name-$(date +%s)" NAME="account-queries-$name"
}

phase baseline 2 false
phase indexed latest false
phase indexed-cached latest true
