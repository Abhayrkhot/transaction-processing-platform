#!/usr/bin/env bash
# GET /accounts/{id} latency under concurrent write load, same offered load in three configurations:
#   baseline        entries indexed by account_id only (the V2 index set), Redis cache off
#   indexed         V3 composite covering index (account_id, id DESC) INCLUDE (...), cache off
#   indexed-cached  V3 index + Redis cache
# Each phase sets the index set explicitly (CONCURRENTLY, then ANALYZE), so the comparison can
# be rerun on one loaded database. Pick READ_RATE so the baseline is not saturated (no dropped
# iterations): a saturated system measures queueing, not the query.
#   READ_RATE=200 WRITE_RATE=1000 DURATION=5m scripts/bench-account-queries.sh
set -euo pipefail
cd "$(dirname "$0")/.."
WRITE_RATE=${WRITE_RATE:-1000}
READ_RATE=${READ_RATE:-200}
DURATION=${DURATION:-5m}
TAG=${TAG:-r${READ_RATE}-w${WRITE_RATE}}
PHASES=${PHASES:-baseline indexed indexed-cached}

psql() { docker compose exec -T postgres psql -U ledger -v ON_ERROR_STOP=1 -q "$@"; }

counters() {
  local db hit miss
  db=$(psql -tAc "SELECT json_build_object('xact_commit', xact_commit, 'tup_returned', tup_returned, 'tup_fetched', tup_fetched, 'blks_hit', blks_hit) FROM pg_stat_database WHERE datname = 'ledger'")
  hit=$(curl -fsS localhost:8080/actuator/prometheus | awk '/^ledger_account_cache_total\{result="hit"/ {print $2}')
  miss=$(curl -fsS localhost:8080/actuator/prometheus | awk '/^ledger_account_cache_total\{result="miss"/ {print $2}')
  echo "{\"pg\":$db,\"cache_hit\":${hit:-0},\"cache_miss\":${miss:-0}}"
}

v2_indexes() {
  psql -c "CREATE INDEX CONCURRENTLY IF NOT EXISTS entries_account_id_idx ON entries (account_id)"
  psql -c "DROP INDEX CONCURRENTLY IF EXISTS entries_account_recent_idx"
  psql -c "ANALYZE entries"
}

v3_indexes() {
  psql -c "CREATE INDEX CONCURRENTLY IF NOT EXISTS entries_account_recent_idx ON entries (account_id, id DESC) INCLUDE (transaction_id, amount, balance_after, posted_at)"
  psql -c "DROP INDEX CONCURRENTLY IF EXISTS entries_account_id_idx"
  psql -c "ANALYZE entries"
}

for name in $PHASES; do
  case $name in
    baseline)       v2_indexes; cache=false ;;
    indexed)        v3_indexes; cache=false ;;
    indexed-cached) v3_indexes; cache=true ;;
    *) echo "unknown phase $name"; exit 2 ;;
  esac
  echo "== $name ($TAG, cache $cache) $(date +%T)"
  ACCOUNT_CACHE_ENABLED=$cache docker compose up -d --wait ledger
  docker compose exec -T redis redis-cli FLUSHALL >/dev/null
  before=$(counters)
  scripts/k6.sh account-queries.js WRITE_RATE="$WRITE_RATE" READ_RATE="$READ_RATE" DURATION="$DURATION" \
    RUN="aq-$name-$(date +%s)" NAME="account-queries-$TAG-$name"
  after=$(counters)
  # Postgres work done during the phase (rows read, transactions) and cache hits/misses.
  echo "{\"phase\":\"$name\",\"before\":$before,\"after\":$after}" > "results/raw/account-queries-$TAG-$name-db.json"
  cat "results/raw/account-queries-$TAG-$name-db.json"
done
