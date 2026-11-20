#!/usr/bin/env bash
# Sustained transfer throughput at a constant arrival rate, then a full ledger integrity check.
#   RATE=2500 DURATION=15m scripts/bench-throughput.sh
set -euo pipefail
cd "$(dirname "$0")/.."
RATE=${RATE:-2500}
DURATION=${DURATION:-15m}
NAME=${NAME:-throughput-${RATE}rps-${DURATION}}
scripts/k6.sh transfers.js RATE="$RATE" DURATION="$DURATION" RUN="tp-$(date +%s)" NAME="$NAME"
curl -fsS localhost:8080/api/v1/ledger/integrity | tee "results/raw/$NAME-integrity.json"
echo
