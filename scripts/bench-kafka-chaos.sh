#!/usr/bin/env bash
# Kafka ingest under failure. Produces EVENTS unique transfer requests, DUPLICATES re-sends of
# earlier requests and POISON invalid messages. While they are being consumed:
#   t+10s  Postgres is stopped for 15 s   -> listener retries with back-off, outbox relay retries
#   t+40s  the ledger is SIGKILLed, restarted 5 s later -> uncommitted offsets are redelivered
# (in that order: after a SIGKILL the group only rebalances once Kafka's 45 s session timeout
# evicts the dead consumers, so an outage right after the kill would hit idle consumers).
# Then verifies every account balance against the exactly-once expectation and that only the
# poison messages were dead-lettered.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=${RUN:-kafka-$(date +%s)}
BENCH=(java -jar bench/target/bench.jar)
metrics() { curl -fsS localhost:8080/actuator/prometheus | grep -E '^ledger_(kafka|transfers)' || true; }

"${BENCH[@]}" kafka-load --run "$RUN" --events "${EVENTS:-100000}" --duplicates "${DUPLICATES:-25000}" \
  --poison "${POISON:-50}" --rate "${RATE:-2000}" &
load=$!

sleep 10
echo "chaos: stopping postgres for 15s at $(date +%T)"
docker compose stop -t 5 postgres
sleep 15
docker compose start postgres
echo "chaos: postgres back at $(date +%T)"
sleep 15
# Counters live in the process; capture them before the SIGKILL resets them.
metrics > "results/raw/kafka-$RUN-metrics-before-kill.txt"
echo "chaos: SIGKILL ledger at $(date +%T)"
docker compose kill -s KILL ledger
sleep 5
docker compose up -d ledger
echo "chaos: ledger restarted at $(date +%T)"

wait "$load"
"${BENCH[@]}" kafka-verify --run "$RUN"
metrics > "results/raw/kafka-$RUN-metrics-after-restart.txt"
echo "-- before kill"; cat "results/raw/kafka-$RUN-metrics-before-kill.txt"
echo "-- after restart"; cat "results/raw/kafka-$RUN-metrics-after-restart.txt"
