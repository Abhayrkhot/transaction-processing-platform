#!/usr/bin/env bash
# Kafka ingest under failure. Produces EVENTS unique transfer requests, DUPLICATES re-sends of
# earlier requests and POISON invalid messages, while the ledger is SIGKILLed mid-stream and
# Postgres is stopped for 15 s. Then verifies every account balance against the
# exactly-once expectation and that only the poison messages were dead-lettered.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=${RUN:-kafka-$(date +%s)}
BENCH=(java -jar bench/target/bench.jar)

"${BENCH[@]}" kafka-load --run "$RUN" --events "${EVENTS:-100000}" --duplicates "${DUPLICATES:-25000}" \
  --poison "${POISON:-50}" --rate "${RATE:-2500}" &
load=$!

sleep 15
echo "chaos: SIGKILL ledger at $(date +%T)"
docker compose kill -s KILL ledger
sleep 5
docker compose up -d ledger
sleep 15
echo "chaos: stopping postgres for 15s at $(date +%T)"
docker compose stop -t 5 postgres
sleep 15
docker compose start postgres
echo "chaos: postgres back at $(date +%T)"

wait "$load"
"${BENCH[@]}" kafka-verify --run "$RUN"
# Counters cover the ledger process started after the SIGKILL (the Postgres outage happens in it).
curl -fsS localhost:8080/actuator/prometheus | grep -E '^ledger_(kafka|transfers)' > "results/raw/kafka-$RUN-metrics.txt"
cat "results/raw/kafka-$RUN-metrics.txt"
