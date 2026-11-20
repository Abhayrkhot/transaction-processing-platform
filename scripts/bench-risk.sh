#!/usr/bin/env bash
# Seeded-anomaly detection: one labelled 14-day stream per seed through the full pipeline,
# then recall of risk alerts and reconciliation breaks against the labels.
# Rules were tuned on seed 1 only; seeds 2+ are held out. Each seed uses its own event-time window,
# so run each seed once per database (a second run of a seed would overlap its reconciliation window).
set -euo pipefail
cd "$(dirname "$0")/.."
BENCH=(java -jar bench/target/bench.jar)
for seed in ${SEEDS:-1 2 3 4 5}; do
  run="risk-s$seed-$(date +%s)"
  "${BENCH[@]}" risk-stream --seed "$seed" --run "$run" --out "bench-data/$run"
  "${BENCH[@]}" risk-eval --dir "bench-data/$run"
done
