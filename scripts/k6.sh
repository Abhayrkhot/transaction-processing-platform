#!/usr/bin/env bash
# Runs a k6 script in Docker against the local stack. Usage: scripts/k6.sh <script.js> [ENV=value ...]
set -euo pipefail
cd "$(dirname "$0")/.."
script=$1; shift
env_args=()
for kv in "$@"; do env_args+=(-e "$kv"); done
mkdir -p results/raw
docker run --rm --network host --user "$(id -u):$(id -g)" \
  -v "$PWD/k6:/scripts:ro" -v "$PWD/bench-data:/data:ro" -v "$PWD/results/raw:/results" \
  "${env_args[@]}" grafana/k6:1.4.2 run --quiet "/scripts/$script"
