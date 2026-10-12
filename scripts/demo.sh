#!/usr/bin/env bash
# End-to-end demo against `make up` (Temporal + Redpanda + worker + API).
#   1. one-call cluster creation from a manifest
#   2. inject a soft GPU fault -> event -> drain/repair/re-validate, no human involved
#   3. inject a permanent fault -> repair budget exhausted -> RMA -> automatic replacement
#   4. tear down
set -euo pipefail

export GPUCTL_SERVER="${GPUCTL_SERVER:-http://localhost:8080}"
SIM_ADMIN="${SIM_ADMIN:-http://localhost:9090}"
G=./bin/gpuctl

echo "== 1. apply examples/cluster.json"
$G apply -f examples/cluster.json
$G wait cluster llama-prod -timeout 5m >/dev/null
$G get cluster llama-prod

HOST=$($G get cluster llama-prod | sed -n 's/.*"id": "\(gpu-node-[0-9]*\)".*/\1/p' | head -1)

echo "== 2. soft fault on $HOST (self-heals)"
curl -fsS -X POST "$SIM_ADMIN/faults/degrade?host=$HOST"
sleep 10
$G get host "$HOST" | grep -E '"state"|"to"' | tail -8

echo "== 3. permanent fault on $HOST (RMA + replacement)"
curl -fsS -X POST "$SIM_ADMIN/faults/break?host=$HOST"
sleep 30
$G get cluster llama-prod

echo "== 4. teardown"
$G delete cluster llama-prod
