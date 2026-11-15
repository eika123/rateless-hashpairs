#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_BIN="$(mktemp /tmp/hashtable.XXXXXX)"
NODE_A_PID=""
NODE_B_PID=""

cleanup() {
  [[ -n "${NODE_A_PID}" ]] && kill "${NODE_A_PID}" 2>/dev/null || true
  [[ -n "${NODE_B_PID}" ]] && kill "${NODE_B_PID}" 2>/dev/null || true
  rm -f "${TMP_BIN}"
}
trap cleanup EXIT

wait_ready() {
  local url="$1"
  for _ in $(seq 1 30); do
    curl -sf "${url}" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "Timed out waiting for ${url}" >&2
  return 1
}

print_items() {
  echo "  $1:"
  curl -sf "$2" | jq -r 'if length == 0 then "    (empty)" else sort[] | "    - \(.)" end'
}

echo "[1/6] Building hashtable..."
(cd "${ROOT_DIR}" && go build -o "${TMP_BIN}" ./cmd/hashtable)

echo "[2/6] Starting Node A (HTTP :18080, gRPC :18081) and Node B (HTTP :18082, gRPC :18083)..."
"${TMP_BIN}" -httpPort=18080 -grpcPort=18081 >/tmp/node-a.log 2>&1 &
NODE_A_PID=$!
"${TMP_BIN}" -httpPort=18082 -grpcPort=18083 >/tmp/node-b.log 2>&1 &
NODE_B_PID=$!

wait_ready "http://127.0.0.1:18080/items"
wait_ready "http://127.0.0.1:18082/items"

echo "[3/6] Inserting names into Node A..."
for name in Alice Bob Cynthia Dave Eve; do
  curl -sf -X POST -H "Content-Type: application/json" -d "\"${name}\"" "http://127.0.0.1:18080/items" >/dev/null
done

echo "[4/6] Stores BEFORE reconciliation:"
print_items "Node A (:18080)" "http://127.0.0.1:18080/items"
print_items "Node B (:18082)" "http://127.0.0.1:18082/items"
echo

echo "[5/6] Registering Node B (127.0.0.1:18083) as peer on Node A and reconciling..."
curl -sf -X POST -H "Content-Type: application/json" \
  -d '{"host":"127.0.0.1","port":"18083"}' \
  "http://127.0.0.1:18080/peers"
echo "  Reconcile summary:"
curl -sf -X POST "http://127.0.0.1:18080/reconcile" | jq . | sed 's/^/  /'
echo

echo "[6/6] Stores AFTER reconciliation:"
print_items "Node A (:18080)" "http://127.0.0.1:18080/items"
print_items "Node B (:18082)" "http://127.0.0.1:18082/items"