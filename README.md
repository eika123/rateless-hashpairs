# Rateless hashpairs
Simple demo with two processes reconciling hash tables containing strings,
from the paper [Practical Rateless Set Reconciliation](https://dl.acm.org/doi/10.1145/3651890.3672219).
Can reconcile large symmetric deltas, in the millions of items.


Standalone two-process testbed for two-phase Rateless IBLT (RIBLT) set reconciliation over gRPC:
1. **Phase 1 (`ReconcileDigests`)**: Streams RIBLT coded symbols to compute $A \setminus B$ and $B \setminus A$ digests.
2. **Phase 2 (`Sync`)**: Streams only the missing items in chunks when diffs are non-empty.

## Running the experiments

### Small human-readable demo
Starts two nodes, inserts five names (`Alice`, `Bob`, `Cynthia`, `Dave`, `Eve`) on Node A, reconciles with Node B, and prints both stores before and after (requires `curl` and `jq`):

```bash
./scripts/demo_reconcile.sh
```

### Large-scale UUID benchmark & correctness test
Starts two nodes, seeds UUIDs, triggers reconciliation, times convergence, and asserts both stores hold identical sets (Python 3 stdlib only):

```bash
# Default: 8,000 unique UUIDs (25% shared, 37.5% A-only, 37.5% B-only)
./scripts/test_reconcile.py

# Custom total count and timeout
./scripts/test_reconcile.py --count 50000 --timeout 600

# Explicit split across shared / Node A / Node B
./scripts/test_reconcile.py --shared 2000 --only-a 3000 --only-b 3000
```

## Running `hashtable` standalone with `curl`

Build the binary (or use `go run ./cmd/hashtable`):

```bash
go build ./cmd/hashtable
```

Start two nodes in separate terminals, giving each its own HTTP and gRPC ports:

```bash
# Terminal 1 (Node A: HTTP :8080, gRPC :8081)
./hashtable -httpPort=8080 -grpcPort=8081

# Terminal 2 (Node B: HTTP :8082, gRPC :8083)
./hashtable -httpPort=8082 -grpcPort=8083
```

Interact with the nodes via `curl`:

```bash
# Add a single item to Node A
curl -X POST -H "Content-Type: application/json" -d '"Alice"' http://localhost:8080/items

# Batch-add multiple items to Node A
curl -X POST -H "Content-Type: application/json" -d '["Bob","Cynthia"]' http://localhost:8080/items/batch

# Add an item to Node B
curl -X POST -H "Content-Type: application/json" -d '"Dave"' http://localhost:8082/items

# List items on each node (or look up a single item by uint64 digest via GET /items/{digest})
curl http://localhost:8080/items
curl http://localhost:8082/items

# Register Node B's gRPC address as a peer on Node A
curl -X POST -H "Content-Type: application/json" \
  -d '{"host":"127.0.0.1","port":"8083"}' \
  http://localhost:8080/peers

# List registered peers on Node A
curl http://localhost:8080/peers

# Trigger two-phase reconciliation from Node A to its registered peers
curl -X POST http://localhost:8080/reconcile

# Verify both nodes now contain the same items
curl http://localhost:8080/items
curl http://localhost:8082/items
```

## HTTP API

| Method & Path | Request Body | Response |
| --- | --- | --- |
| `POST /items` | JSON value (e.g. `"Alice"`) | `201`/`200` `{"digest": <uint64>, "added": <bool>}` |
| `POST /items/batch` | JSON array (e.g. `["Bob","Cynthia"]`, up to 4 MB) | `201`/`200` `{"added": <int>}` |
| `GET /items` | — | `200` JSON array of all stored items |
| `GET /items/{digest}` | — | `200` JSON item for the `uint64` digest, or `404` |
| `POST /peers` | `{"host": "<addr>", "port": "<grpcPort>"}` | `201` (new) or `200` (already registered) |
| `GET /peers` | — | `200` JSON array of registered peers |
| `POST /reconcile` | — | `200` JSON array of `ReconcileSummary` per peer |
