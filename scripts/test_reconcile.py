#!/usr/bin/env python3
"""End-to-end test for two-phase RIBLT + gRPC Sync with thousands of UUIDs."""

import argparse
import asyncio
import contextlib
import http.client
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path


def request_json(conn: http.client.HTTPConnection, method: str, path: str, body=None):
    headers = {}
    payload = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        payload = json.dumps(body).encode("utf-8")
    conn.request(method, path, body=payload, headers=headers)
    resp = conn.getresponse()
    raw = resp.read()
    if resp.status >= 400:
        raise RuntimeError(f"{method} {path} failed ({resp.status}): {raw.decode(errors='replace')}")
    return json.loads(raw) if raw else None


def wait_ready(host: str, port: int, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        conn = http.client.HTTPConnection(host, port, timeout=1.0)
        try:
            request_json(conn, "GET", "/items")
            return
        except OSError:
            time.sleep(0.05)
        finally:
            conn.close()
    raise TimeoutError(f"Timed out waiting for http://{host}:{port}/items")


def _insert_batches(host: str, port: int, batches: list[list[str]], timeout_s: float) -> None:
    with contextlib.closing(http.client.HTTPConnection(host, port, timeout=timeout_s)) as conn:
        for batch in batches:
            request_json(conn, "POST", "/items/batch", batch)


async def insert_items_async(
    host: str,
    port: int,
    items: list[str],
    timeout_s: float = 600.0,
    batch_size: int = 50000,
    workers: int = 2,
) -> None:
    if not items:
        return
    batches = [items[i : i + batch_size] for i in range(0, len(items), batch_size)]
    num_workers = max(1, min(workers, len(batches)))
    worker_batches = [batches[i::num_workers] for i in range(num_workers)]
    await asyncio.gather(
        *(asyncio.to_thread(_insert_batches, host, port, wb, timeout_s) for wb in worker_batches)
    )


async def ingest_both(
    items_a: list[str],
    items_b: list[str],
    timeout_s: float,
    batch_size: int,
    workers: int,
) -> None:
    await asyncio.gather(
        insert_items_async("127.0.0.1", 18080, items_a, timeout_s, batch_size, workers),
        insert_items_async("127.0.0.1", 18082, items_b, timeout_s, batch_size, workers),
    )


def fetch_items(host: str, port: int, timeout_s: float = 600.0) -> set[str]:
    with contextlib.closing(http.client.HTTPConnection(host, port, timeout=timeout_s)) as conn:
        data = request_json(conn, "GET", "/items")
        return set(data or [])


def fetch_fingerprint(host: str, port: int, timeout_s: float = 600.0) -> dict:
    with contextlib.closing(http.client.HTTPConnection(host, port, timeout=timeout_s)) as conn:
        return request_json(conn, "GET", "/fingerprint")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test RIBLT reconciliation across two nodes.")
    parser.add_argument("-n", "--count", type=int, default=8000, help="Total unique UUIDs when --shared/--only-a/--only-b are not set")
    parser.add_argument("--delta", type=int, default=None, help="Fixed symmetric difference |A Δ B| when using --count")
    parser.add_argument("--shared", type=int, default=None, help="UUIDs present on both nodes")
    parser.add_argument("--only-a", type=int, default=None, help="UUIDs initially only on Node A")
    parser.add_argument("--only-b", type=int, default=None, help="UUIDs initially only on Node B")
    parser.add_argument("--timeout", type=float, default=600.0, help="HTTP request timeout in seconds (default: 600)")
    parser.add_argument("--batch-size", type=int, default=50000, help="Items per POST /items/batch request (default: 50000)")
    parser.add_argument("--ingest-workers", type=int, default=2, help="Concurrent HTTP connections per node during ingestion (default: 2)")
    parser.add_argument("--verify-fingerprint", action="store_true", help="Verify equality via O(1) GET /fingerprint instead of fetching all items")
    return parser.parse_args()


def resolve_counts(args: argparse.Namespace) -> tuple[int, int, int]:
    if args.shared is not None or args.only_a is not None or args.only_b is not None:
        return args.shared or 0, args.only_a or 0, args.only_b or 0
    if args.delta is not None:
        delta = min(args.delta, args.count)
        n_only_a = delta // 2
        return args.count - delta, n_only_a, delta - n_only_a
    n_shared = args.count // 4
    rem = args.count - n_shared
    n_only_a = rem // 2
    return n_shared, n_only_a, rem - n_only_a


def build_binary(root_dir: Path, tmpdir: str) -> str:
    bin_path = os.path.join(tmpdir, "hashtable")
    print("Building hashtable binary...", end="", flush=True)
    subprocess.run(["go", "build", "-o", bin_path, "./cmd/hashtable"], cwd=root_dir, check=True)
    print(" done")
    return bin_path


@contextlib.contextmanager
def running_nodes(bin_path: str, tmpdir: str):
    log_a_path = os.path.join(tmpdir, "node-a.log")
    log_b_path = os.path.join(tmpdir, "node-b.log")
    with (
        open(log_a_path, "w", encoding="utf-8") as log_a,
        open(log_b_path, "w", encoding="utf-8") as log_b,
    ):
        proc_a = subprocess.Popen([bin_path, "-httpPort=18080", "-grpcPort=18081"], stdout=log_a, stderr=subprocess.STDOUT)
        proc_b = subprocess.Popen([bin_path, "-httpPort=18082", "-grpcPort=18083"], stdout=log_b, stderr=subprocess.STDOUT)
        try:
            wait_ready("127.0.0.1", 18080)
            wait_ready("127.0.0.1", 18082)
            yield
        except Exception:
            for label, path in (("node-a.log", log_a_path), ("node-b.log", log_b_path)):
                try:
                    content = Path(path).read_text(encoding="utf-8").strip()
                    if content:
                        print(f"\n--- {label} ---\n{content}", file=sys.stderr)
                except OSError:
                    pass
            raise
        finally:
            for proc in (proc_a, proc_b):
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()


def verify_reconciled(expected: set[str], verify_fingerprint: bool, timeout_s: float) -> str:
    if verify_fingerprint:
        fp_a = fetch_fingerprint("127.0.0.1", 18080, timeout_s=timeout_s)
        fp_b = fetch_fingerprint("127.0.0.1", 18082, timeout_s=timeout_s)
        assert fp_a["fingerprint"] == fp_b["fingerprint"], (
            f"Fingerprint mismatch: A={fp_a['fingerprint']} B={fp_b['fingerprint']}"
        )
        assert fp_a["size"] == len(expected) and fp_b["size"] == len(expected), (
            f"Size mismatch: expected={len(expected)} A={fp_a['size']} B={fp_b['size']}"
        )
        return f"fingerprints & sizes ({len(expected)})"

    set_a = fetch_items("127.0.0.1", 18080, timeout_s=timeout_s)
    set_b = fetch_items("127.0.0.1", 18082, timeout_s=timeout_s)
    assert set_a == expected, f"Node A mismatch: missing={len(expected - set_a)}, extra={len(set_a - expected)}"
    assert set_b == expected, f"Node B mismatch: missing={len(expected - set_b)}, extra={len(set_b - expected)}"
    assert set_a == set_b, "Node A and Node B sets differ!"
    return "equal sets"


def main() -> int:
    args = parse_args()
    n_shared, n_only_a, n_only_b = resolve_counts(args)
    print(f"Running set reconciliation experiment with symmetric difference |delta(A, B)| = {n_only_a + n_only_b}")

    root_dir = Path(__file__).resolve().parent.parent
    with tempfile.TemporaryDirectory() as tmpdir:
        bin_path = build_binary(root_dir, tmpdir)
        with running_nodes(bin_path, tmpdir):
            shared = [str(uuid.uuid4()) for _ in range(n_shared)]
            only_a = [str(uuid.uuid4()) for _ in range(n_only_a)]
            only_b = [str(uuid.uuid4()) for _ in range(n_only_b)]
            expected = set(shared) | set(only_a) | set(only_b)

            t0 = time.monotonic()
            asyncio.run(ingest_both(shared + only_a, shared + only_b, args.timeout, args.batch_size, args.ingest_workers))
            print(f"Inserted {len(shared) + len(only_a)} items on Node A and {len(shared) + len(only_b)} items on Node B in {time.monotonic() - t0:.2f}s")

            with contextlib.closing(http.client.HTTPConnection("127.0.0.1", 18080, timeout=args.timeout)) as conn_a:
                request_json(conn_a, "POST", "/peers", {"host": "127.0.0.1", "port": "18083"})

                t1 = time.monotonic()
                summaries = request_json(conn_a, "POST", "/reconcile")
                rpc_elapsed = time.monotonic() - t1

                verified_what = verify_reconciled(expected, args.verify_fingerprint, args.timeout)
                total_elapsed = time.monotonic() - t1

                print(f"Reconciliation RPC finished in {rpc_elapsed:.3f}s; {verified_what} verified in {total_elapsed:.3f}s:")
                print(json.dumps(summaries, indent=2))
                print(f"PASS: Node A and Node B both hold all {len(expected)} identical UUIDs.")

                noop = request_json(conn_a, "POST", "/reconcile")
                assert noop[0]["digests_requested"] == 0 and noop[0]["digests_available"] == 0, (
                    f"Expected 0 diff on second reconcile, got {noop}"
                )
                print("PASS: Second reconciliation confirmed 0 digest differences.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
