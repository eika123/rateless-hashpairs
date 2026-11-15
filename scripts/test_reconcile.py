#!/usr/bin/env python3
"""End-to-end test for two-phase RIBLT + gRPC Sync with thousands of UUIDs."""

import argparse
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


def insert_items(host: str, port: int, items: list[str], timeout_s: float = 600.0, batch_size: int = 5000) -> None:
    conn = http.client.HTTPConnection(host, port, timeout=timeout_s)
    try:
        for i in range(0, len(items), batch_size):
            request_json(conn, "POST", "/items/batch", items[i : i + batch_size])
    finally:
        conn.close()


def fetch_items(host: str, port: int, timeout_s: float = 600.0) -> set[str]:
    conn = http.client.HTTPConnection(host, port, timeout=timeout_s)
    try:
        data = request_json(conn, "GET", "/items")
        return set(data or [])
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Test RIBLT reconciliation across two nodes.")
    parser.add_argument("-n", "--count", type=int, default=8000, help="Total unique UUIDs when --shared/--only-a/--only-b are not set")
    parser.add_argument("--shared", type=int, default=None, help="UUIDs present on both nodes")
    parser.add_argument("--only-a", type=int, default=None, help="UUIDs initially only on Node A")
    parser.add_argument("--only-b", type=int, default=None, help="UUIDs initially only on Node B")
    parser.add_argument("--timeout", type=float, default=600.0, help="HTTP request timeout in seconds (default: 600)")
    args = parser.parse_args()

    if args.shared is None and args.only_a is None and args.only_b is None:
        n_shared = args.count // 4
        rem = args.count - n_shared
        n_only_a = rem // 2
        n_only_b = rem - n_only_a
    else:
        n_shared = args.shared or 0
        n_only_a = args.only_a or 0
        n_only_b = args.only_b or 0
    print(f"Running set reconciliation experiment with symmetric difference |delta(A, B)| = {n_only_a + n_only_b}")

    root_dir = Path(__file__).resolve().parent.parent
    with tempfile.TemporaryDirectory() as tmpdir:
        bin_path = os.path.join(tmpdir, "hashtable")
        print("Building hashtable binary...")
        subprocess.run(
            ["go", "build", "-o", bin_path, "./cmd/hashtable"],
            cwd=root_dir,
            check=True,
        )

        log_a_path = os.path.join(tmpdir, "node-a.log")
        log_b_path = os.path.join(tmpdir, "node-b.log")
        with (
            open(log_a_path, "w", encoding="utf-8") as log_a,
            open(log_b_path, "w", encoding="utf-8") as log_b,
        ):
            proc_a = subprocess.Popen(
                [bin_path, "-httpPort=18080", "-grpcPort=18081"],
                stdout=log_a,
                stderr=subprocess.STDOUT,
            )
            proc_b = subprocess.Popen(
                [bin_path, "-httpPort=18082", "-grpcPort=18083"],
                stdout=log_b,
                stderr=subprocess.STDOUT,
            )

            try:
                wait_ready("127.0.0.1", 18080)
                wait_ready("127.0.0.1", 18082)

                shared = [str(uuid.uuid4()) for _ in range(n_shared)]
                only_a = [str(uuid.uuid4()) for _ in range(n_only_a)]
                only_b = [str(uuid.uuid4()) for _ in range(n_only_b)]
                expected = set(shared) | set(only_a) | set(only_b)

                t0 = time.monotonic()
                insert_items("127.0.0.1", 18080, shared + only_a, timeout_s=args.timeout)
                insert_items("127.0.0.1", 18082, shared + only_b, timeout_s=args.timeout)
                print(
                    f"Inserted {len(shared) + len(only_a)} items on Node A and "
                    f"{len(shared) + len(only_b)} items on Node B in {time.monotonic() - t0:.2f}s"
                )

                with contextlib.closing(http.client.HTTPConnection("127.0.0.1", 18080, timeout=args.timeout)) as conn_a:
                    request_json(conn_a, "POST", "/peers", {"host": "127.0.0.1", "port": "18083"})

                    t1 = time.monotonic()
                    summaries = request_json(conn_a, "POST", "/reconcile")
                    rpc_elapsed = time.monotonic() - t1

                    set_a = fetch_items("127.0.0.1", 18080, timeout_s=args.timeout)
                    set_b = fetch_items("127.0.0.1", 18082, timeout_s=args.timeout)

                    assert set_a == expected, f"Node A mismatch: missing={len(expected - set_a)}, extra={len(set_a - expected)}"
                    assert set_b == expected, f"Node B mismatch: missing={len(expected - set_b)}, extra={len(set_b - expected)}"
                    assert set_a == set_b, "Node A and Node B sets differ!"
                    total_elapsed = time.monotonic() - t1

                    print(f"Reconciliation RPC finished in {rpc_elapsed:.3f}s; equal sets verified in {total_elapsed:.3f}s:")
                    print(json.dumps(summaries, indent=2))
                    print(f"PASS: Node A and Node B both hold all {len(expected)} identical UUIDs.")

                    noop = request_json(conn_a, "POST", "/reconcile")
                    assert noop[0]["digests_requested"] == 0 and noop[0]["digests_available"] == 0, (
                        f"Expected 0 diff on second reconcile, got {noop}"
                    )
                    print("PASS: Second reconciliation confirmed 0 digest differences.")
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

    return 0


if __name__ == "__main__":
    sys.exit(main())
