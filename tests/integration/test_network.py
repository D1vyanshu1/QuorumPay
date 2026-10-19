#!/usr/bin/env python3
"""End-to-end network test with 5 real ledger-node processes.

    python3 tests/integration/test_network.py [--build-dir build]

Scenarios: full mesh forms; a SIGKILLed node is detected and rejoins after
restart; a {1,2}|{3,4,5} partition cuts exactly the cross-group links and
healing restores them.
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools"))
from cluster import Cluster  # noqa: E402


def reach_map(c, ids):
    """{node: set of peers it currently reaches}."""
    return {n: {p["id"] for p in c.status(n)["peers"] if p["reachable"]} for n in ids}


def wait_for(desc, pred, timeout=5.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = pred()
            if last is True:
                print(f"  ok: {desc}")
                return
        except Exception as e:  # noqa: BLE001 - node may be mid-restart
            last = e
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for: {desc} (last: {last})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-dir", default="build")
    args = ap.parse_args()

    c = Cluster(build_dir=args.build_dir)
    ids = sorted(c.nodes)
    full = {n: set(ids) - {n} for n in ids}

    c.stop()
    try:
        print("1. start 5 nodes -> full mesh")
        c.start()
        wait_for("every node reaches every other node", lambda: reach_map(c, ids) == full)

        print("2. kill node 3 -> others notice; restart -> it rejoins")
        c.kill([3])
        others = [n for n in ids if n != 3]
        wait_for("nobody reaches node 3",
                 lambda: all(3 not in r for r in reach_map(c, others).values()))
        c.start([3])
        wait_for("full mesh again", lambda: reach_map(c, ids) == full)

        print("3. partition {1,2} | {3,4,5} -> only same-side links work")
        c.partition([1, 2], [3, 4, 5])
        expected = {1: {2}, 2: {1}, 3: {4, 5}, 4: {3, 5}, 5: {3, 4}}
        wait_for("links match the partition", lambda: reach_map(c, ids) == expected)

        print("4. heal -> full mesh")
        c.heal()
        wait_for("full mesh again", lambda: reach_map(c, ids) == full)

        print("PASS")
    finally:
        c.stop()


if __name__ == "__main__":
    main()
