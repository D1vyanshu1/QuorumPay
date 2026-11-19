#!/usr/bin/env python3
"""Snapshots, log compaction and InstallSnapshot on 5 real ledger-node processes.

    python3 tests/integration/test_snapshot.py [--build-dir build]

Every node snapshots its ledger. A follower that was down while the leader
moved on and compacted its log past it catches up through InstallSnapshot.
After a SIGKILL of all 5 nodes they boot from their snapshots plus the WAL
after them. A node whose newest snapshot file is damaged falls back to the
older one. Balances and state hashes agree everywhere throughout.

Uses its own keystore and genesis in a temp directory (see test_client.py).
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools"))
from cluster import REPO, Cluster  # noqa: E402
from test_client import SUPPLY, Cli, converged  # noqa: E402
from test_election import wait_leader  # noqa: E402
from test_network import wait_for  # noqa: E402
from test_persistence import last_event  # noqa: E402

MARGIN = 100  # RaftConfig::compaction_margin


def balances(cli, names, node=None):
    if node:  # this node's own state, whether or not it is leader
        return {n: cli.ok("balance", n, "--stale", node=node)["balance"] for n in names}
    return {n: cli.balance(n)["balance"] for n in names}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-dir", default="build")
    args = ap.parse_args()

    tmp = tempfile.mkdtemp(prefix="ledger-test-")
    keys = os.path.join(tmp, "keys")
    genesis = os.path.join(tmp, "genesis.json")
    cli = Cli(args.build_dir, keys, genesis)
    cli.ok("keygen", "faucet")
    subprocess.run([sys.executable, os.path.join(REPO, "tools", "make_genesis.py"),
                    "--faucet-key", os.path.join(keys, "faucet.key"),
                    "--supply", str(SUPPLY), "--out", genesis], check=True, capture_output=True)

    c = Cluster(build_dir=args.build_dir, genesis=genesis)
    ids = sorted(c.nodes)
    names = ["faucet", "alice", "bob"]
    c.stop()
    c.wipe()
    try:
        c.start()
        leader = wait_leader(c, ids, "one agreed leader")
        for n in ("alice", "bob"):
            cli.ok("create_account", n)
        cli.ok("fund", "alice", 500)
        for _ in range(10):
            cli.ok("transfer", "alice", "bob", 3)
        converged(c, ids)

        print("1. every node snapshots its ledger")
        for n in ids:
            c.admin(n, "snapshot")
        for n in ids:
            s = c.status(n)
            assert s["snapshot_index"] == s["applied_index"] > 0, s
        print(f"  ok: all at snapshot index {c.status(leader)['snapshot_index']}")

        follower = next(n for n in ids if n != leader)
        behind = c.status(follower)["last_log_index"]
        print(f"2. follower {follower} goes down at index {behind}; the leader moves "
              f"{MARGIN + 50}+ entries on and compacts past it")
        c.kill([follower])
        for i in range(MARGIN + 50):
            assert c.propose(f"filler-{i}")["accepted"]
        for _ in range(5):
            cli.ok("transfer", "alice", "bob", 7)
        c.admin(leader, "snapshot")
        first = c.status(leader)["first_log_index"]
        assert first > behind + 1, (first, behind)
        print(f"  ok: the leader's log now starts at {first}; the follower needs {behind + 1}")

        print(f"3. restart follower {follower}: it can only catch up through InstallSnapshot")
        c.start([follower])
        converged(c, ids)
        ev = last_event(c, follower, "snapshot_installed")
        assert ev and ev["index"] >= first, ev
        before = balances(cli, names)
        assert balances(cli, names, node=follower) == before
        assert before["bob"] == 10 * 3 + 5 * 7, before
        print(f"  ok: it installed the leader's snapshot at {ev['index']} ({ev['bytes']} bytes); "
              f"balances agree {before}")

        print("4. SIGKILL all 5 nodes; they boot from their snapshots + the WAL after them")
        c.kill()
        c.start()
        wait_leader(c, ids, "a leader again")
        converged(c, ids)
        for n in ids:
            rec = last_event(c, n, "recovered")
            assert rec["snapshot_index"] > 0, (n, rec)
        assert balances(cli, names) == before
        print("  ok: every node started from a snapshot, nothing lost")

        victim = next(n for n in ids if n not in (leader, follower))
        cli.ok("transfer", "alice", "bob", 1)
        converged(c, ids)
        c.admin(victim, "snapshot")  # a second, newer snapshot on this node
        older, newer = sorted(os.listdir(os.path.join(c._data_dir(victim), "snap")))[-2:]
        print(f"5. damage node {victim}'s newest snapshot ({newer}); it falls back to {older}")
        c.stop([victim])
        path = os.path.join(c._data_dir(victim), "snap", newer)
        with open(path, "r+b") as f:
            f.seek(50)
            b = f.read(1)
            f.seek(50)
            f.write(bytes([b[0] ^ 0xFF]))
        c.start([victim])
        wait_for(f"node {victim} is up", lambda: c.status(victim)["role"] != "", timeout=5)
        assert last_event(c, victim, "snapshot_damaged")["skipped"] == 1
        rec = last_event(c, victim, "recovered")
        assert rec["snapshot_index"] == int(older[5:25]), (rec, older)
        converged(c, ids)
        assert balances(cli, names, node=victim) == balances(cli, names)
        print(f"  ok: skipped the damaged file, started from snapshot {rec['snapshot_index']}, "
              "replayed the WAL after it, and agrees with everyone")

        print("PASS")
    finally:
        c.stop()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
