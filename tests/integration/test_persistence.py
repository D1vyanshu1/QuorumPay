#!/usr/bin/env python3
"""Crash recovery on 5 real ledger-node processes (step 7: persistence).

    python3 tests/integration/test_persistence.py [--build-dir build]

Nodes keep Raft's term, vote and log on disk (meta + WAL). A follower
SIGKILLed while transfers run comes back from its own WAL; SIGKILLing ALL 5
nodes loses no committed transaction and doesn't reset the term; a torn write
at the end of a WAL is cut off on restart; and a node refuses to start on a
genesis other than the one its log was written with.

Uses its own keystore and genesis in a temp directory (see test_client.py).
"""

import argparse
import json
import os
import shutil
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools"))
from cluster import Cluster  # noqa: E402
from ledger_cli import Cli, make_genesis  # noqa: E402
from test_client import SUPPLY, converged  # noqa: E402
from test_election import wait_leader  # noqa: E402
from test_network import wait_for  # noqa: E402


def last_event(c, nid, event):
    """The node's most recent log line with this event, as a dict (or None)."""
    found = None
    with open(os.path.join(c._data_dir(nid), "node.log")) as f:
        for line in f:
            if f'"event":"{event}"' in line:
                found = json.loads(line)
    return found


def balances(cli, names):
    return {n: cli.balance(n)["balance"] for n in names}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-dir", default="build")
    args = ap.parse_args()

    tmp = tempfile.mkdtemp(prefix="ledger-test-")
    keys, genesis = make_genesis(tmp, args.build_dir, SUPPLY)
    cli = Cli(args.build_dir, keys, genesis)

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
        cli.ok("fund", "alice", 1000)

        follower = next(n for n in ids if n != leader)
        print(f"1. SIGKILL follower {follower} while transfers run; it restarts from its WAL")
        done = []
        stop = threading.Event()

        def pay():
            while not stop.is_set():
                done.append(cli.ok("transfer", "alice", "bob", 1)["status"])
        t = threading.Thread(target=pay)
        t.start()
        time.sleep(0.3)
        c.kill([follower])
        time.sleep(0.3)
        c.start([follower])
        time.sleep(0.3)
        stop.set()
        t.join()
        assert done and all(s == "OK" for s in done), done
        converged(c, ids)
        rec = last_event(c, follower, "recovered")
        assert rec and rec["entries"] > 0, rec
        print(f"  ok: {len(done)} transfers OK; node {follower} recovered {rec['entries']} "
              f"entries from disk (term {rec['term']})")
        b = cli.balance("bob")["balance"]
        assert b == len(done), (b, len(done))

        before = balances(cli, names)
        term_before = max(c.status(n)["term"] for n in ids)
        applied_before = c.status(leader)["applied_index"]
        print(f"2. SIGKILL ALL 5 nodes (term {term_before}, applied {applied_before}), restart them")
        c.kill()
        c.start()
        leader = wait_leader(c, ids, "a leader again")
        converged(c, ids)
        after = balances(cli, names)
        assert after == before, (before, after)
        term_after = c.status(leader)["term"]
        assert term_after > term_before, (term_before, term_after)
        assert c.status(leader)["applied_index"] > applied_before  # + the new leader's no-op
        print(f"  ok: every balance survived {after}; term went on from {term_before} to "
              f"{term_after} instead of restarting at 1")

        victim = next(n for n in ids if n != leader)
        print(f"3. tear the end of node {victim}'s WAL (a write cut short by a crash)")
        c.kill([victim])
        wal_dir = os.path.join(c._data_dir(victim), "wal")
        seg = os.path.join(wal_dir, sorted(os.listdir(wal_dir))[-1])
        with open(seg, "ab") as f:
            f.write(b"\x00\x00\x00\x40\xde\xad")  # a header promising 64 bytes, then nothing
        c.start([victim])
        wait_for(f"node {victim} is up", lambda: c.status(victim)["role"] != "", timeout=5)
        rec = last_event(c, victim, "recovered")
        assert rec["truncated_bytes"] == 6, rec
        cli.ok("transfer", "alice", "bob", 1)
        converged(c, ids)
        print(f"  ok: recovery cut the 6 torn bytes and node {victim} caught up")

        print(f"4. node {victim} refuses to start on a different genesis")
        c.stop([victim])
        other = Cluster(build_dir=args.build_dir, genesis=os.path.join("config", "genesis.json"))
        other.start([victim])
        wait_for("it exits", lambda: not c.is_running(victim) or "still running", timeout=5)
        err = last_event(c, victim, "fatal")
        assert err and "different genesis" in err["error"], err
        c.start([victim])
        converged(c, ids)
        print("  ok: it refused ('written with a different genesis'); "
              "with the right genesis it rejoins")

        print("PASS")
    finally:
        c.stop()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
