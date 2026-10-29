#!/usr/bin/env python3
"""Raft log replication on 5 real ledger-node processes.

    python3 tests/integration/test_replication.py [--build-dir build]

Entries proposed to the leader are committed and applied on every node (same
applied index + same hash). A follower that was down catches up. A leader cut
off in a minority can append entries but never commit them; after healing
they are overwritten and every node converges on the majority's log.
Linearizable reads (ReadIndex) see every committed write, and a cut-off old
leader never answers a read (its answer could be stale).
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools"))
from cluster import Cluster  # noqa: E402
from test_election import agreed_leader, wait_leader  # noqa: E402
from test_network import wait_for  # noqa: E402


def applied(c, ids):
    """{node: (applied_index, state_hash)}"""
    out = {}
    for n in ids:
        s = c.status(n)
        out[n] = (s["applied_index"], s["state_hash"])
    return out


def wait_converged(c, ids, desc, timeout=5.0):
    """All `ids` applied the same entries, up to the leader's last index."""
    def pred():
        lead = agreed_leader(c, ids)
        if lead is None:
            return "no agreed leader"
        target = c.status(lead)["last_log_index"]
        a = applied(c, ids)
        ok = len(set(a.values())) == 1 and next(iter(a.values()))[0] == target
        return ok or f"target {target}, applied {a}"
    wait_for(desc, pred, timeout)
    a = applied(c, ids)
    print(f"     all at applied index {next(iter(a.values()))[0]}")
    return a


def propose_n(c, n, prefix, node=None):
    results = []
    for i in range(n):
        r = c.propose(f"{prefix}-{i}", node)
        assert r["accepted"], r
        results.append(r)
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-dir", default="build")
    args = ap.parse_args()

    c = Cluster(build_dir=args.build_dir)
    ids = sorted(c.nodes)

    c.stop()
    try:
        print("1. start, propose 20 entries -> all 5 nodes apply the same log")
        c.start()
        leader = wait_leader(c, ids, "one agreed leader")
        propose_n(c, 20, "a")
        wait_converged(c, ids, "all nodes applied all entries")
        r = c.read()
        assert r and r["ok"] and r["applied_index"] >= 21, r
        print(f"  ok: read on leader reflects index {r['applied_index']}")

        print("2. a follower proposal is refused with a leader hint")
        follower = next(n for n in ids if n != leader)
        r = c.propose("x", node=follower)
        assert not r["accepted"] and r["leader_hint"] == leader, r
        print(f"  ok: node {follower} says 'not leader, try {r['leader_hint']}'")

        follower = next(n for n in ids if n != leader)
        print(f"3. kill follower {follower}, propose 30 more, restart it -> it catches up")
        c.kill([follower])
        propose_n(c, 30, "b")
        c.start([follower])
        wait_converged(c, ids, "restarted follower caught up (from an empty log)")

        leader = agreed_leader(c, ids)
        others = [n for n in ids if n != leader]
        minority, majority = [leader, others[0]], others[1:]
        print(f"4. partition {minority} | {majority}; old leader {leader} takes 5 entries")
        c.partition(minority, majority)
        lost = propose_n(c, 5, "lost", node=leader)
        new_leader = wait_leader(c, majority, "majority elects a new leader")
        propose_n(c, 10, "kept", node=new_leader)
        wait_for("old leader's 5 entries never commit",
                 lambda: c.status(leader)["commit_index"] < lost[0]["index"] or
                 f"commit_index {c.status(leader)['commit_index']}", timeout=1.0)
        r = c.read(node=leader)
        assert r is None, f"cut-off old leader answered a read: {r}"
        print(f"  ok: old leader {leader} never answered a read (it would be stale)")
        r = c.read(node=new_leader)
        assert r and r["ok"], r
        print(f"  ok: new leader {new_leader} serves reads (applied index {r['applied_index']})")

        print("5. heal -> old leader's uncommitted entries are overwritten; all converge")
        c.heal()
        wait_converged(c, ids, "all 5 nodes applied the majority's log")
        assert c.status(leader)["role"] == "follower"

        print("PASS")
    finally:
        c.stop()


if __name__ == "__main__":
    main()
