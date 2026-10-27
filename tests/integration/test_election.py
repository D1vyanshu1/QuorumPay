#!/usr/bin/env python3
"""Raft leader election on 5 real ledger-node processes.

    python3 tests/integration/test_election.py [--build-dir build]

The same scenarios as tests/sim/raft_sim_test.cpp, but over real TCP with
real timers: one leader is elected; killing it elects a new one in a higher
term; the old leader rejoins as a follower; a minority partition can't elect;
healing leaves exactly one leader.
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools"))
from cluster import Cluster  # noqa: E402
from test_network import wait_for  # noqa: E402


def statuses(c, ids):
    return {n: c.status(n) for n in ids}


def agreed_leader(c, ids):
    """The leader every node in `ids` agrees on (same leader, same term), or None."""
    s = statuses(c, ids)
    leaders = [n for n, st in s.items() if st["role"] == "leader"]
    if len(leaders) != 1:
        return None
    lead = leaders[0]
    term = s[lead]["term"]
    if all(st["leader"] == lead and st["term"] == term for st in s.values()):
        return lead
    return None


def wait_leader(c, ids, desc, timeout=5.0):
    found = {}

    def pred():
        found["leader"] = agreed_leader(c, ids)
        return found["leader"] is not None or f"no agreed leader: {statuses(c, ids)}"

    wait_for(desc, pred, timeout)
    lead = found["leader"]
    print(f"     leader = node {lead}, term {c.status(lead)['term']}")
    return lead


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-dir", default="build")
    args = ap.parse_args()

    c = Cluster(build_dir=args.build_dir)
    ids = sorted(c.nodes)

    c.stop()
    try:
        print("1. start 5 nodes -> exactly one leader, everyone agrees")
        c.start()
        leader = wait_leader(c, ids, "one agreed leader")
        term = c.status(leader)["term"]

        print("2. stays stable: no new elections while nothing fails")
        time.sleep(1.0)
        assert agreed_leader(c, ids) == leader, statuses(c, ids)
        assert c.status(leader)["term"] == term, "term changed without a failure"
        print("  ok: same leader, same term after 1s")

        print(f"3. kill leader (node {leader}) -> survivors elect a new one, higher term")
        c.kill([leader])
        survivors = [n for n in ids if n != leader]
        new_leader = wait_leader(c, survivors, "new leader among survivors")
        new_term = c.status(new_leader)["term"]
        assert new_term > term, (new_term, term)

        print(f"4. restart node {leader} -> rejoins as a follower of node {new_leader}")
        c.start([leader])
        rejoined = wait_leader(c, ids, "all 5 agree again")
        st = c.status(leader)
        assert st["role"] == "follower", st
        # The restarted node must hear the leader before its election timeout
        # (peers redial it as soon as it says Hello), so it must not depose a
        # healthy leader. Pre-vote (week 6) will make this hold even when it
        # can't hear the leader at all.
        assert rejoined == new_leader, f"restart disrupted the leader: {statuses(c, ids)}"
        assert c.status(rejoined)["term"] == new_term, "restart bumped the term"
        print("  ok: same leader and term; the restart caused no election")

        leader = agreed_leader(c, ids)
        term = c.status(leader)["term"]
        minority = [leader] + [n for n in ids if n != leader][:1]
        majority = [n for n in ids if n not in minority]
        print(f"5. partition {minority} | {majority} (leader in minority)")
        c.partition(minority, majority)
        maj_leader = wait_leader(c, majority, "majority elects its own leader")
        assert c.status(maj_leader)["term"] > term
        # The minority can't gather 3 votes. Its old leader keeps believing it
        # leads in the old term (harmless: it can't commit anything).
        for n in minority:
            st = c.status(n)
            assert st["role"] != "leader" or st["term"] == term, st
        print("  ok: minority elected nobody new")

        print("6. heal -> exactly one leader, the higher term wins")
        c.heal()
        final = wait_leader(c, ids, "one agreed leader after heal")
        assert c.status(final)["term"] >= c.status(maj_leader)["term"]

        print("PASS")
    finally:
        c.stop()


if __name__ == "__main__":
    main()
