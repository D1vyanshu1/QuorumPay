#!/usr/bin/env python3
"""Demo 3: a network partition, with the leader on the small side.

    python3 tools/demos/demo_partition.py

The network splits {leader, one follower} | {three others}. The old leader
can still accept a write, but can never commit it, and refuses to answer reads
(it can't prove its data is current). The majority elects a new leader and
keeps going. After the heal the old leader steps down, its uncommitted write
is thrown away, and everyone converges. Then pre-vote: a follower cut off on
its own can't disturb the leader when it comes back.
"""

import subprocess
import threading
import time

from demo_util import Demo, check, say

import checker  # noqa: E402
import workload  # noqa: E402


def read_on(d, node):
    """A linearizable read on `node`; None if it never answers (2s)."""
    out = subprocess.run([d.c.build_dir + "/ledger-admin", "--config", d.c.config_path,
                          "--node", str(node), "read"], capture_output=True, text=True)
    return out.stdout.strip() or None


def main():
    d = Demo(__doc__)
    c, cli = d.c, d.cli
    try:
        d.start()
        history = workload.History()
        names = workload.account_names(8)
        workload.setup_accounts(cli, history, names, fund=1000)
        stop = threading.Event()
        threads = workload.run(cli, history, names, 4, stop, seed=3, nodes=d.ids, timeout_ms=4000)
        time.sleep(2)

        old = c.wait_leader()
        buddy = next(n for n in d.ids if n != old)
        minority, majority = [old, buddy], [n for n in d.ids if n not in (old, buddy)]
        old_term = c.status(old)["term"]
        d.step(f"Partition {minority} | {majority}: the leader (node {old}) is on the small side")
        c.partition(minority, majority)

        new = None
        deadline = time.time() + 10
        while time.time() < deadline and new is None:
            views = {c.status(n)["leader"] for n in majority}
            if len(views) == 1 and 0 not in views and old not in views:
                new = views.pop()
            time.sleep(0.05)
        check(new is not None, f"the majority elected node {new} (term {c.status(new)['term']})")
        check(c.status(old)["role"] == "leader" and c.status(old)["term"] == old_term,
              f"node {old} doesn't know yet: it still thinks it leads, in old term {old_term}")

        p = c.propose("written-to-the-old-leader", node=old)
        commit = c.status(old)["commit_index"]
        say(f"a write to node {old} is accepted into its log at index {p['index']} ...")
        time.sleep(2)
        check(c.status(old)["commit_index"] == commit < p["index"],
              "... but never commits: 2 of 5 nodes is not a majority")
        check(read_on(d, old) is None,
              f"a read on node {old} gets no answer at all, rather than possibly stale data")
        before = workload.summarize(history.ops)["ok"]
        time.sleep(2)
        check(workload.summarize(history.ops)["ok"] > before,
              f"meanwhile payments keep committing on the majority side (node {new})")

        d.step("Heal the partition")
        c.heal()
        stop.set()
        for t in threads:
            t.join()
        st = c.wait_converged(timeout=30)
        s_old = c.status(old)
        check(s_old["role"] == "follower" and s_old["term"] > old_term,
              f"node {old} saw the higher term and stepped down to follower")
        check(s_old["applied_index"] == st["applied_index"],
              f"its uncommitted write at index {p['index']} was replaced by the majority's "
              "entries; it now holds exactly their log")
        problems, stats = checker.check(c, cli, history.ops, d.genesis)
        checker.report(problems, stats)
        check(not problems, "every acknowledged payment applied once, none lost in the split")

        d.step("Pre-vote: a follower cut off on its own comes back without disruption")
        leader = c.wait_leader()
        term = c.status(leader)["term"]
        lone = next(n for n in d.ids if n != leader)
        c.isolate(lone)
        say(f"node {lone} is cut off for 3 s (10+ election timeouts)")
        time.sleep(3)
        s = c.status(lone)
        check(s["term"] == term,
              f"it is a {s['role']}: it asked for pre-votes, nobody answered, so it never "
              f"started a real election and its term is still {term}")
        c.heal()
        time.sleep(1)
        check(c.wait_leader() == leader and c.status(leader)["term"] == term,
              f"after the heal node {leader} is still leader in term {term}: undisturbed")
        d.show_nodes()
        d.finish()
    finally:
        d.c.stop()


if __name__ == "__main__":
    main()
