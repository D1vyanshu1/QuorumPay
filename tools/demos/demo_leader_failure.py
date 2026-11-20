#!/usr/bin/env python3
"""Demo 2: the leader crashes in the middle of a stream of payments.

    python3 tools/demos/demo_leader_failure.py

Four clients send payments continuously. The leader is SIGKILLed (no clean
shutdown). The others elect a new leader, clients retry there, and payments
continue; no acknowledged payment is lost or applied twice. The old leader
restarts, recovers from its own disk, and catches up.
"""

import threading
import time

from demo_util import Demo, check, say

import checker  # noqa: E402
import workload  # noqa: E402


def main():
    d = Demo(__doc__)
    c, cli = d.c, d.cli
    try:
        d.start()
        history = workload.History()
        names = workload.account_names(8)
        workload.setup_accounts(cli, history, names, fund=1000)
        say("8 accounts created and funded")

        d.step("4 clients send payments continuously")
        stop = threading.Event()
        threads = workload.run(cli, history, names, 4, stop, seed=1, nodes=d.ids)
        time.sleep(3)
        say(f"{workload.summarize(history.ops)['ok']} payments acknowledged so far")

        d.step("SIGKILL the leader")
        old = c.wait_leader()
        survivors = [n for n in d.ids if n != old]
        commit_before = max(c.status(n)["commit_index"] for n in survivors)
        t0 = time.time()
        c.kill([old])
        say(f"node {old} killed at commit index {commit_before}")
        new = elected = committed = None
        while time.time() - t0 < 10 and committed is None:
            try:
                views = {c.status(n)["leader"] for n in survivors}
                if len(views) == 1 and 0 not in views and old not in views:
                    new = views.pop()
                    elected = elected or time.time() - t0
                    if c.status(new)["commit_index"] > commit_before:
                        committed = time.time() - t0
            except Exception:  # noqa: BLE001 - a node mid-election
                pass
            time.sleep(0.01)
        check(committed is not None, f"node {new} took over: elected after {elected * 1000:.0f} ms, "
              f"first new payment committed after {committed * 1000:.0f} ms")
        ok_at_kill = workload.summarize(history.ops)["ok"]
        time.sleep(3)
        check(workload.summarize(history.ops)["ok"] > ok_at_kill,
              "clients kept going: their retries found the new leader")

        d.step(f"Restart node {old}: it recovers from its own WAL and catches up")
        c.start([old])
        time.sleep(2)
        stop.set()
        for t in threads:
            t.join()
        st = c.wait_converged(timeout=30)
        check(c.status(old)["applied_index"] == st["applied_index"],
              f"node {old} is back, as a follower, at the same applied index ({st['applied_index']})")

        d.step("Check every payment against what its client was told")
        problems, stats = checker.check(c, cli, history.ops, d.genesis)
        checker.report(problems, stats)
        check(not problems, f"all {stats['ok']} acknowledged payments are in the ledger exactly "
              "once; none that failed took effect")
        d.show_nodes()
        d.finish()
    finally:
        d.c.stop()


if __name__ == "__main__":
    main()
