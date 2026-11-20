#!/usr/bin/env python3
"""Random faults under load, then the checker.

    tools/chaos.py --duration 300 --seed 7          # the plan's 5-minute run
    tools/chaos.py --duration 30                    # quick (random seed, printed)

Starts a fresh 5-node cluster with its own faucet, creates and funds accounts,
and runs the workload (tools/workload.py) the whole time. Every 2-5 seconds it
injects one random fault:

  kill a random node    kill the leader     restart the dead nodes
  2|3 partition         isolate the leader  heal the partition

never letting more than 2 of the 5 nodes be down or cut off from the
majority at once (Raft only promises progress with a majority). At the end it
heals everything, restarts every node, waits for them to converge, and runs
the checker on the full history.

Everything goes to results/chaos-<seed>/: the history, the actions taken,
the checker's report, and each node's log. The same seed replays the same
fault schedule (timing on a real machine still varies a little).
"""

import argparse
import json
import os
import random
import shutil
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import checker  # noqa: E402
import workload  # noqa: E402
from cluster import REPO, Cluster  # noqa: E402
from ledger_cli import Cli, make_genesis  # noqa: E402


class Chaos:
    def __init__(self, cluster, rng, log):
        self.c = cluster
        self.rng = rng
        self.log = log
        self.down = set()
        self.minority = set()  # cut off by the current partition (empty = healed)

    def unavailable(self, extra=()):
        return len(self.down | self.minority | set(extra))

    def options(self):
        ids = list(self.c.nodes)
        live = [n for n in ids if n not in self.down]
        opts = []
        if self.unavailable() < 2:
            opts.append(("kill", self.rng.choice(live)))
            opts.append(("kill_leader", None))
        if not self.minority and not self.down:
            opts.append(("partition", None))
        if not self.minority and self.unavailable() < 2:
            opts.append(("isolate_leader", None))
        if self.down and not self.minority:  # restart only when healed (see below)
            opts.append(("restart", None))
        if self.minority:
            opts.append(("heal", None))
        return opts

    def leader(self):
        try:
            return self.c.leader()
        except Exception:  # noqa: BLE001
            return None

    def act(self):
        kind, arg = self.rng.choice(self.options())
        ids = list(self.c.nodes)
        if kind == "kill_leader":
            arg = self.leader()
            if arg is None or arg in self.down or arg in self.minority:
                return self.note("skip", "no reachable leader")
            kind = "kill"
        if kind == "kill":
            self.c.kill([arg])
            self.down.add(arg)
            return self.note("kill", arg)
        if kind == "restart":
            nodes = sorted(self.down)
            self.c.start(nodes)
            self.down.clear()
            return self.note("restart", nodes)
        if kind in ("partition", "isolate_leader"):
            if kind == "partition":
                shuffled = ids[:]
                self.rng.shuffle(shuffled)
                minority = shuffled[:2]
            else:
                lead = self.leader()
                if lead is None:
                    return self.note("skip", "no leader to isolate")
                minority = [lead]
            majority = [n for n in ids if n not in minority]
            # Down nodes can't take block commands; a node that restarts while
            # partitioned would forget its blocks, which is why restarts wait
            # for a heal.
            for a in minority:
                for b in majority:
                    for x, y in ((a, b), (b, a)):
                        if x not in self.down:
                            self.c.admin(x, "block", y)
            self.minority = set(minority)
            return self.note(kind, {"minority": sorted(minority), "majority": majority})
        if kind == "heal":
            self.c.heal()
            self.minority = set()
            return self.note("heal", None)

    def note(self, action, detail):
        rec = {"t": round(time.time(), 3), "action": action, "detail": detail}
        self.log.write(json.dumps(rec) + "\n")
        self.log.flush()
        print(f"  {time.strftime('%H:%M:%S')}  {action:<15} {detail if detail is not None else ''}")


def run(duration, seed, clients=4, accounts=8, build_dir="build", out=None, quiet=False):
    """Runs one chaos session. Returns (problems, stats, out_dir)."""
    rng = random.Random(seed)
    out = out or os.path.join(REPO, "results", f"chaos-{seed}")
    shutil.rmtree(out, ignore_errors=True)
    os.makedirs(out)
    keys, genesis = make_genesis(out, build_dir)
    c = Cluster(build_dir=build_dir, genesis=genesis)
    cli = Cli(build_dir, keys, genesis)
    history = workload.History(os.path.join(out, "history.jsonl"))
    actions = open(os.path.join(out, "actions.jsonl"), "w")
    print(f"chaos: seed {seed}, {duration:.0f}s, {clients} clients, {accounts} accounts -> {out}")
    c.stop()
    c.wipe(keep_log=False)  # the logs copied into `out` cover only this run
    try:
        c.start()
        c.wait_leader()
        names = workload.account_names(accounts)
        # Little money and a short client timeout, on purpose: some transfers
        # fail (INSUFFICIENT_FUNDS) and some end UNKNOWN (a client gives up on
        # a cut-off leader), so the checker sees every kind of outcome.
        workload.setup_accounts(cli, history, names, fund=200)
        stop = threading.Event()
        threads = workload.run(cli, history, names, clients, stop, seed=seed, max_amount=40,
                               timeout_ms=2000, nodes=list(c.nodes))

        chaos = Chaos(c, rng, actions)
        end = time.time() + duration
        while time.time() < end:
            time.sleep(min(rng.uniform(2, 5), max(0, end - time.time())))
            if time.time() < end:
                chaos.act()

        print("  healing, restarting every node, waiting for the cluster to settle")
        c.heal()
        c.start(sorted(chaos.down))
        stop.set()
        for t in threads:
            t.join()
        history.close()
        c.wait_converged(timeout=60)
        problems, stats = checker.check(c, cli, history.ops, genesis)
        checker.report(problems, stats)
        with open(os.path.join(out, "report.json"), "w") as f:
            json.dump({"seed": seed, "duration": duration, "problems": problems, "stats": stats},
                      f, indent=2)
        return problems, stats, out
    finally:
        c.stop()
        actions.close()
        for n in c.nodes:  # keep the node logs with the run
            src = os.path.join(c._data_dir(n), "node.log")
            if os.path.exists(src):
                shutil.copy(src, os.path.join(out, f"node-{n}.log"))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--duration", type=float, default=300)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--clients", type=int, default=4)
    ap.add_argument("--accounts", type=int, default=8)
    ap.add_argument("--build-dir", default="build")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    seed = a.seed if a.seed is not None else random.randrange(1, 1_000_000)
    problems, _, out = run(a.duration, seed, a.clients, a.accounts, a.build_dir, a.out)
    print(f"seed {seed}: {'FAIL' if problems else 'PASS'} (details in {out})")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
