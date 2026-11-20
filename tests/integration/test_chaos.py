#!/usr/bin/env python3
"""Chaos + checker on 5 real ledger-node processes (step 8).

    python3 tests/integration/test_chaos.py [--build-dir build] [--duration 30] [--seed 42]

1. A seeded chaos run (random kills, leader kills, partitions, heals under a
   continuous transfer workload) must pass the checker.
2. The checker must be able to FAIL: the cluster is restarted from disk and
   the checker is fed doctored histories (an acknowledged transfer that never
   happened, a "failed" one that did, a wrong amount, a transfer nobody sent)
   and a genesis with the wrong balances. Each must be reported.
"""

import argparse
import copy
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools"))
import chaos  # noqa: E402
import checker  # noqa: E402
from cluster import Cluster  # noqa: E402
from ledger_cli import Cli  # noqa: E402
from workload import History  # noqa: E402


def expect_problem(desc, problems, needle):
    hits = [p for p in problems if needle in p]
    assert hits, f"checker missed: {desc} (problems: {problems[:5]})"
    print(f"  ok: caught {desc}: '{hits[0][:90]}'")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-dir", default="build")
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    print(f"1. chaos for {args.duration:.0f}s (seed {args.seed}) must pass the checker")
    problems, stats, out = chaos.run(args.duration, args.seed, build_dir=args.build_dir)
    assert not problems, problems
    assert stats["ok"] > 500, stats
    print(f"  ok: {stats['ok']} acknowledged transfers, all accounted for")

    print("2. restart the cluster from disk; the checker must catch doctored histories")
    genesis = os.path.join(out, "genesis.json")
    c = Cluster(build_dir=args.build_dir, genesis=genesis)
    cli = Cli(args.build_dir, os.path.join(out, "keys"), genesis)
    ops = History.load(os.path.join(out, "history.jsonl"))
    try:
        c.start()
        problems, _ = checker.check(c, cli, ops, genesis)
        assert not problems, problems
        print("  ok: still passes after a full restart (nothing lost on disk)")

        oks = [i for i, o in enumerate(ops) if o["type"] == "ok" and o["kind"] == "transfer"]

        lost = copy.deepcopy(ops)
        lost.append(dict(lost[oks[0]], op=10**9, tx_id="ab" * 32, index=1))
        expect_problem("an acknowledged transfer missing from the ledger",
                       checker.check(c, cli, lost, genesis)[0], "LOST")

        failed = copy.deepcopy(ops)
        failed[oks[1]].update(type="fail", status="BAD_NONCE")
        expect_problem("a 'failed' transfer that took effect",
                       checker.check(c, cli, failed, genesis)[0], "took effect at")

        amount = copy.deepcopy(ops)
        amount[oks[2]]["amount"] += 1
        expect_problem("a wrong amount", checker.check(c, cli, amount, genesis)[0],
                       "ledger has")

        unsent = [o for o in ops if o.get("tx_id") != ops[oks[3]]["tx_id"]]
        expect_problem("a transfer no client sent", checker.check(c, cli, unsent, genesis)[0],
                       "no client sent it")

        # Unknown outcomes ("info": the client timed out) must be resolved
        # from the ledger: one that took effect, one that didn't.
        fails = [i for i, o in enumerate(ops) if o["type"] == "fail" and o.get("tx_id")]
        assert fails, "the chaos run should have produced some failed transfers"
        unknown = copy.deepcopy(ops)
        unknown[oks[4]]["type"] = "info"
        unknown[fails[0]]["type"] = "info"
        p, s = checker.check(c, cli, unknown, genesis)
        assert not p, p
        assert s["info_resolved"] == {"applied": 1, "not_applied": 1}, s["info_resolved"]
        print("  ok: unknown outcomes resolved from the ledger (1 took effect, 1 didn't)")

        with open(genesis) as f:
            g = json.load(f)
        g["accounts"][0]["balance"] += 5
        wrong = os.path.join(out, "wrong-genesis.json")
        with open(wrong, "w") as f:
            json.dump(g, f)
        p = checker.check(c, cli, ops, wrong)[0]
        expect_problem("balances that don't follow from the genesis", p, "model:")
        expect_problem("money not conserved", p, "supply")
        print("PASS")
    finally:
        c.stop()


if __name__ == "__main__":
    main()
