#!/usr/bin/env python3
"""Jepsen-style checker: does the cluster's final state agree with what the
clients were told?

    tools/checker.py --history results/history.jsonl --keys-dir K --genesis G

Run it after a workload (and after chaos), once the cluster is healed. It
checks (project_details.md §6.9):

  1. Convergence: every live node has applied the same entries and holds the
     same state hash.
  2. Acknowledged = applied: every `ok` transfer appears in the ledger exactly
     once, at the index the client was told; no `fail` transfer appears;
     `info` (unknown) ones may go either way, and are reported. Nothing
     appears that no client sent.
  3. Independent model: replaying the applied transfers in log order on the
     genesis balances, with a separate Python implementation of the rules
     (no overdraft, both accounts exist), gives exactly the cluster's balances.
  4. Conservation: the balances add up to the genesis supply, on every node.
  5. No double spend: no (sender, nonce) took effect twice.
  6. Durability: covered by 2. The history spans every crash, so an `ok`
     transfer lost to a crash would be missing at the end.

What it does NOT prove: that reads were linearizable (it only checks final
state and acknowledged writes), or anything about Byzantine (lying) nodes.
"""

import argparse
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cluster import Cluster  # noqa: E402
from ledger_cli import Cli, genesis_supply  # noqa: E402
from workload import History  # noqa: E402


def check(cluster, cli, ops, genesis_path):
    """Returns (problems, stats). problems is empty when everything holds."""
    problems = []
    stats = {}
    supply = genesis_supply(genesis_path)
    with open(genesis_path) as f:
        genesis = {a["name"]: a["balance"] for a in json.load(f)["accounts"]}

    # ---- 1. convergence
    st = cluster.wait_converged(timeout=30)
    nodes = {n: cluster.status(n) for n in cluster.running()}
    views = {(s["applied_index"], s["state_hash"]) for s in nodes.values()}
    if len(views) != 1:
        problems.append(f"nodes disagree: {views}")
    stats["applied_index"] = st["applied_index"]
    stats["nodes"] = len(nodes)

    # ---- the cluster's ledger, from its point of view
    created = {o["account"] for o in ops if o["kind"] == "create" and o["type"] == "ok"}
    names = sorted(set(genesis) | created)
    if st["accounts"] != len(names):
        problems.append(f"cluster has {st['accounts']} accounts, clients made {len(names)}")
    balance, nonce, applied = {}, {}, {}  # applied: tx_id -> (index, from, to, amount)
    incoming = Counter()
    for name in names:
        a = cli.balance(name)
        balance[name], nonce[name] = a["balance"], a["next_nonce"]
        for h in cli.ok("history", name)["records"]:
            key = (h["tx_id"], h["index"], h["amount"])
            if h["direction"] == "out":
                if h["tx_id"] in applied:
                    problems.append(f"tx {h['tx_id'][:12]} applied twice")
                applied[h["tx_id"]] = (h["index"], name, h["counterparty"], h["amount"])
            else:
                incoming[key + (h["counterparty"], name)] += 1
    for tx, (index, frm, to, amount) in applied.items():
        if incoming[(tx, index, amount, frm, to)] != 1:
            problems.append(f"tx {tx[:12]}: sender's history and receiver's disagree")
    stats["applied_transfers"] = len(applied)

    # ---- 2. what clients were told vs what the ledger holds
    sent = {}
    info = {"applied": 0, "not_applied": 0}
    for o in ops:
        if o["kind"] != "transfer" or o["type"] == "invoke" or not o.get("tx_id"):
            continue
        sent[o["tx_id"]] = o
        got = applied.get(o["tx_id"])
        if o["type"] == "ok":
            if not got:
                problems.append(f"LOST: op {o['op']} was acknowledged OK but isn't in the ledger")
            elif got != (o["index"], o["from"], o["to"], o["amount"]):
                problems.append(f"op {o['op']}: told index {o['index']}, ledger has {got}")
        elif o["type"] == "fail" and got:
            problems.append(f"op {o['op']} was told {o['status']} but took effect at {got[0]}")
        elif o["type"] == "info":
            info["applied" if got else "not_applied"] += 1
    for tx in applied:
        if tx not in sent:
            problems.append(f"tx {tx[:12]} took effect but no client sent it")
    stats["info_resolved"] = info

    # ---- 3. independent model: replay in log order with our own rules
    model = {n: genesis.get(n, 0) for n in names}
    for tx, (index, frm, to, amount) in sorted(applied.items(), key=lambda kv: kv[1][0]):
        if frm not in model or to not in model or frm == to or amount <= 0:
            problems.append(f"model: tx at {index} is not a valid transfer ({frm} -> {to})")
            continue
        if model[frm] < amount:
            problems.append(f"model: overdraft at index {index}: {frm} has {model[frm]}, "
                            f"sent {amount}")
        model[frm] -= amount
        model[to] += amount
    for n in names:
        if model[n] != balance[n]:
            problems.append(f"model: {n} should have {model[n]}, cluster says {balance[n]}")

    # ---- 4. conservation
    if sum(balance.values()) != supply:
        problems.append(f"balances add up to {sum(balance.values())}, supply is {supply}")
    for n, s in nodes.items():
        if s["total_supply"] != supply:
            problems.append(f"node {n} reports supply {s['total_supply']}")

    # ---- 5. no double spend: one effect per (sender, nonce)
    spends = Counter((frm, sent[tx]["nonce"]) for tx, (_, frm, _, _) in applied.items()
                     if tx in sent)
    for (frm, n), k in spends.items():
        if k > 1:
            problems.append(f"DOUBLE SPEND: {frm} nonce {n} took effect {k} times")
    for n in names:
        used = sum(1 for (_, frm, _, _) in applied.values() if frm == n)
        first = 1 if n in created else 0  # a create uses nonce 0
        if used > nonce[n] - first:
            problems.append(f"{n}: {used} transfers applied but only {nonce[n] - first} nonces used")

    stats["ok"] = sum(1 for o in ops if o["kind"] == "transfer" and o["type"] == "ok")
    stats["fail"] = sum(1 for o in ops if o["kind"] == "transfer" and o["type"] == "fail")
    stats["info"] = sum(1 for o in ops if o["kind"] == "transfer" and o["type"] == "info")
    return problems, stats


def report(problems, stats):
    print(f"checked: {stats['nodes']} nodes at applied index {stats['applied_index']}; "
          f"{stats['applied_transfers']} transfers in the ledger; client ops: "
          f"{stats['ok']} ok, {stats['fail']} fail, {stats['info']} unknown "
          f"({stats['info_resolved']['applied']} of those took effect)")
    if problems:
        print(f"FAIL: {len(problems)} problem(s)")
        for p in problems[:50]:
            print("  - " + p)
    else:
        print("PASS: converged, every acknowledged transfer applied exactly once, nothing "
              "failed took effect, the model agrees, money conserved, no double spends")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--history", required=True)
    ap.add_argument("--keys-dir", required=True)
    ap.add_argument("--genesis", required=True)
    ap.add_argument("--build-dir", default="build")
    a = ap.parse_args()
    cluster = Cluster(build_dir=a.build_dir, genesis=a.genesis)
    problems, stats = check(cluster, Cli(a.build_dir, a.keys_dir, a.genesis),
                            History.load(a.history), a.genesis)
    report(problems, stats)
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
