#!/usr/bin/env python3
"""Concurrent clients doing random transfers; records every operation.

    tools/workload.py --keys-dir K --genesis G --setup --duration 30 \\
                      --out results/history.jsonl

Each client is a thread that sends transfers with ledger-cli from the
accounts it owns (so clients don't fight over each other's nonces) to random
accounts. Every operation is written to the history, Jepsen style:

    {"type": "invoke", "client": 2, "op": 17, "from": "acct02", "to": "acct05", "amount": 3, "t": ...}
    {"type": "ok",   "client": 2, "op": 17, "tx_id": ..., "nonce": 4, "index": 812, "status": "OK", "t": ...}

  ok    the transaction took effect (status OK, applied at `index`)
  fail  it definitely did NOT take effect: a final non-OK status (BAD_NONCE,
        INSUFFICIENT_FUNDS, ...), or it was never sent
  info  outcome unknown: the client timed out. It may or may not have
        committed; the checker finds out.

--setup first creates the accounts and funds them from the faucet (recorded
in the history too, so the checker knows every transaction).
"""

import argparse
import json
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ledger_cli import FAILED, OK, Cli  # noqa: E402


class History:
    """Append-only, thread-safe operation log (JSON lines), also kept in memory."""

    def __init__(self, path=None):
        self.ops = []
        self._lock = threading.Lock()
        self._next = 0
        self._f = None
        if path:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            self._f = open(path, "w")

    def new_op(self):
        with self._lock:
            self._next += 1
            return self._next

    def add(self, rec):
        rec["t"] = round(time.time(), 4)
        with self._lock:
            self.ops.append(rec)
            if self._f:
                self._f.write(json.dumps(rec) + "\n")
                self._f.flush()

    def close(self):
        if self._f:
            self._f.close()

    @staticmethod
    def load(path):
        with open(path) as f:
            return [json.loads(line) for line in f if line.strip()]


def account_names(n):
    return [f"acct{i:02d}" for i in range(n)]


def record_call(cli, history, client, kind, args, info, timeout_ms=10000, node=None):
    """Runs one ledger-cli transaction command and records invoke + outcome."""
    op = history.new_op()
    history.add({"type": "invoke", "client": client, "op": op, "kind": kind, **info})
    rc, r = cli.run(*args, timeout_ms=timeout_ms, node=node)
    done = {"client": client, "op": op, "kind": kind, **info,
            "tx_id": r.get("tx_id"), "nonce": r.get("nonce"), "status": r.get("status"),
            "index": r.get("index")}
    if rc == OK and r.get("status") == "OK":
        done["type"] = "ok"
    elif rc == FAILED and r.get("status") not in (None, "UNKNOWN"):
        done["type"] = "fail"
    elif r.get("tx_id"):
        done["type"] = "info"  # sent, outcome unknown (timed out)
    else:
        # Failed before a transaction was even signed (e.g. the nonce lookup
        # timed out): nothing was sent, so it can't have taken effect.
        done["type"] = "fail"
        done["status"] = "NOT_SENT"
        done["error"] = r.get("error") or r.get("stderr")
    history.add(done)
    return done


def setup_accounts(cli, history, names, fund):
    """Creates the accounts and gives each `fund` from the faucet."""
    for name in names:
        r = record_call(cli, history, 0, "create", ["create_account", name], {"account": name})
        assert r["type"] == "ok", r
    for name in names:
        r = record_call(cli, history, 0, "transfer", ["fund", name, fund],
                        {"from": "faucet", "to": name, "amount": fund})
        assert r["type"] == "ok", r


def run(cli, history, names, clients, stop, seed=0, max_amount=10, timeout_ms=5000, nodes=None):
    """Starts `clients` threads sending transfers until `stop` is set. Each
    operation starts at a random node of `nodes` (followers redirect), like
    clients spread over the cluster. Returns the threads (join them after
    setting `stop`)."""
    assert len(names) >= clients, "need at least one sender account per client"

    def client(cid):
        rng = random.Random(seed * 1000 + cid)
        mine = names[cid - 1::clients]  # each client owns some senders
        while not stop.is_set():
            frm = rng.choice(mine)
            to = rng.choice([n for n in names if n != frm])
            amount = rng.randint(1, max_amount)
            record_call(cli, history, cid, "transfer", ["transfer", frm, to, amount],
                        {"from": frm, "to": to, "amount": amount}, timeout_ms=timeout_ms,
                        node=rng.choice(nodes) if nodes else None)

    threads = [threading.Thread(target=client, args=(c,), daemon=True)
               for c in range(1, clients + 1)]
    for t in threads:
        t.start()
    return threads


def summarize(ops):
    out = {"ok": 0, "fail": 0, "info": 0}
    for o in ops:
        if o["type"] in out and o.get("kind") == "transfer":
            out[o["type"]] += 1
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build-dir", default="build")
    ap.add_argument("--keys-dir", required=True)
    ap.add_argument("--genesis", required=True)
    ap.add_argument("--clients", type=int, default=4)
    ap.add_argument("--accounts", type=int, default=8)
    ap.add_argument("--fund", type=int, default=1000)
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--setup", action="store_true", help="create + fund the accounts first")
    ap.add_argument("--out", default="results/history.jsonl")
    a = ap.parse_args()

    cli = Cli(a.build_dir, a.keys_dir, a.genesis)
    history = History(a.out)
    names = account_names(a.accounts)
    if a.setup:
        setup_accounts(cli, history, names, a.fund)
    stop = threading.Event()
    threads = run(cli, history, names, a.clients, stop, a.seed)
    time.sleep(a.duration)
    stop.set()
    for t in threads:
        t.join()
    history.close()
    s = summarize(history.ops)
    print(f"{s['ok']} ok, {s['fail']} fail, {s['info']} info -> {a.out}")


if __name__ == "__main__":
    main()
