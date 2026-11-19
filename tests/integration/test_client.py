#!/usr/bin/env python3
"""ledger-cli against 5 real ledger-node processes (step 6: clients).

    python3 tests/integration/test_client.py [--build-dir build]

Accounts, funding and transfers through any node (followers redirect to the
leader); failed transfers report their status; a double spend (two transfers
with one nonce, sent to two nodes at once) takes effect exactly once; resending
a transaction returns its original result; killing the leader mid-transfer and
partitioning it away never loses or duplicates a payment. All nodes end with
the same state hash, and money is conserved.

Uses its own keystore and genesis (a fresh faucet) in a temp directory, so
~/.ledger and config/genesis.json are untouched.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools"))
from cluster import REPO, Cluster  # noqa: E402
from test_election import agreed_leader, wait_leader  # noqa: E402
from test_network import wait_for  # noqa: E402

SUPPLY = 1_000_000


class Cli:
    def __init__(self, build_dir, keys_dir, genesis):
        self.exe = os.path.join(REPO, build_dir, "ledger-cli")
        self.keys_dir = keys_dir
        self.genesis = genesis

    def cmd(self, *args, node=None, keys_dir=None, timeout_ms=10000):
        c = [self.exe, "--keys-dir", keys_dir or self.keys_dir, "--genesis", self.genesis,
             "--timeout", str(timeout_ms)]
        if node:
            c += ["--node", str(node)]
        return c + [str(a) for a in args]

    def run(self, *args, **kw):
        """(exit code, parsed JSON reply)"""
        out = subprocess.run(self.cmd(*args, **kw), cwd=REPO, capture_output=True, text=True,
                             timeout=30)
        try:
            reply = json.loads(out.stdout.strip().splitlines()[-1])
        except (IndexError, json.JSONDecodeError):
            reply = {"stderr": out.stderr.strip()}
        return out.returncode, reply

    def ok(self, *args, **kw):
        rc, r = self.run(*args, **kw)
        assert rc == 0, f"{args}: exit {rc}, {r}"
        return r

    def balance(self, name, **kw):
        return self.ok("balance", name, **kw)


def converged(c, ids):
    """All `ids` applied up to the leader's last entry with the same hash."""
    def pred():
        lead = agreed_leader(c, ids)
        if lead is None:
            return "no agreed leader"
        target = c.status(lead)["last_log_index"]
        st = {n: c.status(n) for n in ids}
        views = {(s["applied_index"], s["state_hash"], s["total_supply"]) for s in st.values()}
        ok = len(views) == 1 and next(iter(views))[0] == target
        return ok or f"target {target}, views {views}"
    wait_for(f"all {len(ids)} nodes: same applied index + state hash", pred, timeout=10)
    s = c.status(ids[0])
    assert s["total_supply"] == SUPPLY, s
    print(f"     applied index {s['applied_index']}, hash {s['state_hash'][:12]}, "
          f"supply {s['total_supply']}")


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
    c.stop()
    c.wipe()  # a fresh cluster: no Raft state from earlier runs
    try:
        c.start()
        leader = wait_leader(c, ids, "one agreed leader")
        follower = next(n for n in ids if n != leader)

        print(f"1. accounts + funding, sent to follower {follower} (it redirects)")
        for name in ("alice", "bob", "carol"):
            r = cli.ok("create_account", name, node=follower)
            assert r["status"] == "OK", r
        r1 = cli.ok("create_account", "alice")
        r2 = cli.ok("create_account", "alice")
        assert r1 == r2, (r1, r2)
        print("  ok: re-running create_account resends the same tx: same original result")
        other_keys = os.path.join(tmp, "other-keys")
        rc, r = cli.run("create_account", "alice", keys_dir=other_keys)
        assert rc == 4 and r["status"] == "DUPLICATE_ACCOUNT", (rc, r)
        print("  ok: another key can't take the name alice (DUPLICATE_ACCOUNT)")
        cli.ok("fund", "alice", 100)
        r = cli.ok("transfer", "alice", "bob", 30, node=follower)
        a, b = cli.balance("alice"), cli.balance("bob")
        assert (a["balance"], a["next_nonce"], b["balance"]) == (70, 2, 30), (a, b)
        print("  ok: alice 70 (next nonce 2), bob 30")
        t = cli.ok("tx", r["tx_id"])
        assert t["status"] == "OK" and t["index"] == r["index"], t
        h = cli.ok("history", "alice", "--limit", 1)
        assert [(x["direction"], x["counterparty"], x["amount"]) for x in h["records"]] == \
            [("out", "bob", 30)], h
        print("  ok: tx lookup and history agree")

        print("2. a failed transfer reports why, and still uses up its nonce")
        rc, r = cli.run("transfer", "alice", "bob", 1000)
        assert rc == 4 and r["status"] == "INSUFFICIENT_FUNDS" and "index" in r, (rc, r)
        a = cli.balance("alice")
        assert (a["balance"], a["next_nonce"]) == (70, 3), a
        print("  ok: INSUFFICIENT_FUNDS, balance unchanged, nonce 2 used up")

        n = a["next_nonce"]
        f1, f2 = [x for x in ids if x != leader][:2]
        print(f"3. double spend: alice sends 60 to bob AND 60 to carol with nonce {n}, "
              f"via nodes {f1} and {f2} at once")
        procs = [subprocess.Popen(cli.cmd("transfer", "alice", to, 60, "--nonce", n, node=nd),
                                  cwd=REPO, stdout=subprocess.PIPE, text=True)
                 for to, nd in (("bob", f1), ("carol", f2))]
        results = [json.loads(p.communicate(timeout=30)[0]) for p in procs]
        statuses = sorted(r["status"] for r in results)
        assert statuses == ["BAD_NONCE", "OK"], results
        winner = next(i for i, r in enumerate(results) if r["status"] == "OK")
        assert cli.balance("alice")["balance"] == 10
        print(f"  ok: exactly one won ({['bob', 'carol'][winner]}); alice 70 -> 10")
        to = ["bob", "carol"][winner]
        again = cli.ok("transfer", "alice", to, 60, "--nonce", n)
        assert again == results[winner], (again, results[winner])
        assert cli.balance("alice")["balance"] == 10
        print("  ok: resending the winner returns its original result, money moves once")
        converged(c, ids)
        stale = [cli.ok("balance", "alice", "--stale", node=x)["balance"]
                 for x in ids if x != leader]
        assert stale == [10] * 4, stale
        print("  ok: stale reads on every follower agree")

        print(f"4. kill leader {leader} mid-transfer; transfers keep working, none lost or doubled")
        bob0 = cli.balance("bob")["balance"]
        nonce0 = cli.balance("faucet")["next_nonce"]
        p = subprocess.Popen(cli.cmd("transfer", "faucet", "bob", 1), cwd=REPO,
                             stdout=subprocess.PIPE, text=True)
        time.sleep(0.005)
        c.kill([leader])
        r = json.loads(p.communicate(timeout=30)[0])
        assert r["status"] == "OK", r
        for _ in range(5):
            assert cli.ok("transfer", "faucet", "bob", 1)["status"] == "OK"
        survivors = [x for x in ids if x != leader]
        new_leader = agreed_leader(c, survivors)
        b, f = cli.balance("bob"), cli.balance("faucet")
        assert (b["balance"] - bob0, f["next_nonce"] - nonce0) == (6, 6), (bob0, b, nonce0, f)
        print(f"  ok: new leader {new_leader}; 6 transfers, bob +6, faucet used exactly 6 nonces")
        c.start([leader])
        converged(c, ids)

        leader = agreed_leader(c, ids)
        others = [x for x in ids if x != leader]
        minority, majority = [leader, others[0]], others[1:]
        print(f"5. partition {minority} | {majority}; send a transfer to the cut-off leader {leader}")
        c.partition(minority, majority)
        bob0 = cli.balance("bob", node=majority[0])["balance"]
        t0 = time.time()
        r = cli.ok("transfer", "faucet", "bob", 1, node=leader, timeout_ms=15000)
        assert r["status"] == "OK", r
        print(f"  ok: the client gave up on the silent leader and got OK from the majority "
              f"({time.time() - t0:.1f}s)")
        c.heal()
        converged(c, ids)
        assert cli.balance("bob")["balance"] == bob0 + 1
        print("  ok: after healing, bob got the 1 exactly once")

        print("PASS")
    finally:
        c.stop()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
