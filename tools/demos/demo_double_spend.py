#!/usr/bin/env python3
"""Demo 1: double spends, overdrafts, replays and forgeries all fail.

    python3 tools/demos/demo_double_spend.py

alice has 100 and tries to spend it twice, over-spend it, replay an old
payment, and forge one. Only the honest payments go through, every node ends
with identical balances, and no money is created.
"""

import subprocess
import json

from demo_util import Demo, check, say
from ledger_cli import REPO


def main():
    d = Demo(__doc__)
    cli = d.cli
    try:
        leader = d.start()
        for name in ("alice", "bob", "carol"):
            cli.ok("create_account", name)
        cli.ok("fund", "alice", 100)
        say("alice, bob and carol exist; alice has 100")

        d.step("Double spend: two payments with the SAME nonce, sent to two nodes at once")
        n = cli.balance("alice")["next_nonce"]
        f1, f2 = [x for x in d.ids if x != leader][:2]
        say(f"alice signs 'pay bob 80' and 'pay carol 80', both with nonce {n};")
        say(f"one goes to node {f1}, the other to node {f2}, at the same moment")
        procs = {to: subprocess.Popen(cli.cmd("transfer", "alice", to, 80, "--nonce", n, node=nd),
                                      cwd=REPO, stdout=subprocess.PIPE, text=True)
                 for to, nd in (("bob", f1), ("carol", f2))}
        res = {to: json.loads(p.communicate(timeout=30)[0]) for to, p in procs.items()}
        for to, r in res.items():
            say(f"  pay {to:<5} 80 -> {r['status']}" + (f" (log index {r['index']})" if "index" in r else ""))
        winner = next((to for to, r in res.items() if r["status"] == "OK"), None)
        check(sorted(r["status"] for r in res.values()) == ["BAD_NONCE", "OK"],
              "exactly one went through; the other was refused: its nonce was already used")
        check(cli.balance("alice")["balance"] == 20, "alice paid once: 100 -> 20")

        d.step("Overdraft: two honest payments of 80 from 100")
        cli.ok("fund", "alice", 80)
        say("alice is topped back up to 100, then pays bob 80, then 80 again")
        first = cli.run("transfer", "alice", "bob", 80)[1]
        nonce_before = cli.balance("alice")["next_nonce"]
        second = cli.run("transfer", "alice", "bob", 80)[1]
        say(f"  first  -> {first['status']}")
        say(f"  second -> {second['status']}")
        check(first["status"] == "OK" and second["status"] == "INSUFFICIENT_FUNDS",
              "the second payment is refused: only 20 left")
        check(cli.balance("alice")["next_nonce"] == nonce_before + 1,
              "but it still used up its nonce, so this signed payment can never be replayed "
              "later, once alice has money again")

        d.step("Replay and forgery")
        again = cli.run("transfer", "alice", winner, 80, "--nonce", n)[1]
        say(f"  resend the exact signed payment from step 1 -> {again['status']} at index "
            f"{again.get('index')} (same tx_id as before: {again['tx_id'] == res[winner]['tx_id']})")
        check(again["tx_id"] == res[winner]["tx_id"] and again.get("index") == res[winner]["index"]
              and cli.balance("alice")["balance"] == 20,
              "a resend gets the ORIGINAL result back; the money does not move a second time")
        forged = cli.run("transfer", "alice", "bob", 5, "--tamper", 500)[1]
        say(f"  sign 'pay bob 5', change it to 500 on the way -> {forged['status']}")
        check(forged["status"] == "BAD_SIGNATURE" and forged.get("rejected"),
              "the altered payment no longer matches alice's signature: refused before it "
              "even reaches the log")

        d.step("Every node agrees")
        b = {n: cli.balance(n)["balance"] for n in ("alice", "bob", "carol")}
        say(f"balances: {b}")
        d.show_nodes()
        d.finish()
    finally:
        d.c.stop()


if __name__ == "__main__":
    main()
