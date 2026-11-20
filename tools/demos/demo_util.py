"""Shared setup and printing for the three demos.

Each demo starts a fresh 5-node cluster with its own faucet in a temporary
directory (your ~/.ledger and config/genesis.json are untouched), tells what
it is doing step by step, checks what it claims, and stops the cluster at the
end. Exit code 0 = every claim held.
"""

import argparse
import os
import shutil
import sys
import tempfile

TOOLS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, TOOLS)
from cluster import Cluster  # noqa: E402,F401
from ledger_cli import Cli, genesis_supply, make_genesis  # noqa: E402


class Demo:
    def __init__(self, description):
        ap = argparse.ArgumentParser(description=description)
        ap.add_argument("--build-dir", default="build")
        self.args = ap.parse_args()
        self.tmp = tempfile.mkdtemp(prefix="ledger-demo-")
        self.keys, self.genesis = make_genesis(self.tmp, self.args.build_dir)
        self.supply = genesis_supply(self.genesis)
        self.c = Cluster(build_dir=self.args.build_dir, genesis=self.genesis)
        self.cli = Cli(self.args.build_dir, self.keys, self.genesis)
        self.ids = sorted(self.c.nodes)
        self.n = 0

    def start(self):
        say(f"Starting a fresh 5-node cluster (faucet holds {self.supply:,} units)")
        self.c.stop()
        self.c.wipe()
        _quiet(self.c.start)
        leader = self.c.wait_leader()
        say(f"node {leader} is the leader")
        return leader

    def step(self, title):
        self.n += 1
        print(f"\n\033[1m{self.n}. {title}\033[0m")

    def show_nodes(self):
        """Every node's view: role, term, applied index, state hash."""
        st = self.c.wait_converged(timeout=30)
        print("     node  role        term  applied  state hash        supply")
        hashes = set()
        for n in self.ids:
            s = self.c.status(n)
            hashes.add(s["state_hash"])
            print(f"     {n:>4}  {s['role']:<10}  {s['term']:>4}  {s['applied_index']:>7}  "
                  f"{s['state_hash'][:16]}  {s['total_supply']:,}")
        check(len(hashes) == 1 and st["total_supply"] == self.supply,
              "all 5 nodes hold the same state, and no money was created or destroyed")

    def finish(self):
        _quiet(self.c.stop)
        shutil.rmtree(self.tmp, ignore_errors=True)
        print("\n\033[1;32mDEMO PASSED\033[0m: every claim above held")


def say(msg):
    print(f"   {msg}")


def check(cond, msg):
    if not cond:
        print(f"   \033[1;31mFAILED\033[0m: {msg}")
        raise SystemExit(1)
    print(f"   \033[32m✓\033[0m {msg}")


def _quiet(fn, *a):
    """Runs a Cluster method without its per-node 'started/stopped' lines."""
    import contextlib
    import io
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a)
