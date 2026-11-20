"""Python side of ledger-cli: a thin wrapper, and a fresh-cluster helper.

Used by the demos, workload.py, chaos.py and the integration tests:

    keys, genesis = make_genesis(workdir, build_dir, supply=1_000_000)
    cli = Cli(build_dir, keys, genesis)
    rc, reply = cli.run("transfer", "alice", "bob", 5)   # reply = the CLI's JSON

Everything runs in its own keystore + genesis under `workdir`, so a user's
~/.ledger and config/genesis.json are never touched.
"""

import json
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ledger-cli exit codes
OK, ERROR, USAGE, FAILED = 0, 1, 2, 4


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
        """(exit code, parsed JSON reply). A reply that isn't JSON comes back
        as {"stderr": ...}."""
        timeout_ms = kw.get("timeout_ms", 10000)
        try:
            out = subprocess.run(self.cmd(*args, **kw), cwd=REPO, capture_output=True,
                                 text=True, timeout=timeout_ms / 1000 + 20)
        except subprocess.TimeoutExpired:
            return ERROR, {"stderr": "ledger-cli hung"}
        try:
            reply = json.loads(out.stdout.strip().splitlines()[-1])
        except (IndexError, json.JSONDecodeError):
            reply = {"stderr": out.stderr.strip()}
        return out.returncode, reply

    def ok(self, *args, **kw):
        rc, r = self.run(*args, **kw)
        assert rc == OK, f"{args}: exit {rc}, {r}"
        return r

    def balance(self, name, **kw):
        return self.ok("balance", name, **kw)


def make_genesis(workdir, build_dir="build", supply=1_000_000):
    """A faucet key in <workdir>/keys and a genesis giving it `supply`.
    Returns (keys_dir, genesis_path)."""
    keys = os.path.join(workdir, "keys")
    genesis = os.path.join(workdir, "genesis.json")
    Cli(build_dir, keys, genesis).ok("keygen", "faucet")
    subprocess.run([sys.executable, os.path.join(REPO, "tools", "make_genesis.py"),
                    "--faucet-key", os.path.join(keys, "faucet.key"),
                    "--supply", str(supply), "--out", genesis], check=True, capture_output=True)
    return keys, genesis


def genesis_supply(genesis_path):
    with open(genesis_path) as f:
        return sum(a["balance"] for a in json.load(f)["accounts"])
