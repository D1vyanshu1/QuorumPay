#!/usr/bin/env python3
"""Write config/genesis.json with one faucet account holding the whole supply.

    ledger-cli keygen faucet                      # once: creates the faucet key
    tools/make_genesis.py                         # faucet key from ~/.ledger/keys
    tools/make_genesis.py --faucet-key K --supply 1000000000 --chain-id quorumpay-dev \\
                          --out config/genesis.json

Only the PUBLIC key goes into genesis.json (it's shared by all nodes and safe
to commit). The faucet's secret stays in its key file; whoever holds it can
hand out money with ordinary signed transfers (`ledger-cli fund`, step 6).
"""

import argparse
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_KEYS = os.environ.get("LEDGER_KEYS_DIR", os.path.expanduser("~/.ledger/keys"))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--faucet-key", default=os.path.join(DEFAULT_KEYS, "faucet.key"))
    ap.add_argument("--supply", type=int, default=1_000_000_000)
    ap.add_argument("--chain-id", default="quorumpay-dev")
    ap.add_argument("--out", default=os.path.join(REPO, "config", "genesis.json"))
    a = ap.parse_args()

    if not 0 < a.supply < 2**64:
        sys.exit("supply must fit in a uint64")
    try:
        with open(a.faucet_key) as f:
            pub = json.load(f)["public_key"]
    except (OSError, KeyError, json.JSONDecodeError) as e:
        sys.exit(f"cannot read faucet key {a.faucet_key}: {e}\n"
                 f"create it first with: ledger-cli keygen faucet")
    if len(pub) != 64:
        sys.exit("faucet public key must be 64 hex chars")

    genesis = {
        "chain_id": a.chain_id,
        "accounts": [{"name": "faucet", "public_key_hex": pub, "balance": a.supply}],
    }
    with open(a.out, "w") as f:
        json.dump(genesis, f, indent=2)
        f.write("\n")
    print(f"wrote {a.out}: faucet {pub[:16]}... with {a.supply} units on chain {a.chain_id}")


if __name__ == "__main__":
    main()
