#!/usr/bin/env python3
"""Inject faults into a running local cluster.

    tools/faults.py partition 1,2 3,4,5     # each side can only talk among itself
    tools/faults.py isolate 3               # cut one node off (or: isolate leader)
    tools/faults.py heal                    # undo all partitions
    tools/faults.py kill 2                  # SIGKILL (or: kill leader)
    tools/faults.py restart 2

Partitions are set through each node's admin port (BlockPeer), so they work
even while the peer network is split: messages to and from a blocked peer
are silently dropped, like a real network failure. "leader" means the node
the running nodes currently agree is leader.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cluster import Cluster  # noqa: E402


def node_arg(c, s):
    if s == "leader":
        n = c.leader()
        if n is None:
            sys.exit("no leader right now")
        print(f"leader is node {n}")
        return n
    return int(s)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config/cluster.json")
    ap.add_argument("--build-dir", default="build")
    ap.add_argument("--genesis", help="genesis for restarted nodes (default config/genesis.json)")
    ap.add_argument("command", choices=["partition", "isolate", "heal", "kill", "restart"])
    ap.add_argument("args", nargs="*")
    a = ap.parse_args()
    c = Cluster(a.config, a.build_dir, a.genesis)

    if a.command == "partition":
        if len(a.args) < 2:
            sys.exit("usage: partition 1,2 3,4,5")
        c.partition(*[[int(x) for x in g.split(",")] for g in a.args])
    elif a.command == "isolate":
        c.isolate(node_arg(c, a.args[0]))
    elif a.command == "heal":
        c.heal()
    elif a.command == "kill":
        c.kill([node_arg(c, a.args[0])])
    elif a.command == "restart":
        c.start([int(x) for x in a.args])


if __name__ == "__main__":
    main()
