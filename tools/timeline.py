#!/usr/bin/env python3
"""One timeline from all the nodes' logs, to see a failure unfold.

    tools/timeline.py                       # data/node-*/node.log
    tools/timeline.py results/chaos-7       # the logs a chaos run saved
    tools/timeline.py --all                 # every event, not just the important ones

Each node writes JSON lines to its node.log. This merges them by timestamp
and prints one line per event:

    10:12:15.031  n3  raft_state        role=leader term=4 leader=3 last_log_index=812

Times are milliseconds since the first event; all nodes run on one machine,
so their clocks agree.
"""

import argparse
import glob
import json
import os
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

IMPORTANT = {"node_started", "recovered", "raft_state", "peer_reachable", "peer_unreachable",
             "fault_block", "fault_unblock", "fault_unblock_all", "snapshot_taken",
             "snapshot_installed", "snapshot_damaged", "shutdown", "fatal"}


def load(paths):
    events = []
    for p in paths:
        with open(p) as f:
            for line in f:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass  # e.g. a line cut by a SIGKILL
    events.sort(key=lambda e: e.get("ts_ms", 0))
    return events


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dir", nargs="?", help="a chaos results dir (default: data/node-*)")
    ap.add_argument("--all", action="store_true", help="show every event")
    ap.add_argument("--event", action="append", help="only these events (repeatable)")
    a = ap.parse_args()

    if a.dir:
        paths = sorted(glob.glob(os.path.join(a.dir, "node-*.log")))
    else:
        paths = sorted(glob.glob(os.path.join(REPO, "data", "node-*", "node.log")))
    if not paths:
        raise SystemExit("no node logs found")
    wanted = set(a.event) if a.event else (None if a.all else IMPORTANT)
    skip = {"ts_ms", "node", "level", "event"}
    for e in load(paths):
        if wanted is not None and e.get("event") not in wanted:
            continue
        ts = e.get("ts_ms", 0)
        clock = time.strftime("%H:%M:%S", time.localtime(ts / 1000)) + f".{ts % 1000:03d}"
        fields = " ".join(f"{k}={v}" for k, v in e.items() if k not in skip)
        print(f"{clock}  n{e.get('node', '?')}  {e.get('event', '?'):<18} {fields}")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:  # e.g. piped into `head`
        pass
