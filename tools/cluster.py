#!/usr/bin/env python3
"""Start, stop, kill and partition a local ledger cluster.

    tools/cluster.py start              # all nodes (or: start 1 3)
    tools/cluster.py status             # role/term/leader + peer links per node
    tools/cluster.py kill 3             # SIGKILL: simulates a crash
    tools/cluster.py restart 3
    tools/cluster.py partition 1,2 3,4,5
    tools/cluster.py heal
    tools/cluster.py propose hello      # append to the Raft log via the leader
    tools/cluster.py read [node]        # linearizable read (default: the leader)
    tools/cluster.py stop               # SIGTERM all

Each node's log goes to data/node-N/node.log; its pid to data/node-N/pid.
Run from the repo root. Binaries are taken from build/ (override: --build-dir).
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Cluster:
    def __init__(self, config_path="config/cluster.json", build_dir="build"):
        self.config_path = os.path.join(REPO, config_path)
        self.build_dir = os.path.join(REPO, build_dir)
        with open(self.config_path) as f:
            self.config = json.load(f)
        self.nodes = {n["id"]: n for n in self.config["nodes"]}

    # ------------------------------------------------------------ processes
    def _data_dir(self, nid):
        return os.path.join(REPO, self.nodes[nid].get("data_dir", f"data/node-{nid}"))

    def _pid(self, nid):
        try:
            with open(os.path.join(self._data_dir(nid), "pid")) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)  # raises if not running
            return pid
        except (FileNotFoundError, ValueError, ProcessLookupError):
            return None

    def is_running(self, nid):
        return self._pid(nid) is not None

    def start(self, ids=None):
        for nid in ids or self.nodes:
            if self.is_running(nid):
                print(f"node {nid} already running")
                continue
            d = self._data_dir(nid)
            os.makedirs(d, exist_ok=True)
            log = open(os.path.join(d, "node.log"), "a")
            proc = subprocess.Popen(
                [os.path.join(self.build_dir, "ledger-node"),
                 "--config", self.config_path, "--id", str(nid)],
                cwd=REPO, stdout=log, stderr=log,
                start_new_session=True)  # Ctrl-C in this shell won't hit nodes
            with open(os.path.join(d, "pid"), "w") as f:
                f.write(str(proc.pid))
            print(f"node {nid} started (pid {proc.pid})")

    def _signal(self, ids, sig, verb):
        for nid in ids or self.nodes:
            pid = self._pid(nid)
            if pid is None:
                continue
            os.kill(pid, sig)
            for _ in range(100):  # wait up to 2s for it to exit
                try:
                    os.kill(pid, 0)
                    time.sleep(0.02)
                except ProcessLookupError:
                    break
            print(f"node {nid} {verb}")

    def stop(self, ids=None):
        self._signal(ids, signal.SIGTERM, "stopped")

    def kill(self, ids=None):
        self._signal(ids, signal.SIGKILL, "killed")

    def restart(self, ids):
        self.kill(ids)
        self.start(ids)

    # ------------------------------------------------------------ admin
    def admin(self, nid, *args):
        out = subprocess.run(
            [os.path.join(self.build_dir, "ledger-admin"),
             "--config", self.config_path, "--node", str(nid), *map(str, args)],
            capture_output=True, text=True, timeout=5)
        if out.returncode != 0:
            raise RuntimeError(f"node {nid}: {out.stderr.strip()}")
        return out.stdout.strip()

    def status(self, nid):
        return json.loads(self.admin(nid, "status"))

    def leader(self):
        """The node that the most live nodes name as leader, or None."""
        votes = {}
        for nid in self.nodes:
            if not self.is_running(nid):
                continue
            try:
                lead = self.status(nid)["leader"]
            except Exception:  # noqa: BLE001 - node may be mid-restart
                continue
            if lead:
                votes[lead] = votes.get(lead, 0) + 1
        return max(votes, key=votes.get) if votes else None

    def propose(self, data, node=None):
        """Propose on `node` (default: the leader). Returns the JSON reply;
        accepted=false with leader_hint if `node` isn't the leader."""
        nid = node or self.leader()
        if nid is None:
            raise RuntimeError("no leader")
        out = subprocess.run(
            [os.path.join(self.build_dir, "ledger-admin"),
             "--config", self.config_path, "--node", str(nid), "propose", data],
            capture_output=True, text=True, timeout=5)
        return json.loads(out.stdout)

    def read(self, node=None):
        """Linearizable read on `node` (default: the leader). Returns the JSON
        reply, or None if the node never answered (e.g. a cut-off old leader)."""
        nid = node or self.leader()
        out = subprocess.run(
            [os.path.join(self.build_dir, "ledger-admin"),
             "--config", self.config_path, "--node", str(nid), "read"],
            capture_output=True, text=True, timeout=5)
        return json.loads(out.stdout) if out.stdout.strip() else None

    def partition(self, *groups):
        """partition([1,2],[3,4,5]): every node blocks every node outside its group."""
        everyone = [n for g in groups for n in g]
        for g in groups:
            for a in g:
                for b in everyone:
                    if b not in g:
                        self.admin(a, "block", b)

    def heal(self, ids=None):
        for nid in ids or self.nodes:
            if self.is_running(nid):
                self.admin(nid, "unblock-all")


def print_status(cluster):
    for nid in cluster.nodes:
        if not cluster.is_running(nid):
            print(f"node {nid}: DOWN")
            continue
        try:
            s = cluster.status(nid)
        except Exception as e:  # noqa: BLE001 - show any failure to the user
            print(f"node {nid}: no reply ({e})")
            continue
        parts = []
        for p in s["peers"]:
            mark = "ok" if p["reachable"] else ("blocked" if p["blocked"] else "--")
            rtt = f" {p['rtt_us']}us" if p["reachable"] else ""
            parts.append(f"{p['id']}:{mark}{rtt}")
        leader = s["leader"] or "?"
        raft = (f"{s['role']:<9} term {s['term']:<3} leader {leader}  "
                f"log {s['last_log_index']:<4} commit {s['commit_index']:<4} "
                f"applied {s['applied_index']:<4} hash {s['state_hash'][:8]}")
        print(f"node {nid}: {raft} | peers " + "  ".join(parts))


def parse_ids(args):
    return [int(a) for a in args] or None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config/cluster.json")
    ap.add_argument("--build-dir", default="build")
    ap.add_argument("command",
                    choices=["start", "stop", "kill", "restart", "status", "partition", "heal",
                             "propose", "read"])
    ap.add_argument("args", nargs="*")
    a = ap.parse_args()

    c = Cluster(a.config, a.build_dir)
    if a.command == "start":
        c.start(parse_ids(a.args))
    elif a.command == "stop":
        c.stop(parse_ids(a.args))
    elif a.command == "kill":
        c.kill(parse_ids(a.args))
    elif a.command == "restart":
        c.restart(parse_ids(a.args))
    elif a.command == "status":
        print_status(c)
    elif a.command == "partition":
        if len(a.args) < 2:
            sys.exit("usage: partition 1,2 3,4,5")
        c.partition(*[[int(x) for x in g.split(",")] for g in a.args])
    elif a.command == "heal":
        c.heal()
    elif a.command == "read":
        print(json.dumps(c.read(int(a.args[0]) if a.args else None)))
    elif a.command == "propose":
        print(json.dumps(c.propose(" ".join(a.args) or "hello")))


if __name__ == "__main__":
    main()
