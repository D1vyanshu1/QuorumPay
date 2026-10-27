#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "raft/raft_core.h"

namespace ledger::sim {

struct SimOptions {
  // Delivery delay per message, in ticks (1 tick = 10ms of virtual time).
  // Random delays also reorder messages.
  int min_delay = 1;
  int max_delay = 3;
  double drop_rate = 0.0;       // chance each message is lost
  double duplicate_rate = 0.0;  // chance each message is delivered twice

  // Election timeout range handed to every RaftCore.
  int election_timeout_min = 15;
  int election_timeout_max = 30;
};

// A whole Raft cluster in one process, driven by a virtual clock.
//
// Every source of nondeterminism (timeouts, delays, drops, duplicates) comes
// from RNGs derived from one seed, and nodes are processed in id order, so a
// seed replays exactly. A failing seed is a reproducible bug.
//
// The simulator handles each Ready the way the real node will: it "persists"
// hard state and entries into a per-node Disk first, then queues messages.
// crash() throws away the RaftCore (all volatile state); restart() builds a new
// one from the Disk, just like a real process recovering from its files.
class SimCluster {
 public:
  SimCluster(int nodes, uint64_t seed, SimOptions opts = {});

  // Advances virtual time by one tick: ticks every live node, then delivers
  // every message that is due.
  void tick();
  void run(int ticks);
  // Runs until exactly one stable leader exists (see stable_leader()) or
  // `max_ticks` pass. Returns the number of ticks taken, or -1 on timeout.
  int run_until_leader(int max_ticks);

  void crash(NodeId id);
  void restart(NodeId id);
  bool is_up(NodeId id) const { return nodes_.at(id) != nullptr; }

  // Messages flow only between nodes in the same group. Nodes not listed are
  // isolated. In-flight messages across groups are dropped on delivery.
  void partition(const std::vector<std::vector<NodeId>>& groups);
  void heal();

  raft::RaftCore& node(NodeId id) { return *nodes_.at(id); }
  std::vector<NodeId> ids() const;

  // Live nodes that currently think they are leader.
  std::vector<NodeId> leaders() const;
  // A leader whose term is the highest among live nodes and that every live
  // node in its group recognizes; kNoNode if there isn't one.
  NodeId stable_leader(const std::vector<NodeId>& group) const;

  // Safety violations seen so far (empty = good). Checked after every step.
  const std::vector<std::string>& violations() const { return violations_; }

  uint64_t now() const { return now_; }

 private:
  struct Disk {
    raft::HardState hard_state;
    std::vector<pb::LogEntry> log;
  };
  std::unique_ptr<raft::RaftCore> make_core(NodeId id);
  void process_ready(NodeId id);
  void enqueue(const pb::RaftMessage& msg);
  bool can_talk(NodeId a, NodeId b) const;
  void check_invariants(NodeId id);

  SimOptions opts_;
  uint64_t seed_;
  std::mt19937_64 net_rng_;
  uint64_t now_ = 0;
  uint64_t seq_ = 0;    // tie-breaker so same-tick messages keep a fixed order
  int restarts_ = 0;    // varies the RNG seed of restarted nodes

  std::map<NodeId, std::unique_ptr<raft::RaftCore>> nodes_;  // null = crashed
  std::map<NodeId, Disk> disks_;
  std::map<NodeId, int> group_;  // empty = fully connected
  // Keyed by (deliver_at, seq): a min-ordered queue that is fully deterministic.
  std::map<std::tuple<uint64_t, uint64_t>, pb::RaftMessage> queue_;

  std::map<Term, NodeId> leader_of_term_;  // for Election Safety
  std::vector<std::string> violations_;
};

}  // namespace ledger::sim
