#include "sim/sim_network.h"

namespace ledger::sim {

SimCluster::SimCluster(int nodes, uint64_t seed, SimOptions opts)
    : opts_(opts), seed_(seed), net_rng_(seed) {
  for (NodeId id = 1; id <= static_cast<NodeId>(nodes); ++id) disks_[id] = Disk{};
  for (NodeId id = 1; id <= static_cast<NodeId>(nodes); ++id) nodes_[id] = make_core(id);
}

std::unique_ptr<raft::RaftCore> SimCluster::make_core(NodeId id) {
  raft::RaftConfig cfg;
  cfg.id = id;
  for (const auto& [peer, _] : disks_)
    if (peer != id) cfg.peers.push_back(peer);
  cfg.election_timeout_min = opts_.election_timeout_min;
  cfg.election_timeout_max = opts_.election_timeout_max;
  // Distinct per node and per restart, but fixed by the cluster seed.
  cfg.seed = seed_ * 1000003 + id * 7919 + static_cast<uint64_t>(restarts_);
  const Disk& d = disks_[id];
  return std::make_unique<raft::RaftCore>(cfg, d.hard_state, d.log);
}

std::vector<NodeId> SimCluster::ids() const {
  std::vector<NodeId> out;
  for (const auto& [id, _] : disks_) out.push_back(id);
  return out;
}

// ---------------------------------------------------------------- time

void SimCluster::tick() {
  ++now_;
  for (auto& [id, core] : nodes_) {
    if (!core) continue;
    core->tick();
    process_ready(id);
  }
  // Deliver everything due by now. Delivering can queue replies, but those
  // have delay >= 1, so they land after now_ and the loop ends.
  while (!queue_.empty() && std::get<0>(queue_.begin()->first) <= now_) {
    pb::RaftMessage msg = std::move(queue_.begin()->second);
    queue_.erase(queue_.begin());
    NodeId to = msg.to();
    if (!nodes_.at(to) || !can_talk(msg.from(), to)) continue;  // lost
    nodes_.at(to)->step(msg);
    process_ready(to);
  }
}

void SimCluster::run(int ticks) {
  for (int i = 0; i < ticks; ++i) tick();
}

int SimCluster::run_until_leader(int max_ticks) {
  std::vector<NodeId> all = ids();
  for (int i = 1; i <= max_ticks; ++i) {
    tick();
    if (stable_leader(all) != kNoNode) return i;
  }
  return -1;
}

// ---------------------------------------------------------------- the host side

void SimCluster::process_ready(NodeId id) {
  raft::RaftCore& core = *nodes_.at(id);
  if (!core.has_ready()) return;
  raft::Ready rd = core.ready();

  // 1. Persist. In the real node this is a write + fsync.
  Disk& disk = disks_[id];
  if (rd.hard_state) disk.hard_state = *rd.hard_state;
  for (auto& e : rd.entries) disk.log.push_back(std::move(e));

  // 2. Only then send.
  for (const auto& m : rd.messages) enqueue(m);

  check_invariants(id);
}

void SimCluster::enqueue(const pb::RaftMessage& msg) {
  std::uniform_real_distribution<double> coin(0.0, 1.0);
  std::uniform_int_distribution<int> delay(opts_.min_delay, opts_.max_delay);
  if (coin(net_rng_) < opts_.drop_rate) return;
  int copies = coin(net_rng_) < opts_.duplicate_rate ? 2 : 1;
  for (int i = 0; i < copies; ++i)
    queue_.emplace(std::make_tuple(now_ + static_cast<uint64_t>(delay(net_rng_)), seq_++), msg);
}

// ---------------------------------------------------------------- faults

void SimCluster::crash(NodeId id) { nodes_.at(id).reset(); }

void SimCluster::restart(NodeId id) {
  ++restarts_;
  nodes_.at(id) = make_core(id);
}

void SimCluster::partition(const std::vector<std::vector<NodeId>>& groups) {
  group_.clear();
  int g = 0;
  for (NodeId id : ids()) group_[id] = -1 - static_cast<int>(id);  // isolated by default
  for (const auto& members : groups) {
    for (NodeId id : members) group_[id] = g;
    ++g;
  }
}

void SimCluster::heal() { group_.clear(); }

bool SimCluster::can_talk(NodeId a, NodeId b) const {
  return group_.empty() || group_.at(a) == group_.at(b);
}

// ---------------------------------------------------------------- observation

std::vector<NodeId> SimCluster::leaders() const {
  std::vector<NodeId> out;
  for (const auto& [id, core] : nodes_)
    if (core && core->role() == raft::Role::kLeader) out.push_back(id);
  return out;
}

NodeId SimCluster::stable_leader(const std::vector<NodeId>& group) const {
  NodeId leader = kNoNode;
  Term term = 0;
  for (NodeId id : group) {
    const auto& core = nodes_.at(id);
    if (!core) continue;
    if (core->role() == raft::Role::kLeader) {
      if (leader != kNoNode) return kNoNode;  // two leaders (different terms)
      leader = id;
      term = core->term();
    }
  }
  if (leader == kNoNode) return kNoNode;
  for (NodeId id : group) {
    const auto& core = nodes_.at(id);
    if (core && (core->term() != term || core->leader() != leader)) return kNoNode;
  }
  return leader;
}

void SimCluster::check_invariants(NodeId id) {
  const raft::RaftCore& core = *nodes_.at(id);
  if (core.role() != raft::Role::kLeader) return;

  // Election Safety: at most one leader per term, ever. The map remembers
  // past terms too, so this also catches a second leader after a crash.
  auto [it, inserted] = leader_of_term_.emplace(core.term(), id);
  if (!inserted && it->second != id) {
    violations_.push_back("two leaders in term " + std::to_string(core.term()) + ": " +
                          std::to_string(it->second) + " and " + std::to_string(id) +
                          " (seed " + std::to_string(seed_) + ", tick " +
                          std::to_string(now_) + ")");
  }
}

}  // namespace ledger::sim
