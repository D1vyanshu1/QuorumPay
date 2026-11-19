#include "sim/sim_network.h"

#include <algorithm>

namespace ledger::sim {

SimCluster::SimCluster(int nodes, uint64_t seed, SimOptions opts)
    : opts_(opts), seed_(seed), net_rng_(seed) {
  for (NodeId id = 1; id <= static_cast<NodeId>(nodes); ++id) {
    disks_[id] = Disk{};
    applied_index_[id] = 0;
    state_[id] = std::make_unique<ledger::StateMachine>(opts_.genesis);
  }
  for (NodeId id = 1; id <= static_cast<NodeId>(nodes); ++id) nodes_[id] = make_core(id);
}

std::unique_ptr<raft::RaftCore> SimCluster::make_core(NodeId id) {
  raft::RaftConfig cfg;
  cfg.id = id;
  for (const auto& [peer, _] : disks_)
    if (peer != id) cfg.peers.push_back(peer);
  cfg.election_timeout_min = opts_.election_timeout_min;
  cfg.election_timeout_max = opts_.election_timeout_max;
  cfg.max_entries_per_append = 8;  // small, so catch-up takes several batches
  cfg.compaction_margin = opts_.compaction_margin;
  cfg.snapshot_chunk_bytes = opts_.snapshot_chunk_bytes;
  // Distinct per node and per restart, but fixed by the cluster seed.
  cfg.seed = seed_ * 1000003 + id * 7919 + static_cast<uint64_t>(restarts_);
  const Disk& d = disks_[id];
  return std::make_unique<raft::RaftCore>(cfg, d.hard_state, d.log, d.snapshot);
}

void SimCluster::reset_state(NodeId id) {
  const Disk& d = disks_.at(id);
  if (d.snapshot) {
    state_.at(id) = std::make_unique<ledger::StateMachine>(
        ledger::StateMachine::restore(d.snapshot->data));
    applied_index_.at(id) = d.snapshot->index;
  } else {
    state_.at(id) = std::make_unique<ledger::StateMachine>(opts_.genesis);
    applied_index_.at(id) = 0;
  }
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
  check_log_matching();
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

int SimCluster::run_until_converged(int max_ticks) {
  std::vector<NodeId> all = ids();
  for (int i = 1; i <= max_ticks; ++i) {
    tick();
    NodeId leader = stable_leader(all);
    if (leader == kNoNode) continue;
    LogIndex target = nodes_.at(leader)->log().last_index();
    bool done = target > 0;
    for (const auto& [id, core] : nodes_) {
      if (core && applied_index_.at(id) != target) done = false;
    }
    if (done) return i;
  }
  return -1;
}

std::optional<raft::ProposeResult> SimCluster::propose(const std::string& data) {
  NodeId best = kNoNode;
  for (NodeId id : leaders()) {
    if (best == kNoNode || nodes_.at(id)->term() > nodes_.at(best)->term()) best = id;
  }
  if (best == kNoNode) return std::nullopt;
  return propose_on(best, data);
}

std::optional<raft::ProposeResult> SimCluster::propose_on(NodeId id, const std::string& data) {
  if (!nodes_.at(id)) return std::nullopt;
  auto r = nodes_.at(id)->propose(data);
  process_ready(id);
  return r;
}

bool SimCluster::read(NodeId id, uint64_t request_id) {
  if (!nodes_.at(id)) return false;
  ReadOutcome o;
  o.node = id;
  o.must_see = applied_history_.empty() ? 0 : applied_history_.rbegin()->first;
  if (!nodes_.at(id)->read_index(request_id)) return false;
  reads_[request_id] = o;
  process_ready(id);
  return true;
}

// ---------------------------------------------------------------- the host side

void SimCluster::process_ready(NodeId id) {
  raft::RaftCore& core = *nodes_.at(id);
  if (!core.has_ready()) return;
  raft::Ready rd = core.ready();
  Disk& disk = disks_[id];

  // 0. A snapshot from the leader replaces the state machine and the stored
  //    log (the entries Raft kept after it come back in rd.entries).
  if (rd.snapshot) {
    const raft::Snapshot& s = *rd.snapshot;
    disk.snapshot = s;
    disk.log.clear();
    ++snapshots_installed_;
    state_.at(id) =
        std::make_unique<ledger::StateMachine>(ledger::StateMachine::restore(s.data));
    applied_index_.at(id) = s.index;
    auto h = state_hash_at_.find(s.index);
    if (h != state_hash_at_.end() && h->second != state_.at(id)->state_hash()) {
      violation("Deterministic apply: snapshot installed on node " + std::to_string(id) +
                " differs from the state others had at index " + std::to_string(s.index));
    }
  }

  // 1. Persist. In the real node this is a write + fsync. New entries
  //    replace whatever is stored from their first index on.
  if (rd.hard_state) disk.hard_state = *rd.hard_state;
  if (!rd.entries.empty()) {
    const LogIndex first = rd.entries.front().index();
    if (first > disk.last_index() + 1) violation("node " + std::to_string(id) + " log gap");
    if (core.role() == raft::Role::kLeader && first <= disk.last_index()) {
      violation("Leader Append-Only: leader " + std::to_string(id) + " overwrote index " +
                std::to_string(first));
    }
    while (!disk.log.empty() && disk.log.back().index() >= first) disk.log.pop_back();
    for (auto& e : rd.entries) disk.log.push_back(std::move(e));
  }

  // 2. Only then send.
  for (const auto& m : rd.messages) enqueue(m);

  // 3. Apply. The "state machine" here is just the sequence of entries.
  for (const auto& e : rd.committed_entries) {
    LogIndex& applied = applied_index_.at(id);
    if (e.index() != applied + 1) {
      violation("node " + std::to_string(id) + " applied index " + std::to_string(e.index()) +
                " after " + std::to_string(applied));
    }
    applied = e.index();
    auto [it, inserted] = applied_history_.emplace(e.index(), e);
    if (!inserted && (it->second.term() != e.term() || it->second.data() != e.data())) {
      violation("State Machine Safety: index " + std::to_string(e.index()) + " applied as term " +
                std::to_string(it->second.term()) + " elsewhere but term " +
                std::to_string(e.term()) + " on node " + std::to_string(id));
    }

    // The real ledger. Same entries must give the same state everywhere.
    ledger::StateMachine& sm = *state_.at(id);
    sm.apply(e);
    auto [h, first] = state_hash_at_.emplace(e.index(), sm.state_hash());
    if (!first && h->second != sm.state_hash()) {
      violation("Deterministic apply: node " + std::to_string(id) + " has a different state hash at index " +
                std::to_string(e.index()));
    }
  }

  // Take a snapshot now and then, like the node does: Raft compacts its log,
  // and the disk drops the entries the snapshot covers.
  if (opts_.snapshot_every > 0) {
    const LogIndex applied = applied_index_.at(id);
    const LogIndex last_snap = disk.snapshot ? disk.snapshot->index : 0;
    if (applied >= last_snap + opts_.snapshot_every) {
      core.compact(applied, state_.at(id)->snapshot());
      disk.snapshot = core.snapshot();
      disk.log.erase(std::remove_if(disk.log.begin(), disk.log.end(),
                                    [&](const pb::LogEntry& e) { return e.index() <= applied; }),
                     disk.log.end());
      ++snapshots_taken_;
    }
  }

  // 4. Serve reads from the state machine as it is now.
  for (const auto& rs : rd.reads_ready) {
    ReadOutcome& o = reads_.at(rs.request_id);
    o.done = true;
    o.served_at = applied_index_.at(id);
    if (o.served_at < rs.index) violation("read served before applying its readIndex");
    if (o.served_at < o.must_see) {
      violation("Read Linearizability: read " + std::to_string(rs.request_id) + " on node " +
                std::to_string(id) + " saw state at index " + std::to_string(o.served_at) +
                " but index " + std::to_string(o.must_see) + " was already committed");
    }
  }
  for (uint64_t rid : rd.reads_failed) reads_.at(rid).failed = true;

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

void SimCluster::crash(NodeId id) {
  nodes_.at(id).reset();
  for (auto& [_, o] : reads_) {
    if (o.node == id && !o.done) o.failed = true;  // its client sees the connection drop
  }
  reset_state(id);  // the state machine lives in memory: back to the snapshot
}

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

// ---------------------------------------------------------------- invariants

void SimCluster::violation(const std::string& what) {
  violations_.push_back(what + " (seed " + std::to_string(seed_) + ", tick " +
                        std::to_string(now_) + ")");
}

void SimCluster::check_invariants(NodeId id) {
  const raft::RaftCore& core = *nodes_.at(id);
  if (core.role() != raft::Role::kLeader) return;

  // Election Safety: at most one leader per term, ever. The map remembers
  // past terms too, so this also catches a second leader after a crash.
  auto [it, inserted] = leader_of_term_.emplace(core.term(), id);
  if (!inserted && it->second != id) {
    violation("Election Safety: two leaders in term " + std::to_string(core.term()) + ": " +
              std::to_string(it->second) + " and " + std::to_string(id));
  }

  // Leader Completeness: checked once, when a leader first appears. Every
  // entry anyone has applied (so it was committed) must be in its log.
  if (inserted) {
    for (const auto& [index, e] : applied_history_) {
      // Entries before the log's start are in the leader's snapshot, which
      // only ever holds committed state.
      if (index < core.log().snapshot_index()) continue;
      if (index > core.log().last_index() || core.log().term_at(index) != e.term()) {
        violation("Leader Completeness: new leader " + std::to_string(id) + " (term " +
                  std::to_string(core.term()) + ") lacks committed index " +
                  std::to_string(index));
        break;
      }
    }
  }
}

// Log Matching, over the persisted logs of every node (crashed ones too):
// if two logs have an entry with the same index and term, they are identical
// up to and including that index.
void SimCluster::check_log_matching() {
  // By index, over the range both disks still hold (snapshots cut the front).
  for (auto a = disks_.begin(); a != disks_.end(); ++a) {
    for (auto b = std::next(a); b != disks_.end(); ++b) {
      const auto& la = a->second.log;
      const auto& lb = b->second.log;
      if (la.empty() || lb.empty()) continue;
      const LogIndex lo = std::max(la.front().index(), lb.front().index());
      const LogIndex hi = std::min(la.back().index(), lb.back().index());
      auto at = [](const std::vector<pb::LogEntry>& l, LogIndex i) -> const pb::LogEntry& {
        return l[i - l.front().index()];
      };
      LogIndex first_diff = hi + 1;
      for (LogIndex i = lo; i <= hi; ++i) {
        if (at(la, i).term() != at(lb, i).term()) {
          first_diff = i;
          break;
        }
        if (at(la, i).data() != at(lb, i).data()) {
          violation("Log Matching: nodes " + std::to_string(a->first) + " and " +
                    std::to_string(b->first) + " differ in data at index " + std::to_string(i));
          return;
        }
      }
      for (LogIndex i = first_diff; i <= hi; ++i) {
        if (at(la, i).term() == at(lb, i).term()) {
          violation("Log Matching: nodes " + std::to_string(a->first) + " and " +
                    std::to_string(b->first) + " agree at index " + std::to_string(i) +
                    " but differ before it");
          return;
        }
      }
    }
  }
}

}  // namespace ledger::sim
