#pragma once

#include <cstdint>
#include <random>
#include <set>
#include <vector>

#include "common/types.h"
#include "raft.pb.h"
#include "raft/raft_log.h"
#include "raft/ready.h"

namespace ledger::raft {

enum class Role { kFollower, kCandidate, kLeader };

const char* role_name(Role r);

struct RaftConfig {
  NodeId id = kNoNode;
  std::vector<NodeId> peers;  // every other node in the cluster (not self)

  // In ticks. The host calls tick() every 10ms, so the defaults are a
  // 150-300ms election timeout and a 50ms heartbeat, as in the Raft paper.
  int election_timeout_min = 15;
  int election_timeout_max = 30;
  int heartbeat_interval = 5;

  // Seeds the election-timeout RNG. Injected so simulations are reproducible.
  uint64_t seed = 0;
};

// Raft consensus as a pure state machine: no sockets, no files, no clocks.
//
// Inputs:  tick() (time passes), step(msg) (a message arrived).
// Outputs: ready() returns what to persist and what to send.
//
// Because every input is explicit, a test can drive five RaftCores with a
// virtual clock and a fake network and replay any run exactly from its seed.
//
// Week 2 scope: leader election, heartbeats, and the no-op entry on election.
// Replication, commit and apply come in week 3.
class RaftCore {
 public:
  // `hard_state` and `log` are what the host recovered from disk (empty on
  // first boot). A node always starts as a follower.
  RaftCore(RaftConfig config, HardState hard_state, std::vector<pb::LogEntry> log);

  void tick();
  void step(const pb::RaftMessage& msg);

  bool has_ready() const;
  // Hands over everything produced since the last call and clears it.
  Ready ready();

  NodeId id() const { return config_.id; }
  Role role() const { return role_; }
  Term term() const { return term_; }
  NodeId voted_for() const { return voted_for_; }
  NodeId leader() const { return leader_; }
  const RaftLog& log() const { return log_; }

 private:
  void become_follower(Term term, NodeId leader);
  void become_candidate();
  void become_leader();

  void handle_request_vote(const pb::RaftMessage& msg);
  void handle_request_vote_resp(const pb::RaftMessage& msg);
  void handle_append_entries(const pb::RaftMessage& msg);

  void broadcast_heartbeat();
  void append_entry(pb::EntryType type);
  void send(NodeId to, pb::RaftMessage msg);
  void reset_election_timer();
  size_t quorum() const { return (config_.peers.size() + 1) / 2 + 1; }

  RaftConfig config_;
  std::mt19937_64 rng_;

  // Persistent: written to HardState / the log before any message that
  // depends on them leaves this node.
  Term term_ = 0;
  NodeId voted_for_ = kNoNode;
  RaftLog log_;

  // Volatile.
  Role role_ = Role::kFollower;
  NodeId leader_ = kNoNode;
  int election_elapsed_ = 0;
  int election_timeout_ = 0;  // re-randomized on every reset
  int heartbeat_elapsed_ = 0;
  std::set<NodeId> votes_;  // a set, so a duplicated vote is only counted once

  // Pending output for the next ready().
  HardState persisted_;  // last HardState handed to the host
  Ready pending_;
};

}  // namespace ledger::raft
