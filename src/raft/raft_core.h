#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "common/types.h"
#include "raft.pb.h"
#include "raft/progress.h"
#include "raft/raft_log.h"
#include "raft/read_index.h"
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
  // Batching: at most this many entries per AppendEntries.
  size_t max_entries_per_append = 64;

  // Seeds the election-timeout RNG. Injected so simulations are reproducible.
  uint64_t seed = 0;
};

// Where a proposal landed in the log. If the entry that is eventually
// committed at `index` has a different term, the proposal was lost (its
// leader was deposed and the entry overwritten) and the client must retry.
struct ProposeResult {
  LogIndex index = 0;
  Term term = 0;
};

// Raft consensus as a pure state machine: no sockets, no files, no clocks.
//
// Inputs:  tick() (time passes), step(msg) (a message arrived),
//          propose(data) (a client wants something in the log).
// Outputs: ready() returns what to persist, what to send and what to apply.
//
// Because every input is explicit, a test can drive five RaftCores with a
// virtual clock and a fake network and replay any run exactly from its seed.
//
// Implemented: leader election, log replication with fast backtracking,
// the commit rule, committed entries for the host to apply, and ReadIndex.
// Still to come: snapshots (week 5), pre-vote (week 6).
class RaftCore {
 public:
  // `hard_state` and `log` are what the host recovered from disk (empty on
  // first boot). A node always starts as a follower.
  RaftCore(RaftConfig config, HardState hard_state, std::vector<pb::LogEntry> log);

  void tick();
  void step(const pb::RaftMessage& msg);
  // Leader only: appends `data` to the log and starts replicating it.
  // Returns nullopt if this node isn't the leader (the caller redirects).
  // Appending is not committing: watch committed_entries for the outcome.
  std::optional<ProposeResult> propose(std::string data);
  // Leader only: starts a linearizable read. Its outcome arrives later in
  // Ready::reads_ready or Ready::reads_failed under `request_id`. Returns
  // false at once if this node isn't the leader (the caller redirects).
  bool read_index(uint64_t request_id);

  bool has_ready() const;
  // Hands over everything produced since the last call and clears it.
  Ready ready();

  NodeId id() const { return config_.id; }
  Role role() const { return role_; }
  Term term() const { return term_; }
  NodeId voted_for() const { return voted_for_; }
  NodeId leader() const { return leader_; }
  const RaftLog& log() const { return log_; }
  LogIndex commit_index() const { return commit_index_; }
  const std::map<NodeId, Progress>& progress() const { return progress_; }

 private:
  void become_follower(Term term, NodeId leader);
  void become_candidate();
  void become_leader();

  void handle_request_vote(const pb::RaftMessage& msg);
  void handle_request_vote_resp(const pb::RaftMessage& msg);
  void handle_append_entries(const pb::RaftMessage& msg);
  void handle_append_entries_resp(const pb::RaftMessage& msg);

  // Sends the follower the entries it is missing from progress.next on.
  // Skipped while a batch is in flight unless `force` (the heartbeat).
  void send_append(NodeId peer, bool force);
  void broadcast_append();
  void maybe_commit();
  bool committed_in_current_term() const {
    return commit_index_ > 0 && log_.term_at(commit_index_) == term_;
  }
  // Registers reads at the current commit index and starts a confirmation round.
  void start_reads(const std::vector<uint64_t>& request_ids);
  void release_confirmed_reads();
  void append_entry(pb::EntryType type, std::string data = {});
  // Queues an entry for the host to store, replacing any queued entries at
  // the same index or later (see Ready::entries).
  void queue_persist(const pb::LogEntry& e);
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
  LogIndex commit_index_ = 0;  // highest index known committed
  LogIndex applied_ = 0;       // highest index handed to the host to apply
  std::map<NodeId, Progress> progress_;  // leader only
  ReadIndexTracker reads_;               // leader only
  // Reads that arrived before this leader committed an entry of its own term.
  // Until then its commit index may be behind the previous leader's, so a
  // readIndex taken from it could miss committed writes.
  std::vector<uint64_t> reads_waiting_;

  // Pending output for the next ready().
  HardState persisted_;  // last HardState handed to the host
  Ready pending_;
};

}  // namespace ledger::raft
