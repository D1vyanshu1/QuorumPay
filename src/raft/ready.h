#pragma once

#include <optional>
#include <vector>

#include "common/types.h"
#include "raft.pb.h"

namespace ledger::raft {

// A linearizable read that may now be served: the host answers it from its
// state machine once it has applied up to `index`.
struct ReadState {
  uint64_t request_id = 0;
  LogIndex index = 0;
};

// The state Raft must have on disk before it answers anyone (Figure 2,
// "persistent state"). If voted_for were lost in a crash, a node could vote
// twice in one term and two leaders could be elected.
struct HardState {
  Term term = 0;
  NodeId voted_for = kNoNode;

  bool operator==(const HardState& o) const { return term == o.term && voted_for == o.voted_for; }
  bool operator!=(const HardState& o) const { return !(*this == o); }
};

// Everything RaftCore wants done since the last ready() call. RaftCore never
// does I/O itself; the host (the real node, or the simulator) must handle a
// Ready in this order:
//
//   1. persist hard_state and entries (fsync)
//   2. send messages
//   3. apply committed_entries to the state machine, in order
//   4. serve reads_ready (each index is <= the last committed entry, so after
//      step 3 the state machine is far enough along); fail reads_failed
//
// Persisting first is what makes a vote or an ack a promise: once another node
// has seen it, it survives a crash of this node.
struct Ready {
  std::optional<HardState> hard_state;  // set only when it changed

  // Log entries to store, contiguous and ascending. They REPLACE anything the
  // host has stored at entries.front().index() or later: the host truncates
  // its log there first, then appends. (That's how a follower's conflicting
  // uncommitted suffix gets overwritten.)
  std::vector<pb::LogEntry> entries;

  std::vector<pb::RaftMessage> messages;

  // Newly committed entries, contiguous and ascending, continuing exactly
  // where the previous Ready's committed_entries ended.
  std::vector<pb::LogEntry> committed_entries;

  // ReadIndex results. Failed reads lost their leader before being confirmed;
  // the client should retry on the new leader.
  std::vector<ReadState> reads_ready;
  std::vector<uint64_t> reads_failed;

  bool empty() const {
    return !hard_state && entries.empty() && messages.empty() && committed_entries.empty() &&
           reads_ready.empty() && reads_failed.empty();
  }
};

}  // namespace ledger::raft
