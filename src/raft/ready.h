#pragma once

#include <optional>
#include <vector>

#include "common/types.h"
#include "raft.pb.h"

namespace ledger::raft {

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
//
// Persisting first is what makes a vote or an ack a promise: once another node
// has seen it, it survives a crash of this node.
//
// Week 3 adds committed entries to apply; week 5 adds snapshots.
struct Ready {
  std::optional<HardState> hard_state;  // set only when it changed
  std::vector<pb::LogEntry> entries;    // new entries to append to the log
  std::vector<pb::RaftMessage> messages;

  bool empty() const { return !hard_state && entries.empty() && messages.empty(); }
};

}  // namespace ledger::raft
