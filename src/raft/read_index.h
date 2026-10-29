#pragma once

#include <cstdint>
#include <deque>
#include <set>
#include <vector>

#include "common/types.h"
#include "raft/ready.h"

namespace ledger::raft {

// Pending linearizable reads on the leader (ReadIndex, Raft thesis §6.4).
//
// A leader can't answer a read from its own state right away: it may have
// been deposed without knowing it (e.g. cut off by a partition) and a newer
// leader may have committed writes it hasn't seen. So each read:
//
//   1. records readIndex = the leader's commit index at the time,
//   2. waits until a majority has answered a message sent AFTER the read was
//      registered, which proves no newer leader existed at that moment,
//   3. is then served once the state machine has applied up to readIndex.
//
// Step 2 uses a counter (ctx) instead of a round of special messages: every
// AppendEntries carries the latest ctx, and a reply carrying ctx c confirms
// every read registered with ctx <= c.
class ReadIndexTracker {
 public:
  // Registers a read at `index` and returns the ctx that confirms it.
  // `self` counts as the first ack.
  uint64_t add(uint64_t request_id, LogIndex index, NodeId self);

  // `from` replied to a message that carried `ctx`.
  void ack(NodeId from, uint64_t ctx);

  // Removes and returns the reads acknowledged by at least `quorum` nodes,
  // oldest first. Reads are confirmed in order: a later read can't be
  // confirmed before an earlier one, because every ack for it covers them too.
  std::vector<ReadState> pop_confirmed(size_t quorum);

  // Removes every pending read (leadership lost) and returns their ids.
  std::vector<uint64_t> take_all();

  uint64_t last_ctx() const { return last_ctx_; }
  bool empty() const { return pending_.empty(); }

 private:
  struct Pending {
    uint64_t request_id;
    LogIndex index;
    uint64_t ctx;
    std::set<NodeId> acks;
  };
  std::deque<Pending> pending_;
  uint64_t last_ctx_ = 0;
};

}  // namespace ledger::raft
