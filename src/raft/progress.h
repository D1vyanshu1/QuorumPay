#pragma once

#include "common/types.h"

namespace ledger::raft {

// What the leader knows about one follower's log (Figure 2, "volatile state on
// leaders"). Reset every time a node becomes leader.
struct Progress {
  // Highest index known to be stored on the follower. Only ever grows; the
  // commit rule counts these.
  LogIndex match = 0;
  // Next index to send. Starts optimistic (leader's last + 1) and moves back
  // when the follower rejects, until the logs agree.
  LogIndex next = 1;
  // An AppendEntries carrying entries is outstanding. While set, new
  // proposals don't trigger another send to this follower: they ride along
  // in the next batch. The heartbeat resends anyway, which covers lost
  // messages. This bounds traffic to one batch in flight per follower.
  bool in_flight = false;
};

}  // namespace ledger::raft
