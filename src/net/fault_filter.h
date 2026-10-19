#pragma once

#include <set>
#include <vector>

#include "common/types.h"

namespace ledger::net {

// Simulated network partitions. Messages to or from a blocked peer are
// silently dropped by PeerManager in both directions, the same way a real
// network failure behaves (no error, the message just never arrives).
// TCP connections stay up; only the messages vanish.
//
// A partition {1,2} | {3,4,5} = nodes 1 and 2 block 3, 4, 5, and nodes 3, 4, 5
// block 1 and 2. The test harness sets this up through admin commands.
class FaultFilter {
 public:
  void block(NodeId peer) { blocked_.insert(peer); }
  void unblock(NodeId peer) { blocked_.erase(peer); }
  void unblock_all() { blocked_.clear(); }
  bool is_blocked(NodeId peer) const { return blocked_.count(peer) > 0; }
  std::vector<NodeId> blocked() const { return {blocked_.begin(), blocked_.end()}; }

 private:
  std::set<NodeId> blocked_;
};

}  // namespace ledger::net
