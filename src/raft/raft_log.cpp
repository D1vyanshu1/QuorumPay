#include "raft/raft_log.h"

#include <cassert>

namespace ledger::raft {

Term RaftLog::term_at(LogIndex index) const {
  if (index == snapshot_index_) return snapshot_term_;
  return at(index).term();
}

const pb::LogEntry& RaftLog::at(LogIndex index) const {
  assert(index > snapshot_index_ && index <= last_index());
  return entries_[index - snapshot_index_ - 1];
}

void RaftLog::append(const pb::LogEntry& entry) {
  assert(entry.index() == last_index() + 1);
  entries_.push_back(entry);
}

}  // namespace ledger::raft
