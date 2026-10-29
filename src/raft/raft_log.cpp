#include "raft/raft_log.h"

#include <algorithm>
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

void RaftLog::truncate_from(LogIndex index) {
  assert(index > snapshot_index_);
  if (index <= last_index()) entries_.resize(index - snapshot_index_ - 1);
}

std::vector<pb::LogEntry> RaftLog::slice(LogIndex from, size_t max) const {
  std::vector<pb::LogEntry> out;
  for (LogIndex i = from; i <= last_index() && out.size() < max; ++i) out.push_back(at(i));
  return out;
}

LogIndex RaftLog::first_index_of_term_at(LogIndex index) const {
  Term t = term_at(index);
  while (index > snapshot_index_ + 1 && term_at(index - 1) == t) --index;
  return index;
}

std::optional<LogIndex> RaftLog::last_index_of_term(Term term) const {
  for (LogIndex i = last_index(); i > snapshot_index_; --i) {
    Term t = term_at(i);
    if (t == term) return i;
    if (t < term) break;  // terms only grow along the log
  }
  return std::nullopt;
}

}  // namespace ledger::raft
