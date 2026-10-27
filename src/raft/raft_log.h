#pragma once

#include <vector>

#include "common/types.h"
#include "raft.pb.h"

namespace ledger::raft {

// In-memory view of the Raft log. Indexes start at 1; index 0 with term 0 is
// the empty "before the log" position, so prev_log_index = 0 always matches.
//
// After a snapshot (week 5) the entries up to snapshot_index are discarded and
// entries_[0] holds index snapshot_index + 1. Until then both offsets are 0.
class RaftLog {
 public:
  RaftLog() = default;
  explicit RaftLog(std::vector<pb::LogEntry> entries) : entries_(std::move(entries)) {}

  LogIndex last_index() const { return snapshot_index_ + entries_.size(); }
  Term last_term() const { return entries_.empty() ? snapshot_term_ : entries_.back().term(); }

  // Term of the entry at `index`; 0 for index 0. Requires
  // snapshot_index <= index <= last_index.
  Term term_at(LogIndex index) const;

  const pb::LogEntry& at(LogIndex index) const;

  // Appends at last_index + 1; the entry's index field must already be set to it.
  void append(const pb::LogEntry& entry);

  // Raft's "at least as up to date" rule (§5.4.1): a later last term wins; with
  // equal last terms, the longer log wins.
  bool is_up_to_date(LogIndex last_index, Term last_term) const {
    return last_term > this->last_term() ||
           (last_term == this->last_term() && last_index >= this->last_index());
  }

 private:
  std::vector<pb::LogEntry> entries_;
  LogIndex snapshot_index_ = 0;
  Term snapshot_term_ = 0;
};

}  // namespace ledger::raft
