#pragma once

#include <cstdint>
#include <string>

#include "common/types.h"

namespace ledger::storage {

// Raft's term and vote (raft::HardState; storage/ may not depend on raft/).
struct Meta {
  Term term = 0;
  NodeId voted_for = kNoNode;

  bool operator==(const Meta& o) const { return term == o.term && voted_for == o.voted_for; }
};

// Keeps currentTerm and votedFor in <dir>/meta.
//
// These must be on disk BEFORE the node answers a RequestVote or sends its
// own: a node that forgot its vote after a crash could vote twice in one term
// and let two leaders be elected (project_details.md §6.6).
//
// File: "QPMETA01" | u64be term | u32be voted_for | u32be crc32(everything
// before it). Replaced with write_file_atomic, so a crash leaves the old or
// the new value, never a torn one.
class MetaStore {
 public:
  explicit MetaStore(std::string dir);

  // {0, none} on first boot (no file). Throws std::runtime_error if the file
  // is damaged: starting with a guessed term or vote could break safety, so
  // refusing to start is the only safe answer.
  Meta load() const;
  void save(const Meta& m);

  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

}  // namespace ledger::storage
