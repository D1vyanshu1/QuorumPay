#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/types.h"

namespace ledger::storage {

// A Raft snapshot as stored (raft::Snapshot; storage/ may not depend on raft/).
struct SnapshotFile {
  LogIndex index = 0;
  Term term = 0;
  std::string data;
};

// Snapshots in <dir>/snap-<index>-<term>.snap (zero-padded, so names sort by
// index). Each file:
//
//   "QPSNAP01" | u64be index | u64be term | u64be data length | data |
//   SHA-256 of everything before it (32 bytes)
//
// Written with write_file_atomic, so a crash never leaves half a snapshot
// under the real name. The newest kKeep are kept: if the newest turns out to
// be damaged, the one before it still works (with a longer WAL replay).
class SnapshotStore {
 public:
  static constexpr size_t kKeep = 2;

  explicit SnapshotStore(std::string dir);

  void save(const SnapshotFile& s);

  // The newest snapshot whose checksum is good. Damaged ones are skipped and
  // counted in *skipped.
  std::optional<SnapshotFile> load_latest(size_t* skipped = nullptr) const;

  // Indexes of the stored snapshots, newest first.
  std::vector<LogIndex> list() const;

  const std::string& dir() const { return dir_; }

 private:
  std::string path_of(LogIndex index, Term term) const;
  std::vector<std::string> files_newest_first() const;

  std::string dir_;
};

}  // namespace ledger::storage
