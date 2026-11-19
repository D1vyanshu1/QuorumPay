#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/types.h"
#include "raft.pb.h"

namespace ledger::storage {

// When the WAL calls fsync (cluster.json "fsync"):
//   entry: after every record. Simplest, slowest.
//   batch: once per sync() call. The node calls sync() once per Ready, before
//          any message leaves, so a whole batch of entries costs one fsync
//          ("group commit"). The default.
//   none:  never. UNSAFE (an acknowledged entry can vanish in a power loss);
//          only to measure what fsync costs.
enum class FsyncMode { kEntry, kBatch, kNone };
bool parse_fsync_mode(std::string_view name, FsyncMode* out);

// Segmented, append-only write-ahead log of Raft entries, in <dir>/NNN.log.
//
// Record:  u32be length | u32be crc32(body) | body
// Body:    u8 entry type | u64be term | u64be index | data
//
// Recovery (in the constructor): read the segments in order and stop at the
// first record that is cut short, fails its CRC, or doesn't continue the log
// (index + 1, term not decreasing). That's a write torn by a crash, or
// damage; the file is cut there and any later segments are deleted. What is
// left is exactly the records that were fully written.
//
// Truncation: Raft replaces uncommitted entries that conflict with the
// leader's. An index -> (segment, offset) map lets the WAL cut the right file
// at the right byte and delete the segments after it.
class Wal {
 public:
  static constexpr uint64_t kDefaultSegmentBytes = 64ull << 20;
  // A length field bigger than this is garbage, not a record.
  static constexpr uint32_t kMaxRecordBytes = 64u << 20;
  static constexpr size_t kHeaderBytes = 8;
  static constexpr size_t kBodyHeaderBytes = 1 + 8 + 8;

  struct Recovery {
    std::vector<pb::LogEntry> entries;  // contiguous, ascending
    uint64_t truncated_bytes = 0;       // torn or damaged tail cut off
    size_t deleted_segments = 0;
  };

  Wal(std::string dir, FsyncMode mode, uint64_t segment_bytes = kDefaultSegmentBytes);
  ~Wal();
  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  // What recovery found. Handed out once (the host passes it to RaftCore).
  Recovery take_recovery() { return std::move(recovery_); }

  // Stores `entries` (contiguous, ascending). Like Ready::entries they
  // REPLACE whatever is stored from entries.front().index() on.
  void write(const std::vector<pb::LogEntry>& entries);
  // Removes `index` and everything after it.
  void truncate_from(LogIndex index);
  // Makes everything written so far durable (in batch mode).
  void sync();

  // Deletes whole segments whose entries are all <= index (they are covered
  // by a snapshot). The segment being written to is never deleted.
  void compact_through(LogIndex index);
  // Deletes everything; the next write may start at any index. For a
  // follower that installs a snapshot past the end of its log.
  void reset();

  bool empty() const { return locs_.empty(); }
  LogIndex first_index() const { return empty() ? 0 : first_index_; }
  LogIndex last_index() const { return empty() ? 0 : first_index_ + locs_.size() - 1; }
  size_t segment_count() const { return segs_.size(); }
  uint64_t fsync_count() const { return fsyncs_; }

  static std::string encode(const pb::LogEntry& e);

 private:
  struct Segment {
    uint64_t seq = 0;
    std::string path;
    uint64_t size = 0;
  };
  struct Loc {
    uint64_t seq;     // segment
    uint64_t offset;  // of the record's first byte
  };

  void recover();
  std::string seg_path(uint64_t seq) const;
  void open_active();  // opens the last segment for appending
  void close_active();
  void new_segment();
  void cut_segment(size_t pos, uint64_t offset);  // ftruncate + fsync
  void delete_segments_from(size_t pos);
  void fsync_active();
  void fsync_dir_now();
  size_t pos_of(uint64_t seq) const { return static_cast<size_t>(seq - segs_.front().seq); }

  std::string dir_;
  FsyncMode mode_;
  uint64_t segment_bytes_;
  std::vector<Segment> segs_;  // ascending, contiguous seq numbers
  int fd_ = -1;                // the last segment, open for appending
  bool dirty_ = false;         // written since the last fsync
  bool dir_dirty_ = false;     // a segment was created since the last dir fsync
  LogIndex first_index_ = 0;
  std::vector<Loc> locs_;  // locs_[i] = where index first_index_ + i is
  uint64_t fsyncs_ = 0;
  Recovery recovery_;
};

}  // namespace ledger::storage
