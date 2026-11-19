#include "storage/wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>

#include "crypto/hash.h"
#include "storage/file_util.h"

namespace ledger::storage {

namespace {

[[noreturn]] void fail(const std::string& what) {
  throw std::runtime_error("wal: " + what + ": " + std::strerror(errno));
}

// "00000000000000000042.log" -> 42; false for anything else.
bool parse_seg_name(const std::string& name, uint64_t* seq) {
  if (name.size() != 24 || name.compare(20, 4, ".log") != 0) return false;
  uint64_t v = 0;
  for (size_t i = 0; i < 20; ++i) {
    if (name[i] < '0' || name[i] > '9') return false;
    v = v * 10 + static_cast<uint64_t>(name[i] - '0');
  }
  *seq = v;
  return true;
}

}  // namespace

bool parse_fsync_mode(std::string_view name, FsyncMode* out) {
  if (name == "entry") *out = FsyncMode::kEntry;
  else if (name == "batch") *out = FsyncMode::kBatch;
  else if (name == "none") *out = FsyncMode::kNone;
  else return false;
  return true;
}

Wal::Wal(std::string dir, FsyncMode mode, uint64_t segment_bytes)
    : dir_(std::move(dir)), mode_(mode), segment_bytes_(segment_bytes) {
  make_dirs(dir_);
  recover();
  if (segs_.empty()) {
    new_segment();
  } else {
    open_active();
  }
}

Wal::~Wal() {
  try {
    sync();
  } catch (...) {
    // Nothing useful to do in a destructor; unsynced data is like a crash.
  }
  close_active();
}

std::string Wal::seg_path(uint64_t seq) const {
  char name[32];
  std::snprintf(name, sizeof name, "%020llu.log", static_cast<unsigned long long>(seq));
  return dir_ + "/" + name;
}

std::string Wal::encode(const pb::LogEntry& e) {
  std::string body;
  body.push_back(static_cast<char>(e.type()));
  put_u64(body, e.term());
  put_u64(body, e.index());
  body.append(e.data());
  std::string rec;
  put_u32(rec, static_cast<uint32_t>(body.size()));
  put_u32(rec, crypto::crc32(body));
  rec.append(body);
  return rec;
}

// ---------------------------------------------------------------- recovery

void Wal::recover() {
  std::vector<uint64_t> seqs;
  for (const auto& name : list_dir(dir_)) {
    uint64_t seq = 0;
    if (parse_seg_name(name, &seq)) seqs.push_back(seq);
  }
  std::sort(seqs.begin(), seqs.end());

  bool stop = false;  // found the end of the valid log: delete what follows
  Term last_term = 0;
  for (size_t k = 0; k < seqs.size(); ++k) {
    const std::string path = seg_path(seqs[k]);
    if (stop || (k > 0 && seqs[k] != seqs[k - 1] + 1)) {
      // After the valid end (or after a missing segment): not part of the log.
      stop = true;
      recovery_.truncated_bytes += read_file(path).value_or("").size();
      if (::unlink(path.c_str()) < 0) fail("unlink " + path);
      ++recovery_.deleted_segments;
      continue;
    }

    const std::string data = read_file(path).value_or("");
    uint64_t off = 0;
    while (off < data.size()) {
      const char* p = data.data() + off;
      const uint64_t left = data.size() - off;
      if (left < kHeaderBytes) break;
      const uint32_t len = get_u32(p);
      if (len < kBodyHeaderBytes || len > kMaxRecordBytes || kHeaderBytes + len > left) break;
      const std::string_view body(p + kHeaderBytes, len);
      if (crypto::crc32(body) != get_u32(p + 4)) break;

      pb::LogEntry e;
      e.set_type(static_cast<pb::EntryType>(static_cast<unsigned char>(body[0])));
      e.set_term(get_u64(body.data() + 1));
      e.set_index(get_u64(body.data() + 9));
      e.set_data(std::string(body.substr(kBodyHeaderBytes)));
      // A valid-looking record that doesn't continue the log can't be ours.
      if (!locs_.empty() && (e.index() != last_index() + 1 || e.term() < last_term)) break;

      if (locs_.empty()) first_index_ = e.index();
      locs_.push_back(Loc{seqs[k], off});
      last_term = e.term();
      recovery_.entries.push_back(std::move(e));
      off += kHeaderBytes + len;
    }
    segs_.push_back(Segment{seqs[k], path, off});

    if (off < data.size()) {
      recovery_.truncated_bytes += data.size() - off;
      int fd = ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
      if (fd < 0) fail("open " + path);
      if (::ftruncate(fd, static_cast<off_t>(off)) < 0) fail("ftruncate " + path);
      fsync_fd(fd, path);
      ::close(fd);
      stop = true;
    }
  }
  if (recovery_.deleted_segments > 0) fsync_dir(dir_);
}

// ---------------------------------------------------------------- writing

void Wal::write(const std::vector<pb::LogEntry>& entries) {
  if (entries.empty()) return;
  const LogIndex first = entries.front().index();
  if (!empty() && first <= last_index()) truncate_from(first);

  for (const auto& e : entries) {
    if (!empty() && e.index() != last_index() + 1) {
      throw std::logic_error("wal: entry " + std::to_string(e.index()) + " doesn't follow " +
                             std::to_string(last_index()));
    }
    const std::string rec = encode(e);
    if (segs_.back().size > 0 && segs_.back().size + rec.size() > segment_bytes_) new_segment();
    write_fully(fd_, rec, segs_.back().path);
    if (empty()) first_index_ = e.index();
    locs_.push_back(Loc{segs_.back().seq, segs_.back().size});
    segs_.back().size += rec.size();
    dirty_ = true;
    if (mode_ == FsyncMode::kEntry) fsync_active();
  }
}

void Wal::truncate_from(LogIndex index) {
  if (empty() || index > last_index()) return;
  if (index <= first_index_) {
    reset();
    return;
  }
  const Loc loc = locs_[index - first_index_];
  const size_t pos = pos_of(loc.seq);
  if (pos + 1 < segs_.size()) {
    close_active();
    delete_segments_from(pos + 1);
  }
  if (fd_ < 0) open_active();
  cut_segment(pos, loc.offset);
  locs_.resize(index - first_index_);
}

void Wal::sync() {
  if (mode_ == FsyncMode::kNone) return;
  if (dirty_) fsync_active();
  if (dir_dirty_) fsync_dir_now();
}

void Wal::compact_through(LogIndex index) {
  // Segment n can go if the first entry of segment n+1 is <= index + 1.
  size_t n = 0;
  size_t loc = 0;  // first loc not in the segments counted so far
  while (n + 1 < segs_.size()) {
    size_t next = loc;
    while (next < locs_.size() && locs_[next].seq <= segs_[n].seq) ++next;
    const LogIndex last_in_n = first_index_ + next - 1;  // == previous last if n is empty
    if (next > 0 && last_in_n > index) break;
    loc = next;
    ++n;
  }
  if (n == 0) return;
  for (size_t i = 0; i < n; ++i) {
    if (::unlink(segs_[i].path.c_str()) < 0) fail("unlink " + segs_[i].path);
  }
  segs_.erase(segs_.begin(), segs_.begin() + static_cast<std::ptrdiff_t>(n));
  locs_.erase(locs_.begin(), locs_.begin() + static_cast<std::ptrdiff_t>(loc));
  first_index_ += loc;
  // Not urgent: if the deletions are lost in a crash, the old segments come
  // back holding valid (already snapshotted) entries, which is harmless.
  dir_dirty_ = true;
}

void Wal::reset() {
  close_active();
  delete_segments_from(0);
  locs_.clear();
  first_index_ = 0;
  new_segment();
}

// ---------------------------------------------------------------- files

void Wal::open_active() {
  const std::string& path = segs_.back().path;
  fd_ = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
  if (fd_ < 0) fail("open " + path);
}

void Wal::close_active() {
  if (fd_ >= 0) ::close(fd_);
  fd_ = -1;
}

void Wal::new_segment() {
  if (fd_ >= 0) {
    // The old segment's tail must be durable before we stop tracking it.
    if (dirty_ && mode_ != FsyncMode::kNone) fsync_active();
    close_active();
  }
  const uint64_t seq = segs_.empty() ? 1 : segs_.back().seq + 1;
  const std::string path = seg_path(seq);
  fd_ = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd_ < 0) fail("create " + path);
  segs_.push_back(Segment{seq, path, 0});
  dir_dirty_ = true;
  if (mode_ == FsyncMode::kEntry) fsync_dir_now();
}

void Wal::cut_segment(size_t pos, uint64_t offset) {
  if (::ftruncate(fd_, static_cast<off_t>(offset)) < 0) fail("ftruncate " + segs_[pos].path);
  segs_[pos].size = offset;
  dirty_ = true;
  if (mode_ == FsyncMode::kEntry) fsync_active();
}

void Wal::delete_segments_from(size_t pos) {
  for (size_t i = pos; i < segs_.size(); ++i) {
    if (::unlink(segs_[i].path.c_str()) < 0) fail("unlink " + segs_[i].path);
  }
  segs_.resize(pos);
  // Right away, not at the next sync: if the deletion were lost in a crash,
  // an old segment could reappear after a rewritten one and be read back as
  // if it continued the new log.
  if (mode_ != FsyncMode::kNone) fsync_dir_now();
}

void Wal::fsync_active() {
  if (fd_ >= 0) {
    fsync_fd(fd_, segs_.back().path);
    ++fsyncs_;
  }
  dirty_ = false;
}

void Wal::fsync_dir_now() {
  fsync_dir(dir_);
  dir_dirty_ = false;
}

}  // namespace ledger::storage
