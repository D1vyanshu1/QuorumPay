#include "storage/snapshot_store.h"

#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <stdexcept>

#include "crypto/hash.h"
#include "storage/file_util.h"

namespace ledger::storage {

namespace {

constexpr std::string_view kMagic = "QPSNAP01";
constexpr size_t kHeader = 8 + 8 + 8 + 8;
constexpr size_t kChecksum = 32;

// "snap-<20 digits>-<20 digits>.snap"
bool is_snapshot_name(const std::string& name) {
  return name.size() == 5 + 20 + 1 + 20 + 5 && name.compare(0, 5, "snap-") == 0 &&
         name.compare(name.size() - 5, 5, ".snap") == 0;
}

}  // namespace

SnapshotStore::SnapshotStore(std::string dir) : dir_(std::move(dir)) { make_dirs(dir_); }

std::string SnapshotStore::path_of(LogIndex index, Term term) const {
  char name[64];
  std::snprintf(name, sizeof name, "snap-%020llu-%020llu.snap",
                static_cast<unsigned long long>(index), static_cast<unsigned long long>(term));
  return dir_ + "/" + name;
}

std::vector<std::string> SnapshotStore::files_newest_first() const {
  std::vector<std::string> names;
  for (const auto& n : list_dir(dir_))
    if (is_snapshot_name(n)) names.push_back(n);
  std::sort(names.rbegin(), names.rend());  // zero-padded: by index, then term
  return names;
}

void SnapshotStore::save(const SnapshotFile& s) {
  std::string out(kMagic);
  put_u64(out, s.index);
  put_u64(out, s.term);
  put_u64(out, s.data.size());
  out.append(s.data);
  out.append(crypto::sha256(out));
  write_file_atomic(path_of(s.index, s.term), out);

  // Keep only the newest few. Deleting is safe even if lost in a crash.
  auto names = files_newest_first();
  for (size_t i = kKeep; i < names.size(); ++i) {
    const std::string p = dir_ + "/" + names[i];
    ::unlink(p.c_str());
  }
}

std::optional<SnapshotFile> SnapshotStore::load_latest(size_t* skipped) const {
  if (skipped) *skipped = 0;
  for (const auto& name : files_newest_first()) {
    auto bytes = read_file(dir_ + "/" + name);
    bool ok = bytes && bytes->size() >= kHeader + kChecksum &&
              bytes->compare(0, kMagic.size(), kMagic) == 0;
    if (ok) {
      const char* p = bytes->data();
      const uint64_t len = get_u64(p + 24);
      ok = len == bytes->size() - kHeader - kChecksum &&
           crypto::sha256(std::string_view(p, kHeader + len)) ==
               std::string_view(p + kHeader + len, kChecksum);
      if (ok) return SnapshotFile{get_u64(p + 8), get_u64(p + 16), bytes->substr(kHeader, len)};
    }
    if (skipped) ++*skipped;  // damaged: try the older one
  }
  return std::nullopt;
}

std::vector<LogIndex> SnapshotStore::list() const {
  std::vector<LogIndex> out;
  for (const auto& name : files_newest_first()) {
    out.push_back(std::stoull(name.substr(5, 20)));
  }
  return out;
}

}  // namespace ledger::storage
