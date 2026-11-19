// Storage: atomic files, the meta file (term + vote), and the WAL (append,
// reopen, truncate, segments, compaction, fsync modes, and recovery from a
// write torn at any byte).
#include "storage/wal.h"

#include <gtest/gtest.h>
#include <stdlib.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <stdexcept>

#include "storage/file_util.h"
#include "storage/meta_store.h"
#include "storage/snapshot_store.h"

using ledger::LogIndex;
using ledger::Term;
using ledger::pb::LogEntry;
using ledger::storage::FsyncMode;
using ledger::storage::Meta;
using ledger::storage::MetaStore;
using ledger::storage::Wal;
namespace fs = std::filesystem;

namespace {

class TempDir {
 public:
  TempDir() {
    std::string tmpl = ::testing::TempDir() + "qp-storage-XXXXXX";
    path_ = ::mkdtemp(tmpl.data());
  }
  ~TempDir() { fs::remove_all(path_); }
  const std::string& path() const { return path_; }
  std::string sub(const std::string& name) const { return path_ + "/" + name; }

 private:
  std::string path_;
};

LogEntry entry(LogIndex index, Term term, const std::string& data = "") {
  LogEntry e;
  e.set_index(index);
  e.set_term(term);
  e.set_type(data.empty() ? ledger::pb::ENTRY_NOOP : ledger::pb::ENTRY_NORMAL);
  e.set_data(data.empty() ? "" : data);
  return e;
}

std::vector<LogEntry> entries(LogIndex from, LogIndex to, Term term, const std::string& tag = "x") {
  std::vector<LogEntry> out;
  for (LogIndex i = from; i <= to; ++i) out.push_back(entry(i, term, tag + std::to_string(i)));
  return out;
}

void expect_same(const std::vector<LogEntry>& got, const std::vector<LogEntry>& want) {
  ASSERT_EQ(got.size(), want.size());
  for (size_t i = 0; i < got.size(); ++i) {
    EXPECT_EQ(got[i].index(), want[i].index());
    EXPECT_EQ(got[i].term(), want[i].term());
    EXPECT_EQ(got[i].type(), want[i].type());
    EXPECT_EQ(got[i].data(), want[i].data());
  }
}

std::vector<LogEntry> reopen(const std::string& dir, uint64_t seg = Wal::kDefaultSegmentBytes) {
  Wal w(dir, FsyncMode::kBatch, seg);
  return w.take_recovery().entries;
}

std::string only_segment(const std::string& dir) {
  auto names = ledger::storage::list_dir(dir);
  EXPECT_EQ(names.size(), 1u);
  return dir + "/" + names.at(0);
}

}  // namespace

// ---------------------------------------------------------------- file_util

TEST(FileUtil, AtomicWriteReplacesAndReadsBack) {
  TempDir d;
  const std::string p = d.sub("f");
  EXPECT_FALSE(ledger::storage::read_file(p).has_value());
  ledger::storage::write_file_atomic(p, "one");
  ledger::storage::write_file_atomic(p, std::string("two\0bytes", 9));
  EXPECT_EQ(*ledger::storage::read_file(p), std::string("two\0bytes", 9));
  EXPECT_FALSE(fs::exists(p + ".tmp"));  // the temp file was renamed away
}

TEST(FileUtil, MakeDirsIsMkdirP) {
  TempDir d;
  ledger::storage::make_dirs(d.sub("a/b/c"));
  ledger::storage::make_dirs(d.sub("a/b/c"));  // already there: fine
  EXPECT_TRUE(fs::is_directory(d.sub("a/b/c")));
}

// ---------------------------------------------------------------- meta

TEST(MetaStore, FirstBootIsTermZeroNoVote) {
  TempDir d;
  EXPECT_EQ(MetaStore(d.path()).load(), (Meta{0, ledger::kNoNode}));
}

TEST(MetaStore, SaveThenLoadSurvivesReopen) {
  TempDir d;
  MetaStore(d.path()).save(Meta{7, 3});
  EXPECT_EQ(MetaStore(d.path()).load(), (Meta{7, 3}));
  MetaStore(d.path()).save(Meta{8, ledger::kNoNode});
  EXPECT_EQ(MetaStore(d.path()).load(), (Meta{8, ledger::kNoNode}));
}

TEST(MetaStore, DamagedFileRefusesToLoad) {
  TempDir d;
  MetaStore m(d.path());
  m.save(Meta{7, 3});
  std::string bytes = *ledger::storage::read_file(m.path());
  bytes[10] ^= 1;  // flip a bit in the term
  ledger::storage::write_file_atomic(m.path(), bytes);
  EXPECT_THROW(m.load(), std::runtime_error);
  ledger::storage::write_file_atomic(m.path(), bytes.substr(0, 12));  // cut short
  EXPECT_THROW(m.load(), std::runtime_error);
}

// ---------------------------------------------------------------- WAL basics

TEST(Wal, WriteThenReopenGivesTheSameEntries) {
  TempDir d;
  std::vector<LogEntry> want = {entry(1, 1), entry(2, 1, std::string("bin\0\xff", 5)),
                                entry(3, 2, std::string(10000, 'z'))};
  {
    Wal w(d.path(), FsyncMode::kBatch);
    EXPECT_TRUE(w.take_recovery().entries.empty());
    w.write(want);
    w.sync();
    EXPECT_EQ(w.first_index(), 1u);
    EXPECT_EQ(w.last_index(), 3u);
  }
  expect_same(reopen(d.path()), want);
}

TEST(Wal, WriteReplacesFromItsFirstIndexLikeReady) {
  TempDir d;
  {
    Wal w(d.path(), FsyncMode::kBatch);
    w.write(entries(1, 5, 1));
    // A new leader overwrites the uncommitted 4..5 with its own 4..6.
    w.write(entries(4, 6, 2, "new"));
    w.sync();
  }
  auto got = reopen(d.path());
  ASSERT_EQ(got.size(), 6u);
  EXPECT_EQ(got[2].data(), "x3");
  EXPECT_EQ(got[3].data(), "new4");
  EXPECT_EQ(got[5].term(), 2u);
}

TEST(Wal, RejectsAGap) {
  TempDir d;
  Wal w(d.path(), FsyncMode::kBatch);
  w.write(entries(1, 2, 1));
  EXPECT_THROW(w.write(entries(5, 5, 1)), std::logic_error);
}

TEST(Wal, TruncateThenAppendAcrossSegments) {
  TempDir d;
  const uint64_t seg = 200;  // a few records per segment
  {
    Wal w(d.path(), FsyncMode::kBatch, seg);
    w.write(entries(1, 30, 1));
    ASSERT_GE(w.segment_count(), 4u);
    const size_t before = w.segment_count();
    w.truncate_from(8);  // cuts inside an early segment, deletes the later ones
    EXPECT_EQ(w.last_index(), 7u);
    EXPECT_LT(w.segment_count(), before);
    // The later segments' files are really gone, not just forgotten.
    EXPECT_EQ(ledger::storage::list_dir(d.path()).size(), w.segment_count());
    w.write(entries(8, 12, 2, "b"));
    w.sync();
  }
  auto got = reopen(d.path(), seg);
  ASSERT_EQ(got.size(), 12u);
  EXPECT_EQ(got[6].data(), "x7");
  EXPECT_EQ(got[7].data(), "b8");
  EXPECT_EQ(got[11].term(), 2u);
}

TEST(Wal, TruncateEverythingThenStartOver) {
  TempDir d;
  {
    Wal w(d.path(), FsyncMode::kBatch, 200);
    w.write(entries(1, 10, 1));
    w.truncate_from(1);
    EXPECT_TRUE(w.empty());
    w.write(entries(1, 2, 3));
    w.sync();
  }
  auto got = reopen(d.path(), 200);
  ASSERT_EQ(got.size(), 2u);
  EXPECT_EQ(got[0].term(), 3u);
}

TEST(Wal, CompactionDropsOnlyWholeOldSegments) {
  TempDir d;
  const uint64_t seg = 200;
  {
    Wal w(d.path(), FsyncMode::kBatch, seg);
    w.write(entries(1, 40, 1));
    const size_t before = w.segment_count();
    w.compact_through(20);
    EXPECT_LT(w.segment_count(), before);
    EXPECT_GT(w.first_index(), 1u);
    EXPECT_LE(w.first_index(), 21u);  // nothing after 20 may go
    EXPECT_EQ(w.last_index(), 40u);
    w.write(entries(41, 41, 1));
    w.sync();
  }
  auto got = reopen(d.path(), seg);
  ASSERT_FALSE(got.empty());
  EXPECT_LE(got.front().index(), 21u);
  EXPECT_EQ(got.back().index(), 41u);
}

TEST(Wal, CompactionNeverDeletesTheSegmentInUse) {
  TempDir d;
  Wal w(d.path(), FsyncMode::kBatch);  // one big segment
  w.write(entries(1, 10, 1));
  w.compact_through(100);
  EXPECT_EQ(w.first_index(), 1u);
  EXPECT_EQ(w.last_index(), 10u);
}

TEST(Wal, ResetThenStartAtAnyIndex) {
  TempDir d;
  {
    Wal w(d.path(), FsyncMode::kBatch, 200);
    w.write(entries(1, 10, 1));
    w.reset();  // e.g. a snapshot at 49 was installed
    w.write(entries(50, 52, 4));
    w.sync();
  }
  auto got = reopen(d.path(), 200);
  ASSERT_EQ(got.size(), 3u);
  EXPECT_EQ(got.front().index(), 50u);
}

TEST(Wal, FsyncModes) {
  TempDir a, b, c;
  Wal entry_mode(a.path(), FsyncMode::kEntry);
  Wal batch_mode(b.path(), FsyncMode::kBatch);
  Wal no_sync(c.path(), FsyncMode::kNone);
  for (Wal* w : {&entry_mode, &batch_mode, &no_sync}) {
    w->write(entries(1, 10, 1));
    w->sync();
  }
  EXPECT_EQ(entry_mode.fsync_count(), 10u);  // one per record
  EXPECT_EQ(batch_mode.fsync_count(), 1u);   // group commit: one per sync()
  EXPECT_EQ(no_sync.fsync_count(), 0u);
}

// ---------------------------------------------------------------- recovery

TEST(Wal, TornLastRecordAtEveryByteKeepsTheValidPrefix) {
  TempDir d;
  std::vector<LogEntry> want = entries(1, 3, 1);
  const std::string src = d.sub("src");
  {
    Wal w(src, FsyncMode::kBatch);
    w.write(want);
  }
  const std::string full = *ledger::storage::read_file(only_segment(src));
  const size_t good = full.size() - Wal::encode(want[2]).size();  // end of record 2

  // A crash can cut the last write anywhere: every prefix of it must recover
  // to exactly records 1-2, and the WAL must then accept index 3 again.
  for (size_t cut = good; cut < full.size(); ++cut) {
    const std::string dir = d.sub("cut" + std::to_string(cut));
    ledger::storage::make_dirs(dir);
    std::ofstream(dir + "/00000000000000000001.log", std::ios::binary) << full.substr(0, cut);
    {
      Wal w(dir, FsyncMode::kBatch);
      auto rec = w.take_recovery();
      ASSERT_EQ(rec.entries.size(), 2u) << "cut at " << cut;
      EXPECT_EQ(rec.truncated_bytes, cut - good);
      EXPECT_EQ(fs::file_size(dir + "/00000000000000000001.log"), good);
      w.write({want[2]});
      w.sync();
    }
    expect_same(reopen(dir), want);
  }
}

TEST(Wal, CorruptRecordEndsTheLogAndLaterSegmentsGo) {
  TempDir d;
  const uint64_t seg = 200;
  {
    Wal w(d.path(), FsyncMode::kBatch, seg);
    w.write(entries(1, 30, 1));
  }
  auto names = ledger::storage::list_dir(d.path());
  ASSERT_GT(names.size(), 3u);
  // Flip one byte of data in the 2nd segment's first record.
  const std::string victim = d.path() + "/" + names[1];
  std::string bytes = *ledger::storage::read_file(victim);
  bytes[Wal::kHeaderBytes + Wal::kBodyHeaderBytes] ^= 0x40;
  ledger::storage::write_file_atomic(victim, bytes);

  Wal w(d.path(), FsyncMode::kBatch, seg);
  auto rec = w.take_recovery();
  ASSERT_FALSE(rec.entries.empty());
  const LogIndex kept = rec.entries.back().index();
  EXPECT_LT(kept, 30u);
  EXPECT_EQ(rec.deleted_segments, names.size() - 2);
  EXPECT_EQ(w.segment_count(), 2u);
  w.write(entries(kept + 1, kept + 1, 2));  // the log continues right after
}

TEST(Wal, ARecordThatDoesNotContinueTheLogEndsIt) {
  TempDir d;
  {
    Wal w(d.path(), FsyncMode::kBatch);
    w.write(entries(1, 2, 1));
  }
  // A well-formed record with a valid CRC, but index 9 after 2: not ours.
  const std::string seg = only_segment(d.path());
  std::ofstream(seg, std::ios::binary | std::ios::app) << Wal::encode(entry(9, 1, "stray"));
  EXPECT_EQ(reopen(d.path()).size(), 2u);
}

TEST(Wal, GarbageLengthIsNotTrusted) {
  TempDir d;
  {
    Wal w(d.path(), FsyncMode::kBatch);
    w.write(entries(1, 2, 1));
  }
  const std::string seg = only_segment(d.path());
  std::ofstream(seg, std::ios::binary | std::ios::app) << std::string("\xff\xff\xff\xff\0\0\0\0", 8);
  EXPECT_EQ(reopen(d.path()).size(), 2u);
}

// ---------------------------------------------------------------- snapshots

using ledger::storage::SnapshotFile;
using ledger::storage::SnapshotStore;

TEST(SnapshotStore, EmptyThenSaveAndLoadTheNewest) {
  TempDir d;
  SnapshotStore s(d.sub("snap"));
  EXPECT_FALSE(s.load_latest().has_value());
  s.save(SnapshotFile{100, 2, "state@100"});
  s.save(SnapshotFile{250, 3, std::string("bin\0ary", 7)});
  auto got = SnapshotStore(d.sub("snap")).load_latest();
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->index, 250u);
  EXPECT_EQ(got->term, 3u);
  EXPECT_EQ(got->data, std::string("bin\0ary", 7));
}

TEST(SnapshotStore, KeepsOnlyTheNewestTwo) {
  TempDir d;
  SnapshotStore s(d.path());
  for (LogIndex i : {9, 100, 1000, 20}) s.save(SnapshotFile{i, 1, "x"});  // 20 is older than 100
  EXPECT_EQ(s.list(), (std::vector<LogIndex>{1000, 100}));
}

TEST(SnapshotStore, DamagedNewestFallsBackToTheOlderOne) {
  TempDir d;
  SnapshotStore s(d.path());
  s.save(SnapshotFile{100, 1, "good old state"});
  s.save(SnapshotFile{200, 1, "newer state"});
  auto names = ledger::storage::list_dir(d.path());
  ASSERT_EQ(names.size(), 2u);
  const std::string newest = d.path() + "/" + names[1];
  std::string bytes = *ledger::storage::read_file(newest);
  bytes[40] ^= 1;  // flip one bit of the data
  ledger::storage::write_file_atomic(newest, bytes);

  size_t skipped = 0;
  auto got = s.load_latest(&skipped);
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->index, 100u);
  EXPECT_EQ(got->data, "good old state");
  EXPECT_EQ(skipped, 1u);

  ledger::storage::write_file_atomic(newest, bytes.substr(0, 20));  // cut short
  EXPECT_EQ(s.load_latest()->index, 100u);
}
