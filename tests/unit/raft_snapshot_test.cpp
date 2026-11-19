// RaftCore snapshots on a single node, driven by hand-built messages:
// compaction, sending InstallSnapshot in chunks, installing it (and which
// log entries survive), and restarting from a snapshot. Random runs with
// snapshots are in tests/sim/; these pin the rare cases down.
#include <gtest/gtest.h>

#include "raft/raft_core.h"

using ledger::LogIndex;
using ledger::NodeId;
using ledger::Term;
using ledger::pb::LogEntry;
using ledger::pb::RaftMessage;
using ledger::raft::HardState;
using ledger::raft::RaftConfig;
using ledger::raft::RaftCore;
using ledger::raft::Ready;
using ledger::raft::Role;
using ledger::raft::Snapshot;

namespace {

RaftConfig config(size_t chunk = 4, LogIndex margin = 2) {
  RaftConfig c;
  c.id = 1;
  c.peers = {2, 3};
  c.seed = 42;
  c.snapshot_chunk_bytes = chunk;
  c.compaction_margin = margin;
  return c;
}

LogEntry entry(LogIndex index, Term term) {
  LogEntry e;
  e.set_index(index);
  e.set_term(term);
  e.set_type(ledger::pb::ENTRY_NORMAL);
  e.set_data("e" + std::to_string(index));
  return e;
}

std::vector<LogEntry> log_of(std::vector<Term> terms) {
  std::vector<LogEntry> out;
  for (size_t i = 0; i < terms.size(); ++i) out.push_back(entry(i + 1, terms[i]));
  return out;
}

RaftMessage from(NodeId sender, Term term) {
  RaftMessage m;
  m.set_from(sender);
  m.set_to(1);
  m.set_term(term);
  return m;
}

RaftMessage chunk(Term term, LogIndex index, Term snap_term, uint64_t offset, std::string data,
                  bool done) {
  RaftMessage m = from(2, term);
  auto* is = m.mutable_install_snapshot();
  is->set_last_index(index);
  is->set_last_term(snap_term);
  is->set_offset(offset);
  is->set_data(std::move(data));
  is->set_done(done);
  return m;
}

const ledger::pb::InstallSnapshotResp& only_snapshot_resp(Ready& rd) {
  static const ledger::pb::InstallSnapshotResp kNone;
  EXPECT_EQ(rd.messages.size(), 1u);
  if (rd.messages.empty() || !rd.messages[0].has_install_snapshot_resp()) return kNone;
  return rd.messages[0].install_snapshot_resp();
}

// Node 1 as leader of {1,2,3} in term 1, with entries 1..n committed and
// applied (node 2 acknowledged them all).
RaftCore leader_with(LogIndex n, size_t chunk_bytes = 4) {
  RaftCore r(config(chunk_bytes), {}, {});
  for (int i = 0; i < 100 && r.role() == Role::kFollower; ++i) r.tick();
  RaftMessage v = from(2, r.term());
  v.mutable_request_vote_resp()->set_granted(true);
  r.step(v);
  EXPECT_EQ(r.role(), Role::kLeader);
  for (LogIndex i = 2; i <= n; ++i) r.propose("p" + std::to_string(i));  // 1 is the no-op
  RaftMessage ack = from(2, r.term());
  ack.mutable_append_entries_resp()->set_success(true);
  ack.mutable_append_entries_resp()->set_match_index(n);
  r.step(ack);
  r.ready();
  EXPECT_EQ(r.commit_index(), n);
  return r;
}

}  // namespace

TEST(RaftSnapshot, CompactKeepsAMarginOfEntries) {
  RaftCore r = leader_with(10);
  r.compact(8, "state@8");
  ASSERT_TRUE(r.snapshot().has_value());
  EXPECT_EQ(r.snapshot()->index, 8u);
  EXPECT_EQ(r.snapshot()->term, 1u);
  EXPECT_EQ(r.log().snapshot_index(), 6u);  // 8 - margin 2
  EXPECT_EQ(r.log().last_index(), 10u);
  r.compact(5, "older");  // not newer than what we have: ignored
  EXPECT_EQ(r.snapshot()->index, 8u);
}

TEST(RaftSnapshot, LeaderSendsSnapshotInChunksThenEntries) {
  RaftCore r = leader_with(10, /*chunk_bytes=*/4);
  r.compact(8, "0123456789");  // 3 chunks of at most 4 bytes

  // Node 3 says its log ends at 2: entry 3 was compacted away.
  RaftMessage rej = from(3, r.term());
  rej.mutable_append_entries_resp()->set_success(false);
  rej.mutable_append_entries_resp()->set_conflict_index(3);
  r.step(rej);

  std::string got;
  for (int i = 0; i < 3; ++i) {
    Ready rd = r.ready();
    ASSERT_EQ(rd.messages.size(), 1u);
    ASSERT_TRUE(rd.messages[0].has_install_snapshot()) << "chunk " << i;
    const auto& is = rd.messages[0].install_snapshot();
    EXPECT_EQ(is.last_index(), 8u);
    EXPECT_EQ(is.offset(), got.size());
    got += is.data();
    EXPECT_EQ(is.done(), i == 2);
    RaftMessage resp = from(3, r.term());
    resp.mutable_install_snapshot_resp()->set_last_index(8);
    resp.mutable_install_snapshot_resp()->set_bytes_received(got.size());
    resp.mutable_install_snapshot_resp()->set_done(is.done());
    r.step(resp);
  }
  EXPECT_EQ(got, "0123456789");
  EXPECT_EQ(r.progress().at(3).match, 8u);
  // Then ordinary replication of 9..10.
  Ready rd = r.ready();
  ASSERT_EQ(rd.messages.size(), 1u);
  ASSERT_TRUE(rd.messages[0].has_append_entries());
  EXPECT_EQ(rd.messages[0].append_entries().prev_log_index(), 8u);
  EXPECT_EQ(rd.messages[0].append_entries().entries_size(), 2);
}

TEST(RaftSnapshot, LeaderResumesFromWhereTheFollowerSaysItIs) {
  RaftCore r = leader_with(10, 4);
  r.compact(8, "0123456789");
  RaftMessage rej = from(3, r.term());
  rej.mutable_append_entries_resp()->set_success(false);
  rej.mutable_append_entries_resp()->set_conflict_index(1);
  r.step(rej);
  r.ready();  // chunk at 0
  RaftMessage resp = from(3, r.term());
  resp.mutable_install_snapshot_resp()->set_last_index(8);
  resp.mutable_install_snapshot_resp()->set_bytes_received(0);  // it lost track
  r.step(resp);
  Ready rd = r.ready();
  ASSERT_TRUE(rd.messages.at(0).has_install_snapshot());
  EXPECT_EQ(rd.messages[0].install_snapshot().offset(), 0u);
}

TEST(RaftSnapshot, FollowerInstallsAndDropsAConflictingLog) {
  // Our log: 1..5, all term 1. Index 4 and 5 came from a deposed leader.
  RaftCore f(config(), HardState{1, 0}, log_of({1, 1, 1, 1, 1}));
  f.step(chunk(2, 4, /*snap_term=*/2, 0, "abc", false));
  Ready rd = f.ready();
  EXPECT_FALSE(rd.snapshot.has_value());
  EXPECT_EQ(only_snapshot_resp(rd).bytes_received(), 3u);

  f.step(chunk(2, 4, 2, 3, "de", true));
  rd = f.ready();
  ASSERT_TRUE(rd.snapshot.has_value());
  EXPECT_EQ(rd.snapshot->index, 4u);
  EXPECT_EQ(rd.snapshot->term, 2u);
  EXPECT_EQ(rd.snapshot->data, "abcde");
  EXPECT_TRUE(only_snapshot_resp(rd).done());
  // Our entry 4 has term 1, the snapshot's has term 2: our 5 came after a
  // different entry 4, so it must go (Log Matching).
  EXPECT_EQ(f.log().snapshot_index(), 4u);
  EXPECT_EQ(f.log().last_index(), 4u);
  EXPECT_TRUE(rd.entries.empty());
  EXPECT_EQ(f.commit_index(), 4u);
  EXPECT_TRUE(rd.committed_entries.empty());  // the snapshot IS the applied state
}

TEST(RaftSnapshot, FollowerKeepsEntriesThatFollowAMatchingSnapshot) {
  RaftCore f(config(), HardState{1, 0}, log_of({1, 1, 1, 1, 1}));
  f.step(chunk(1, 3, /*snap_term=*/1, 0, "s", true));
  Ready rd = f.ready();
  ASSERT_TRUE(rd.snapshot.has_value());
  EXPECT_EQ(f.log().snapshot_index(), 3u);
  EXPECT_EQ(f.log().last_index(), 5u);  // 4 and 5 kept
  // Handed back for the host to store after the snapshot.
  ASSERT_EQ(rd.entries.size(), 2u);
  EXPECT_EQ(rd.entries[0].index(), 4u);
}

TEST(RaftSnapshot, FollowerRefusesAChunkThatDoesNotFit) {
  RaftCore f(config(), {}, {});
  f.step(chunk(1, 9, 1, 0, "ab", false));
  f.ready();
  f.step(chunk(1, 9, 1, 5, "zz", false));  // skips 2..4 (a chunk was lost)
  Ready rd = f.ready();
  EXPECT_FALSE(rd.snapshot.has_value());
  EXPECT_EQ(only_snapshot_resp(rd).bytes_received(), 2u);  // "resume at 2"
}

TEST(RaftSnapshot, SnapshotWeAlreadyHaveIsAcknowledgedNotInstalled) {
  RaftCore f(config(), HardState{1, 0}, log_of({1, 1, 1}));
  RaftMessage ae = from(2, 1);
  ae.mutable_append_entries()->set_prev_log_index(3);
  ae.mutable_append_entries()->set_prev_log_term(1);
  ae.mutable_append_entries()->set_leader_commit(3);
  f.step(ae);
  f.ready();
  f.step(chunk(1, 2, 1, 0, "old", true));  // covers only 1..2, all committed here
  Ready rd = f.ready();
  EXPECT_FALSE(rd.snapshot.has_value());
  EXPECT_TRUE(only_snapshot_resp(rd).done());
  EXPECT_EQ(f.log().last_index(), 3u);
}

TEST(RaftSnapshot, AppendEntriesBelowOurSnapshotReportsTheSnapshotIndex) {
  RaftCore f(config(), HardState{1, 0}, {}, Snapshot{6, 1, "s"});
  RaftMessage ae = from(2, 1);
  ae.mutable_append_entries()->set_prev_log_index(2);
  ae.mutable_append_entries()->set_prev_log_term(1);
  *ae.mutable_append_entries()->add_entries() = entry(3, 1);
  f.step(ae);
  Ready rd = f.ready();
  ASSERT_EQ(rd.messages.size(), 1u);
  const auto& resp = rd.messages[0].append_entries_resp();
  EXPECT_TRUE(resp.success());
  EXPECT_EQ(resp.match_index(), 6u);  // not 3: what we know matches is the snapshot
}

TEST(RaftSnapshot, RestartFromSnapshotSkipsCoveredEntries) {
  // The WAL can still hold entries the snapshot covers (it drops whole
  // segments only): they're ignored; the log continues after the snapshot.
  RaftCore r(config(), HardState{2, 0}, log_of({1, 1, 1, 2, 2}), Snapshot{3, 1, "s"});
  EXPECT_EQ(r.log().snapshot_index(), 3u);
  EXPECT_EQ(r.log().last_index(), 5u);
  EXPECT_EQ(r.log().term_at(3), 1u);
  EXPECT_EQ(r.commit_index(), 3u);
  EXPECT_TRUE(r.ready().committed_entries.empty());  // 1..3 are in the snapshot
}
