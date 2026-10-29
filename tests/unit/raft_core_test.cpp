// RaftCore rules on a single node, driven by hand-built messages. Cluster-wide
// behavior (elections under faults) is covered in tests/sim/.
#include "raft/raft_core.h"

#include <gtest/gtest.h>

using ledger::NodeId;
using ledger::Term;
using ledger::pb::RaftMessage;
using ledger::raft::HardState;
using ledger::raft::RaftConfig;
using ledger::raft::RaftCore;
using ledger::raft::Ready;
using ledger::raft::Role;

namespace {

RaftConfig config(NodeId id, std::vector<NodeId> peers) {
  RaftConfig c;
  c.id = id;
  c.peers = std::move(peers);
  c.seed = 42;
  return c;
}

RaftCore five_node(NodeId id = 1, HardState hs = {}, std::vector<ledger::pb::LogEntry> log = {}) {
  std::vector<NodeId> peers;
  for (NodeId p = 1; p <= 5; ++p)
    if (p != id) peers.push_back(p);
  return RaftCore(config(id, peers), hs, std::move(log));
}

RaftMessage request_vote(NodeId from, Term term, uint64_t last_index = 0, Term last_term = 0) {
  RaftMessage m;
  m.set_from(from);
  m.set_to(1);
  m.set_term(term);
  m.mutable_request_vote()->set_last_log_index(last_index);
  m.mutable_request_vote()->set_last_log_term(last_term);
  return m;
}

RaftMessage vote_resp(NodeId from, Term term, bool granted) {
  RaftMessage m;
  m.set_from(from);
  m.set_to(1);
  m.set_term(term);
  m.mutable_request_vote_resp()->set_granted(granted);
  return m;
}

RaftMessage heartbeat(NodeId from, Term term) {
  RaftMessage m;
  m.set_from(from);
  m.set_to(1);
  m.set_term(term);
  m.mutable_append_entries();
  return m;
}

void tick_until_candidate(RaftCore& r) {
  for (int i = 0; i < 100 && r.role() != Role::kCandidate; ++i) r.tick();
  ASSERT_EQ(r.role(), Role::kCandidate);
}

// The single reply a node produced to one message.
RaftMessage only_message(RaftCore& r) {
  Ready rd = r.ready();
  EXPECT_EQ(rd.messages.size(), 1u);
  return rd.messages.empty() ? RaftMessage{} : rd.messages[0];
}

}  // namespace

TEST(RaftCore, TimeoutStartsElection) {
  RaftCore r = five_node();
  EXPECT_EQ(r.role(), Role::kFollower);
  tick_until_candidate(r);

  EXPECT_EQ(r.term(), 1u);
  EXPECT_EQ(r.voted_for(), 1u);
  Ready rd = r.ready();
  // The new term and self-vote must be persisted along with the requests.
  ASSERT_TRUE(rd.hard_state.has_value());
  EXPECT_EQ(rd.hard_state->term, 1u);
  EXPECT_EQ(rd.hard_state->voted_for, 1u);
  ASSERT_EQ(rd.messages.size(), 4u);
  for (const auto& m : rd.messages) EXPECT_TRUE(m.has_request_vote());
}

TEST(RaftCore, VotesOncePerTerm) {
  RaftCore r = five_node();
  r.step(request_vote(2, 1));
  EXPECT_TRUE(only_message(r).request_vote_resp().granted());
  r.step(request_vote(3, 1));
  EXPECT_FALSE(only_message(r).request_vote_resp().granted());
  // Asking again (e.g. a retransmit) gets the same answer.
  r.step(request_vote(2, 1));
  EXPECT_TRUE(only_message(r).request_vote_resp().granted());
}

TEST(RaftCore, RecoveredVoteIsHonored) {
  // Restarted from disk having voted for 2 in term 5.
  RaftCore r = five_node(1, HardState{5, 2});
  r.step(request_vote(3, 5));
  EXPECT_FALSE(only_message(r).request_vote_resp().granted());
}

TEST(RaftCore, RejectsCandidateWithStaleLog) {
  ledger::pb::LogEntry e;
  e.set_term(2);
  e.set_index(1);
  RaftCore r = five_node(1, HardState{2, ledger::kNoNode}, {e});

  // Higher term, but its last log term (1) is older than ours (2).
  r.step(request_vote(2, 3, /*last_index=*/5, /*last_term=*/1));
  RaftMessage reply = only_message(r);
  EXPECT_FALSE(reply.request_vote_resp().granted());
  EXPECT_EQ(r.term(), 3u);  // still adopts the higher term
  EXPECT_EQ(reply.term(), 3u);

  // Same last term and a log at least as long: granted.
  r.step(request_vote(3, 3, /*last_index=*/1, /*last_term=*/2));
  EXPECT_TRUE(only_message(r).request_vote_resp().granted());
}

TEST(RaftCore, RejectsStaleCandidateTerm) {
  RaftCore r = five_node(1, HardState{4, ledger::kNoNode});
  r.step(request_vote(2, 3));
  RaftMessage reply = only_message(r);
  EXPECT_FALSE(reply.request_vote_resp().granted());
  EXPECT_EQ(reply.term(), 4u);  // tells the candidate it is behind
}

TEST(RaftCore, MajorityMakesLeaderWithNoop) {
  RaftCore r = five_node();
  tick_until_candidate(r);
  r.ready();

  r.step(vote_resp(2, 1, true));
  EXPECT_EQ(r.role(), Role::kCandidate);  // 2 of 5
  r.step(vote_resp(2, 1, true));          // duplicate delivery
  EXPECT_EQ(r.role(), Role::kCandidate);  // still 2 of 5
  r.step(vote_resp(3, 1, false));
  EXPECT_EQ(r.role(), Role::kCandidate);
  r.step(vote_resp(4, 1, true));
  EXPECT_EQ(r.role(), Role::kLeader);  // 3 of 5
  EXPECT_EQ(r.leader(), 1u);

  Ready rd = r.ready();
  ASSERT_EQ(rd.entries.size(), 1u);
  EXPECT_EQ(rd.entries[0].type(), ledger::pb::ENTRY_NOOP);
  EXPECT_EQ(rd.entries[0].term(), 1u);
  EXPECT_EQ(rd.entries[0].index(), 1u);
  EXPECT_EQ(rd.messages.size(), 4u);  // immediate heartbeat
  for (const auto& m : rd.messages) EXPECT_TRUE(m.has_append_entries());
}

TEST(RaftCore, IgnoresVotesFromOldTerm) {
  RaftCore r = five_node();
  tick_until_candidate(r);  // term 1
  // Nobody answers (a split vote): the candidate times out and retries.
  for (int i = 0; i < 100 && r.term() == 1; ++i) r.tick();
  ASSERT_EQ(r.term(), 2u);
  r.step(vote_resp(2, 1, true));
  r.step(vote_resp(3, 1, true));
  EXPECT_EQ(r.role(), Role::kCandidate);
}

TEST(RaftCore, LeaderSendsHeartbeats) {
  RaftCore r = five_node();
  tick_until_candidate(r);
  r.step(vote_resp(2, 1, true));
  r.step(vote_resp(3, 1, true));
  ASSERT_EQ(r.role(), Role::kLeader);
  r.ready();

  for (int i = 0; i < 4; ++i) r.tick();
  EXPECT_FALSE(r.has_ready());
  r.tick();  // heartbeat_interval = 5
  EXPECT_EQ(r.ready().messages.size(), 4u);
}

TEST(RaftCore, HigherTermStepsLeaderDown) {
  RaftCore r = five_node();
  tick_until_candidate(r);
  r.step(vote_resp(2, 1, true));
  r.step(vote_resp(3, 1, true));
  ASSERT_EQ(r.role(), Role::kLeader);
  r.ready();

  r.step(heartbeat(4, 2));
  EXPECT_EQ(r.role(), Role::kFollower);
  EXPECT_EQ(r.term(), 2u);
  EXPECT_EQ(r.leader(), 4u);
  Ready rd = r.ready();
  ASSERT_TRUE(rd.hard_state.has_value());
  EXPECT_EQ(rd.hard_state->voted_for, ledger::kNoNode);  // fresh vote in term 2
}

TEST(RaftCore, CandidateYieldsToLeaderOfSameTerm) {
  RaftCore r = five_node();
  tick_until_candidate(r);
  r.step(heartbeat(3, 1));
  EXPECT_EQ(r.role(), Role::kFollower);
  EXPECT_EQ(r.leader(), 3u);
  EXPECT_EQ(r.term(), 1u);
}

TEST(RaftCore, RejectsStaleLeader) {
  RaftCore r = five_node(1, HardState{5, ledger::kNoNode});
  r.step(heartbeat(2, 3));
  RaftMessage reply = only_message(r);
  EXPECT_FALSE(reply.append_entries_resp().success());
  EXPECT_EQ(reply.term(), 5u);
  EXPECT_EQ(r.leader(), ledger::kNoNode);
}

TEST(RaftCore, HeartbeatsPreventElection) {
  RaftCore r = five_node();
  for (int i = 0; i < 1000; ++i) {
    r.tick();
    if (i % 5 == 0) r.step(heartbeat(2, 1));
  }
  EXPECT_EQ(r.role(), Role::kFollower);
  EXPECT_EQ(r.term(), 1u);
}

TEST(RaftCore, SingleNodeElectsItself) {
  RaftCore r(config(1, {}), {}, {});
  for (int i = 0; i < 100; ++i) r.tick();
  EXPECT_EQ(r.role(), Role::kLeader);  // its own vote is a majority of one
  EXPECT_EQ(r.term(), 1u);
}

// ---------------------------------------------------------------- replication

namespace {

ledger::pb::LogEntry entry(uint64_t index, Term term, const std::string& data = "") {
  ledger::pb::LogEntry e;
  e.set_index(index);
  e.set_term(term);
  e.set_type(ledger::pb::ENTRY_NORMAL);
  e.set_data(data);
  return e;
}

RaftMessage append(NodeId from, Term term, uint64_t prev, Term prev_term,
                   std::vector<ledger::pb::LogEntry> entries, uint64_t leader_commit) {
  RaftMessage m;
  m.set_from(from);
  m.set_to(1);
  m.set_term(term);
  auto* ae = m.mutable_append_entries();
  ae->set_prev_log_index(prev);
  ae->set_prev_log_term(prev_term);
  for (auto& e : entries) *ae->add_entries() = e;
  ae->set_leader_commit(leader_commit);
  return m;
}

RaftMessage append_resp(NodeId from, Term term, bool success, uint64_t match,
                        uint64_t conflict_index = 0, Term conflict_term = 0) {
  RaftMessage m;
  m.set_from(from);
  m.set_to(1);
  m.set_term(term);
  auto* r = m.mutable_append_entries_resp();
  r->set_success(success);
  r->set_match_index(match);
  r->set_conflict_index(conflict_index);
  r->set_conflict_term(conflict_term);
  return m;
}

// Node 1 as leader of a 5-node cluster, with Ready drained.
void make_leader(RaftCore& r) {
  tick_until_candidate(r);
  Term t = r.term();
  r.step(vote_resp(2, t, true));
  r.step(vote_resp(3, t, true));
  ASSERT_EQ(r.role(), Role::kLeader);
  r.ready();
}

std::vector<uint64_t> indexes(const std::vector<ledger::pb::LogEntry>& es) {
  std::vector<uint64_t> out;
  for (const auto& e : es) out.push_back(e.index());
  return out;
}

}  // namespace

TEST(RaftCore, FollowerAppendsAndCommits) {
  RaftCore r = five_node();
  r.step(append(2, 1, 0, 0, {entry(1, 1, "a"), entry(2, 1, "b")}, 1));
  Ready rd = r.ready();
  EXPECT_EQ(indexes(rd.entries), (std::vector<uint64_t>{1, 2}));  // to persist
  EXPECT_EQ(indexes(rd.committed_entries), (std::vector<uint64_t>{1}));  // to apply
  ASSERT_EQ(rd.messages.size(), 1u);
  EXPECT_TRUE(rd.messages[0].append_entries_resp().success());
  EXPECT_EQ(rd.messages[0].append_entries_resp().match_index(), 2u);
}

TEST(RaftCore, FollowerRejectsWhenLogTooShort) {
  RaftCore r = five_node(1, HardState{1, ledger::kNoNode}, {entry(1, 1)});
  r.step(append(2, 1, 5, 1, {entry(6, 1)}, 0));
  auto resp = only_message(r).append_entries_resp();
  EXPECT_FALSE(resp.success());
  EXPECT_EQ(resp.conflict_index(), 2u);  // "send me everything from 2"
  EXPECT_EQ(resp.conflict_term(), 0u);
  EXPECT_EQ(r.log().last_index(), 1u);
}

TEST(RaftCore, FollowerRejectsMismatchWithTermHint) {
  // Entries 2..4 came from a term-2 leader that was deposed.
  RaftCore r = five_node(1, HardState{2, ledger::kNoNode},
                         {entry(1, 1), entry(2, 2), entry(3, 2), entry(4, 2)});
  r.step(append(5, 3, 4, 3, {}, 0));  // leader's entry 4 has term 3
  auto resp = only_message(r).append_entries_resp();
  EXPECT_FALSE(resp.success());
  EXPECT_EQ(resp.conflict_term(), 2u);
  EXPECT_EQ(resp.conflict_index(), 2u);  // first index of our term-2 run
}

TEST(RaftCore, FollowerOverwritesConflictingSuffix) {
  RaftCore r = five_node(1, HardState{2, ledger::kNoNode},
                         {entry(1, 1), entry(2, 2, "old"), entry(3, 2, "old")});
  r.step(append(5, 3, 1, 1, {entry(2, 3, "new"), entry(3, 3, "new"), entry(4, 3, "new")}, 0));
  Ready rd = r.ready();
  EXPECT_EQ(r.log().last_index(), 4u);
  EXPECT_EQ(r.log().at(2).data(), "new");
  // The host must replace its stored entries from index 2 on.
  EXPECT_EQ(indexes(rd.entries), (std::vector<uint64_t>{2, 3, 4}));
}

TEST(RaftCore, StaleShorterAppendDoesNotTruncate) {
  RaftCore r = five_node();
  r.step(append(2, 1, 0, 0, {entry(1, 1), entry(2, 1), entry(3, 1)}, 0));
  r.ready();
  // A delayed duplicate of an earlier, shorter message arrives late.
  r.step(append(2, 1, 0, 0, {entry(1, 1)}, 0));
  EXPECT_EQ(r.log().last_index(), 3u);  // entries 2 and 3 must survive
  EXPECT_TRUE(r.ready().entries.empty());
}

TEST(RaftCore, FollowerCommitsOnlyUpToVerifiedEntries) {
  // Entries 3-4 are stale leftovers (term 1) the new leader doesn't have.
  RaftCore r = five_node(1, HardState{1, ledger::kNoNode},
                         {entry(1, 1), entry(2, 1), entry(3, 1, "stale"), entry(4, 1, "stale")});
  // The leader (term 2) has committed up to 3, but this message only proves
  // our log matches its log up to index 2. Committing 3 here would apply our
  // stale entry 3 instead of the leader's.
  r.step(append(2, 2, 2, 1, {}, 3));
  EXPECT_EQ(r.commit_index(), 2u);
}

TEST(RaftCore, LeaderBacktracksUsingConflictHint) {
  RaftCore r = five_node(1, HardState{3, ledger::kNoNode},
                         {entry(1, 1), entry(2, 1), entry(3, 3), entry(4, 3)});
  make_leader(r);  // term 4, no-op at 5; next for everyone = 5
  // Follower 2 has a term-2 run starting at 2 that we never had.
  r.step(append_resp(2, r.term(), false, 0, /*conflict_index=*/2, /*conflict_term=*/2));
  // We have no term-2 entries, so skip the whole run: resend from 2.
  EXPECT_EQ(r.progress().at(2).next, 2u);
  auto m = only_message(r);
  EXPECT_EQ(m.append_entries().prev_log_index(), 1u);
  EXPECT_EQ(m.append_entries().entries_size(), 4);  // 2..5

  // Follower 3's last term-1 entry agrees with ours: resume after our last term-1 entry.
  r.step(append_resp(3, r.term(), false, 0, /*conflict_index=*/2, /*conflict_term=*/1));
  EXPECT_EQ(r.progress().at(3).next, 3u);
}

TEST(RaftCore, ProposeOnlyOnLeader) {
  RaftCore r = five_node();
  EXPECT_FALSE(r.propose("x").has_value());
  make_leader(r);
  auto p = r.propose("x");
  ASSERT_TRUE(p.has_value());
  EXPECT_EQ(p->index, 2u);  // after the no-op
  EXPECT_EQ(p->term, r.term());
}

TEST(RaftCore, LeaderCommitsOnMajority) {
  RaftCore r = five_node();
  make_leader(r);         // no-op at 1
  r.propose("a");         // 2
  r.ready();
  r.step(append_resp(2, r.term(), true, 2));
  EXPECT_EQ(r.commit_index(), 0u);  // leader + 1 follower = 2 of 5
  r.step(append_resp(3, r.term(), true, 1));
  EXPECT_EQ(r.commit_index(), 1u);  // index 1 is on 3 of 5
  r.step(append_resp(3, r.term(), true, 2));
  EXPECT_EQ(r.commit_index(), 2u);
  EXPECT_EQ(indexes(r.ready().committed_entries), (std::vector<uint64_t>{1, 2}));
}

// Figure 8 of the Raft paper, from S1's point of view. S1 led term 2 and got
// entry 2 onto S2 only, then crashed. S5 won term 3 and wrote its own entry 2
// on itself only, then crashed. Now S1 wins term 4 and copies its old entry 2
// to S3, so entry 2 (term 2) sits on a majority: S1, S2, S3.
//
// It must still NOT count as committed. S5 can win term 5 with votes from
// S2, S3, S4 (its last term 3 beats their last term 2) and overwrite entry 2
// everywhere. Only an entry of the leader's own term may be committed by
// counting; once one is, everything before it is safe too.
TEST(RaftCore, Figure8OldTermEntryNotCommittedByCounting) {
  RaftCore s1 = five_node(1, HardState{3, ledger::kNoNode}, {entry(1, 1), entry(2, 2, "s1")});
  make_leader(s1);  // term 4; appends no-op at index 3
  ASSERT_EQ(s1.term(), 4u);

  s1.step(append_resp(2, 4, true, 2));
  s1.step(append_resp(3, 4, true, 2));
  EXPECT_EQ(s1.commit_index(), 0u) << "committed an old-term entry by counting";
  EXPECT_TRUE(s1.ready().committed_entries.empty());

  // The no-op (term 4) reaches a majority: now 1..3 are all committed.
  s1.step(append_resp(2, 4, true, 3));
  s1.step(append_resp(3, 4, true, 3));
  EXPECT_EQ(s1.commit_index(), 3u);
  EXPECT_EQ(indexes(s1.ready().committed_entries), (std::vector<uint64_t>{1, 2, 3}));
}

TEST(RaftCore, SingleNodeCommitsImmediately) {
  RaftCore r(config(1, {}), {}, {});
  for (int i = 0; i < 100; ++i) r.tick();
  ASSERT_EQ(r.role(), Role::kLeader);
  r.propose("x");
  EXPECT_EQ(r.commit_index(), 2u);
}

TEST(RaftCore, LeaderResendsToFollowerThatLostItsLog) {
  RaftCore r = five_node();
  make_leader(r);  // no-op at 1
  r.propose("a");  // 2
  r.ready();
  r.step(append_resp(2, r.term(), true, 2));
  ASSERT_EQ(r.progress().at(2).match, 2u);

  // Node 2 restarted with an empty log (in-memory storage) and rejects.
  r.step(append_resp(2, r.term(), false, 0, /*conflict_index=*/1));
  EXPECT_EQ(r.progress().at(2).match, 0u);
  EXPECT_EQ(r.progress().at(2).next, 1u);
  auto m = only_message(r);
  EXPECT_EQ(m.append_entries().prev_log_index(), 0u);
  EXPECT_EQ(m.append_entries().entries_size(), 2);  // everything again
}

// ---------------------------------------------------------------- ReadIndex

namespace {

RaftMessage ack_ctx(NodeId from, Term term, uint64_t match, uint64_t ctx) {
  RaftMessage m = append_resp(from, term, true, match);
  m.mutable_append_entries_resp()->set_read_ctx(ctx);
  return m;
}

// Leader of term 1 whose no-op (index 1) is committed; Ready drained.
void make_committed_leader(RaftCore& r) {
  make_leader(r);
  r.step(append_resp(2, r.term(), true, 1));
  r.step(append_resp(3, r.term(), true, 1));
  ASSERT_EQ(r.commit_index(), 1u);
  r.ready();
}

}  // namespace

TEST(RaftCore, FollowerRefusesRead) {
  RaftCore r = five_node();
  EXPECT_FALSE(r.read_index(7));
}

TEST(RaftCore, ReadNeedsMajorityAckAfterItStarted) {
  RaftCore r = five_node();
  make_committed_leader(r);

  ASSERT_TRUE(r.read_index(7));
  Ready rd = r.ready();
  EXPECT_TRUE(rd.reads_ready.empty());
  ASSERT_EQ(rd.messages.size(), 4u);  // immediate confirmation round
  const uint64_t ctx = rd.messages[0].append_entries().read_ctx();
  EXPECT_GT(ctx, 0u);

  r.step(ack_ctx(2, r.term(), 1, ctx));
  EXPECT_TRUE(r.ready().reads_ready.empty());  // self + 1 = 2 of 5
  r.step(ack_ctx(3, r.term(), 1, ctx));
  rd = r.ready();
  ASSERT_EQ(rd.reads_ready.size(), 1u);  // self + 2 = 3 of 5
  EXPECT_EQ(rd.reads_ready[0].request_id, 7u);
  EXPECT_EQ(rd.reads_ready[0].index, 1u);  // the commit index when it started
}

TEST(RaftCore, ReplyToMessageSentBeforeReadDoesNotConfirmIt) {
  RaftCore r = five_node();
  make_committed_leader(r);
  ASSERT_TRUE(r.read_index(7));
  r.ready();
  // Late replies to heartbeats sent before the read (ctx 0). They prove we
  // were leader back then, not now: a newer leader may have been elected
  // and committed writes in between.
  r.step(ack_ctx(2, r.term(), 1, 0));
  r.step(ack_ctx(3, r.term(), 1, 0));
  r.step(ack_ctx(4, r.term(), 1, 0));
  EXPECT_TRUE(r.ready().reads_ready.empty());
}

TEST(RaftCore, ReadWaitsForNewLeaderToCommitInItsTerm) {
  // Inherited entries 1-2 from term 1. They may well be committed already
  // (by the old leader), but this new leader's commit index is still 0.
  RaftCore r = five_node(1, HardState{1, ledger::kNoNode}, {entry(1, 1), entry(2, 1)});
  make_leader(r);  // term 2, no-op at 3, commit 0
  ASSERT_TRUE(r.read_index(7));
  r.ready();
  // Even a full confirmation round must not serve it at readIndex 0.
  r.step(ack_ctx(2, r.term(), 0, 99));
  r.step(ack_ctx(3, r.term(), 0, 99));
  EXPECT_TRUE(r.ready().reads_ready.empty());

  // The no-op commits -> the read starts at index 3, then gets confirmed.
  r.step(append_resp(2, r.term(), true, 3));
  r.step(append_resp(3, r.term(), true, 3));
  ASSERT_EQ(r.commit_index(), 3u);
  Ready rd = r.ready();
  ASSERT_FALSE(rd.messages.empty());
  uint64_t ctx = rd.messages.back().append_entries().read_ctx();
  r.step(ack_ctx(2, r.term(), 3, ctx));
  r.step(ack_ctx(3, r.term(), 3, ctx));
  rd = r.ready();
  ASSERT_EQ(rd.reads_ready.size(), 1u);
  EXPECT_EQ(rd.reads_ready[0].index, 3u);
}

TEST(RaftCore, PendingReadsFailWhenLeaderStepsDown) {
  RaftCore r = five_node();
  make_committed_leader(r);
  ASSERT_TRUE(r.read_index(7));
  ASSERT_TRUE(r.read_index(8));
  r.ready();
  r.step(heartbeat(4, r.term() + 1));  // a newer leader exists
  Ready rd = r.ready();
  EXPECT_EQ(rd.reads_failed, (std::vector<uint64_t>{7, 8}));
  EXPECT_TRUE(rd.reads_ready.empty());
}

TEST(RaftCore, FollowerEchoesReadCtx) {
  RaftCore r = five_node();
  RaftMessage m = heartbeat(2, 1);
  m.mutable_append_entries()->set_read_ctx(42);
  r.step(m);
  EXPECT_EQ(only_message(r).append_entries_resp().read_ctx(), 42u);
}

TEST(RaftCore, SingleNodeServesReadImmediately) {
  RaftCore r(config(1, {}), {}, {});
  for (int i = 0; i < 100; ++i) r.tick();
  r.ready();
  ASSERT_TRUE(r.read_index(7));
  EXPECT_EQ(r.ready().reads_ready.size(), 1u);
}
