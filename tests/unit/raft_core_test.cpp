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
