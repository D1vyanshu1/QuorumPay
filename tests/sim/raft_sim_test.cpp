// Leader election on a simulated 5-node cluster, across many seeds. Each seed
// is a different but exactly reproducible schedule of timeouts and message
// delays. A failure message names its seed so the run can be replayed.
#include <gtest/gtest.h>

#include <algorithm>

#include "sim/sim_network.h"

using ledger::NodeId;
using ledger::Term;
using ledger::kNoNode;
using ledger::raft::Role;
using ledger::sim::SimCluster;
using ledger::sim::SimOptions;

namespace {

void expect_safe(const SimCluster& c) {
  for (const auto& v : c.violations()) ADD_FAILURE() << v;
}

}  // namespace

TEST(RaftSim, ElectsExactlyOneLeader) {
  for (uint64_t seed = 1; seed <= 1000; ++seed) {
    SimCluster c(5, seed);
    int ticks = c.run_until_leader(300);  // 3s of virtual time
    ASSERT_NE(ticks, -1) << "no leader, seed " << seed;

    // With no faults the leader keeps everyone's timers reset, so there must
    // be no further elections.
    NodeId leader = c.leaders()[0];
    Term term = c.node(leader).term();
    c.run(500);
    EXPECT_EQ(c.leaders(), std::vector<NodeId>{leader}) << "seed " << seed;
    EXPECT_EQ(c.node(leader).term(), term) << "seed " << seed;
    expect_safe(c);
  }
}

TEST(RaftSim, NewLeaderAfterLeaderCrash) {
  for (uint64_t seed = 1; seed <= 300; ++seed) {
    SimCluster c(5, seed);
    ASSERT_NE(c.run_until_leader(300), -1) << "seed " << seed;
    NodeId old_leader = c.leaders()[0];
    Term old_term = c.node(old_leader).term();

    c.crash(old_leader);
    std::vector<NodeId> survivors;
    for (NodeId id : c.ids())
      if (id != old_leader) survivors.push_back(id);
    NodeId new_leader = kNoNode;
    for (int i = 0; i < 300 && new_leader == kNoNode; ++i) {
      c.tick();
      new_leader = c.stable_leader(survivors);
    }
    ASSERT_NE(new_leader, kNoNode) << "seed " << seed;
    EXPECT_GT(c.node(new_leader).term(), old_term) << "seed " << seed;

    // The old leader comes back with its term from disk, sees the higher
    // term, and rejoins as a follower.
    c.restart(old_leader);
    c.run(100);
    EXPECT_EQ(c.node(old_leader).role(), Role::kFollower) << "seed " << seed;
    EXPECT_EQ(c.node(old_leader).leader(), new_leader) << "seed " << seed;
    expect_safe(c);
  }
}

TEST(RaftSim, MinorityPartitionCannotElect) {
  for (uint64_t seed = 1; seed <= 300; ++seed) {
    SimCluster c(5, seed);
    ASSERT_NE(c.run_until_leader(300), -1) << "seed " << seed;
    NodeId old_leader = c.leaders()[0];
    Term old_term = c.node(old_leader).term();

    // Put the leader in the minority with one other node.
    std::vector<NodeId> minority{old_leader}, majority;
    for (NodeId id : c.ids()) {
      if (id == old_leader) continue;
      (minority.size() < 2 ? minority : majority).push_back(id);
    }
    c.partition({minority, majority});
    c.run(1000);

    NodeId new_leader = c.stable_leader(majority);
    ASSERT_NE(new_leader, kNoNode) << "majority has no leader, seed " << seed;
    EXPECT_GT(c.node(new_leader).term(), old_term);

    // Nobody in the minority can collect 3 votes, so it never gets a new
    // leader. The old leader still *believes* it leads in its old term: it
    // has no way to learn otherwise. That is harmless, because it can't
    // commit anything without a majority (and check-quorum could fix it).
    for (NodeId id : minority) {
      const auto& n = c.node(id);
      if (n.role() == Role::kLeader) {
        EXPECT_EQ(n.term(), old_term) << "seed " << seed;
      }
    }
    EXPECT_EQ(c.node(old_leader).role(), Role::kLeader);

    // After healing, the higher term wins and the old leader steps down.
    c.heal();
    ASSERT_NE(c.run_until_leader(500), -1) << "no leader after heal, seed " << seed;
    EXPECT_EQ(c.leaders().size(), 1u);
    expect_safe(c);
  }
}

TEST(RaftSim, ConvergesUnderLossDuplicationAndReordering) {
  SimOptions opts;
  opts.drop_rate = 0.25;
  opts.duplicate_rate = 0.10;
  opts.max_delay = 6;  // wide delay range -> lots of reordering
  for (uint64_t seed = 1; seed <= 300; ++seed) {
    SimCluster c(5, seed, opts);
    ASSERT_NE(c.run_until_leader(3000), -1) << "seed " << seed;
    expect_safe(c);
  }
}

TEST(RaftSim, FixedTimeoutsSplitVoteForever) {
  // Every node uses the same timeout, so all time out on the same tick, all
  // vote for themselves, and nobody ever gets a majority. This is the reason
  // Raft randomizes election timeouts.
  SimOptions opts;
  opts.election_timeout_min = 20;
  opts.election_timeout_max = 20;
  SimCluster c(5, 1, opts);
  c.run(2000);
  EXPECT_TRUE(c.leaders().empty());
  EXPECT_GE(c.node(1).term(), 50u);  // one failed election every 20 ticks

  SimCluster randomized(5, 1);  // default 15-30 tick range
  EXPECT_NE(randomized.run_until_leader(300), -1);
}

TEST(RaftSim, RestartKeepsTermAndVote) {
  SimCluster c(5, 7);
  ASSERT_NE(c.run_until_leader(300), -1);
  NodeId follower = c.leaders()[0] == 1 ? 2 : 1;
  Term term = c.node(follower).term();
  NodeId vote = c.node(follower).voted_for();

  c.crash(follower);
  c.restart(follower);
  EXPECT_EQ(c.node(follower).term(), term);
  EXPECT_EQ(c.node(follower).voted_for(), vote);
  EXPECT_EQ(c.node(follower).role(), Role::kFollower);
}

TEST(RaftSim, ElectionSafetyUnderRandomCrashes) {
  SimOptions opts;
  opts.drop_rate = 0.10;
  opts.duplicate_rate = 0.05;
  for (uint64_t seed = 1; seed <= 200; ++seed) {
    SimCluster c(5, seed, opts);
    std::mt19937_64 chaos(seed);
    std::vector<NodeId> down;
    for (int round = 0; round < 30; ++round) {
      c.run(50);
      // Crash a random node (at most 2 down at once) or bring one back.
      if (down.size() < 2 && chaos() % 2 == 0) {
        NodeId id = static_cast<NodeId>(chaos() % 5 + 1);
        if (std::find(down.begin(), down.end(), id) == down.end()) {
          c.crash(id);
          down.push_back(id);
        }
      } else if (!down.empty()) {
        c.restart(down.back());
        down.pop_back();
      }
    }
    for (NodeId id : down) c.restart(id);
    ASSERT_NE(c.run_until_leader(3000), -1) << "seed " << seed;
    expect_safe(c);
  }
}
