// Leader election on a simulated 5-node cluster, across many seeds. Each seed
// is a different but exactly reproducible schedule of timeouts and message
// delays. A failure message names its seed so the run can be replayed.
#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <set>

#include "sim/sim_network.h"

namespace raft = ledger::raft;

using ledger::LogIndex;
using ledger::NodeId;
using ledger::Term;
using ledger::kNoNode;
using ledger::raft::Role;
using ledger::sim::SimCluster;
using ledger::sim::SimOptions;
using ledger::pb::LogEntry;

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

// ---------------------------------------------------------------- replication

namespace {

// Every live node has applied the same entries (State Machine Safety makes
// equal applied indexes mean equal contents).
void expect_all_applied(SimCluster& c, LogIndex index, uint64_t seed) {
  for (NodeId id : c.ids()) {
    if (c.is_up(id)) {
      EXPECT_EQ(c.applied_index(id), index) << "node " << id << ", seed " << seed;
    }
  }
}

}  // namespace

TEST(RaftSim, ReplicatesAndCommitsProposals) {
  for (uint64_t seed = 1; seed <= 200; ++seed) {
    SimCluster c(5, seed);
    ASSERT_NE(c.run_until_leader(300), -1) << "seed " << seed;
    for (int i = 0; i < 50; ++i) {
      ASSERT_TRUE(c.propose("tx-" + std::to_string(i)).has_value());
      if (i % 7 == 0) c.run(3);  // some proposals batch up, some go alone
    }
    ASSERT_NE(c.run_until_converged(500), -1) << "seed " << seed;
    // 1 no-op from the election + 50 proposals, in proposal order.
    expect_all_applied(c, 51, seed);
    const auto& h = c.applied_history();
    for (int i = 0; i < 50; ++i) EXPECT_EQ(h.at(static_cast<LogIndex>(i + 2)).data(), "tx-" + std::to_string(i));
    expect_safe(c);
  }
}

TEST(RaftSim, CrashedFollowerCatchesUp) {
  for (uint64_t seed = 1; seed <= 200; ++seed) {
    SimCluster c(5, seed);
    ASSERT_NE(c.run_until_leader(300), -1);
    NodeId leader = c.leaders()[0];
    NodeId follower = leader == 1 ? 2 : 1;

    c.crash(follower);
    for (int i = 0; i < 40; ++i) c.propose("while-down-" + std::to_string(i));
    c.run(50);  // committed by the other 4 nodes without it
    c.restart(follower);
    // It rebuilds its state by re-applying from index 1 as it relearns the
    // commit index; 40 entries at 8 per batch takes several round trips.
    ASSERT_NE(c.run_until_converged(1000), -1) << "seed " << seed;
    expect_all_applied(c, 41, seed);
    expect_safe(c);
  }
}

TEST(RaftSim, DivergentLogOfDeposedLeaderIsRepaired) {
  for (uint64_t seed = 1; seed <= 200; ++seed) {
    SimCluster c(5, seed);
    ASSERT_NE(c.run_until_leader(300), -1);
    NodeId old = c.leaders()[0];
    std::vector<NodeId> rest;
    for (NodeId id : c.ids())
      if (id != old) rest.push_back(id);

    // Cut the leader off and let it take proposals it can never commit.
    c.partition({{old}, rest});
    std::vector<raft::ProposeResult> lost;
    for (int i = 0; i < 10; ++i) lost.push_back(*c.propose_on(old, "lost-" + std::to_string(i)));

    // The majority moves on and commits its own entries at the same indexes.
    NodeId fresh = kNoNode;
    for (int i = 0; i < 500 && fresh == kNoNode; ++i) {
      c.tick();
      fresh = c.stable_leader(rest);
    }
    ASSERT_NE(fresh, kNoNode) << "seed " << seed;
    for (int i = 0; i < 20; ++i) c.propose_on(fresh, "kept-" + std::to_string(i));
    c.run(50);

    // Heal: the old leader steps down, its 10 uncommitted entries are
    // truncated and replaced by the new leader's.
    c.heal();
    ASSERT_NE(c.run_until_converged(1000), -1) << "seed " << seed;
    for (const auto& p : lost) {
      const auto& e = c.applied_history().at(p.index);
      EXPECT_NE(e.term(), p.term) << "a deposed leader's entry was committed, seed " << seed;
      EXPECT_EQ(e.data().rfind("lost-", 0), std::string::npos);
    }
    // Byte-for-byte the same log as the new leader now.
    const auto& lo = c.disk_log(old);
    const auto& lf = c.disk_log(fresh);
    ASSERT_EQ(lo.size(), lf.size()) << "seed " << seed;
    for (size_t i = 0; i < lo.size(); ++i) {
      EXPECT_EQ(lo[i].SerializeAsString(), lf[i].SerializeAsString()) << "index " << i + 1;
    }
    expect_safe(c);
  }
}

TEST(RaftSim, AllInvariantsHoldUnderRandomFaults) {
  SimOptions opts;
  opts.drop_rate = 0.10;
  opts.duplicate_rate = 0.05;
  opts.max_delay = 5;
  size_t total_committed = 0;
  for (uint64_t seed = 1; seed <= 300; ++seed) {
    SimCluster c(5, seed, opts);
    std::mt19937_64 chaos(seed * 7 + 1);
    std::vector<NodeId> down;
    int next_tx = 0;
    for (int round = 0; round < 60; ++round) {
      c.run(static_cast<int>(chaos() % 30));
      for (int k = static_cast<int>(chaos() % 4); k > 0; --k) c.propose("tx-" + std::to_string(next_tx++));

      switch (chaos() % 6) {
        case 0:  // crash someone (keep a majority alive)
          if (down.size() < 2) {
            NodeId id = static_cast<NodeId>(chaos() % 5 + 1);
            if (std::find(down.begin(), down.end(), id) == down.end()) {
              c.crash(id);
              down.push_back(id);
            }
          }
          break;
        case 1:  // bring someone back
          if (!down.empty()) {
            c.restart(down.back());
            down.pop_back();
          }
          break;
        case 2: {  // random 2|3 split
          std::vector<NodeId> ids = c.ids();
          std::shuffle(ids.begin(), ids.end(), chaos);
          c.partition({{ids[0], ids[1]}, {ids[2], ids[3], ids[4]}});
          break;
        }
        case 3:
          c.heal();
          break;
        default:
          break;
      }
    }
    c.heal();
    for (NodeId id : down) c.restart(id);
    ASSERT_NE(c.run_until_converged(3000), -1) << "no convergence, seed " << seed;
    total_committed += c.applied_history().size();
    expect_safe(c);
    if (!c.violations().empty()) break;  // one seed's report is enough
  }
  EXPECT_GT(total_committed, 1000u);  // the chaos still let real work commit
}

// ---------------------------------------------------------------- reads

TEST(RaftSim, StaleLeaderNeverServesReads) {
  for (uint64_t seed = 1; seed <= 200; ++seed) {
    SimCluster c(5, seed);
    ASSERT_NE(c.run_until_leader(300), -1);
    NodeId old = c.leaders()[0];
    std::vector<NodeId> rest;
    for (NodeId id : c.ids())
      if (id != old) rest.push_back(id);

    c.partition({{old}, rest});
    NodeId fresh = kNoNode;
    for (int i = 0; i < 500 && fresh == kNoNode; ++i) {
      c.tick();
      fresh = c.stable_leader(rest);
    }
    ASSERT_NE(fresh, kNoNode);
    for (int i = 0; i < 5; ++i) c.propose_on(fresh, "w" + std::to_string(i));
    c.run(50);  // committed on the majority side

    // The old leader still believes it leads, but can't confirm it: a read
    // there must never be served (it would miss the 5 new writes).
    ASSERT_TRUE(c.read(old, 1));
    c.run(300);
    EXPECT_FALSE(c.reads().at(1).done) << "stale leader served a read, seed " << seed;

    // The real leader serves reads, and they include the new writes.
    ASSERT_TRUE(c.read(fresh, 2));
    c.run(20);
    ASSERT_TRUE(c.reads().at(2).done) << "seed " << seed;
    EXPECT_GE(c.reads().at(2).served_at, c.reads().at(2).must_see);

    // Healing makes the old leader step down; its read fails (client retries).
    c.heal();
    c.run(100);
    EXPECT_TRUE(c.reads().at(1).failed) << "seed " << seed;
    expect_safe(c);
  }
}

TEST(RaftSim, ReadsAreLinearizableUnderRandomFaults) {
  SimOptions opts;
  opts.drop_rate = 0.10;
  opts.duplicate_rate = 0.05;
  opts.max_delay = 5;
  size_t served = 0, failed = 0;
  for (uint64_t seed = 1; seed <= 300; ++seed) {
    SimCluster c(5, seed, opts);
    std::mt19937_64 chaos(seed * 13 + 5);
    uint64_t next_read = 1;
    for (int round = 0; round < 80; ++round) {
      c.run(static_cast<int>(chaos() % 20));
      for (int k = static_cast<int>(chaos() % 3); k > 0; --k) c.propose("tx");
      // Read on every node that thinks it's leader: old leaders cut off by a
      // partition, and fresh leaders that haven't committed their no-op yet,
      // are exactly the risky cases.
      for (NodeId id : c.leaders()) c.read(id, next_read++);
      switch (chaos() % 5) {
        case 0: {
          NodeId id = static_cast<NodeId>(chaos() % 5 + 1);
          if (c.is_up(id)) {
            c.crash(id);
          } else {
            c.restart(id);
          }
          break;
        }
        case 1: {
          std::vector<NodeId> ids = c.ids();
          std::shuffle(ids.begin(), ids.end(), chaos);
          c.partition({{ids[0], ids[1]}, {ids[2], ids[3], ids[4]}});
          break;
        }
        case 2:
          c.heal();
          break;
        default:
          break;
      }
    }
    c.heal();
    for (NodeId id : c.ids())
      if (!c.is_up(id)) c.restart(id);
    ASSERT_NE(c.run_until_converged(3000), -1) << "seed " << seed;
    for (const auto& [_, o] : c.reads()) {
      served += o.done;
      failed += o.failed;
      EXPECT_FALSE(o.done && o.failed);
    }
    expect_safe(c);
    if (!c.violations().empty()) break;
  }
  EXPECT_GT(served, 1000u);  // most reads still get served
  std::cout << "reads served " << served << ", failed (leader lost) " << failed << "\n";
}

// ---------------------------------------------------------------- the ledger on Raft

namespace {

namespace lg = ledger::ledger;
using ledger::TxStatus;
using ledger::crypto::KeyPair;

KeyPair sim_key(char c) { return ledger::crypto::keypair_from_seed(std::string(32, c)); }

SimOptions ledger_opts() {
  SimOptions o;
  o.genesis.chain_id = "sim-chain";
  o.genesis.accounts = {{"alice", sim_key('a').public_key, 100},
                        {"bob", sim_key('b').public_key, 0},
                        {"carol", sim_key('c').public_key, 0}};
  return o;
}

std::string signed_transfer(const std::string& from, const std::string& to, uint64_t amount,
                            uint64_t nonce, char key) {
  lg::Transaction t;
  t.type = lg::TxType::kTransfer;
  t.chain_id = "sim-chain";
  t.sender = from;
  t.receiver = to;
  t.amount = amount;
  t.nonce = nonce;
  t.sign(sim_key(key));
  return t.to_proto().SerializeAsString();
}

void expect_same_ledger_everywhere(SimCluster& c, uint64_t seed) {
  std::string hash;
  for (NodeId id : c.ids()) {
    if (!c.is_up(id)) continue;
    if (hash.empty()) hash = c.state(id).state_hash();
    EXPECT_EQ(c.state(id).state_hash(), hash) << "node " << id << ", seed " << seed;
  }
}

}  // namespace

// The plan's "done when" for Week 3: the same nonce proposed twice under
// failover yields exactly one apply. Alice (100) signs two conflicting
// transfers of 80 with nonce 0. The first goes to a leader that gets cut off
// and deposed; the client then retries it and also sends the other one to
// the new leader, in a random order.
TEST(RaftSim, DoubleSpendUnderFailoverAppliesExactlyOnce) {
  const std::string to_bob = signed_transfer("alice", "bob", 80, 0, 'a');
  const std::string to_carol = signed_transfer("alice", "carol", 80, 0, 'a');
  for (uint64_t seed = 1; seed <= 200; ++seed) {
    SimCluster c(5, seed, ledger_opts());
    ASSERT_NE(c.run_until_leader(300), -1);
    NodeId old = c.leaders()[0];
    std::vector<NodeId> rest;
    for (NodeId id : c.ids())
      if (id != old) rest.push_back(id);

    // Seeds alternate between "old leader is cut off" and "old leader crashes".
    if (seed % 2) {
      c.partition({{old}, rest});
    }
    c.propose_on(old, to_bob);
    if (seed % 2 == 0) c.crash(old);

    NodeId fresh = kNoNode;
    for (int i = 0; i < 500 && fresh == kNoNode; ++i) {
      c.tick();
      fresh = c.stable_leader(rest);
    }
    ASSERT_NE(fresh, kNoNode) << "seed " << seed;
    std::mt19937 order(static_cast<uint32_t>(seed));
    std::vector<std::string> sends = {to_bob, to_carol, to_bob};  // includes a retry
    std::shuffle(sends.begin(), sends.end(), order);
    for (const auto& s : sends) c.propose_on(fresh, s);

    c.heal();
    if (!c.is_up(old)) c.restart(old);
    ASSERT_NE(c.run_until_converged(1000), -1) << "seed " << seed;

    const auto& sm = c.state(fresh);
    uint64_t bob = sm.account("bob")->balance, carol = sm.account("carol")->balance;
    EXPECT_EQ(sm.account("alice")->balance, 20u) << "seed " << seed;
    EXPECT_EQ(bob + carol, 80u) << "money spent twice or not at all, seed " << seed;
    EXPECT_EQ(sm.account("alice")->next_nonce, 1u);
    EXPECT_EQ(sm.total_supply(), 100u);
    expect_same_ledger_everywhere(c, seed);
    expect_safe(c);
  }
}

// Clients send real signed transfers (including conflicting ones with the
// same nonce, and retries of the same bytes) while nodes crash and the
// network splits. At the end: one ledger state everywhere, no money created
// or destroyed, and no (sender, nonce) applied twice.
TEST(RaftSim, LedgerStaysConsistentUnderRandomFaults) {
  const std::vector<std::string> names = {"alice", "bob", "carol"};
  const std::vector<char> keys = {'a', 'b', 'c'};
  SimOptions opts = ledger_opts();
  opts.genesis.accounts[1].balance = 100;
  opts.genesis.accounts[2].balance = 100;
  opts.drop_rate = 0.10;
  opts.duplicate_rate = 0.05;
  size_t ok_total = 0;
  for (uint64_t seed = 1; seed <= 100; ++seed) {
    SimCluster c(5, seed, opts);
    std::mt19937_64 rng(seed * 31 + 7);
    std::vector<std::string> sent;
    for (int round = 0; round < 50; ++round) {
      c.run(static_cast<int>(rng() % 25));
      for (int k = static_cast<int>(rng() % 3); k > 0; --k) {
        size_t from = rng() % 3, to = (from + 1 + rng() % 2) % 3;
        std::string tx;
        if (!sent.empty() && rng() % 4 == 0) {
          tx = sent[rng() % sent.size()];  // retry of earlier signed bytes
        } else {
          // Like the real client: read the sender's next nonce from the
          // current leader's state first. Sometimes use an older one, which
          // makes a conflicting spend of an already-used nonce.
          uint64_t n = 0;
          if (!c.leaders().empty()) n = c.state(c.leaders()[0]).account(names[from])->next_nonce;
          if (n > 0 && rng() % 5 == 0) --n;
          tx = signed_transfer(names[from], names[to], 1 + rng() % 60, n, keys[from]);
          sent.push_back(tx);
        }
        c.propose(tx);
      }
      switch (rng() % 6) {
        case 0: {
          NodeId id = static_cast<NodeId>(rng() % 5 + 1);
          if (c.is_up(id)) {
            c.crash(id);
          } else {
            c.restart(id);
          }
          break;
        }
        case 1: {
          std::vector<NodeId> ids = c.ids();
          std::shuffle(ids.begin(), ids.end(), rng);
          c.partition({{ids[0], ids[1]}, {ids[2], ids[3], ids[4]}});
          break;
        }
        case 2:
          c.heal();
          break;
        default:
          break;
      }
    }
    c.heal();
    for (NodeId id : c.ids())
      if (!c.is_up(id)) c.restart(id);
    ASSERT_NE(c.run_until_converged(3000), -1) << "seed " << seed;
    expect_same_ledger_everywhere(c, seed);

    // Conservation, and no (sender, nonce) applied twice.
    NodeId any = c.stable_leader(c.ids());
    const auto& sm = c.state(any);
    EXPECT_EQ(sm.total_supply(), 300u) << "seed " << seed;
    std::set<std::pair<std::string, uint64_t>> spent;
    for (const auto& [index, e] : c.applied_history()) {
      if (e.type() != ledger::pb::ENTRY_NORMAL) continue;
      ledger::pb::Transaction p;
      ASSERT_TRUE(p.ParseFromString(e.data()));
      lg::Transaction t = lg::Transaction::from_proto(p);
      auto rec = sm.tx(t.tx_id());
      ASSERT_TRUE(rec.has_value());
      if (rec->index == index && ledger::consumes_nonce(rec->status)) {
        EXPECT_TRUE(spent.insert({t.sender, t.nonce}).second)
            << t.sender << " nonce " << t.nonce << " used twice, seed " << seed;
        ok_total += rec->status == TxStatus::kOk;
      }
    }
    expect_safe(c);
    if (!c.violations().empty()) break;
  }
  EXPECT_GT(ok_total, 500u);  // plenty of transfers really happened
  std::cout << "successful transfers across all seeds: " << ok_total << "\n";
}
