// ClientService: the submit lifecycle (reply on apply, pre-check, RETRY when
// leadership is lost or the entry is overwritten, exactly-once on resend) and
// the read paths. Driven with a real RaftCore + StateMachine and a tiny host
// that runs the same Ready loop as the node, so no sockets are involved.
#include "server/client_service.h"

#include <gtest/gtest.h>

#include <utility>
#include <vector>

#include "crypto/keys.h"

using ledger::Genesis;
using ledger::GenesisAccount;
using ledger::NodeId;
using ledger::Term;
using ledger::TxStatus;
using ledger::crypto::KeyPair;
using ledger::crypto::keypair_from_seed;
using ledger::ledger::StateMachine;
using ledger::ledger::Transaction;
using ledger::ledger::TxType;
using ledger::raft::RaftConfig;
using ledger::raft::RaftCore;
using ledger::raft::Role;
using ledger::server::ClientService;
namespace pb = ledger::pb;

namespace {

KeyPair key(char c) { return keypair_from_seed(std::string(32, c)); }

const std::string kChain = "test-chain";

Genesis test_genesis() {
  Genesis g;
  g.chain_id = kChain;
  g.accounts.push_back(GenesisAccount{"faucet", key('f').public_key, 1000});
  g.accounts.push_back(GenesisAccount{"alice", key('a').public_key, 100});
  return g;
}

Transaction transfer(const std::string& from, const std::string& to, uint64_t amount,
                     uint64_t nonce, const KeyPair& signer) {
  Transaction t;
  t.type = TxType::kTransfer;
  t.chain_id = kChain;
  t.sender = from;
  t.receiver = to;
  t.amount = amount;
  t.nonce = nonce;
  t.sign(signer);
  return t;
}

pb::Envelope submit_req(const Transaction& t) {
  pb::Envelope e;
  *e.mutable_submit_tx()->mutable_tx() = t.to_proto();
  return e;
}

pb::Envelope account_req(const std::string& name, bool stale) {
  pb::Envelope e;
  e.mutable_get_account()->set_name(name);
  e.mutable_get_account()->set_stale(stale);
  return e;
}

pb::Envelope tx_req(const std::string& tx_id) {
  pb::Envelope e;
  e.mutable_get_tx()->set_tx_id(tx_id);
  e.mutable_get_tx()->set_stale(true);
  return e;
}

// One node: RaftCore + ledger + ClientService, with the node's Ready loop.
// Replies are collected instead of sent; Raft messages are dropped (tests
// feed the replies they need by hand).
class Host {
 public:
  explicit Host(std::vector<NodeId> peers)
      : raft(config(std::move(peers)), {}, {}),
        sm(test_genesis()),
        svc(raft, sm, [this](uint64_t cid, const pb::Envelope& m) { replies.emplace_back(cid, m); }) {}

  static RaftConfig config(std::vector<NodeId> peers) {
    RaftConfig c;
    c.id = 1;
    c.peers = std::move(peers);
    c.seed = 7;
    return c;
  }

  void handle(uint64_t cid, const pb::Envelope& req) {
    ASSERT_TRUE(svc.handle(cid, req));
    process();
  }

  void step(const pb::RaftMessage& m) {
    raft.step(m);
    process();
  }

  // Same order as Node::process_ready.
  void process() {
    auto rd = raft.ready();
    for (const auto& e : rd.committed_entries) {
      sm.apply(e);
      svc.on_applied(e);
    }
    for (const auto& rs : rd.reads_ready) svc.on_read(rs.request_id, &rs);
    for (uint64_t id : rd.reads_failed) svc.on_read(id, nullptr);
    svc.after_ready();
  }

  void become_leader() {
    // A single node elects itself; otherwise node 2's vote makes a majority.
    for (int i = 0; i < 100 && raft.role() == Role::kFollower; ++i) raft.tick();
    if (raft.role() == Role::kCandidate) step(vote(2, raft.term()));
    process();
    ASSERT_EQ(raft.role(), Role::kLeader);
    // Commit the no-op so the leader's state is current.
    if (raft.commit_index() < raft.log().last_index()) step(ack(2, raft.log().last_index()));
    ASSERT_EQ(raft.commit_index(), raft.log().last_index());
    replies.clear();
  }

  pb::RaftMessage vote(NodeId from, Term term) {
    pb::RaftMessage m;
    m.set_from(from);
    m.set_to(1);
    m.set_term(term);
    m.mutable_request_vote_resp()->set_granted(true);
    return m;
  }

  pb::RaftMessage ack(NodeId from, uint64_t match) {
    pb::RaftMessage m;
    m.set_from(from);
    m.set_to(1);
    m.set_term(raft.term());
    m.mutable_append_entries_resp()->set_success(true);
    m.mutable_append_entries_resp()->set_match_index(match);
    return m;
  }

  const pb::Envelope& only_reply() {
    static const pb::Envelope kNone;  // so a missing reply fails, not crashes
    EXPECT_EQ(replies.size(), 1u);
    return replies.empty() ? kNone : replies.back().second;
  }

  RaftCore raft;
  StateMachine sm;
  std::vector<std::pair<uint64_t, pb::Envelope>> replies;
  ClientService svc;
};

TxStatus status_of(const pb::SubmitTxResp& r) { return static_cast<TxStatus>(r.tx_status()); }

}  // namespace

TEST(ClientService, FollowerRedirectsToLeaderButServesStaleReads) {
  Host h({2, 3});
  pb::RaftMessage hb;
  hb.set_from(2);
  hb.set_to(1);
  hb.set_term(1);
  hb.mutable_append_entries();
  h.step(hb);  // now follows leader 2

  h.handle(1, submit_req(transfer("alice", "faucet", 30, 0, key('a'))));
  ASSERT_TRUE(h.only_reply().has_submit_tx_resp());
  EXPECT_EQ(h.only_reply().submit_tx_resp().code(), pb::RESP_NOT_LEADER);
  EXPECT_EQ(h.only_reply().submit_tx_resp().leader_hint(), 2u);
  h.replies.clear();

  h.handle(1, account_req("alice", /*stale=*/false));
  EXPECT_EQ(h.only_reply().account_resp().code(), pb::RESP_NOT_LEADER);
  EXPECT_EQ(h.only_reply().account_resp().leader_hint(), 2u);
  h.replies.clear();

  h.handle(1, account_req("alice", /*stale=*/true));
  EXPECT_EQ(h.only_reply().account_resp().code(), pb::RESP_OK);
  EXPECT_TRUE(h.only_reply().account_resp().found());
  EXPECT_EQ(h.only_reply().account_resp().balance(), 100u);
}

TEST(ClientService, SubmitIsAnsweredOnlyOnceApplied) {
  Host h({2, 3});
  h.become_leader();
  const Transaction t = transfer("alice", "faucet", 30, 0, key('a'));
  h.handle(7, submit_req(t));
  EXPECT_TRUE(h.replies.empty());  // appended, not committed: no answer yet
  EXPECT_EQ(h.svc.pending_submits(), 1u);

  h.step(h.ack(2, h.raft.log().last_index()));  // majority -> commit -> apply
  const auto& r = h.only_reply();
  EXPECT_EQ(h.replies.back().first, 7u);
  EXPECT_EQ(r.submit_tx_resp().code(), pb::RESP_OK);
  EXPECT_EQ(status_of(r.submit_tx_resp()), TxStatus::kOk);
  EXPECT_EQ(r.submit_tx_resp().log_index(), h.raft.log().last_index());
  EXPECT_EQ(r.submit_tx_resp().tx_id(), t.tx_id());
  EXPECT_EQ(h.sm.account("alice")->balance, 70u);
}

TEST(ClientService, FailedTransferReportsItsStatus) {
  Host h({});  // single node: commits at once
  h.become_leader();
  h.handle(1, submit_req(transfer("alice", "faucet", 5000, 0, key('a'))));
  EXPECT_EQ(h.only_reply().submit_tx_resp().code(), pb::RESP_OK);
  EXPECT_EQ(status_of(h.only_reply().submit_tx_resp()), TxStatus::kInsufficientFunds);
  EXPECT_EQ(h.sm.account("alice")->next_nonce, 1u);  // still used up its nonce
}

TEST(ClientService, PrecheckRejectionNeverEntersTheLog) {
  Host h({});
  h.become_leader();
  const auto last = h.raft.log().last_index();
  h.handle(1, submit_req(transfer("alice", "faucet", 30, 0, key('x'))));  // wrong key
  EXPECT_EQ(h.only_reply().submit_tx_resp().code(), pb::RESP_REJECTED);
  EXPECT_EQ(status_of(h.only_reply().submit_tx_resp()), TxStatus::kBadSignature);
  EXPECT_EQ(h.raft.log().last_index(), last);
}

TEST(ClientService, ResendOfAppliedTxGetsOriginalResultWithoutNewEntry) {
  Host h({});
  h.become_leader();
  const Transaction t = transfer("alice", "faucet", 30, 0, key('a'));
  h.handle(1, submit_req(t));
  const auto first_index = h.only_reply().submit_tx_resp().log_index();
  h.replies.clear();
  const auto last = h.raft.log().last_index();

  h.handle(2, submit_req(t));  // e.g. the client timed out and resent
  EXPECT_EQ(h.only_reply().submit_tx_resp().code(), pb::RESP_OK);
  EXPECT_EQ(status_of(h.only_reply().submit_tx_resp()), TxStatus::kOk);
  EXPECT_EQ(h.only_reply().submit_tx_resp().log_index(), first_index);
  EXPECT_EQ(h.raft.log().last_index(), last);
  EXPECT_EQ(h.sm.account("alice")->balance, 70u);  // moved once
}

TEST(ClientService, ResendWhilePendingTakesEffectOnceBothGetOk) {
  Host h({2, 3});
  h.become_leader();
  const Transaction t = transfer("alice", "faucet", 30, 0, key('a'));
  h.handle(1, submit_req(t));
  h.handle(2, submit_req(t));  // resent before the first committed: 2 entries
  EXPECT_EQ(h.svc.pending_submits(), 2u);

  h.step(h.ack(2, h.raft.log().last_index()));
  ASSERT_EQ(h.replies.size(), 2u);
  for (const auto& [cid, r] : h.replies) {
    EXPECT_EQ(r.submit_tx_resp().code(), pb::RESP_OK);
    // The second entry failed BAD_NONCE, but the tx_id's record keeps the
    // first entry's OK: both clients learn the one real outcome.
    EXPECT_EQ(status_of(r.submit_tx_resp()), TxStatus::kOk);
    EXPECT_EQ(r.submit_tx_resp().log_index(), h.replies[0].second.submit_tx_resp().log_index());
  }
  EXPECT_EQ(h.sm.account("alice")->balance, 70u);
  EXPECT_EQ(h.sm.account("alice")->next_nonce, 1u);
}

TEST(ClientService, DoubleSpendWithSameNonceExactlyOneWins) {
  Host h({2, 3});
  h.become_leader();
  h.handle(1, submit_req(transfer("alice", "faucet", 80, 0, key('a'))));
  h.handle(2, submit_req(transfer("alice", "faucet", 70, 0, key('a'))));  // same nonce
  h.step(h.ack(2, h.raft.log().last_index()));
  ASSERT_EQ(h.replies.size(), 2u);
  EXPECT_EQ(status_of(h.replies[0].second.submit_tx_resp()), TxStatus::kOk);
  EXPECT_EQ(status_of(h.replies[1].second.submit_tx_resp()), TxStatus::kBadNonce);
  EXPECT_EQ(h.sm.account("alice")->balance, 20u);
}

TEST(ClientService, LeadershipLostMeansRetry) {
  Host h({2, 3});
  h.become_leader();
  h.handle(5, submit_req(transfer("alice", "faucet", 30, 0, key('a'))));
  ASSERT_TRUE(h.replies.empty());

  pb::RaftMessage hb;  // a new leader, node 3, in a higher term
  hb.set_from(3);
  hb.set_to(1);
  hb.set_term(h.raft.term() + 1);
  hb.mutable_append_entries()->set_prev_log_index(0);
  h.step(hb);

  EXPECT_EQ(h.raft.role(), Role::kFollower);
  EXPECT_EQ(h.only_reply().submit_tx_resp().code(), pb::RESP_RETRY);
  EXPECT_EQ(h.only_reply().submit_tx_resp().leader_hint(), 3u);
  EXPECT_EQ(h.svc.pending_submits(), 0u);
}

TEST(ClientService, OverwrittenEntryMeansRetry) {
  Host h({2, 3});
  h.become_leader();
  const Term old_term = h.raft.term();
  h.handle(5, submit_req(transfer("alice", "faucet", 30, 0, key('a'))));
  const auto idx = h.raft.log().last_index();

  // The new leader (term+1) overwrites our uncommitted entry at `idx` and
  // commits it, all in one message, so the entry is applied before the
  // step-down check runs.
  pb::RaftMessage ae;
  ae.set_from(3);
  ae.set_to(1);
  ae.set_term(old_term + 1);
  auto* a = ae.mutable_append_entries();
  a->set_prev_log_index(idx - 1);
  a->set_prev_log_term(old_term);
  pb::LogEntry* e = a->add_entries();
  e->set_index(idx);
  e->set_term(old_term + 1);
  e->set_type(pb::ENTRY_NORMAL);
  e->set_data("someone else's entry");
  a->set_leader_commit(idx);
  h.step(ae);

  EXPECT_EQ(h.only_reply().submit_tx_resp().code(), pb::RESP_RETRY);
  EXPECT_EQ(h.sm.account("alice")->balance, 100u);  // ours never took effect
}

TEST(ClientService, LinearizableReadOnLeaderAndRetryOnStepDown) {
  Host single({});
  single.become_leader();
  single.handle(1, account_req("alice", /*stale=*/false));
  EXPECT_EQ(single.only_reply().account_resp().code(), pb::RESP_OK);
  EXPECT_EQ(single.only_reply().account_resp().balance(), 100u);
  EXPECT_EQ(single.only_reply().account_resp().next_nonce(), 0u);

  Host h({2, 3});
  h.become_leader();
  h.handle(1, account_req("alice", /*stale=*/false));
  EXPECT_TRUE(h.replies.empty());  // waiting for the majority to confirm
  EXPECT_EQ(h.svc.pending_reads(), 1u);
  pb::RaftMessage hb;
  hb.set_from(3);
  hb.set_to(1);
  hb.set_term(h.raft.term() + 1);
  hb.mutable_append_entries();
  h.step(hb);
  EXPECT_EQ(h.only_reply().account_resp().code(), pb::RESP_RETRY);
  EXPECT_EQ(h.svc.pending_reads(), 0u);
}

TEST(ClientService, GetTxShowsPendingThenResult) {
  Host h({2, 3});
  h.become_leader();
  const Transaction t = transfer("alice", "faucet", 30, 0, key('a'));
  h.handle(1, submit_req(t));
  h.handle(2, tx_req(t.tx_id()));
  EXPECT_TRUE(h.only_reply().tx_resp().pending());
  EXPECT_FALSE(h.only_reply().tx_resp().found());
  h.replies.clear();

  h.step(h.ack(2, h.raft.log().last_index()));
  h.replies.clear();
  h.handle(2, tx_req(t.tx_id()));
  EXPECT_TRUE(h.only_reply().tx_resp().found());
  EXPECT_EQ(static_cast<TxStatus>(h.only_reply().tx_resp().tx_status()), TxStatus::kOk);
}

TEST(ClientService, HistoryLimitKeepsNewest) {
  Host h({});
  h.become_leader();
  for (uint64_t n = 0; n < 3; ++n) {
    h.handle(1, submit_req(transfer("alice", "faucet", 10 + n, n, key('a'))));
  }
  h.replies.clear();
  pb::Envelope req;
  req.mutable_get_history()->set_name("alice");
  req.mutable_get_history()->set_limit(2);
  h.handle(1, req);
  const auto& r = h.only_reply().history_resp();
  ASSERT_EQ(r.records_size(), 2);
  EXPECT_EQ(r.records(0).amount(), 11u);
  EXPECT_EQ(r.records(1).amount(), 12u);
  EXPECT_FALSE(r.records(1).incoming());
}

TEST(ClientService, NonLedgerRequestsAreLeftToTheHost) {
  Host h({});
  pb::Envelope status;
  status.mutable_get_status();
  EXPECT_FALSE(h.svc.handle(1, status));
}
