// The ledger state machine: one test per row of the validation table
// (project_details.md §6.5), plus double spends, replays, determinism and
// conservation of money.
#include "ledger/state_machine.h"

#include <gtest/gtest.h>

#include <limits>
#include <random>
#include <stdexcept>

#include "crypto/keys.h"

using ledger::Genesis;
using ledger::GenesisAccount;
using ledger::TxStatus;
using ledger::crypto::KeyPair;
using ledger::crypto::keypair_from_seed;
using ledger::ledger::StateMachine;
using ledger::ledger::Transaction;
using ledger::ledger::TxType;
using ledger::pb::LogEntry;

namespace {

KeyPair key(char c) { return keypair_from_seed(std::string(32, c)); }

const std::string kChain = "test-chain";

// faucet: 1000, alice: 100 (both genesis accounts, next nonce 0).
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

Transaction create(const std::string& name, const KeyPair& kp, const KeyPair& signer) {
  Transaction t;
  t.type = TxType::kCreateAccount;
  t.chain_id = kChain;
  t.sender = name;
  t.public_key = kp.public_key;
  t.nonce = 0;
  t.sign(signer);
  return t;
}

// Feeds transactions to a state machine as consecutive log entries.
class Ledger {
 public:
  Ledger() : sm(test_genesis()) {}

  TxStatus apply(const Transaction& t) {
    LogEntry e;
    e.set_index(sm.last_applied() + 1);
    e.set_term(1);
    e.set_type(ledger::pb::ENTRY_NORMAL);
    e.set_data(t.to_proto().SerializeAsString());
    auto r = sm.apply(e);
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->tx_id, t.tx_id());
    return r->status;
  }

  uint64_t balance(const std::string& name) const { return sm.account(name)->balance; }
  uint64_t nonce(const std::string& name) const { return sm.account(name)->next_nonce; }

  StateMachine sm;
};

}  // namespace

TEST(StateMachine, GenesisState) {
  Ledger l;
  EXPECT_EQ(l.balance("faucet"), 1000u);
  EXPECT_EQ(l.balance("alice"), 100u);
  EXPECT_EQ(l.nonce("alice"), 0u);
  EXPECT_EQ(l.sm.total_supply(), 1100u);
  EXPECT_EQ(l.sm.last_applied(), 0u);
  EXPECT_EQ(l.sm.state_hash().size(), 32u);
}

TEST(StateMachine, GenesisRejectsBadNames) {
  Genesis g = test_genesis();
  g.accounts.push_back(GenesisAccount{"Bad Name", key('x').public_key, 1});
  EXPECT_THROW(StateMachine{g}, std::runtime_error);
}

TEST(StateMachine, GenesisRejectsSupplyOverflow) {
  // Which is why OVERFLOW (step 6) can't actually happen: no balance can
  // exceed the total supply, and the total supply fits in uint64.
  Genesis g = test_genesis();
  g.accounts.push_back(GenesisAccount{"whale", key('w').public_key,
                                      std::numeric_limits<uint64_t>::max()});
  EXPECT_THROW(StateMachine{g}, std::runtime_error);
}

TEST(StateMachine, TransferOk) {
  Ledger l;
  EXPECT_EQ(l.apply(transfer("alice", "faucet", 30, 0, key('a'))), TxStatus::kOk);
  EXPECT_EQ(l.balance("alice"), 70u);
  EXPECT_EQ(l.balance("faucet"), 1030u);
  EXPECT_EQ(l.nonce("alice"), 1u);
  ASSERT_EQ(l.sm.account("alice")->history.size(), 1u);
  EXPECT_FALSE(l.sm.account("alice")->history[0].incoming);
  EXPECT_TRUE(l.sm.account("faucet")->history[0].incoming);
}

TEST(StateMachine, CreateAccount) {
  Ledger l;
  EXPECT_EQ(l.apply(create("bob", key('b'), key('b'))), TxStatus::kOk);
  EXPECT_EQ(l.balance("bob"), 0u);
  EXPECT_EQ(l.nonce("bob"), 1u);  // the create used nonce 0
  EXPECT_EQ(l.apply(transfer("faucet", "bob", 50, 0, key('f'))), TxStatus::kOk);
  EXPECT_EQ(l.apply(transfer("bob", "alice", 20, 1, key('b'))), TxStatus::kOk);
  EXPECT_EQ(l.balance("bob"), 30u);
}

// ---------------------------------------------------------------- row 1: MALFORMED

TEST(StateMachine, Row1Malformed) {
  Ledger l;
  auto malformed = [&](Transaction t) {
    EXPECT_EQ(l.apply(t), TxStatus::kMalformed) << t.sender << "->" << t.receiver;
  };
  malformed(transfer("alice", "faucet", 0, 0, key('a')));    // zero amount
  malformed(transfer("alice", "alice", 5, 0, key('a')));     // to self
  malformed(transfer("alice", "Faucet", 5, 0, key('a')));    // invalid receiver name
  Transaction other_chain = transfer("alice", "faucet", 5, 0, key('a'));
  other_chain.chain_id = "other-chain";
  other_chain.sign(key('a'));
  malformed(other_chain);  // validly signed, but for another ledger
  Transaction bad_type = transfer("alice", "faucet", 5, 0, key('a'));
  bad_type.type = TxType::kInvalid;
  malformed(bad_type);
  Transaction create_with_amount = create("carol", key('c'), key('c'));
  create_with_amount.amount = 5;
  create_with_amount.sign(key('c'));
  malformed(create_with_amount);

  // None of these touched any state.
  EXPECT_EQ(l.balance("alice"), 100u);
  EXPECT_EQ(l.nonce("alice"), 0u);
}

TEST(StateMachine, GarbageEntryIsMalformed) {
  Ledger l;
  LogEntry e;
  e.set_index(1);
  e.set_type(ledger::pb::ENTRY_NORMAL);
  e.set_data("\xff\xff not a protobuf");
  auto r = l.sm.apply(e);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->status, TxStatus::kMalformed);
  EXPECT_EQ(l.sm.last_applied(), 1u);
}

// ---------------------------------------------------------------- row 2

TEST(StateMachine, Row2UnknownAccountAndDuplicate) {
  Ledger l;
  EXPECT_EQ(l.apply(transfer("nobody", "alice", 5, 0, key('n'))), TxStatus::kUnknownAccount);
  EXPECT_EQ(l.apply(create("alice", key('z'), key('z'))), TxStatus::kDuplicateAccount);
  EXPECT_EQ(l.sm.account("alice")->public_key, key('a').public_key);  // not replaced
}

// ---------------------------------------------------------------- row 3

TEST(StateMachine, Row3BadSignatureConsumesNoNonce) {
  Ledger l;
  // Mallory signs a transfer out of alice's account with her own key.
  EXPECT_EQ(l.apply(transfer("alice", "faucet", 50, 0, key('m'))), TxStatus::kBadSignature);
  // Tampered amount after alice signed.
  Transaction t = transfer("alice", "faucet", 5, 0, key('a'));
  t.amount = 99;
  EXPECT_EQ(l.apply(t), TxStatus::kBadSignature);
  // A create signed by a different key than the one it registers.
  EXPECT_EQ(l.apply(create("carol", key('c'), key('m'))), TxStatus::kBadSignature);

  EXPECT_EQ(l.nonce("alice"), 0u);  // junk can't burn alice's nonces
  EXPECT_EQ(l.balance("alice"), 100u);
  EXPECT_EQ(l.apply(transfer("alice", "faucet", 5, 0, key('a'))), TxStatus::kOk);
}

// ---------------------------------------------------------------- row 4

TEST(StateMachine, Row4BadNonceConsumesNoNonce) {
  Ledger l;
  EXPECT_EQ(l.apply(transfer("alice", "faucet", 5, 3, key('a'))), TxStatus::kBadNonce);  // ahead
  EXPECT_EQ(l.nonce("alice"), 0u);
  Transaction create_nonce = create("carol", key('c'), key('c'));
  create_nonce.nonce = 1;
  create_nonce.sign(key('c'));
  EXPECT_EQ(l.apply(create_nonce), TxStatus::kBadNonce);
}

// ---------------------------------------------------------------- rows 5-6

TEST(StateMachine, Row5UnknownReceiverConsumesNonce) {
  Ledger l;
  EXPECT_EQ(l.apply(transfer("alice", "nobody", 5, 0, key('a'))), TxStatus::kUnknownReceiver);
  EXPECT_EQ(l.nonce("alice"), 1u);  // signed and in order: its nonce is used up
  EXPECT_EQ(l.balance("alice"), 100u);
}

TEST(StateMachine, Row6InsufficientFundsConsumesNonce) {
  Ledger l;
  EXPECT_EQ(l.apply(transfer("alice", "faucet", 101, 0, key('a'))), TxStatus::kInsufficientFunds);
  EXPECT_EQ(l.nonce("alice"), 1u);
  EXPECT_EQ(l.balance("alice"), 100u);
}

// ---------------------------------------------------------------- double spends and replays

TEST(StateMachine, DoubleSpendSameNonceOnlyFirstApplies) {
  // Demo 1a: two different transfers signed with the same nonce, as if sent
  // to two different nodes at once. The log puts them in one order; the
  // first wins, the second hits a used nonce.
  Ledger l;
  l.apply(create("bob", key('b'), key('b')));
  l.apply(create("carol", key('c'), key('c')));
  EXPECT_EQ(l.apply(transfer("alice", "bob", 80, 0, key('a'))), TxStatus::kOk);
  EXPECT_EQ(l.apply(transfer("alice", "carol", 80, 0, key('a'))), TxStatus::kBadNonce);
  EXPECT_EQ(l.balance("alice"), 20u);
  EXPECT_EQ(l.balance("bob"), 80u);
  EXPECT_EQ(l.balance("carol"), 0u);
}

TEST(StateMachine, ConsecutiveSpendsSecondLacksFunds) {
  // Demo 1b: nonces 0 and 1, 80 each, but alice only has 100.
  Ledger l;
  EXPECT_EQ(l.apply(transfer("alice", "faucet", 80, 0, key('a'))), TxStatus::kOk);
  EXPECT_EQ(l.apply(transfer("alice", "faucet", 80, 1, key('a'))), TxStatus::kInsufficientFunds);
  EXPECT_EQ(l.nonce("alice"), 2u);  // so this signed transfer can never be replayed
  EXPECT_EQ(l.balance("alice"), 20u);
}

TEST(StateMachine, ReplayFailsAndKeepsOriginalResult) {
  // Demo 1c + exactly-once: a client retries the same signed bytes after the
  // original already committed (e.g. the reply was lost in a failover).
  Ledger l;
  Transaction t = transfer("alice", "faucet", 30, 0, key('a'));
  EXPECT_EQ(l.apply(t), TxStatus::kOk);
  EXPECT_EQ(l.apply(t), TxStatus::kBadNonce);  // not applied twice
  EXPECT_EQ(l.balance("alice"), 70u);
  // Looking it up by tx_id still reports the original success.
  auto rec = l.sm.tx(t.tx_id());
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->status, TxStatus::kOk);
  EXPECT_EQ(rec->index, 1u);
}

TEST(StateMachine, EarlyTransferCanSucceedOnRetry) {
  Ledger l;
  Transaction second = transfer("alice", "faucet", 10, 1, key('a'));
  EXPECT_EQ(l.apply(second), TxStatus::kBadNonce);  // nonce 0 not used yet
  EXPECT_EQ(l.apply(transfer("alice", "faucet", 10, 0, key('a'))), TxStatus::kOk);
  EXPECT_EQ(l.apply(second), TxStatus::kOk);  // the retry now fits
  EXPECT_EQ(l.sm.tx(second.tx_id())->status, TxStatus::kOk);  // rejection replaced
}

// ---------------------------------------------------------------- determinism

TEST(StateMachine, NoopAdvancesIndexOnly) {
  Ledger l;
  std::string before = l.sm.state_hash();
  LogEntry e;
  e.set_index(1);
  e.set_type(ledger::pb::ENTRY_NOOP);
  EXPECT_FALSE(l.sm.apply(e).has_value());
  EXPECT_EQ(l.sm.last_applied(), 1u);
  EXPECT_NE(l.sm.state_hash(), before);  // the hash includes last_applied
  EXPECT_EQ(l.balance("alice"), 100u);
}

TEST(StateMachine, OutOfOrderApplyIsABug) {
  Ledger l;
  LogEntry e;
  e.set_index(5);
  EXPECT_THROW(l.sm.apply(e), std::logic_error);
}

TEST(StateMachine, SameEntriesSameHashAndMoneyIsConserved) {
  // Random workload, applied to two independent instances: identical state.
  std::vector<std::string> names = {"faucet", "alice", "bob", "carol"};
  std::vector<char> seeds = {'f', 'a', 'b', 'c'};
  std::vector<Transaction> txs;
  txs.push_back(create("bob", key('b'), key('b')));
  txs.push_back(create("carol", key('c'), key('c')));
  std::mt19937 rng(42);
  std::vector<uint64_t> nonce = {0, 0, 1, 1};
  for (int i = 0; i < 300; ++i) {
    size_t from = rng() % 4, to = rng() % 4;
    bool honest = rng() % 10 != 0;  // ~10% bad nonces
    uint64_t n = honest ? nonce[from] : nonce[from] + 5;
    txs.push_back(transfer(names[from], names[to], rng() % 200, n, key(seeds[from])));
    if (honest && from != to && txs.back().amount > 0) ++nonce[from];
  }

  Ledger a, b;
  for (const auto& t : txs) a.apply(t);
  for (const auto& t : txs) b.apply(t);
  EXPECT_EQ(a.sm.state_hash(), b.sm.state_hash());
  EXPECT_EQ(a.sm.total_supply(), 1100u);  // transfers only move money

  // And the hash really depends on the state: one more transfer changes it.
  b.apply(transfer("faucet", "alice", 1, b.nonce("faucet"), key('f')));
  EXPECT_NE(a.sm.state_hash(), b.sm.state_hash());
}
