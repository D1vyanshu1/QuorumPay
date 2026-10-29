#include "ledger/transaction.h"

#include <gtest/gtest.h>

#include "common/hex.h"

using ledger::ledger::is_valid_account_name;
using ledger::ledger::Transaction;
using ledger::ledger::TxType;

namespace {

Transaction transfer() {
  Transaction t;
  t.type = TxType::kTransfer;
  t.chain_id = "c1";
  t.sender = "alice";
  t.receiver = "bob";
  t.amount = 30;
  t.nonce = 7;
  t.signature = std::string(64, 's');
  return t;
}

}  // namespace

TEST(Transaction, SigningBytesExactLayout) {
  // Pinned byte for byte: if this changes, every existing signature breaks,
  // so it must only ever change together with the "V1" domain tag.
  EXPECT_EQ(ledger::to_hex(transfer().signing_bytes()),
            ledger::to_hex("LEDGER_TX_V1") +
                "00000002" + ledger::to_hex("c1") +      // chain_id
                "02" +                                   // type TRANSFER
                "00000005" + ledger::to_hex("alice") +   // sender
                "00000003" + ledger::to_hex("bob") +     // receiver
                "000000000000001e" +                     // amount 30
                "0000000000000007");                     // nonce 7
}

TEST(Transaction, EveryFieldIsCovered) {
  const std::string base = transfer().signing_bytes();
  auto differs = [&](auto mutate) {
    Transaction t = transfer();
    mutate(t);
    return t.signing_bytes() != base;
  };
  EXPECT_TRUE(differs([](Transaction& t) { t.chain_id = "c2"; }));
  EXPECT_TRUE(differs([](Transaction& t) { t.type = TxType::kCreateAccount; }));
  EXPECT_TRUE(differs([](Transaction& t) { t.sender = "alicf"; }));
  EXPECT_TRUE(differs([](Transaction& t) { t.receiver = "bod"; }));
  EXPECT_TRUE(differs([](Transaction& t) { t.amount = 31; }));
  EXPECT_TRUE(differs([](Transaction& t) { t.nonce = 8; }));
  // The signature itself is not part of what it signs.
  EXPECT_FALSE(differs([](Transaction& t) { t.signature = std::string(64, 'x'); }));
}

TEST(Transaction, LengthPrefixesPreventAmbiguity) {
  Transaction a = transfer(), b = transfer();
  a.sender = "abc";
  a.receiver = "defg";
  b.sender = "abcd";
  b.receiver = "efg";
  EXPECT_NE(a.signing_bytes(), b.signing_bytes());
}

TEST(Transaction, CreateAccountSignsItsPublicKey) {
  Transaction t;
  t.type = TxType::kCreateAccount;
  t.chain_id = "c1";
  t.sender = "carol";
  t.public_key = std::string(32, 'k');
  std::string with_key = t.signing_bytes();
  EXPECT_EQ(with_key.substr(with_key.size() - 32), t.public_key);
  t.public_key = std::string(32, 'j');
  EXPECT_NE(t.signing_bytes(), with_key);  // can't swap in another key
}

TEST(Transaction, ProtoRoundTrip) {
  Transaction t = transfer();
  Transaction u = Transaction::from_proto(t.to_proto());
  EXPECT_EQ(u.signing_bytes(), t.signing_bytes());
  EXPECT_EQ(u.signature, t.signature);
}

TEST(Transaction, UnknownProtoTypeBecomesInvalid) {
  ledger::pb::Transaction p = transfer().to_proto();
  p.set_type(static_cast<ledger::pb::TxType>(99));
  EXPECT_EQ(Transaction::from_proto(p).type, TxType::kInvalid);
}

TEST(AccountName, Rules) {
  EXPECT_TRUE(is_valid_account_name("bob"));
  EXPECT_TRUE(is_valid_account_name("alice_99"));
  EXPECT_TRUE(is_valid_account_name(std::string(32, 'a')));
  EXPECT_FALSE(is_valid_account_name("ab"));               // too short
  EXPECT_FALSE(is_valid_account_name(std::string(33, 'a')));  // too long
  EXPECT_FALSE(is_valid_account_name("Alice"));            // uppercase
  EXPECT_FALSE(is_valid_account_name("bob smith"));
  EXPECT_FALSE(is_valid_account_name("b\xc3\xb6""b"));     // non-ASCII look-alike
}
