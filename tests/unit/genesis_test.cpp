#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

#include "common/config.h"
#include "common/hex.h"

using ledger::Genesis;

namespace {

const std::string kKey(64, 'a');  // 32 bytes of 0xaa as hex

std::string genesis_json(const std::string& accounts) {
  return R"({"chain_id": "test-chain", "accounts": [)" + accounts + "]}";
}

std::string account(const std::string& name, const std::string& balance,
                    const std::string& key = kKey) {
  return R"({"name": ")" + name + R"(", "public_key_hex": ")" + key + R"(", "balance": )" +
         balance + "}";
}

void expect_rejected(const std::string& json, const std::string& why) {
  try {
    Genesis::parse(json);
    ADD_FAILURE() << "accepted: " << json;
  } catch (const std::runtime_error& e) {
    EXPECT_NE(std::string(e.what()).find(why), std::string::npos) << e.what();
  }
}

}  // namespace

TEST(Hex, RoundTrip) {
  std::string bytes("\x00\x01\xab\xff", 4);
  EXPECT_EQ(ledger::to_hex(bytes), "0001abff");
  EXPECT_EQ(ledger::from_hex("0001ABff"), bytes);
  EXPECT_FALSE(ledger::from_hex("abc").has_value());  // odd length
  EXPECT_FALSE(ledger::from_hex("zz").has_value());
}

TEST(Genesis, ParsesAccounts) {
  Genesis g = Genesis::parse(genesis_json(account("faucet", "1000000000") + "," +
                                          account("alice", "5")));
  EXPECT_EQ(g.chain_id, "test-chain");
  ASSERT_EQ(g.accounts.size(), 2u);
  EXPECT_EQ(g.accounts[0].name, "faucet");
  EXPECT_EQ(g.accounts[0].public_key, std::string(32, '\xaa'));
  EXPECT_EQ(g.accounts[0].balance, 1000000000u);
  EXPECT_EQ(g.total_supply(), 1000000005u);
}

TEST(Genesis, RejectsBadInput) {
  expect_rejected(R"({"chain_id": "", "accounts": []})", "chain_id is empty");
  expect_rejected(genesis_json(account("a1", "1") + "," + account("a1", "2")), "duplicate");
  expect_rejected(genesis_json(account("bob", "1", "abcd")), "64 hex chars");
  expect_rejected(genesis_json(account("bob", "-5")), "non-negative integer");
  expect_rejected(genesis_json(account("bob", "1.5")), "non-negative integer");
  expect_rejected(genesis_json(account("big", "18446744073709551615") + "," + account("one", "1")),
                  "overflows");
  expect_rejected("{not json", "invalid JSON");
}

TEST(Genesis, LoadsRepoFile) {
  Genesis g = Genesis::load_file(std::string(LEDGER_SOURCE_DIR) + "/config/genesis.json");
  EXPECT_FALSE(g.chain_id.empty());
}
