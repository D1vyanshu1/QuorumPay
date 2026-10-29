#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "common/types.h"

namespace ledger::ledger {

// One completed transfer, as seen from one account.
struct HistoryRecord {
  LogIndex index = 0;
  std::string tx_id;
  std::string counterparty;
  uint64_t amount = 0;
  bool incoming = false;
};

struct Account {
  std::string name;
  std::string public_key;  // 32 bytes; verifies this account's transfers
  uint64_t balance = 0;
  uint64_t next_nonce = 0;  // the nonce its next transaction must carry
  std::vector<HistoryRecord> history;
};

// All accounts, keyed by name in a std::map. Sorted iteration order is what
// makes the state hash deterministic: an unordered_map would iterate in an
// order that can differ between processes, giving different hashes for the
// same balances.
class AccountStore {
 public:
  // std::less<> lets lookups use a string_view without building a string.
  using Map = std::map<std::string, Account, std::less<>>;

  const Account* find(std::string_view name) const;
  Account* find(std::string_view name);
  // Requires that `name` doesn't exist yet.
  Account& create(const std::string& name, const std::string& public_key, uint64_t balance,
                  uint64_t next_nonce);

  const Map& all() const { return accounts_; }
  uint64_t total_balance() const;

 private:
  Map accounts_;
};

}  // namespace ledger::ledger
