#include "ledger/account_store.h"

#include <cassert>

namespace ledger::ledger {

const Account* AccountStore::find(std::string_view name) const {
  auto it = accounts_.find(name);
  return it == accounts_.end() ? nullptr : &it->second;
}

Account* AccountStore::find(std::string_view name) {
  auto it = accounts_.find(name);
  return it == accounts_.end() ? nullptr : &it->second;
}

Account& AccountStore::create(const std::string& name, const std::string& public_key,
                              uint64_t balance, uint64_t next_nonce) {
  auto [it, inserted] = accounts_.emplace(name, Account{name, public_key, balance, next_nonce, {}});
  assert(inserted && "account already exists");
  return it->second;
}

uint64_t AccountStore::total_balance() const {
  uint64_t total = 0;
  for (const auto& [_, a] : accounts_) total += a.balance;
  return total;
}

}  // namespace ledger::ledger
