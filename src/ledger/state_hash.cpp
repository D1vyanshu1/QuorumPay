#include "ledger/state_hash.h"

#include "crypto/hash.h"
#include "ledger/encoding.h"

namespace ledger::ledger {

std::string compute_state_hash(const AccountStore& accounts, LogIndex last_applied) {
  std::string buf = "LEDGER_STATE_V1";
  put_u64(buf, last_applied);
  for (const auto& [name, a] : accounts.all()) {  // sorted by name
    put_str(buf, name);
    put_str(buf, a.public_key);
    put_u64(buf, a.balance);
    put_u64(buf, a.next_nonce);
  }
  return crypto::sha256(buf);
}

}  // namespace ledger::ledger
