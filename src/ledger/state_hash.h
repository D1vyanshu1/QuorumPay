#pragma once

#include <string>

#include "common/types.h"
#include "ledger/account_store.h"

namespace ledger::ledger {

// SHA-256 over the ledger state: last_applied, then every account in name
// order as (name, public key, balance, next_nonce), in a fixed binary layout.
// Two nodes with equal hashes at the same last_applied hold identical
// balances, keys and nonces. Compare hashes only at equal applied indexes.
//
// History isn't hashed: it is a log of what happened, fully determined by
// the same entries that produced the balances.
std::string compute_state_hash(const AccountStore& accounts, LogIndex last_applied);

}  // namespace ledger::ledger
