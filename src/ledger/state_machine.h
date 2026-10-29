#pragma once

#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "common/config.h"
#include "common/status.h"
#include "common/types.h"
#include "ledger/account_store.h"
#include "ledger/transaction.h"
#include "raft.pb.h"

namespace ledger::ledger {

// What happened to one transaction, by tx_id (the "tx_index").
struct TxRecord {
  LogIndex index = 0;
  TxStatus status = TxStatus::kMalformed;
};

struct ApplyResult {
  std::string tx_id;
  TxStatus status = TxStatus::kMalformed;
};

// The replicated state machine: accounts, balances, nonces, history.
//
// Every node applies the same committed log entries in the same order, so
// every node must compute exactly the same result. That is why nothing in
// here may read the clock, use randomness, or iterate an unordered
// container: given the same genesis and entries, the state hash is
// bit-for-bit identical everywhere.
//
// Validation pipeline for each transaction (the first failing check decides):
//   1. well-formed (type, chain_id, names, amount > 0, not to self)  MALFORMED
//   2. sender exists / new name is free          UNKNOWN_ACCOUNT / DUPLICATE_ACCOUNT
//   3. signature valid                           BAD_SIGNATURE
//   4. nonce == sender's next nonce              BAD_NONCE
//      -- the sender is now proven: its nonce is used up from here on --
//   5. receiver exists                           UNKNOWN_RECEIVER
//   6. enough balance, no overflow               INSUFFICIENT_FUNDS / OVERFLOW
//   7. move the money                            OK
class StateMachine {
 public:
  // The state before log index 1. Throws std::runtime_error if the genesis
  // breaks a ledger rule (an invalid account name).
  explicit StateMachine(const Genesis& genesis);

  // Applies the committed entry at last_applied() + 1 (anything else is a
  // bug in the caller and throws std::logic_error). Returns nullopt for a
  // Raft no-op; otherwise the transaction's id and status. Any bytes at all
  // get a deterministic result: garbage is simply MALFORMED.
  std::optional<ApplyResult> apply(const pb::LogEntry& entry);

  const Account* account(std::string_view name) const { return accounts_.find(name); }
  const AccountStore& accounts() const { return accounts_; }
  std::optional<TxRecord> tx(std::string_view tx_id) const;
  LogIndex last_applied() const { return last_applied_; }
  const std::string& chain_id() const { return chain_id_; }
  std::string state_hash() const;
  // Sum of all balances. Equal to the genesis supply forever: transfers only
  // move money. The checker relies on this ("conservation").
  uint64_t total_supply() const { return accounts_.total_balance(); }

 private:
  TxStatus execute(const Transaction& t, const std::string& tx_id, LogIndex index);
  bool well_formed(const Transaction& t) const;
  void record(const std::string& tx_id, LogIndex index, TxStatus status);

  std::string chain_id_;
  AccountStore accounts_;
  std::map<std::string, TxRecord, std::less<>> tx_index_;
  LogIndex last_applied_ = 0;
};

}  // namespace ledger::ledger
