#include "ledger/state_machine.h"

#include <limits>
#include <stdexcept>

#include "crypto/hash.h"
#include "crypto/signer.h"
#include "ledger/state_hash.h"

namespace ledger::ledger {

StateMachine::StateMachine(const Genesis& genesis) : chain_id_(genesis.chain_id) {
  uint64_t total = 0;
  for (const auto& a : genesis.accounts) {
    if (!is_valid_account_name(a.name)) throw std::runtime_error("genesis: invalid name " + a.name);
    if (accounts_.find(a.name)) throw std::runtime_error("genesis: duplicate account " + a.name);
    if (a.balance > std::numeric_limits<uint64_t>::max() - total) {
      throw std::runtime_error("genesis: total supply overflows uint64");
    }
    total += a.balance;
    // Genesis accounts were never "created" by a transaction, so their first
    // transaction uses nonce 0. (A created account starts at 1: the create
    // itself used nonce 0.)
    accounts_.create(a.name, a.public_key, a.balance, /*next_nonce=*/0);
  }
}

std::optional<ApplyResult> StateMachine::apply(const pb::LogEntry& entry) {
  if (entry.index() != last_applied_ + 1) {
    throw std::logic_error("apply out of order: index " + std::to_string(entry.index()) +
                           " after " + std::to_string(last_applied_));
  }
  last_applied_ = entry.index();
  if (entry.type() == pb::ENTRY_NOOP) return std::nullopt;

  pb::Transaction p;
  if (!p.ParseFromString(entry.data())) {
    // Unparseable bytes still get an id (hash of the raw bytes) and a
    // deterministic result, so every node agrees on what happened.
    ApplyResult r{crypto::sha256(entry.data()), TxStatus::kMalformed};
    record(r.tx_id, entry.index(), r.status);
    return r;
  }
  Transaction t = Transaction::from_proto(p);
  ApplyResult r{t.tx_id(), TxStatus::kOk};
  r.status = execute(t, r.tx_id, entry.index());
  record(r.tx_id, entry.index(), r.status);
  return r;
}

TxStatus StateMachine::execute(const Transaction& t, const std::string& tx_id, LogIndex index) {
  // 1. Well-formed.
  if (!well_formed(t)) return TxStatus::kMalformed;

  if (t.type == TxType::kCreateAccount) {
    // 2. The name must be free. The first create for a name that commits
    //    wins; every later one fails, on every node alike.
    if (accounts_.find(t.sender)) return TxStatus::kDuplicateAccount;
    // 3. Signed by the new key itself: proves the creator holds the secret
    //    key, so nobody can register an account under someone else's key.
    if (!crypto::verify(t.signing_bytes(), t.signature, t.public_key)) {
      return TxStatus::kBadSignature;
    }
    // 4. A create uses nonce 0.
    if (t.nonce != 0) return TxStatus::kBadNonce;
    accounts_.create(t.sender, t.public_key, /*balance=*/0, /*next_nonce=*/1);
    return TxStatus::kOk;
  }

  // TRANSFER.
  // 2. Sender exists.
  Account* from = accounts_.find(t.sender);
  if (!from) return TxStatus::kUnknownAccount;
  // 3. Signed by the sender's registered key. Checked before the nonce, and
  //    neither failure touches state: otherwise anyone could burn someone
  //    else's nonces by sending unsigned junk in their name.
  if (!crypto::verify(t.signing_bytes(), t.signature, from->public_key)) {
    return TxStatus::kBadSignature;
  }
  // 4. Exactly the next nonce. This is what stops double spends and
  //    replays: of two transactions with the same nonce, only the first
  //    one applied can pass; a replay of a committed one always fails.
  if (t.nonce != from->next_nonce) return TxStatus::kBadNonce;

  // The transaction is now proven to be the sender's next one, so its nonce
  // is used up whatever happens below. Otherwise a signed transfer that
  // failed for lack of funds could be replayed later to succeed.
  ++from->next_nonce;

  // 5. Receiver exists.
  Account* to = accounts_.find(t.receiver);
  if (!to) return TxStatus::kUnknownReceiver;
  // 6. Funds, and no overflow. (Overflow can't happen while total supply
  //    fits in uint64, which genesis guarantees; checked anyway, because an
  //    unchecked overflow would silently create money.)
  if (from->balance < t.amount) return TxStatus::kInsufficientFunds;
  if (to->balance > std::numeric_limits<uint64_t>::max() - t.amount) return TxStatus::kOverflow;

  // 7. Apply.
  from->balance -= t.amount;
  to->balance += t.amount;
  from->history.push_back(HistoryRecord{index, tx_id, t.receiver, t.amount, false});
  to->history.push_back(HistoryRecord{index, tx_id, t.sender, t.amount, true});
  return TxStatus::kOk;
}

bool StateMachine::well_formed(const Transaction& t) const {
  if (t.chain_id != chain_id_) return false;  // meant for another ledger
  if (!is_valid_account_name(t.sender)) return false;
  switch (t.type) {
    case TxType::kCreateAccount:
      return t.public_key.size() == crypto::kPublicKeySize && t.receiver.empty() && t.amount == 0;
    case TxType::kTransfer:
      return is_valid_account_name(t.receiver) && t.amount > 0 && t.sender != t.receiver &&
             t.public_key.empty();
    case TxType::kInvalid:
      break;
  }
  return false;
}

void StateMachine::record(const std::string& tx_id, LogIndex index, TxStatus status) {
  // A result that used up the nonce is final. When a client retries the same
  // signed bytes after its transaction already committed, the retry fails
  // BAD_NONCE; that must not hide the original OK. A rejection, though, can
  // be replaced: a transfer sent too early (nonce ahead) may succeed later.
  auto it = tx_index_.find(tx_id);
  if (it != tx_index_.end() && consumes_nonce(it->second.status)) return;
  tx_index_[tx_id] = TxRecord{index, status};
}

std::optional<TxRecord> StateMachine::tx(std::string_view tx_id) const {
  auto it = tx_index_.find(tx_id);
  if (it == tx_index_.end()) return std::nullopt;
  return it->second;
}

std::string StateMachine::state_hash() const { return compute_state_hash(accounts_, last_applied_); }

}  // namespace ledger::ledger
