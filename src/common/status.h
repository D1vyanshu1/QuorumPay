#pragma once

#include <cstdint>

namespace ledger {

// Outcome of applying one transaction. Every node computes the same status
// for the same log entry, so it is part of the replicated state.
//
// The order follows the validation pipeline (project_details.md §6.5): the
// first check that fails decides the status.
enum class TxStatus : uint8_t {
  kOk = 0,
  kMalformed = 1,          // bad name, zero amount, self-transfer, wrong chain_id...
  kUnknownAccount = 2,     // TRANSFER from an account that doesn't exist
  kDuplicateAccount = 3,   // CREATE_ACCOUNT for a name already taken
  kBadSignature = 4,
  kBadNonce = 5,           // not the sender's next nonce (includes replays)
  kUnknownReceiver = 6,
  kInsufficientFunds = 7,
  kOverflow = 8,           // receiver's balance would overflow uint64
};

inline const char* tx_status_name(TxStatus s) {
  switch (s) {
    case TxStatus::kOk:
      return "OK";
    case TxStatus::kMalformed:
      return "MALFORMED";
    case TxStatus::kUnknownAccount:
      return "UNKNOWN_ACCOUNT";
    case TxStatus::kDuplicateAccount:
      return "DUPLICATE_ACCOUNT";
    case TxStatus::kBadSignature:
      return "BAD_SIGNATURE";
    case TxStatus::kBadNonce:
      return "BAD_NONCE";
    case TxStatus::kUnknownReceiver:
      return "UNKNOWN_RECEIVER";
    case TxStatus::kInsufficientFunds:
      return "INSUFFICIENT_FUNDS";
    case TxStatus::kOverflow:
      return "OVERFLOW";
  }
  return "?";
}

// Whether a transaction that ended with this status used up the sender's
// nonce. Only failures AFTER the sender is proven (valid signature + the
// expected nonce) consume it. Otherwise anyone could burn someone else's
// nonces with unsigned junk, while a signed transfer that merely fails a
// business rule must still use up its nonce so it can't be replayed later.
inline bool consumes_nonce(TxStatus s) {
  return s == TxStatus::kOk || s == TxStatus::kUnknownReceiver ||
         s == TxStatus::kInsufficientFunds || s == TxStatus::kOverflow;
}

}  // namespace ledger
