#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "crypto/keys.h"
#include "ledger.pb.h"

namespace ledger::ledger {

enum class TxType : uint8_t {
  kInvalid = 0,
  kCreateAccount = 1,
  kTransfer = 2,
};

// One signed ledger operation. It travels as pb::Transaction (inside a Raft
// log entry) but is signed over signing_bytes(), never over protobuf bytes.
struct Transaction {
  TxType type = TxType::kInvalid;
  std::string chain_id;
  std::string sender;    // CREATE_ACCOUNT: the new account's name
  std::string receiver;  // TRANSFER only
  uint64_t amount = 0;   // TRANSFER only
  uint64_t nonce = 0;
  std::string public_key;  // CREATE_ACCOUNT only: 32 raw bytes
  std::string signature;   // 64 raw bytes

  // The exact bytes the signature covers. A fixed layout, so every client
  // and every node produce the same bytes for the same transaction:
  //
  //   "LEDGER_TX_V1"                         domain separation
  //   u32be len + chain_id                   no replay on another ledger
  //   u8 type
  //   u32be len + sender
  //   u32be len + receiver
  //   u64be amount
  //   u64be nonce                            no replay on this ledger
  //   [32-byte public key, CREATE_ACCOUNT only]
  //
  // Lengths make the encoding unambiguous: without them, sender "ab" +
  // receiver "c" and sender "a" + receiver "bc" would sign the same bytes.
  std::string signing_bytes() const;

  // SHA-256(signing bytes || signature). Clients look up a transaction's
  // result by this id. Retrying the same signed bytes gives the same id, so
  // a client can always find out what happened to its transaction.
  std::string tx_id() const;

  // Sets `signature` from the key pair (the client side).
  void sign(const crypto::KeyPair& kp);

  pb::Transaction to_proto() const;
  // Never fails: an unknown type becomes kInvalid and is rejected later as
  // MALFORMED, so a garbage entry still gets a deterministic result.
  static Transaction from_proto(const pb::Transaction& p);
};

// Account names: 3-32 characters from [a-z0-9_]. Restricting the alphabet
// rules out look-alike names (Unicode, case) that could trick a user.
bool is_valid_account_name(std::string_view name);

}  // namespace ledger::ledger
