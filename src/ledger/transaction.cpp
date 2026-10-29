#include "ledger/transaction.h"

#include "crypto/hash.h"
#include "crypto/signer.h"
#include "ledger/encoding.h"

namespace ledger::ledger {

namespace {

constexpr std::string_view kDomain = "LEDGER_TX_V1";

}  // namespace

std::string Transaction::signing_bytes() const {
  std::string out(kDomain);
  put_str(out, chain_id);
  out.push_back(static_cast<char>(type));
  put_str(out, sender);
  put_str(out, receiver);
  put_u64(out, amount);
  put_u64(out, nonce);
  if (type == TxType::kCreateAccount) out.append(public_key);
  return out;
}

std::string Transaction::tx_id() const { return crypto::sha256(signing_bytes() + signature); }

void Transaction::sign(const crypto::KeyPair& kp) {
  signature = crypto::sign(signing_bytes(), kp);
}

pb::Transaction Transaction::to_proto() const {
  pb::Transaction p;
  p.set_type(static_cast<pb::TxType>(type));
  p.set_chain_id(chain_id);
  p.set_sender(sender);
  p.set_receiver(receiver);
  p.set_amount(amount);
  p.set_nonce(nonce);
  p.set_public_key(public_key);
  p.set_signature(signature);
  return p;
}

Transaction Transaction::from_proto(const pb::Transaction& p) {
  Transaction t;
  switch (p.type()) {
    case pb::TX_CREATE_ACCOUNT:
      t.type = TxType::kCreateAccount;
      break;
    case pb::TX_TRANSFER:
      t.type = TxType::kTransfer;
      break;
    default:
      t.type = TxType::kInvalid;
  }
  t.chain_id = p.chain_id();
  t.sender = p.sender();
  t.receiver = p.receiver();
  t.amount = p.amount();
  t.nonce = p.nonce();
  t.public_key = p.public_key();
  t.signature = p.signature();
  return t;
}

bool is_valid_account_name(std::string_view name) {
  if (name.size() < 3 || name.size() > 32) return false;
  for (char c : name) {
    bool ok = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
    if (!ok) return false;
  }
  return true;
}

}  // namespace ledger::ledger
