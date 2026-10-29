#include "crypto/signer.h"

#include <sodium.h>

#include <stdexcept>

namespace ledger::crypto {

std::string sign(std::string_view message, const KeyPair& kp) {
  ensure_sodium();
  if (kp.secret_key.size() != kSecretKeySize) throw std::invalid_argument("bad secret key");
  std::string sig(kSignatureSize, '\0');
  crypto_sign_detached(reinterpret_cast<unsigned char*>(sig.data()), nullptr,
                       reinterpret_cast<const unsigned char*>(message.data()), message.size(),
                       reinterpret_cast<const unsigned char*>(kp.secret_key.data()));
  return sig;
}

bool verify(std::string_view message, std::string_view signature, std::string_view public_key) {
  ensure_sodium();
  if (signature.size() != kSignatureSize || public_key.size() != kPublicKeySize) return false;
  return crypto_sign_verify_detached(reinterpret_cast<const unsigned char*>(signature.data()),
                                     reinterpret_cast<const unsigned char*>(message.data()),
                                     message.size(),
                                     reinterpret_cast<const unsigned char*>(public_key.data())) == 0;
}

}  // namespace ledger::crypto
