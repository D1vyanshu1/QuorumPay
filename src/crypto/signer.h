#pragma once

#include <string>
#include <string_view>

#include "crypto/keys.h"

namespace ledger::crypto {

constexpr size_t kSignatureSize = 64;

// Ed25519 signature (64 bytes) of `message` with the pair's secret key.
// Deterministic: signing the same message with the same key twice gives the
// same signature (no random nonce that a bad RNG could leak the key through).
std::string sign(std::string_view message, const KeyPair& kp);

// True only if `signature` is a valid signature of exactly `message` by the
// owner of `public_key`. Wrong sizes are simply invalid.
bool verify(std::string_view message, std::string_view signature, std::string_view public_key);

}  // namespace ledger::crypto
