#pragma once

#include <string>
#include <string_view>

namespace ledger::crypto {

constexpr size_t kSeedSize = 32;
constexpr size_t kPublicKeySize = 32;
constexpr size_t kSecretKeySize = 64;  // libsodium's form: seed || public key

// An Ed25519 key pair, as raw bytes. The 32-byte seed is the real secret:
// the whole pair is derived from it, so key files store only the seed.
struct KeyPair {
  std::string public_key;  // 32 bytes, safe to share
  std::string secret_key;  // 64 bytes, never leaves its owner

  std::string seed() const { return secret_key.substr(0, kSeedSize); }
};

// Initializes libsodium once (thread-safe). Every crypto function calls it,
// so callers never have to.
void ensure_sodium();

KeyPair generate_keypair();  // from the OS's secure random source
// Deterministic: the same seed always gives the same pair. For tests and the
// simulator, which need reproducible keys; real keys use generate_keypair().
KeyPair keypair_from_seed(std::string_view seed);

// Key file: JSON {"name", "public_key", "seed"} (hex), created with mode 0600
// (owner read/write only) because it holds the secret. Both throw
// std::runtime_error on failure; save refuses to overwrite an existing file.
void save_keyfile(const std::string& path, const std::string& name, const KeyPair& kp);
KeyPair load_keyfile(const std::string& path);

}  // namespace ledger::crypto
