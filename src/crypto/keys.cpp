#include "crypto/keys.h"

#include <fcntl.h>
#include <sodium.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <stdexcept>

#include "common/hex.h"

namespace ledger::crypto {

void ensure_sodium() {
  // A function-local static is initialized exactly once, even with threads.
  static const bool ok = sodium_init() >= 0;
  if (!ok) throw std::runtime_error("libsodium failed to initialize");
}

KeyPair generate_keypair() {
  ensure_sodium();
  std::string seed(kSeedSize, '\0');
  randombytes_buf(seed.data(), seed.size());
  return keypair_from_seed(seed);
}

KeyPair keypair_from_seed(std::string_view seed) {
  ensure_sodium();
  if (seed.size() != kSeedSize) throw std::invalid_argument("seed must be 32 bytes");
  KeyPair kp;
  kp.public_key.resize(kPublicKeySize);
  kp.secret_key.resize(kSecretKeySize);
  crypto_sign_seed_keypair(reinterpret_cast<unsigned char*>(kp.public_key.data()),
                           reinterpret_cast<unsigned char*>(kp.secret_key.data()),
                           reinterpret_cast<const unsigned char*>(seed.data()));
  return kp;
}

void save_keyfile(const std::string& path, const std::string& name, const KeyPair& kp) {
  nlohmann::json j = {{"name", name},
                      {"public_key", to_hex(kp.public_key)},
                      {"seed", to_hex(kp.seed())}};
  const std::string text = j.dump(2) + "\n";
  // O_EXCL: never silently replace a key (that would lose the old account).
  // Mode 0600 is applied at creation, so the secret is never world-readable,
  // not even for a moment.
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) throw std::runtime_error("cannot create " + path + ": " + std::strerror(errno));
  ssize_t n = ::write(fd, text.data(), text.size());
  bool ok = n == static_cast<ssize_t>(text.size()) && ::fsync(fd) == 0;
  ::close(fd);
  if (!ok) throw std::runtime_error("cannot write " + path);
}

KeyPair load_keyfile(const std::string& path) {
  std::ifstream in(path);
  if (!in) throw std::runtime_error("cannot open key file " + path);
  std::stringstream ss;
  ss << in.rdbuf();
  try {
    auto j = nlohmann::json::parse(ss.str());
    auto seed = from_hex(j.at("seed").get<std::string>());
    if (!seed || seed->size() != kSeedSize) throw std::runtime_error("bad seed");
    KeyPair kp = keypair_from_seed(*seed);
    // The stored public key is redundant; a mismatch means a corrupted file.
    if (to_hex(kp.public_key) != j.at("public_key").get<std::string>()) {
      throw std::runtime_error("public key doesn't match seed");
    }
    return kp;
  } catch (const std::exception& e) {
    throw std::runtime_error("bad key file " + path + ": " + e.what());
  }
}

}  // namespace ledger::crypto
