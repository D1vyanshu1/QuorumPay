#pragma once

#include <string>

#include "crypto/keys.h"

namespace ledger::client {

// A directory of key files, one per account: <dir>/<name>.key. The directory
// is created with mode 0700 and each file with 0600, because the files hold
// secret keys. Only the client ever has these; nodes only see public keys.
class KeyStore {
 public:
  explicit KeyStore(std::string dir) : dir_(std::move(dir)) {}

  // $LEDGER_KEYS_DIR if set, else ~/.ledger/keys.
  static std::string default_dir();

  std::string path(const std::string& name) const { return dir_ + "/" + name + ".key"; }
  bool exists(const std::string& name) const;

  // Generates and saves a new key pair. Throws std::runtime_error if `name`
  // isn't a valid account name or already has a key (never overwrites).
  crypto::KeyPair create(const std::string& name);
  crypto::KeyPair load(const std::string& name) const;

 private:
  std::string dir_;
};

}  // namespace ledger::client
