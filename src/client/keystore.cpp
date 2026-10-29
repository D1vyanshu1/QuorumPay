#include "client/keystore.h"

#include <sys/stat.h>

#include <cstdlib>
#include <filesystem>
#include <stdexcept>

#include "ledger/transaction.h"

namespace ledger::client {

std::string KeyStore::default_dir() {
  if (const char* d = std::getenv("LEDGER_KEYS_DIR"); d && *d) return d;
  const char* home = std::getenv("HOME");
  return std::string(home ? home : ".") + "/.ledger/keys";
}

bool KeyStore::exists(const std::string& name) const { return std::filesystem::exists(path(name)); }

crypto::KeyPair KeyStore::create(const std::string& name) {
  if (!ledger::is_valid_account_name(name)) {
    throw std::runtime_error("invalid account name '" + name + "' (3-32 chars of a-z 0-9 _)");
  }
  std::filesystem::create_directories(dir_);
  ::chmod(dir_.c_str(), 0700);
  crypto::KeyPair kp = crypto::generate_keypair();
  crypto::save_keyfile(path(name), name, kp);  // refuses to overwrite
  return kp;
}

crypto::KeyPair KeyStore::load(const std::string& name) const {
  return crypto::load_keyfile(path(name));
}

}  // namespace ledger::client
