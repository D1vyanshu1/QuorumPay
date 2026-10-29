// ledger-cli: the wallet and client.
//
//   ledger-cli keygen <name>     # new key pair in the keystore; prints the public key
//   ledger-cli pubkey <name>     # print a stored public key (hex)
//
//   [--keys-dir DIR]             # default: $LEDGER_KEYS_DIR or ~/.ledger/keys
//
// Talking to the cluster (create_account, fund, transfer, balance, history,
// tx, status) comes with the client service in step 6.

#include <exception>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "client/keystore.h"
#include "common/hex.h"

namespace {

int usage() {
  std::cerr << "usage: ledger-cli [--keys-dir DIR] keygen <name> | pubkey <name>\n";
  return 2;
}

}  // namespace

int main(int argc, char** argv) {
  std::string keys_dir = ledger::client::KeyStore::default_dir();
  std::vector<std::string> args;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--keys-dir" && i + 1 < argc) {
      keys_dir = argv[++i];
    } else {
      args.push_back(a);
    }
  }
  if (args.size() != 2) return usage();
  const std::string& cmd = args[0];
  const std::string& name = args[1];
  ledger::client::KeyStore ks(keys_dir);

  try {
    if (cmd == "keygen") {
      auto kp = ks.create(name);
      nlohmann::json out = {{"name", name},
                            {"public_key", ledger::to_hex(kp.public_key)},
                            {"key_file", ks.path(name)}};
      std::cout << out.dump() << "\n";
      return 0;
    }
    if (cmd == "pubkey") {
      std::cout << ledger::to_hex(ks.load(name).public_key) << "\n";
      return 0;
    }
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << "\n";
    return 1;
  }
  return usage();
}
