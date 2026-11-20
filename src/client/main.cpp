// ledger-cli: the wallet and client.
//
//   ledger-cli keygen <name>                  # new key pair in the keystore
//   ledger-cli pubkey <name>                  # print a stored public key (hex)
//   ledger-cli create_account <name>          # keygen if needed, then CREATE_ACCOUNT
//   ledger-cli fund <name> <amount>           # transfer from the faucet (needs its key)
//   ledger-cli transfer <from> <to> <amount> [--nonce N] [--tamper N]
//   ledger-cli balance <name> [--stale]       # linearizable unless --stale
//   ledger-cli history <name> [--limit N] [--stale]
//   ledger-cli tx <tx_id> [--stale]
//   ledger-cli status [--node N]              # every node, or just N
//
// Global flags:
//   --config config/cluster.json   --genesis config/genesis.json (for chain_id)
//   --node N (node to try first)   --timeout MS (default 10000)
//   --keys-dir DIR (default $LEDGER_KEYS_DIR or ~/.ledger/keys)
//
// Output is one JSON object on stdout. Exit codes: 0 ok, 1 error (including
// timeout: the outcome is then UNKNOWN, check with `tx <tx_id>`), 2 usage,
// 4 the transaction failed / the account doesn't exist.
//
// --nonce skips the nonce lookup. Normally the CLI reads the sender's next
// nonce first (a linearizable read); the demos use --nonce to send two
// different transfers with the same nonce (a double spend).
// --tamper N (demos only) signs the transfer, then changes its amount to N
// before sending: what an attacker who intercepted it would try. The
// signature no longer matches, so the cluster refuses it (BAD_SIGNATURE).

#include <cstdint>
#include <exception>
#include <iostream>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <vector>

#include "client/client_lib.h"
#include "client/keystore.h"
#include "common/config.h"
#include "common/hex.h"
#include "common/status.h"
#include "ledger/transaction.h"

namespace {

using ledger::NodeId;
using ledger::TxStatus;
using ledger::client::ClusterClient;
using ledger::client::KeyStore;
using ledger::ledger::Transaction;
using ledger::ledger::TxType;
using nlohmann::json;
namespace pb = ledger::pb;

constexpr int kExitOk = 0;
constexpr int kExitError = 1;
constexpr int kExitUsage = 2;
constexpr int kExitFailed = 4;

struct Options {
  std::string config_path = "config/cluster.json";
  std::string genesis_path = "config/genesis.json";
  std::string keys_dir = KeyStore::default_dir();
  NodeId node = ledger::kNoNode;
  uint64_t timeout_ms = 10000;
  bool stale = false;
  uint64_t limit = 0;
  std::optional<uint64_t> nonce;
  std::optional<uint64_t> tamper;
  std::vector<std::string> args;
};

int usage() {
  std::cerr <<
      "usage: ledger-cli [--config F] [--genesis F] [--node N] [--timeout MS] [--keys-dir D]\n"
      "         keygen <name> | pubkey <name> | create_account <name>\n"
      "       | fund <name> <amount> | transfer <from> <to> <amount> [--nonce N] [--tamper N]\n"
      "       | balance <name> [--stale] | history <name> [--limit N] [--stale]\n"
      "       | tx <tx_id> [--stale] | status\n";
  return kExitUsage;
}

// Digits only. std::stoull would accept "-5" (as 2^64 - 5) and "12abc".
bool parse_u64(const std::string& s, uint64_t* out) {
  if (s.empty() || s.size() > 20) return false;
  uint64_t v = 0;
  for (char c : s) {
    if (c < '0' || c > '9') return false;
    uint64_t d = static_cast<uint64_t>(c - '0');
    if (v > (UINT64_MAX - d) / 10) return false;
    v = v * 10 + d;
  }
  *out = v;
  return true;
}

bool parse_args(int argc, char** argv, Options* o) {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto value = [&](std::string* v) {
      if (i + 1 >= argc) return false;
      *v = argv[++i];
      return true;
    };
    std::string v;
    uint64_t n = 0;
    if (a == "--config") {
      if (!value(&o->config_path)) return false;
    } else if (a == "--genesis") {
      if (!value(&o->genesis_path)) return false;
    } else if (a == "--keys-dir") {
      if (!value(&o->keys_dir)) return false;
    } else if (a == "--node") {
      if (!value(&v) || !parse_u64(v, &n) || n == 0 || n > UINT32_MAX) return false;
      o->node = static_cast<NodeId>(n);
    } else if (a == "--timeout") {
      if (!value(&v) || !parse_u64(v, &o->timeout_ms) || o->timeout_ms == 0) return false;
    } else if (a == "--limit") {
      if (!value(&v) || !parse_u64(v, &n) || n > UINT32_MAX) return false;
      o->limit = n;
    } else if (a == "--nonce") {
      if (!value(&v) || !parse_u64(v, &n)) return false;
      o->nonce = n;
    } else if (a == "--tamper") {
      if (!value(&v) || !parse_u64(v, &n)) return false;
      o->tamper = n;
    } else if (a == "--stale") {
      o->stale = true;
    } else if (a == "--all") {
      // `status --all` (the default anyway)
    } else if (a.rfind("--", 0) == 0) {
      return false;
    } else {
      o->args.push_back(a);
    }
  }
  return !o->args.empty();
}

void print(const json& j) { std::cout << j.dump() << "\n"; }

// ---------------------------------------------------------------- ledger API

class Cli {
 public:
  explicit Cli(const Options& o)
      : o_(o),
        keys_(o.keys_dir),
        client_(ledger::ClusterConfig::load_file(o.config_path), o.node, o.timeout_ms) {}

  int create_account(const std::string& name) {
    // Reuse an existing key: re-running the command then resends the very
    // same signed transaction, which can't create the account twice.
    ledger::crypto::KeyPair kp = keys_.exists(name) ? keys_.load(name) : keys_.create(name);
    Transaction t;
    t.type = TxType::kCreateAccount;
    t.chain_id = chain_id();
    t.sender = name;
    t.public_key = kp.public_key;
    t.nonce = 0;
    t.sign(kp);
    return submit(t);
  }

  int transfer(const std::string& from, const std::string& to, uint64_t amount) {
    ledger::crypto::KeyPair kp = keys_.load(from);
    uint64_t nonce = 0;
    if (o_.nonce) {
      nonce = *o_.nonce;
    } else {
      // Linearizable, so it reflects every transfer already acknowledged.
      pb::AccountResp a = account(from, /*stale=*/false);
      if (!a.found()) {
        print({{"error", "no account " + from}});
        return kExitFailed;
      }
      nonce = a.next_nonce();
    }
    Transaction t;
    t.type = TxType::kTransfer;
    t.chain_id = chain_id();
    t.sender = from;
    t.receiver = to;
    t.amount = amount;
    t.nonce = nonce;
    t.sign(kp);
    if (o_.tamper) t.amount = *o_.tamper;  // after signing: the signature no longer fits
    return submit(t);
  }

  int balance(const std::string& name) {
    pb::AccountResp a = account(name, o_.stale);
    if (!a.found()) {
      print({{"name", name}, {"found", false}, {"applied_index", a.applied_index()}});
      return kExitFailed;
    }
    print({{"name", name},
           {"balance", a.balance()},
           {"next_nonce", a.next_nonce()},
           {"applied_index", a.applied_index()}});
    return kExitOk;
  }

  int history(const std::string& name) {
    pb::Envelope req;
    req.mutable_get_history()->set_name(name);
    req.mutable_get_history()->set_limit(static_cast<uint32_t>(o_.limit));
    req.mutable_get_history()->set_stale(o_.stale);
    pb::Envelope resp = client_.call(req);
    if (!resp.has_history_resp()) throw std::runtime_error("unexpected reply");
    const auto& h = resp.history_resp();
    json records = json::array();
    for (const auto& r : h.records()) {
      records.push_back({{"index", r.index()},
                         {"tx_id", ledger::to_hex(r.tx_id())},
                         {"direction", r.incoming() ? "in" : "out"},
                         {"counterparty", r.counterparty()},
                         {"amount", r.amount()}});
    }
    print({{"name", name},
           {"found", h.found()},
           {"records", records},
           {"applied_index", h.applied_index()}});
    return h.found() ? kExitOk : kExitFailed;
  }

  int tx(const std::string& tx_id_hex) {
    auto id = ledger::from_hex(tx_id_hex);
    if (!id || id->size() != 32) {
      std::cerr << "error: tx_id must be 64 hex characters\n";
      return kExitUsage;
    }
    pb::Envelope req;
    req.mutable_get_tx()->set_tx_id(*id);
    req.mutable_get_tx()->set_stale(o_.stale);
    pb::Envelope resp = client_.call(req);
    if (!resp.has_tx_resp()) throw std::runtime_error("unexpected reply");
    const auto& t = resp.tx_resp();
    json out = {{"tx_id", tx_id_hex}, {"applied_index", t.applied_index()}};
    if (t.found()) {
      out["status"] = ledger::tx_status_name(static_cast<TxStatus>(t.tx_status()));
      out["index"] = t.log_index();
    } else {
      out["status"] = t.pending() ? "PENDING" : "UNKNOWN";
    }
    print(out);
    return t.found() ? kExitOk : kExitFailed;
  }

  int status() {
    std::vector<NodeId> ids;
    if (o_.node != ledger::kNoNode) {
      ids.push_back(o_.node);
    } else {
      for (const auto& n : client_.config().nodes) ids.push_back(n.id);
    }
    int rc = kExitOk;
    for (NodeId id : ids) {
      pb::Envelope req;
      req.mutable_get_status();
      auto resp = client_.call_on(id, req);
      if (!resp || !resp->has_status_resp()) {
        print({{"node", id}, {"error", "unreachable"}});
        rc = kExitError;
        continue;
      }
      const auto& s = resp->status_resp();
      print({{"node", s.node_id()},
             {"role", s.role()},
             {"term", s.term()},
             {"leader", s.leader()},
             {"commit_index", s.commit_index()},
             {"applied_index", s.applied_index()},
             {"state_hash", s.state_hash()},
             {"total_supply", s.total_supply()},
             {"accounts", s.accounts()}});
    }
    return rc;
  }

 private:
  pb::AccountResp account(const std::string& name, bool stale) {
    pb::Envelope req;
    req.mutable_get_account()->set_name(name);
    req.mutable_get_account()->set_stale(stale);
    pb::Envelope resp = client_.call(req);
    if (!resp.has_account_resp()) throw std::runtime_error("unexpected reply");
    return resp.account_resp();
  }

  int submit(const Transaction& t) {
    pb::Envelope req;
    *req.mutable_submit_tx()->mutable_tx() = t.to_proto();
    const std::string tx_id = ledger::to_hex(t.tx_id());
    pb::Envelope resp;
    try {
      resp = client_.call(req);
    } catch (const ledger::client::TimeoutError& e) {
      // It may or may not have committed: say so, with the id to look it up.
      print({{"tx_id", tx_id}, {"nonce", t.nonce}, {"status", "UNKNOWN"}, {"error", e.what()}});
      return kExitError;
    }
    if (!resp.has_submit_tx_resp()) throw std::runtime_error("unexpected reply");
    const auto& r = resp.submit_tx_resp();
    const auto status = static_cast<TxStatus>(r.tx_status());
    json out = {{"tx_id", tx_id}, {"nonce", t.nonce}, {"status", ledger::tx_status_name(status)}};
    if (r.code() == pb::RESP_OK) {
      out["index"] = r.log_index();
    } else {
      out["rejected"] = true;  // by the leader's pre-check: it never entered the log
    }
    print(out);
    return r.code() == pb::RESP_OK && status == TxStatus::kOk ? kExitOk : kExitFailed;
  }

  const std::string& chain_id() {
    if (chain_id_.empty()) chain_id_ = ledger::Genesis::load_file(o_.genesis_path).chain_id;
    return chain_id_;
  }

  const Options& o_;
  KeyStore keys_;
  ClusterClient client_;
  std::string chain_id_;
};

}  // namespace

int main(int argc, char** argv) {
  Options o;
  if (!parse_args(argc, argv, &o)) return usage();
  const std::string& cmd = o.args[0];
  const size_t n = o.args.size() - 1;  // arguments after the command
  auto arg = [&o](size_t i) -> const std::string& { return o.args[i]; };

  try {
    // Keystore-only commands: no cluster needed.
    if (cmd == "keygen" && n == 1) {
      KeyStore ks(o.keys_dir);
      auto kp = ks.create(arg(1));
      print({{"name", arg(1)},
             {"public_key", ledger::to_hex(kp.public_key)},
             {"key_file", ks.path(arg(1))}});
      return kExitOk;
    }
    if (cmd == "pubkey" && n == 1) {
      std::cout << ledger::to_hex(KeyStore(o.keys_dir).load(arg(1)).public_key) << "\n";
      return kExitOk;
    }

    uint64_t amount = 0;
    if (cmd == "create_account" && n == 1) return Cli(o).create_account(arg(1));
    if (cmd == "fund" && n == 2) {
      if (!parse_u64(arg(2), &amount)) return usage();
      return Cli(o).transfer("faucet", arg(1), amount);
    }
    if (cmd == "transfer" && n == 3) {
      if (!parse_u64(arg(3), &amount)) return usage();
      return Cli(o).transfer(arg(1), arg(2), amount);
    }
    if (cmd == "balance" && n == 1) return Cli(o).balance(arg(1));
    if (cmd == "history" && n == 1) return Cli(o).history(arg(1));
    if (cmd == "tx" && n == 1) return Cli(o).tx(arg(1));
    if (cmd == "status" && n == 0) return Cli(o).status();
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << "\n";
    return kExitError;
  }
  return usage();
}
