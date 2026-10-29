#include "common/config.h"

#include "common/hex.h"

#include <fstream>
#include <limits>
#include <nlohmann/json.hpp>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace ledger {

namespace {

[[noreturn]] void fail(const std::string& msg) { throw std::runtime_error("config: " + msg); }

uint16_t parse_port(const nlohmann::json& j, const char* field) {
  int64_t v = j.at(field).get<int64_t>();
  if (v < 1 || v > 65535) fail(std::string(field) + " out of range: " + std::to_string(v));
  return static_cast<uint16_t>(v);
}

uint32_t parse_u32(const nlohmann::json& j, const char* field, uint32_t def) {
  if (!j.contains(field)) return def;
  int64_t v = j.at(field).get<int64_t>();
  if (v < 0 || v > std::numeric_limits<uint32_t>::max()) {
    fail(std::string(field) + " out of range: " + std::to_string(v));
  }
  return static_cast<uint32_t>(v);
}

std::string read_file(const std::string& path) {
  std::ifstream in(path);
  if (!in) fail("cannot open " + path);
  std::stringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

}  // namespace

const NodeConfig* ClusterConfig::find(NodeId id) const {
  for (const auto& n : nodes) {
    if (n.id == id) return &n;
  }
  return nullptr;
}

ClusterConfig ClusterConfig::load_file(const std::string& path) { return parse(read_file(path)); }

ClusterConfig ClusterConfig::parse(const std::string& json_text) {
  nlohmann::json j;
  try {
    j = nlohmann::json::parse(json_text);
  } catch (const nlohmann::json::parse_error& e) {
    fail(std::string("invalid JSON: ") + e.what());
  }

  ClusterConfig c;
  try {
    c.cluster_id = j.at("cluster_id").get<std::string>();
    for (const auto& n : j.at("nodes")) {
      NodeConfig nc;
      int64_t id = n.at("id").get<int64_t>();
      if (id < 1 || id > std::numeric_limits<NodeId>::max()) {
        fail("node id out of range: " + std::to_string(id));
      }
      nc.id = static_cast<NodeId>(id);
      nc.host = n.at("host").get<std::string>();
      nc.peer_port = parse_port(n, "peer_port");
      nc.client_port = parse_port(n, "client_port");
      nc.data_dir = n.value("data_dir", "data/node-" + std::to_string(nc.id));
      c.nodes.push_back(std::move(nc));
    }
    if (j.contains("raft")) {
      const auto& r = j.at("raft");
      c.raft.tick_ms = parse_u32(r, "tick_ms", c.raft.tick_ms);
      c.raft.election_ticks_min = parse_u32(r, "election_ticks_min", c.raft.election_ticks_min);
      c.raft.election_ticks_max = parse_u32(r, "election_ticks_max", c.raft.election_ticks_max);
      c.raft.heartbeat_ticks = parse_u32(r, "heartbeat_ticks", c.raft.heartbeat_ticks);
      c.raft.max_entries_per_append =
          parse_u32(r, "max_entries_per_append", c.raft.max_entries_per_append);
      c.raft.snapshot_every = parse_u32(r, "snapshot_every", c.raft.snapshot_every);
      c.raft.fsync = r.value("fsync", c.raft.fsync);
    }
  } catch (const nlohmann::json::exception& e) {
    fail(e.what());
  }

  c.validate();
  return c;
}

void ClusterConfig::validate() const {
  if (cluster_id.empty()) fail("cluster_id is empty");
  if (nodes.empty()) fail("no nodes");

  std::set<NodeId> ids;
  std::set<std::pair<std::string, uint16_t>> endpoints;
  for (const auto& n : nodes) {
    if (!ids.insert(n.id).second) fail("duplicate node id " + std::to_string(n.id));
    if (n.host.empty()) fail("node " + std::to_string(n.id) + " has empty host");
    if (!endpoints.insert({n.host, n.peer_port}).second ||
        !endpoints.insert({n.host, n.client_port}).second) {
      fail("node " + std::to_string(n.id) + " reuses a host:port");
    }
  }

  if (raft.tick_ms == 0) fail("tick_ms must be > 0");
  if (raft.heartbeat_ticks == 0) fail("heartbeat_ticks must be > 0");
  if (raft.election_ticks_min >= raft.election_ticks_max) {
    fail("election_ticks_min must be < election_ticks_max");
  }
  if (raft.heartbeat_ticks >= raft.election_ticks_min) {
    fail("heartbeat_ticks must be < election_ticks_min");
  }
}

// ---------------------------------------------------------------- genesis

uint64_t Genesis::total_supply() const {
  uint64_t total = 0;
  for (const auto& a : accounts) total += a.balance;  // parse() rules out overflow
  return total;
}

Genesis Genesis::load_file(const std::string& path) { return parse(read_file(path)); }

Genesis Genesis::parse(const std::string& json_text) {
  nlohmann::json j;
  try {
    j = nlohmann::json::parse(json_text);
  } catch (const nlohmann::json::parse_error& e) {
    fail(std::string("genesis: invalid JSON: ") + e.what());
  }

  Genesis g;
  std::set<std::string> names;
  uint64_t total = 0;
  try {
    g.chain_id = j.at("chain_id").get<std::string>();
    if (g.chain_id.empty()) fail("genesis: chain_id is empty");
    for (const auto& a : j.at("accounts")) {
      GenesisAccount ga;
      ga.name = a.at("name").get<std::string>();
      if (!names.insert(ga.name).second) fail("genesis: duplicate account " + ga.name);

      auto key = from_hex(a.at("public_key_hex").get<std::string>());
      if (!key || key->size() != 32) fail("genesis: " + ga.name + ": public_key_hex must be 64 hex chars");
      ga.public_key = std::move(*key);

      // Money is integer units; a float or negative number is a mistake.
      const auto& bal = a.at("balance");
      if (!bal.is_number_unsigned()) fail("genesis: " + ga.name + ": balance must be a non-negative integer");
      ga.balance = bal.get<uint64_t>();
      if (ga.balance > std::numeric_limits<uint64_t>::max() - total) {
        fail("genesis: total supply overflows uint64");
      }
      total += ga.balance;
      g.accounts.push_back(std::move(ga));
    }
  } catch (const nlohmann::json::exception& e) {
    fail(std::string("genesis: ") + e.what());
  }
  return g;
}

}  // namespace ledger
