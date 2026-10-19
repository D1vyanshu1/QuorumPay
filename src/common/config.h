#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/types.h"

namespace ledger {

struct NodeConfig {
  NodeId id = kNoNode;
  std::string host;
  uint16_t peer_port = 0;    // node-to-node traffic (Raft)
  uint16_t client_port = 0;  // ledger-cli / admin traffic
  std::string data_dir;
};

struct RaftConfig {
  uint32_t tick_ms = 10;
  uint32_t election_ticks_min = 15;
  uint32_t election_ticks_max = 30;
  uint32_t heartbeat_ticks = 5;
  uint32_t max_entries_per_append = 64;
  uint32_t snapshot_every = 1000;
  std::string fsync = "batch";
};

// Loaded from config/cluster.json. Every node gets the identical file and
// learns which entry is itself from the --id flag.
struct ClusterConfig {
  std::string cluster_id;
  std::vector<NodeConfig> nodes;
  RaftConfig raft;

  // nullptr if `id` isn't in the cluster.
  const NodeConfig* find(NodeId id) const;

  // Both throw std::runtime_error with a readable message on any problem.
  static ClusterConfig load_file(const std::string& path);
  static ClusterConfig parse(const std::string& json_text);

 private:
  void validate() const;
};

}  // namespace ledger
