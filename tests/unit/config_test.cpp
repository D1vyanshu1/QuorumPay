#include "common/config.h"

#include <gtest/gtest.h>

#include <stdexcept>

using ledger::ClusterConfig;

namespace {

const char* kValid = R"({
  "cluster_id": "test",
  "nodes": [
    {"id": 1, "host": "127.0.0.1", "peer_port": 7001, "client_port": 8001},
    {"id": 2, "host": "127.0.0.1", "peer_port": 7002, "client_port": 8002, "data_dir": "d2"}
  ],
  "raft": {"tick_ms": 20, "heartbeat_ticks": 3}
})";

}  // namespace

TEST(Config, ParsesValidConfig) {
  auto c = ClusterConfig::parse(kValid);
  EXPECT_EQ(c.cluster_id, "test");
  ASSERT_EQ(c.nodes.size(), 2u);
  EXPECT_EQ(c.nodes[0].peer_port, 7001);
  EXPECT_EQ(c.nodes[0].data_dir, "data/node-1");  // default
  EXPECT_EQ(c.nodes[1].data_dir, "d2");
  EXPECT_EQ(c.raft.tick_ms, 20u);
  EXPECT_EQ(c.raft.heartbeat_ticks, 3u);
  EXPECT_EQ(c.raft.election_ticks_min, 15u);  // default kept
}

TEST(Config, FindNode) {
  auto c = ClusterConfig::parse(kValid);
  ASSERT_NE(c.find(2), nullptr);
  EXPECT_EQ(c.find(2)->client_port, 8002);
  EXPECT_EQ(c.find(9), nullptr);
}

TEST(Config, RejectsBadInput) {
  // Invalid JSON
  EXPECT_THROW(ClusterConfig::parse("{"), std::runtime_error);
  // Missing cluster_id
  EXPECT_THROW(ClusterConfig::parse(R"({"nodes":[]})"), std::runtime_error);
  // No nodes
  EXPECT_THROW(ClusterConfig::parse(R"({"cluster_id":"x","nodes":[]})"), std::runtime_error);
  // Duplicate id
  EXPECT_THROW(ClusterConfig::parse(R"({"cluster_id":"x","nodes":[
      {"id":1,"host":"h","peer_port":1,"client_port":2},
      {"id":1,"host":"h","peer_port":3,"client_port":4}]})"),
               std::runtime_error);
  // Two nodes on the same host:port
  EXPECT_THROW(ClusterConfig::parse(R"({"cluster_id":"x","nodes":[
      {"id":1,"host":"h","peer_port":1,"client_port":2},
      {"id":2,"host":"h","peer_port":1,"client_port":4}]})"),
               std::runtime_error);
  // Port out of range
  EXPECT_THROW(ClusterConfig::parse(R"({"cluster_id":"x","nodes":[
      {"id":1,"host":"h","peer_port":70000,"client_port":2}]})"),
               std::runtime_error);
  // Node id 0 is reserved
  EXPECT_THROW(ClusterConfig::parse(R"({"cluster_id":"x","nodes":[
      {"id":0,"host":"h","peer_port":1,"client_port":2}]})"),
               std::runtime_error);
  // Heartbeat must be shorter than the election timeout
  EXPECT_THROW(ClusterConfig::parse(R"({"cluster_id":"x","nodes":[
      {"id":1,"host":"h","peer_port":1,"client_port":2}],
      "raft":{"heartbeat_ticks":20}})"),
               std::runtime_error);
}

TEST(Config, LoadsRepoConfig) {
  // The real config/cluster.json must always be valid.
  auto c = ClusterConfig::load_file(std::string(LEDGER_SOURCE_DIR) + "/config/cluster.json");
  EXPECT_EQ(c.nodes.size(), 5u);
}
