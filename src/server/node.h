#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/config.h"
#include "common/types.h"
#include "envelope.pb.h"
#include "net/connection.h"
#include "net/event_loop.h"
#include "net/fault_filter.h"
#include "net/listener.h"
#include "net/peer_manager.h"

namespace ledger {

// One ledger node process. For now it only runs the network: it keeps the
// peer mesh up, pings every peer each heartbeat to measure liveness and RTT,
// and serves admin commands on its client port. In week 4 the Ping/Pong
// traffic is replaced by RaftCore's messages.
class Node {
 public:
  // A peer counts as unreachable if no Pong arrived within this window
  // (10 heartbeats at the default 50ms heartbeat).
  static constexpr uint64_t kUnreachableAfterMs = 500;

  Node(ClusterConfig config, NodeId self);

  // Blocks until SIGINT/SIGTERM.
  void run();

 private:
  struct PeerHealth {
    uint64_t last_pong_ms = 0;  // 0 = never
    uint64_t last_rtt_us = 0;
    bool reachable = false;
  };

  void on_tick();
  void send_pings();
  void update_reachability(uint64_t now);
  void on_peer_message(NodeId from, const pb::Envelope& msg, net::Connection& conn);

  void on_client_accept(int fd, std::string remote);
  void on_client_frame(net::Connection& conn, std::string_view payload);
  pb::Envelope handle_admin(const pb::Envelope& req);
  pb::StatusResp build_status() const;

  ClusterConfig config_;
  NodeId self_;
  net::EventLoop loop_;
  net::FaultFilter faults_;
  net::PeerManager peers_;
  std::unique_ptr<net::Listener> client_listener_;
  std::unordered_map<uint64_t, std::shared_ptr<net::Connection>> clients_;

  uint64_t tick_count_ = 0;
  uint64_t ping_seq_ = 0;
  std::map<NodeId, PeerHealth> health_;
};

}  // namespace ledger
