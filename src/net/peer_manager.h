#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/types.h"
#include "envelope.pb.h"
#include "net/connection.h"
#include "net/event_loop.h"
#include "net/fault_filter.h"
#include "net/listener.h"

namespace ledger::net {

// Keeps this node connected to every other node in the cluster.
//
// Topology: each node DIALS every peer for its outgoing messages and ACCEPTS
// connections for incoming ones, so there are two TCP connections per pair,
// one per direction. A reply goes back on the connection its request came in
// on. This avoids "who dials whom" races and makes reconnects symmetric: each
// side only ever re-dials its own outbound connections.
//
//   node 1 ──outbound──▶ node 2      1 sends requests to 2 here, 2 replies here
//   node 1 ◀──outbound── node 2      2 sends requests to 1 here, 1 replies here
//
// There are no transport-level retries: a message lost to a crash or
// partition is just gone. Raft already resends whatever it still needs.
class PeerManager {
 public:
  // `conn` is where the message arrived; pass it to reply() to answer.
  using MessageHandler =
      std::function<void(NodeId from, const pb::Envelope& msg, Connection& conn)>;

  static constexpr uint32_t kMinBackoffMs = 50;
  static constexpr uint32_t kMaxBackoffMs = 1000;
  static constexpr uint64_t kHandshakeTimeoutMs = 5000;

  PeerManager(EventLoop& loop, const ClusterConfig& config, NodeId self, FaultFilter& faults);
  PeerManager(const PeerManager&) = delete;
  PeerManager& operator=(const PeerManager&) = delete;

  void set_message_handler(MessageHandler h) { handler_ = std::move(h); }

  // Binds the peer port and starts dialing every peer.
  void start();

  // Call periodically (every Raft tick): retries due reconnects and drops
  // inbound connections that never completed the handshake.
  void tick(uint64_t now_ms);

  // Sends on the outbound connection to `to`. Returns false if the message
  // was dropped (not connected, blocked, or back-pressure). Callers don't
  // retry; the protocol above handles loss.
  bool send(NodeId to, const pb::Envelope& msg);

  // Answers on the connection a request arrived on.
  bool reply(Connection& conn, const pb::Envelope& msg);

  bool is_connected(NodeId peer) const;
  std::vector<NodeId> peer_ids() const;
  NodeId self() const { return self_; }

 private:
  struct Outbound {
    NodeConfig config;
    std::shared_ptr<Connection> conn;  // null while waiting to reconnect
    bool connected = false;
    uint32_t backoff_ms = kMinBackoffMs;
    uint64_t next_attempt_ms = 0;
    uint32_t failed_attempts = 0;
  };

  void dial(Outbound& ob, uint64_t now_ms);
  void schedule_reconnect(Outbound& ob, uint64_t now_ms);
  void on_accept(int fd, std::string remote);
  void on_frame(Connection& conn, std::string_view payload, bool inbound);
  bool handle_hello(Connection& conn, const pb::Envelope& msg);
  bool send_envelope(Connection& conn, const pb::Envelope& msg);

  EventLoop& loop_;
  const ClusterConfig& config_;
  NodeId self_;
  FaultFilter& faults_;
  MessageHandler handler_;
  std::unique_ptr<Listener> listener_;

  std::map<NodeId, Outbound> outbound_;
  struct Inbound {
    std::shared_ptr<Connection> conn;
    uint64_t accepted_ms;
  };
  std::unordered_map<uint64_t, Inbound> inbound_;  // by Connection::id()

  std::mt19937 rng_;  // backoff jitter only; not part of any replicated state
};

}  // namespace ledger::net
