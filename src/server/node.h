#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/config.h"
#include "common/types.h"
#include "ledger/state_machine.h"
#include "envelope.pb.h"
#include "net/connection.h"
#include "net/event_loop.h"
#include "net/fault_filter.h"
#include "net/listener.h"
#include "net/peer_manager.h"
#include "raft/raft_core.h"

namespace ledger {

// One ledger node process. It hosts a RaftCore on top of the network layer:
//
//   timer tick ──▶ raft_.tick() ─┐
//   Raft message ─▶ raft_.step() ─┴─▶ process_ready(): persist, send, apply
//
// Committed entries are applied to the ledger state machine, built from the
// same genesis on every node; `status` shows its state hash so you can see
// that all nodes hold identical balances.
//
// Separately it pings every peer each heartbeat to measure reachability and
// RTT for `status` (a network-health probe; Raft doesn't use it), and serves
// admin commands on its client port.
class Node {
 public:
  // A peer counts as unreachable if no Pong arrived within this window
  // (10 heartbeats at the default 50ms heartbeat).
  static constexpr uint64_t kUnreachableAfterMs = 500;

  Node(ClusterConfig config, NodeId self, const Genesis& genesis);

  // Blocks until SIGINT/SIGTERM.
  void run();

 private:
  struct PeerHealth {
    uint64_t last_pong_ms = 0;  // 0 = never
    uint64_t last_rtt_us = 0;
    bool reachable = false;
  };

  raft::RaftConfig make_raft_config() const;
  void process_ready();
  void apply(const pb::LogEntry& e);

  void on_tick();
  void send_pings();
  void update_reachability(uint64_t now);
  void on_peer_message(NodeId from, const pb::Envelope& msg, net::Connection& conn);

  void on_client_accept(int fd, std::string remote);
  void on_client_frame(net::Connection& conn, std::string_view payload);
  pb::Envelope handle_admin(const pb::Envelope& req);
  // Reads are answered later, once Raft confirms them; see pending_reads_.
  void start_read(net::Connection& conn);
  void finish_read(uint64_t read_id, const raft::ReadState* ok);
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

  raft::RaftCore raft_;
  // Stand-in for the disk until week 5 (MetaStore + WAL). Lost on restart, so
  // a restarted node comes back at term 0 and simply catches up. Known gap:
  // forgetting a vote can in theory allow a double vote in one term.
  raft::HardState hard_state_;
  std::vector<pb::LogEntry> log_;
  // The replicated ledger. Volatile until week 5, like the log: after a
  // restart it is rebuilt from genesis by re-applying committed entries.
  ledger::StateMachine ledger_;

  // Reads waiting for Raft's confirmation: read id -> client connection id.
  // The connection is looked up again on completion; it may have gone away.
  uint64_t next_read_id_ = 0;
  std::unordered_map<uint64_t, uint64_t> pending_reads_;
  // Last role/term/leader logged, so only changes are logged.
  raft::Role logged_role_ = raft::Role::kFollower;
  Term logged_term_ = 0;
  NodeId logged_leader_ = kNoNode;
};

}  // namespace ledger
