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
#include "server/client_service.h"
#include "storage/meta_store.h"
#include "storage/snapshot_store.h"
#include "storage/wal.h"

namespace ledger {

// One ledger node process. It hosts a RaftCore on top of the network layer:
//
//   timer tick ──▶ raft_.tick() ─┐
//   Raft message ─▶ raft_.step() ─┼─▶ process_ready(): persist, send, apply,
//   client request ─▶ service_ ───┘     then tell service_ (replies, reads)
//
// process_ready() runs once per event-loop batch, so everything that arrived
// together is persisted with ONE fsync before any reply leaves (group commit).
// Raft's term/vote live in meta_, its log in wal_, ledger snapshots in snaps_
// (all under data_dir from cluster.json). On startup the node loads the newest
// snapshot and replays the WAL after it, so a crashed node keeps every
// promise it made. Every snapshot_every applied entries it snapshots the
// ledger and compacts the log.
//
// Committed entries are applied to the ledger state machine, built from the
// same genesis on every node; `status` shows its state hash so you can see
// that all nodes hold identical balances.
//
// Separately it pings every peer each heartbeat to measure reachability and
// RTT for `status` (a network-health probe; Raft doesn't use it). Its client
// port serves the ledger API (ClientService) and admin commands.
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
  static std::string data_dir_of(const ClusterConfig& config, NodeId self);
  static storage::FsyncMode fsync_mode_of(const ClusterConfig& config);
  raft::HardState load_hard_state() const;
  std::optional<raft::Snapshot> load_snapshot();
  std::vector<pb::LogEntry> recover_log();
  static ledger::StateMachine initial_ledger(const Genesis& genesis,
                                             const std::optional<raft::Snapshot>& snap);
  void check_genesis(const Genesis& genesis);
  // Snapshots the ledger at its applied index, compacts Raft's log and the WAL.
  void take_snapshot();
  // A snapshot from the leader replaces the ledger and the stored log.
  void install_snapshot(const raft::Snapshot& s);
  void process_ready();
  void apply(const pb::LogEntry& e);

  void on_tick();
  void send_pings();
  void update_reachability(uint64_t now);
  void on_peer_message(NodeId from, const pb::Envelope& msg, net::Connection& conn);

  void on_client_accept(int fd, std::string remote);
  void on_client_frame(net::Connection& conn, std::string_view payload);
  pb::Envelope handle_admin(const pb::Envelope& req);
  pb::StatusResp build_status() const;
  void reply_to_client(uint64_t conn_id, const pb::Envelope& msg);

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

  // Raft's durable state. Declared before raft_, which is built from them.
  std::string data_dir_;
  storage::MetaStore meta_;
  storage::Wal wal_;
  storage::SnapshotStore snaps_;
  std::optional<raft::Snapshot> boot_snapshot_;  // only during construction
  raft::RaftCore raft_;
  // The replicated ledger, in memory: after a restart it is rebuilt from the
  // newest snapshot (or genesis) by re-applying the committed entries after
  // it (the leader says how far).
  ledger::StateMachine ledger_;
  LogIndex last_snapshot_index_ = 0;
  // Declared after raft_ and ledger_, which it holds references to.
  server::ClientService service_;

  // Last role/term/leader logged, so only changes are logged.
  raft::Role logged_role_ = raft::Role::kFollower;
  Term logged_term_ = 0;
  NodeId logged_leader_ = kNoNode;
};

}  // namespace ledger
