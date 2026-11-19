#include "server/node.h"

#include <sys/signalfd.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <random>
#include <stdexcept>
#include <utility>

#include "common/clock.h"
#include "common/hex.h"
#include "common/logger.h"
#include "storage/file_util.h"

namespace ledger {

Node::Node(ClusterConfig config, NodeId self, const Genesis& genesis)
    : config_(std::move(config)),
      self_(self),
      peers_(loop_, config_, self_, faults_),
      data_dir_(data_dir_of(config_, self_)),
      meta_(data_dir_),
      wal_(data_dir_ + "/wal", fsync_mode_of(config_)),
      snaps_(data_dir_ + "/snap"),
      boot_snapshot_(load_snapshot()),
      // What the last run of this node promised: its term, its vote, its log.
      raft_(make_raft_config(), load_hard_state(), recover_log(), boot_snapshot_),
      ledger_(initial_ledger(genesis, boot_snapshot_)),
      service_(raft_, ledger_,
               [this](uint64_t cid, const pb::Envelope& m) { reply_to_client(cid, m); }) {
  check_genesis(genesis);
  if (boot_snapshot_) last_snapshot_index_ = boot_snapshot_->index;
  boot_snapshot_.reset();  // RaftCore and the ledger have their copies
  for (NodeId id : peers_.peer_ids()) health_[id] = PeerHealth{};
}

std::optional<raft::Snapshot> Node::load_snapshot() {
  size_t skipped = 0;
  auto f = snaps_.load_latest(&skipped);
  if (skipped > 0) log_warn("snapshot_damaged", {{"skipped", skipped}});
  if (!f) return std::nullopt;
  return raft::Snapshot{f->index, f->term, std::move(f->data)};
}

ledger::StateMachine Node::initial_ledger(const Genesis& genesis,
                                          const std::optional<raft::Snapshot>& snap) {
  if (!snap) return ledger::StateMachine(genesis);
  ledger::StateMachine sm = ledger::StateMachine::restore(snap->data);
  if (sm.chain_id() != genesis.chain_id || sm.last_applied() != snap->index) {
    throw std::runtime_error("snapshot at " + std::to_string(snap->index) +
                             " doesn't belong to this genesis/log");
  }
  return sm;
}

std::string Node::data_dir_of(const ClusterConfig& config, NodeId self) {
  const NodeConfig* me = config.find(self);
  if (me == nullptr) {
    throw std::runtime_error("node id " + std::to_string(self) + " not in cluster config");
  }
  storage::make_dirs(me->data_dir);
  return me->data_dir;
}

storage::FsyncMode Node::fsync_mode_of(const ClusterConfig& config) {
  storage::FsyncMode mode;
  if (!storage::parse_fsync_mode(config.raft.fsync, &mode)) {
    throw std::runtime_error("config: fsync must be entry, batch or none, not " +
                             config.raft.fsync);
  }
  return mode;
}

raft::HardState Node::load_hard_state() const {
  storage::Meta m = meta_.load();
  return raft::HardState{m.term, m.voted_for};
}

std::vector<pb::LogEntry> Node::recover_log() {
  storage::Wal::Recovery r = wal_.take_recovery();
  storage::Meta m = meta_.load();
  // The WAL must continue the snapshot (or genesis) without a hole, or the
  // entries in between are gone and this node can't rebuild its state.
  const LogIndex base = boot_snapshot_ ? boot_snapshot_->index : 0;
  if (!r.entries.empty() && r.entries.front().index() > base + 1) {
    throw std::runtime_error("WAL starts at " + std::to_string(r.entries.front().index()) +
                             " but the newest usable snapshot ends at " + std::to_string(base) +
                             "; wipe this node and let it catch up from the leader");
  }
  log_info("recovered", {{"term", m.term},
                         {"snapshot_index", base},
                         {"voted_for", m.voted_for},
                         {"entries", r.entries.size()},
                         {"first_index", r.entries.empty() ? 0 : r.entries.front().index()},
                         {"last_index", r.entries.empty() ? 0 : r.entries.back().index()},
                         {"truncated_bytes", r.truncated_bytes},
                         {"deleted_segments", r.deleted_segments},
                         {"fsync", config_.raft.fsync}});
  return std::move(r.entries);
}

// The log only makes sense on top of the genesis it was written against.
// Restarting with a different genesis.json would replay old transactions
// against other accounts and quietly produce a different ledger, so refuse.
// (The ledger's state hash before any entry is a fingerprint of the genesis.)
void Node::check_genesis(const Genesis& genesis) {
  const std::string path = data_dir_ + "/genesis_hash";
  const std::string mine = to_hex(ledger::StateMachine(genesis).state_hash());
  auto stored = storage::read_file(path);
  if (!stored) {
    storage::write_file_atomic(path, mine + "\n");
  } else if (*stored != mine + "\n") {
    throw std::runtime_error(data_dir_ + " was written with a different genesis; " +
                             "wipe it (tools/cluster.py wipe) or use the original genesis");
  }
}

raft::RaftConfig Node::make_raft_config() const {
  raft::RaftConfig rc;
  rc.id = self_;
  rc.peers = peers_.peer_ids();
  rc.election_timeout_min = static_cast<int>(config_.raft.election_ticks_min);
  rc.election_timeout_max = static_cast<int>(config_.raft.election_ticks_max);
  rc.heartbeat_interval = static_cast<int>(config_.raft.heartbeat_ticks);
  rc.max_entries_per_append = config_.raft.max_entries_per_append;
  // Real nodes want different timeouts on every run; only the simulator needs
  // reproducible seeds.
  rc.seed = (static_cast<uint64_t>(std::random_device{}()) << 32) ^ self_;
  return rc;
}

void Node::run() {
  // Turn SIGINT/SIGTERM into a readable fd so shutdown is handled inside the
  // event loop like everything else, instead of in an async signal handler.
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGINT);
  sigaddset(&mask, SIGTERM);
  sigprocmask(SIG_BLOCK, &mask, nullptr);
  int sfd = ::signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
  if (sfd < 0) throw std::runtime_error("signalfd failed");
  loop_.add_fd(
      sfd,
      [this, sfd] {
        signalfd_siginfo si{};
        if (::read(sfd, &si, sizeof si) == sizeof si) {
          log_info("shutdown", {{"signal", si.ssi_signo}});
        }
        loop_.stop();
      },
      nullptr);

  peers_.set_message_handler([this](NodeId from, const pb::Envelope& msg, net::Connection& c) {
    on_peer_message(from, msg, c);
  });
  peers_.start();

  const NodeConfig* me = config_.find(self_);
  client_listener_ = std::make_unique<net::Listener>(
      loop_, me->host, me->client_port,
      [this](int fd, std::string remote) { on_client_accept(fd, std::move(remote)); });

  loop_.add_periodic_timer(config_.raft.tick_ms, [this] { on_tick(); });
  // All the Raft work of one batch of events is persisted, sent and applied
  // together: one fsync per batch, however many messages arrived.
  loop_.set_after_batch([this] { process_ready(); });

  log_info("node_started", {{"peer_port", me->peer_port},
                            {"client_port", me->client_port},
                            {"tick_ms", config_.raft.tick_ms},
                            {"peers", peers_.peer_ids()}});
  loop_.run();

  loop_.remove_fd(sfd);
  ::close(sfd);
}

void Node::on_tick() {
  ++tick_count_;
  uint64_t now = now_ms();
  peers_.tick(now);
  if (tick_count_ % config_.raft.heartbeat_ticks == 0) send_pings();
  update_reachability(now);

  raft_.tick();
}

// The host half of the Ready contract: persist, then send, then apply.
// Persisting means durable: the vote and the entries are fsynced before any
// message that depends on them (a vote, an ack) can leave this node.
void Node::process_ready() {
  if (!raft_.has_ready()) {
    service_.after_ready();
    return;
  }
  raft::Ready rd = raft_.ready();

  // 0. A snapshot from the leader (we were too far behind for log entries).
  if (rd.snapshot) install_snapshot(*rd.snapshot);

  if (rd.hard_state) meta_.save(storage::Meta{rd.hard_state->term, rd.hard_state->voted_for});
  // New entries replace anything stored from their first index on.
  if (!rd.entries.empty()) wal_.write(rd.entries);
  wal_.sync();  // one fsync for this whole batch (a no-op if nothing was written)

  for (auto& m : rd.messages) {
    NodeId to = m.to();
    pb::Envelope env;
    *env.mutable_raft() = std::move(m);
    peers_.send(to, env);  // may drop (down, blocked); Raft copes with loss
  }

  for (const auto& e : rd.committed_entries) {
    apply(e);
    service_.on_applied(e);  // answers the client that submitted it, if any
  }
  if (config_.raft.snapshot_every > 0 &&
      ledger_.last_applied() >= last_snapshot_index_ + config_.raft.snapshot_every) {
    take_snapshot();
  }

  // Reads go last: the entries above bring the ledger up to (at least)
  // each read's index.
  for (const auto& rs : rd.reads_ready) service_.on_read(rs.request_id, &rs);
  for (uint64_t id : rd.reads_failed) service_.on_read(id, nullptr);
  // Leadership may have changed: submits this node can't finish get RETRY.
  service_.after_ready();

  if (raft_.role() != logged_role_ || raft_.term() != logged_term_ ||
      raft_.leader() != logged_leader_) {
    logged_role_ = raft_.role();
    logged_term_ = raft_.term();
    logged_leader_ = raft_.leader();
    log_info("raft_state", {{"role", raft::role_name(logged_role_)},
                            {"term", logged_term_},
                            {"leader", logged_leader_},
                            {"last_log_index", raft_.log().last_index()}});
  }
}

void Node::take_snapshot() {
  const LogIndex index = ledger_.last_applied();
  if (index <= last_snapshot_index_) return;
  raft_.compact(index, ledger_.snapshot());
  const raft::Snapshot& s = *raft_.snapshot();
  // Durable first; only then may the log entries it covers go.
  snaps_.save(storage::SnapshotFile{s.index, s.term, s.data});
  // Compact the WAL only up to the OLDER of the kept snapshots: if the newest
  // file turns out to be damaged, recovery falls back to the older one and
  // still needs the entries after it.
  auto kept = snaps_.list();
  if (kept.size() >= 2) wal_.compact_through(kept[1]);
  last_snapshot_index_ = index;
  log_info("snapshot_taken", {{"index", s.index},
                              {"term", s.term},
                              {"bytes", s.data.size()},
                              {"log_first_index", raft_.log().snapshot_index() + 1},
                              {"wal_first_index", wal_.first_index()}});
}

void Node::install_snapshot(const raft::Snapshot& s) {
  snaps_.save(storage::SnapshotFile{s.index, s.term, s.data});
  // The stored log is replaced: the entries Raft kept after the snapshot (if
  // any) arrive in this same Ready's entries and are written next.
  wal_.reset();
  ledger_ = ledger::StateMachine::restore(s.data);
  last_snapshot_index_ = s.index;
  log_info("snapshot_installed", {{"index", s.index}, {"term", s.term}, {"bytes", s.data.size()}});
}

void Node::apply(const pb::LogEntry& e) {
  auto r = ledger_.apply(e);
  if (r) {
    log_debug("apply", {{"index", e.index()},
                        {"tx_id", to_hex(r->tx_id)},
                        {"status", tx_status_name(r->status)}});
  }
}

// Replies can come long after the request (a submit waits for its commit), so
// the connection is looked up again: the client may have given up and gone.
void Node::reply_to_client(uint64_t conn_id, const pb::Envelope& msg) {
  auto it = clients_.find(conn_id);
  if (it == clients_.end()) return;
  it->second->send_frame(msg.SerializeAsString());
}

void Node::send_pings() {
  pb::Envelope msg;
  msg.mutable_ping()->set_seq(++ping_seq_);
  msg.mutable_ping()->set_sent_us(now_us());
  for (NodeId id : peers_.peer_ids()) peers_.send(id, msg);
}

void Node::update_reachability(uint64_t now) {
  for (auto& [id, h] : health_) {
    bool reachable = h.last_pong_ms != 0 && now - h.last_pong_ms < kUnreachableAfterMs;
    if (reachable == h.reachable) continue;
    h.reachable = reachable;
    if (reachable) {
      log_info("peer_reachable", {{"peer", id}, {"rtt_us", h.last_rtt_us}});
    } else {
      log_warn("peer_unreachable", {{"peer", id}, {"ms_since_pong", now - h.last_pong_ms}});
    }
  }
}

void Node::on_peer_message(NodeId from, const pb::Envelope& msg, net::Connection& conn) {
  switch (msg.msg_case()) {
    case pb::Envelope::kPing: {
      // Echo the sender's timestamp back on the connection the Ping came in on.
      pb::Envelope reply;
      reply.mutable_pong()->set_seq(msg.ping().seq());
      reply.mutable_pong()->set_sent_us(msg.ping().sent_us());
      peers_.reply(conn, reply);
      break;
    }
    case pb::Envelope::kPong: {
      PeerHealth& h = health_[from];
      h.last_pong_ms = now_ms();
      h.last_rtt_us = now_us() - msg.pong().sent_us();
      break;
    }
    case pb::Envelope::kRaft:
      // `from` is the peer authenticated by the Hello handshake; a message
      // claiming to be from someone else is a bug or a misrouted frame.
      if (msg.raft().from() != from || msg.raft().to() != self_) {
        log_warn("raft_misrouted", {{"peer", from}, {"claims_from", msg.raft().from()}});
        break;
      }
      raft_.step(msg.raft());  // persisted + answered at the end of the batch
      break;
    default:
      log_warn("unexpected_peer_message", {{"peer", from}, {"type", msg.msg_case()}});
  }
}

void Node::on_client_accept(int fd, std::string remote) {
  auto conn = net::Connection::from_accepted(loop_, fd, std::move(remote));
  const uint64_t cid = conn->id();
  conn->set_frame_handler(
      [this](net::Connection& c, std::string_view payload) { on_client_frame(c, payload); });
  conn->set_close_handler([this, cid](net::Connection&) { clients_.erase(cid); });
  clients_[cid] = conn;
  conn->start();
}

void Node::on_client_frame(net::Connection& conn, std::string_view payload) {
  pb::Envelope req;
  if (!req.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
    conn.close("unparseable frame");
    return;
  }
  if (service_.handle(conn.id(), req)) return;  // proposal/read goes out at the end of the batch
  std::string bytes;
  handle_admin(req).SerializeToString(&bytes);
  conn.send_frame(bytes);
}

pb::Envelope Node::handle_admin(const pb::Envelope& req) {
  pb::Envelope resp;
  auto ok = [&resp] { resp.mutable_admin_resp()->set_ok(true); };
  auto known_peer = [this](NodeId id) { return id != self_ && config_.find(id) != nullptr; };

  switch (req.msg_case()) {
    case pb::Envelope::kGetStatus:
      *resp.mutable_status_resp() = build_status();
      break;
    case pb::Envelope::kProposeReq: {
      auto* pr = resp.mutable_propose_resp();
      auto r = raft_.propose(req.propose_req().data());
      if (!r) {
        pr->set_error("not leader");
        pr->set_leader_hint(raft_.leader());
        break;
      }
      pr->set_accepted(true);
      pr->set_index(r->index);
      pr->set_term(r->term);
      break;
    }
    case pb::Envelope::kBlockPeer:
      if (!known_peer(req.block_peer().peer_id())) {
        resp.mutable_admin_resp()->set_error("unknown peer");
        break;
      }
      faults_.block(req.block_peer().peer_id());
      log_info("fault_block", {{"peer", req.block_peer().peer_id()}});
      ok();
      break;
    case pb::Envelope::kUnblockPeer:
      faults_.unblock(req.unblock_peer().peer_id());
      log_info("fault_unblock", {{"peer", req.unblock_peer().peer_id()}});
      ok();
      break;
    case pb::Envelope::kTakeSnapshot:
      take_snapshot();
      ok();
      break;
    case pb::Envelope::kUnblockAll:
      faults_.unblock_all();
      log_info("fault_unblock_all");
      ok();
      break;
    default:
      resp.mutable_admin_resp()->set_error("unsupported request");
  }
  return resp;
}

pb::StatusResp Node::build_status() const {
  pb::StatusResp s;
  s.set_node_id(self_);
  s.set_role(raft::role_name(raft_.role()));
  s.set_term(raft_.term());
  s.set_leader(raft_.leader());
  s.set_last_log_index(raft_.log().last_index());
  s.set_commit_index(raft_.commit_index());
  s.set_applied_index(ledger_.last_applied());
  s.set_state_hash(to_hex(ledger_.state_hash()));
  s.set_total_supply(ledger_.total_supply());
  s.set_accounts(static_cast<uint32_t>(ledger_.accounts().all().size()));
  s.set_snapshot_index(raft_.snapshot() ? raft_.snapshot()->index : 0);
  s.set_first_log_index(raft_.log().snapshot_index() + 1);
  uint64_t now = now_ms();
  for (const auto& [id, h] : health_) {
    pb::PeerStatus* p = s.add_peers();
    p->set_id(id);
    p->set_connected(peers_.is_connected(id));
    p->set_reachable(h.reachable);
    p->set_blocked(faults_.is_blocked(id));
    p->set_last_rtt_us(h.last_rtt_us);
    p->set_ms_since_pong(h.last_pong_ms == 0 ? -1 : static_cast<int64_t>(now - h.last_pong_ms));
  }
  return s;
}

}  // namespace ledger
