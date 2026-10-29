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

namespace ledger {

Node::Node(ClusterConfig config, NodeId self, const Genesis& genesis)
    : config_(std::move(config)),
      self_(self),
      peers_(loop_, config_, self_, faults_),
      // Nothing on disk yet (week 5 loads HardState + log here). Note raft_ is
      // declared before hard_state_/log_, so it must not read them here.
      raft_(make_raft_config(), raft::HardState{}, {}),
      ledger_(genesis) {
  if (config_.find(self_) == nullptr) {
    throw std::runtime_error("node id " + std::to_string(self_) + " not in cluster config");
  }
  for (NodeId id : peers_.peer_ids()) health_[id] = PeerHealth{};
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
  process_ready();
}

// The host half of the Ready contract: persist, then send, then apply.
// Persisting is in memory for now; week 5 makes it a write + fsync, and
// because the order is already right here nothing else has to change.
void Node::process_ready() {
  if (!raft_.has_ready()) return;
  raft::Ready rd = raft_.ready();

  if (rd.hard_state) hard_state_ = *rd.hard_state;
  if (!rd.entries.empty()) {
    // New entries replace anything stored from their first index on.
    log_.resize(std::min<size_t>(log_.size(), rd.entries.front().index() - 1));
    for (auto& e : rd.entries) log_.push_back(std::move(e));
  }

  for (auto& m : rd.messages) {
    NodeId to = m.to();
    pb::Envelope env;
    *env.mutable_raft() = std::move(m);
    peers_.send(to, env);  // may drop (down, blocked); Raft copes with loss
  }

  for (const auto& e : rd.committed_entries) apply(e);

  // Reads go last: the entries above bring the ledger up to (at least)
  // each read's index.
  for (const auto& rs : rd.reads_ready) finish_read(rs.request_id, &rs);
  for (uint64_t id : rd.reads_failed) finish_read(id, nullptr);

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

void Node::apply(const pb::LogEntry& e) {
  auto r = ledger_.apply(e);
  if (r) {
    log_debug("apply", {{"index", e.index()},
                        {"tx_id", to_hex(r->tx_id)},
                        {"status", tx_status_name(r->status)}});
  }
}

void Node::start_read(net::Connection& conn) {
  const uint64_t id = ++next_read_id_;
  if (!raft_.read_index(id)) {
    pb::Envelope resp;
    resp.mutable_read_resp()->set_error("not leader");
    resp.mutable_read_resp()->set_leader_hint(raft_.leader());
    conn.send_frame(resp.SerializeAsString());
    return;
  }
  pending_reads_[id] = conn.id();
  process_ready();  // sends the confirmation round now
}

void Node::finish_read(uint64_t read_id, const raft::ReadState* ok) {
  auto it = pending_reads_.find(read_id);
  if (it == pending_reads_.end()) return;
  auto conn = clients_.find(it->second);
  pending_reads_.erase(it);
  if (conn == clients_.end()) return;  // client gave up and disconnected

  pb::Envelope resp;
  auto* r = resp.mutable_read_resp();
  if (ok) {
    r->set_ok(true);
    r->set_read_index(ok->index);
    r->set_applied_index(ledger_.last_applied());
    r->set_state_hash(to_hex(ledger_.state_hash()));
    r->set_total_supply(ledger_.total_supply());
  } else {
    r->set_error("leadership lost, retry");
    r->set_leader_hint(raft_.leader());
  }
  conn->second->send_frame(resp.SerializeAsString());
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
      raft_.step(msg.raft());
      process_ready();
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
  if (req.has_read_req()) {
    start_read(conn);
    return;
  }
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
      process_ready();  // start replicating now, not on the next tick
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
