#include "net/peer_manager.h"

#include <algorithm>
#include <string>

#include "common/clock.h"
#include "common/logger.h"

namespace ledger::net {

PeerManager::PeerManager(EventLoop& loop, const ClusterConfig& config, NodeId self,
                         FaultFilter& faults)
    : loop_(loop), config_(config), self_(self), faults_(faults),
      rng_(std::random_device{}() ^ self) {
  for (const auto& n : config_.nodes) {
    if (n.id == self_) continue;
    Outbound ob;
    ob.config = n;
    outbound_.emplace(n.id, std::move(ob));
  }
}

void PeerManager::start() {
  const NodeConfig* me = config_.find(self_);
  listener_ = std::make_unique<Listener>(
      loop_, me->host, me->peer_port,
      [this](int fd, std::string remote) { on_accept(fd, std::move(remote)); });
  log_info("peer_listening", {{"port", me->peer_port}});

  uint64_t now = now_ms();
  for (auto& [id, ob] : outbound_) dial(ob, now);
}

void PeerManager::tick(uint64_t now) {
  for (auto& [id, ob] : outbound_) {
    if (!ob.conn && now >= ob.next_attempt_ms) dial(ob, now);
  }

  // Drop inbound connections that never identified themselves, so a stray
  // client on the peer port can't hold a socket open forever. Collect first:
  // close() erases from inbound_.
  std::vector<std::shared_ptr<Connection>> stale;
  for (auto& [cid, in] : inbound_) {
    if (in.conn->peer_id() == kNoNode && now - in.accepted_ms > kHandshakeTimeoutMs) {
      stale.push_back(in.conn);
    }
  }
  for (auto& c : stale) c->close("handshake timeout");
}

void PeerManager::dial(Outbound& ob, uint64_t now) {
  const NodeId id = ob.config.id;
  std::string err;
  auto conn = Connection::dial(loop_, ob.config.host, ob.config.peer_port, &err);
  if (!conn) {
    log_debug("dial_failed", {{"peer", id}, {"error", err}});
    schedule_reconnect(ob, now);
    return;
  }
  conn->set_peer_id(id);

  // Handlers look the peer up by id and check the connection is still the
  // current one: a stale connection's late callback must not touch new state.
  conn->set_connected_handler([this, id](Connection& c) {
    Outbound& o = outbound_.at(id);
    if (o.conn.get() != &c) return;
    o.connected = true;
    o.backoff_ms = kMinBackoffMs;
    log_info("peer_connected", {{"peer", id}, {"failed_attempts", o.failed_attempts}});
    o.failed_attempts = 0;
  });
  conn->set_frame_handler(
      [this](Connection& c, std::string_view payload) { on_frame(c, payload, false); });
  conn->set_close_handler([this, id](Connection& c) {
    Outbound& o = outbound_.at(id);
    if (o.conn.get() != &c) return;
    if (o.connected) {
      log_info("peer_disconnected", {{"peer", id}, {"reason", c.close_reason()}});
    } else {
      log_debug("dial_failed", {{"peer", id}, {"error", c.close_reason()}});
    }
    o.connected = false;
    o.conn.reset();  // safe: close() holds its own reference while running
    schedule_reconnect(o, now_ms());
  });

  ob.conn = conn;
  conn->start();

  // Hello is queued now and goes out first, as soon as the connect completes.
  pb::Envelope hello;
  hello.mutable_hello()->set_node_id(self_);
  hello.mutable_hello()->set_cluster_id(config_.cluster_id);
  send_envelope(*conn, hello);
}

void PeerManager::schedule_reconnect(Outbound& ob, uint64_t now) {
  // Exponential backoff with jitter: 50ms, 100ms, 200ms, ... up to 1s, plus
  // up to 50% random extra so that, after a crash, nodes don't all hammer
  // the restarted node at the same instant.
  std::uniform_int_distribution<uint32_t> jitter(0, ob.backoff_ms / 2);
  ob.next_attempt_ms = now + ob.backoff_ms + jitter(rng_);
  ob.backoff_ms = std::min(ob.backoff_ms * 2, kMaxBackoffMs);
  ob.failed_attempts++;
}

void PeerManager::on_accept(int fd, std::string remote) {
  auto conn = Connection::from_accepted(loop_, fd, std::move(remote));
  const uint64_t cid = conn->id();
  conn->set_frame_handler(
      [this](Connection& c, std::string_view payload) { on_frame(c, payload, true); });
  conn->set_close_handler([this, cid](Connection& c) {
    if (c.peer_id() != kNoNode) {
      log_info("peer_inbound_closed", {{"peer", c.peer_id()}, {"reason", c.close_reason()}});
    }
    inbound_.erase(cid);
  });
  inbound_[cid] = Inbound{conn, now_ms()};
  conn->start();
}

void PeerManager::on_frame(Connection& conn, std::string_view payload, bool inbound) {
  pb::Envelope msg;
  if (!msg.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
    log_warn("bad_frame", {{"remote", conn.remote()}});
    conn.close("unparseable frame");
    return;
  }

  // The first frame on an inbound connection must be a valid Hello.
  if (inbound && conn.peer_id() == kNoNode) {
    handle_hello(conn, msg);
    return;
  }
  if (msg.has_hello()) {
    log_warn("unexpected_hello", {{"peer", conn.peer_id()}});
    return;
  }

  NodeId from = conn.peer_id();
  if (faults_.is_blocked(from)) return;  // partitioned: drop silently
  if (handler_) handler_(from, msg, conn);
}

bool PeerManager::handle_hello(Connection& conn, const pb::Envelope& msg) {
  if (!msg.has_hello()) {
    conn.close("first frame was not Hello");
    return false;
  }
  const pb::Hello& hello = msg.hello();
  if (hello.cluster_id() != config_.cluster_id) {
    log_warn("hello_rejected", {{"remote", conn.remote()},
                                {"reason", "cluster_id mismatch"},
                                {"their_cluster", hello.cluster_id()}});
    conn.close("cluster_id mismatch");
    return false;
  }
  if (hello.node_id() == self_ || config_.find(hello.node_id()) == nullptr) {
    log_warn("hello_rejected",
             {{"remote", conn.remote()}, {"reason", "unknown node"}, {"node", hello.node_id()}});
    conn.close("unknown node id");
    return false;
  }
  conn.set_peer_id(hello.node_id());
  log_info("peer_inbound", {{"peer", hello.node_id()}, {"remote", conn.remote()}});
  return true;
}

bool PeerManager::send(NodeId to, const pb::Envelope& msg) {
  auto it = outbound_.find(to);
  if (it == outbound_.end()) return false;
  if (faults_.is_blocked(to)) return false;  // partitioned: drop silently
  Outbound& ob = it->second;
  if (!ob.conn || !ob.connected) return false;
  return send_envelope(*ob.conn, msg);
}

bool PeerManager::reply(Connection& conn, const pb::Envelope& msg) {
  if (faults_.is_blocked(conn.peer_id())) return false;
  return send_envelope(conn, msg);
}

bool PeerManager::send_envelope(Connection& conn, const pb::Envelope& msg) {
  std::string bytes;
  msg.SerializeToString(&bytes);
  if (!conn.send_frame(bytes)) {
    log_debug("send_dropped", {{"peer", conn.peer_id()}, {"pending", conn.pending_write_bytes()}});
    return false;
  }
  return true;
}

bool PeerManager::is_connected(NodeId peer) const {
  auto it = outbound_.find(peer);
  return it != outbound_.end() && it->second.connected;
}

std::vector<NodeId> PeerManager::peer_ids() const {
  std::vector<NodeId> ids;
  for (const auto& [id, ob] : outbound_) ids.push_back(id);
  return ids;
}

}  // namespace ledger::net
