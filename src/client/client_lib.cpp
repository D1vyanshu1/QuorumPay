#include "client/client_lib.h"

#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <set>
#include <thread>

#include "common/clock.h"
#include "net/framing.h"

namespace ledger::client {

namespace {

void set_timeouts(int fd, uint64_t ms) {
  ms = std::max<uint64_t>(ms, 1);  // 0 would mean "no timeout"
  timeval tv{static_cast<time_t>(ms / 1000), static_cast<suseconds_t>((ms % 1000) * 1000)};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);  // also bounds connect()
}

int connect_to(const std::string& host, uint16_t port, uint64_t timeout_ms) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0) return -1;
  int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (fd >= 0) {
    set_timeouts(fd, timeout_ms);
    if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
      ::close(fd);
      fd = -1;
    }
  }
  ::freeaddrinfo(res);
  return fd;
}

bool send_all(int fd, const std::string& data) {
  size_t off = 0;
  while (off < data.size()) {
    ssize_t n = ::send(fd, data.data() + off, data.size() - off, MSG_NOSIGNAL);
    if (n <= 0) return false;
    off += static_cast<size_t>(n);
  }
  return true;
}

bool recv_frame(int fd, uint64_t deadline_ms, std::string* payload) {
  std::string buf;
  char chunk[4096];
  while (true) {
    auto r = net::decode_frame(buf);
    if (r.status == net::DecodeStatus::kFrame) {
      *payload = std::string(r.payload);
      return true;
    }
    if (r.status == net::DecodeStatus::kTooLarge) return false;
    const uint64_t now = now_ms();
    if (now >= deadline_ms) return false;
    set_timeouts(fd, deadline_ms - now);
    ssize_t n = ::recv(fd, chunk, sizeof chunk, 0);
    if (n <= 0) return false;  // closed, error, or timed out
    buf.append(chunk, static_cast<size_t>(n));
  }
}

struct Route {
  pb::RespCode code = pb::RESP_OK;
  NodeId hint = kNoNode;
};

// Whether a reply is final, and if not, where to go next.
Route route_of(const pb::Envelope& m) {
  switch (m.msg_case()) {
    case pb::Envelope::kSubmitTxResp:
      return {m.submit_tx_resp().code(), m.submit_tx_resp().leader_hint()};
    case pb::Envelope::kAccountResp:
      return {m.account_resp().code(), m.account_resp().leader_hint()};
    case pb::Envelope::kHistoryResp:
      return {m.history_resp().code(), m.history_resp().leader_hint()};
    case pb::Envelope::kTxResp:
      return {m.tx_resp().code(), m.tx_resp().leader_hint()};
    default:
      return {};
  }
}

void sleep_ms(uint64_t ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

}  // namespace

std::optional<pb::Envelope> call_node(const NodeConfig& node, const pb::Envelope& req,
                                      uint64_t timeout_ms) {
  const uint64_t deadline = now_ms() + timeout_ms;
  int fd = connect_to(node.host, node.client_port, timeout_ms);
  if (fd < 0) return std::nullopt;
  std::string frame;
  net::append_frame(frame, req.SerializeAsString());
  std::string payload;
  bool ok = send_all(fd, frame) && recv_frame(fd, deadline, &payload);
  ::close(fd);
  pb::Envelope resp;
  if (!ok || !resp.ParseFromString(payload)) return std::nullopt;
  return resp;
}

ClusterClient::ClusterClient(ClusterConfig config, NodeId first, uint64_t timeout_ms)
    : config_(std::move(config)), node_(first), timeout_ms_(timeout_ms) {
  if (config_.nodes.empty()) throw std::runtime_error("cluster config has no nodes");
  if (node_ == kNoNode || config_.find(node_) == nullptr) node_ = config_.nodes.front().id;
}

std::optional<pb::Envelope> ClusterClient::call_on(NodeId node, const pb::Envelope& req) {
  const NodeConfig* nc = config_.find(node);
  if (nc == nullptr) return std::nullopt;
  return call_node(*nc, req, std::min(timeout_ms_, kAttemptTimeoutMs));
}

pb::Envelope ClusterClient::call(const pb::Envelope& req) {
  const uint64_t deadline = now_ms() + timeout_ms_;
  NodeId node = node_;
  uint64_t backoff = 20;
  size_t hops = 0;  // hint-following in a row without waiting
  std::string last = "no reply";
  // Nodes that were down or silent during this call. A follower cut off
  // along with an old leader keeps naming that leader, which will never
  // answer; following the hint back to it would loop until the timeout.
  std::set<NodeId> silent;
  auto next_live = [&](NodeId from) {
    if (silent.size() >= config_.nodes.size()) silent.clear();  // all tried: start over
    NodeId n = next_after(from);
    while (silent.count(n)) n = next_after(n);
    return n;
  };

  while (true) {
    const uint64_t now = now_ms();
    if (now >= deadline) {
      throw TimeoutError("no answer within " + std::to_string(timeout_ms_) + "ms (last: " + last +
                         ")");
    }
    const NodeConfig* nc = config_.find(node);
    auto resp = call_node(*nc, req, std::min(kAttemptTimeoutMs, deadline - now));
    if (resp) {
      Route r = route_of(*resp);
      if (r.code != pb::RESP_NOT_LEADER && r.code != pb::RESP_RETRY) {
        node_ = node;
        return *resp;
      }
      last = "node " + std::to_string(node) +
             (r.code == pb::RESP_RETRY ? ": retry" : ": not leader");
      // Follow a hint at once, but not forever: two nodes with stale views
      // could point at each other.
      if (r.hint != kNoNode && r.hint != node && config_.find(r.hint) && !silent.count(r.hint) &&
          hops < config_.nodes.size()) {
        node = r.hint;
        ++hops;
        continue;
      }
      // NOT_LEADER with no usable hint: an election is running, and any node
      // may win it. RETRY: ask the same node again once things settle.
      if (r.code == pb::RESP_NOT_LEADER) node = next_live(node);
    } else {
      last = "node " + std::to_string(node) + " unreachable or silent";
      silent.insert(node);
      node = next_live(node);
    }
    hops = 0;
    sleep_ms(std::min(backoff, deadline - std::min(deadline, now_ms())));
    backoff = std::min<uint64_t>(backoff * 2, 200);
  }
}

NodeId ClusterClient::next_after(NodeId id) const {
  const auto& nodes = config_.nodes;
  for (size_t i = 0; i < nodes.size(); ++i) {
    if (nodes[i].id == id) return nodes[(i + 1) % nodes.size()].id;
  }
  return nodes.front().id;
}

}  // namespace ledger::client
