// ledger-admin: sends one admin command to a node's client port and prints
// the reply. Used by tools/cluster.py to inspect nodes and inject partitions.
// (Folded into ledger-cli once the client library exists.)
//
//   ledger-admin --node 1 status
//   ledger-admin --node 1 block 3
//   ledger-admin --node 1 unblock 3
//   ledger-admin --node 1 unblock-all
//   ledger-admin --node 1 propose <text>   # test hook: append to the Raft log
//   ledger-admin --node 1 read             # linearizable read (ReadIndex)
//
// A CLI that sends one request and exits has nothing else to do while it
// waits, so plain blocking sockets are fine here (unlike in the node).

#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "common/config.h"
#include "envelope.pb.h"
#include "net/framing.h"

namespace {

int usage() {
  std::cerr << "usage: ledger-admin [--config cluster.json] --node N "
               "status | block <peer> | unblock <peer> | unblock-all | propose <text> | read\n";
  return 2;
}

int connect_to(const std::string& host, uint16_t port) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0) return -1;
  int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (fd >= 0) {
    timeval tv{2, 0};  // don't hang forever on a stopped (SIGSTOP) node
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
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

bool recv_frame(int fd, std::string* payload) {
  std::string buf;
  char chunk[4096];
  while (true) {
    auto r = ledger::net::decode_frame(buf);
    if (r.status == ledger::net::DecodeStatus::kFrame) {
      *payload = std::string(r.payload);
      return true;
    }
    if (r.status == ledger::net::DecodeStatus::kTooLarge) return false;
    ssize_t n = ::recv(fd, chunk, sizeof chunk, 0);
    if (n <= 0) return false;
    buf.append(chunk, static_cast<size_t>(n));
  }
}

}  // namespace

int main(int argc, char** argv) {
  std::string config_path = "config/cluster.json";
  long node = 0;
  std::vector<std::string> args;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--config" && i + 1 < argc) config_path = argv[++i];
    else if (a == "--node" && i + 1 < argc) node = std::strtol(argv[++i], nullptr, 10);
    else args.push_back(a);
  }
  if (node <= 0 || args.empty()) return usage();

  ledger::pb::Envelope req;
  const std::string& cmd = args[0];
  if (cmd == "status" && args.size() == 1) {
    req.mutable_get_status();
  } else if (cmd == "block" && args.size() == 2) {
    req.mutable_block_peer()->set_peer_id(std::stoul(args[1]));
  } else if (cmd == "unblock" && args.size() == 2) {
    req.mutable_unblock_peer()->set_peer_id(std::stoul(args[1]));
  } else if (cmd == "unblock-all" && args.size() == 1) {
    req.mutable_unblock_all();
  } else if (cmd == "read" && args.size() == 1) {
    req.mutable_read_req();
  } else if (cmd == "propose" && args.size() == 2) {
    req.mutable_propose_req()->set_data(args[1]);
  } else {
    return usage();
  }

  ledger::ClusterConfig config;
  try {
    config = ledger::ClusterConfig::load_file(config_path);
  } catch (const std::exception& e) {
    std::cerr << e.what() << "\n";
    return 1;
  }
  const ledger::NodeConfig* target = config.find(static_cast<ledger::NodeId>(node));
  if (target == nullptr) {
    std::cerr << "node " << node << " not in config\n";
    return 1;
  }

  int fd = connect_to(target->host, target->client_port);
  if (fd < 0) {
    std::cerr << "cannot connect to node " << node << ": " << std::strerror(errno) << "\n";
    return 1;
  }
  std::string frame;
  ledger::net::append_frame(frame, req.SerializeAsString());
  std::string payload;
  bool ok = send_all(fd, frame) && recv_frame(fd, &payload);
  ::close(fd);
  ledger::pb::Envelope resp;
  if (!ok || !resp.ParseFromString(payload)) {
    std::cerr << "no valid reply from node " << node << "\n";
    return 1;
  }

  if (resp.has_status_resp()) {
    // JSON so scripts can parse it.
    const auto& s = resp.status_resp();
    nlohmann::json out = {{"node", s.node_id()},
                          {"role", s.role()},
                          {"term", s.term()},
                          {"leader", s.leader()},
                          {"last_log_index", s.last_log_index()},
                          {"commit_index", s.commit_index()},
                          {"applied_index", s.applied_index()},
                          {"applied_hash", s.applied_hash()},
                          {"peers", nlohmann::json::array()}};
    for (const auto& p : s.peers()) {
      out["peers"].push_back({{"id", p.id()},
                              {"connected", p.connected()},
                              {"reachable", p.reachable()},
                              {"blocked", p.blocked()},
                              {"rtt_us", p.last_rtt_us()},
                              {"ms_since_pong", p.ms_since_pong()}});
    }
    std::cout << out.dump() << "\n";
    return 0;
  }
  if (resp.has_read_resp()) {
    const auto& r = resp.read_resp();
    nlohmann::json out = {{"ok", r.ok()},
                          {"read_index", r.read_index()},
                          {"applied_index", r.applied_index()},
                          {"applied_hash", r.applied_hash()},
                          {"leader_hint", r.leader_hint()},
                          {"error", r.error()}};
    std::cout << out.dump() << "\n";
    return r.ok() ? 0 : 3;
  }
  if (resp.has_propose_resp()) {
    const auto& p = resp.propose_resp();
    nlohmann::json out = {{"accepted", p.accepted()}, {"index", p.index()}, {"term", p.term()},
                          {"leader_hint", p.leader_hint()}, {"error", p.error()}};
    std::cout << out.dump() << "\n";
    return p.accepted() ? 0 : 3;  // 3 = not leader (try leader_hint)
  }
  if (resp.has_admin_resp()) {
    if (resp.admin_resp().ok()) {
      std::cout << "ok\n";
      return 0;
    }
    std::cerr << "error: " << resp.admin_resp().error() << "\n";
    return 1;
  }
  std::cerr << "unexpected reply\n";
  return 1;
}
