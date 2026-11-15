#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>

#include "common/config.h"
#include "common/types.h"
#include "envelope.pb.h"

namespace ledger::client {

// Sends one request to one node and waits for one reply frame. Returns nullopt
// if the node is down, closes the connection, or stays silent for timeout_ms
// (a leader cut off from the majority never answers a linearizable request).
//
// A CLI that sends a request and exits has nothing else to do while it waits,
// so plain blocking sockets with timeouts are fine here (unlike in the node).
std::optional<pb::Envelope> call_node(const NodeConfig& node, const pb::Envelope& req,
                                      uint64_t timeout_ms);

struct TimeoutError : std::runtime_error {
  using std::runtime_error::runtime_error;
};

// Gets a request to whichever node can answer it (§6.7 "leader redirect"):
//
//   reply NOT_LEADER, hint h  -> go to node h at once
//   reply NOT_LEADER, no hint -> election running: wait a little, next node
//   reply RETRY               -> leadership changed mid-request: wait, resend
//   down / silent             -> next node in config order
//
// It resends the SAME request every time. For a SubmitTx that is what makes
// retrying safe: the same signed bytes can take effect at most once, and a
// resend of one that already did gets its original result (see ClientService).
class ClusterClient {
 public:
  // Per attempt: long enough for a commit, short enough to move on from a
  // node that will never answer.
  static constexpr uint64_t kAttemptTimeoutMs = 1500;

  // `first`: node to try first (kNoNode = the first node in the config).
  ClusterClient(ClusterConfig config, NodeId first, uint64_t timeout_ms);

  // Throws TimeoutError if no node gives a final answer within the timeout.
  pb::Envelope call(const pb::Envelope& req);

  // One attempt on one node, no redirects (stale reads, status).
  std::optional<pb::Envelope> call_on(NodeId node, const pb::Envelope& req);

  const ClusterConfig& config() const { return config_; }

 private:
  NodeId next_after(NodeId id) const;

  ClusterConfig config_;
  NodeId node_;  // where to start: the last node that answered
  uint64_t timeout_ms_;
};

}  // namespace ledger::client
