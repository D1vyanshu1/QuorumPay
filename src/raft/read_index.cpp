#include "raft/read_index.h"

namespace ledger::raft {

uint64_t ReadIndexTracker::add(uint64_t request_id, LogIndex index, NodeId self) {
  pending_.push_back(Pending{request_id, index, ++last_ctx_, {self}});
  return last_ctx_;
}

void ReadIndexTracker::ack(NodeId from, uint64_t ctx) {
  for (auto& p : pending_) {
    if (p.ctx > ctx) break;  // registered after that message was sent
    p.acks.insert(from);
  }
}

std::vector<ReadState> ReadIndexTracker::pop_confirmed(size_t quorum) {
  std::vector<ReadState> out;
  while (!pending_.empty() && pending_.front().acks.size() >= quorum) {
    out.push_back(ReadState{pending_.front().request_id, pending_.front().index});
    pending_.pop_front();
  }
  return out;
}

std::vector<uint64_t> ReadIndexTracker::take_all() {
  std::vector<uint64_t> ids;
  for (const auto& p : pending_) ids.push_back(p.request_id);
  pending_.clear();
  return ids;
}

}  // namespace ledger::raft
