#include "raft/raft_core.h"

namespace ledger::raft {

const char* role_name(Role r) {
  switch (r) {
    case Role::kFollower:
      return "follower";
    case Role::kCandidate:
      return "candidate";
    case Role::kLeader:
      return "leader";
  }
  return "?";
}

RaftCore::RaftCore(RaftConfig config, HardState hard_state, std::vector<pb::LogEntry> log)
    : config_(std::move(config)),
      rng_(config_.seed),
      term_(hard_state.term),
      voted_for_(hard_state.voted_for),
      log_(std::move(log)),
      persisted_(hard_state) {
  reset_election_timer();
}

// ---------------------------------------------------------------- inputs

void RaftCore::tick() {
  if (role_ == Role::kLeader) {
    if (++heartbeat_elapsed_ >= config_.heartbeat_interval) {
      heartbeat_elapsed_ = 0;
      broadcast_heartbeat();
    }
    return;
  }
  // Follower or candidate: no word from a leader for a whole timeout, so start
  // an election. A candidate whose election split also lands here and retries
  // with a new term and a fresh random timeout.
  if (++election_elapsed_ >= election_timeout_) become_candidate();
}

void RaftCore::step(const pb::RaftMessage& msg) {
  // Rule for all servers (Figure 2): a higher term means our term is stale.
  // Step down before handling the message. An AppendEntries can only come
  // from the leader of that term, so we learn the leader too.
  if (msg.term() > term_) {
    NodeId leader = msg.has_append_entries() ? msg.from() : kNoNode;
    become_follower(msg.term(), leader);
  }

  switch (msg.body_case()) {
    case pb::RaftMessage::kRequestVote:
      handle_request_vote(msg);
      break;
    case pb::RaftMessage::kRequestVoteResp:
      handle_request_vote_resp(msg);
      break;
    case pb::RaftMessage::kAppendEntries:
      handle_append_entries(msg);
      break;
    case pb::RaftMessage::kAppendEntriesResp:
      // Week 3: update matchIndex/nextIndex. The term check above is all a
      // heartbeat response needs for now.
      break;
    case pb::RaftMessage::BODY_NOT_SET:
      break;
  }
}

// ---------------------------------------------------------------- roles

void RaftCore::become_follower(Term term, NodeId leader) {
  if (term > term_) {
    term_ = term;
    voted_for_ = kNoNode;  // a new term starts with a fresh vote
  }
  role_ = Role::kFollower;
  leader_ = leader;
  votes_.clear();
  reset_election_timer();
}

void RaftCore::become_candidate() {
  role_ = Role::kCandidate;
  ++term_;
  voted_for_ = config_.id;
  leader_ = kNoNode;
  votes_ = {config_.id};
  reset_election_timer();

  if (votes_.size() >= quorum()) {  // single-node cluster
    become_leader();
    return;
  }
  for (NodeId peer : config_.peers) {
    pb::RaftMessage m;
    auto* rv = m.mutable_request_vote();
    rv->set_last_log_index(log_.last_index());
    rv->set_last_log_term(log_.last_term());
    send(peer, std::move(m));
  }
}

void RaftCore::become_leader() {
  role_ = Role::kLeader;
  leader_ = config_.id;
  heartbeat_elapsed_ = 0;
  // A leader may only count replicas for entries of its own term when it
  // decides what is committed (§5.4.2). The no-op gives it one right away.
  append_entry(pb::ENTRY_NOOP);
  broadcast_heartbeat();
}

// ---------------------------------------------------------------- handlers

void RaftCore::handle_request_vote(const pb::RaftMessage& msg) {
  const auto& rv = msg.request_vote();
  // After the step-down in step(), msg.term() <= term_. A lower term is a
  // stale candidate: refuse, and our term in the reply tells it to step down.
  bool grant = msg.term() == term_ &&
               (voted_for_ == kNoNode || voted_for_ == msg.from()) &&
               log_.is_up_to_date(rv.last_log_index(), rv.last_log_term());
  if (grant) {
    voted_for_ = msg.from();
    // Granting a vote counts as hearing from a would-be leader; without the
    // reset we could time out and disrupt the election we just voted in.
    reset_election_timer();
  }

  pb::RaftMessage reply;
  reply.mutable_request_vote_resp()->set_granted(grant);
  send(msg.from(), std::move(reply));
}

void RaftCore::handle_request_vote_resp(const pb::RaftMessage& msg) {
  // Ignore late replies from an older election, or replies after we already
  // won or lost this one.
  if (role_ != Role::kCandidate || msg.term() != term_) return;
  if (!msg.request_vote_resp().granted()) return;

  votes_.insert(msg.from());
  if (votes_.size() >= quorum()) become_leader();
}

void RaftCore::handle_append_entries(const pb::RaftMessage& msg) {
  pb::RaftMessage reply;
  auto* resp = reply.mutable_append_entries_resp();

  if (msg.term() < term_) {
    // An old leader that hasn't heard about the new term. Our term in the
    // reply makes it step down.
    resp->set_success(false);
    send(msg.from(), std::move(reply));
    return;
  }

  // Same term: msg.from() is this term's leader (there is at most one). A
  // candidate that receives this lost the election.
  become_follower(msg.term(), msg.from());

  // Week 2: heartbeats only (prev_log_index = 0, no entries), which always
  // match. Week 3 adds the prevLogIndex/prevLogTerm check and appending.
  resp->set_success(true);
  send(msg.from(), std::move(reply));
}

// ---------------------------------------------------------------- helpers

void RaftCore::broadcast_heartbeat() {
  for (NodeId peer : config_.peers) {
    pb::RaftMessage m;
    m.mutable_append_entries();  // empty: prev_log_index 0, no entries
    send(peer, std::move(m));
  }
}

void RaftCore::append_entry(pb::EntryType type) {
  pb::LogEntry e;
  e.set_term(term_);
  e.set_index(log_.last_index() + 1);
  e.set_type(type);
  log_.append(e);
  pending_.entries.push_back(std::move(e));
}

void RaftCore::send(NodeId to, pb::RaftMessage msg) {
  msg.set_from(config_.id);
  msg.set_to(to);
  msg.set_term(term_);
  pending_.messages.push_back(std::move(msg));
}

void RaftCore::reset_election_timer() {
  election_elapsed_ = 0;
  // Randomized so that nodes rarely time out together. If they did, each
  // would vote for itself and nobody would get a majority (a split vote).
  std::uniform_int_distribution<int> dist(config_.election_timeout_min,
                                          config_.election_timeout_max);
  election_timeout_ = dist(rng_);
}

// ---------------------------------------------------------------- output

bool RaftCore::has_ready() const {
  return HardState{term_, voted_for_} != persisted_ || !pending_.entries.empty() ||
         !pending_.messages.empty();
}

Ready RaftCore::ready() {
  Ready rd = std::move(pending_);
  pending_ = Ready{};
  HardState hs{term_, voted_for_};
  if (hs != persisted_) {
    rd.hard_state = hs;
    persisted_ = hs;
  }
  return rd;
}

}  // namespace ledger::raft
