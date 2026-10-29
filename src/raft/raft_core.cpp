#include "raft/raft_core.h"

#include <algorithm>
#include <cassert>
#include <functional>

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
      broadcast_append();
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
      handle_append_entries_resp(msg);
      break;
    case pb::RaftMessage::BODY_NOT_SET:
      break;
  }
}

std::optional<ProposeResult> RaftCore::propose(std::string data) {
  if (role_ != Role::kLeader) return std::nullopt;
  append_entry(pb::ENTRY_NORMAL, std::move(data));
  ProposeResult r{log_.last_index(), term_};
  maybe_commit();  // a single-node cluster commits right away
  for (NodeId peer : config_.peers) send_append(peer, /*force=*/false);
  return r;
}

bool RaftCore::read_index(uint64_t request_id) {
  if (role_ != Role::kLeader) return false;
  if (committed_in_current_term()) {
    start_reads({request_id});
  } else {
    reads_waiting_.push_back(request_id);  // started by maybe_commit()
  }
  return true;
}

void RaftCore::start_reads(const std::vector<uint64_t>& request_ids) {
  for (uint64_t id : request_ids) reads_.add(id, commit_index_, config_.id);
  release_confirmed_reads();  // a single-node cluster needs no round
  // Confirm leadership now rather than waiting up to a heartbeat interval.
  if (!reads_.empty()) broadcast_append();
}

void RaftCore::release_confirmed_reads() {
  for (const auto& rs : reads_.pop_confirmed(quorum())) pending_.reads_ready.push_back(rs);
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
  progress_.clear();
  // Unconfirmed reads can't be served by a non-leader: fail them so the
  // client retries on the new leader.
  for (uint64_t id : reads_.take_all()) pending_.reads_failed.push_back(id);
  for (uint64_t id : reads_waiting_) pending_.reads_failed.push_back(id);
  reads_waiting_.clear();
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
  // Optimistic start: assume every follower already has our whole log. The
  // first AppendEntries checks that, and rejections walk `next` back.
  progress_.clear();
  for (NodeId peer : config_.peers) progress_[peer] = Progress{0, log_.last_index() + 1, false};

  // A leader may only commit by counting replicas for an entry of its own
  // term (see maybe_commit). The no-op gives it one right away, which also
  // commits everything before it that earlier leaders left uncommitted.
  append_entry(pb::ENTRY_NOOP);
  maybe_commit();
  broadcast_append();
}

// ---------------------------------------------------------------- elections

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

// ---------------------------------------------------------------- replication

void RaftCore::handle_append_entries(const pb::RaftMessage& msg) {
  pb::RaftMessage reply;
  auto* resp = reply.mutable_append_entries_resp();
  resp->set_read_ctx(msg.append_entries().read_ctx());

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

  const auto& ae = msg.append_entries();
  const LogIndex prev = ae.prev_log_index();

  // Consistency check: we must hold the entry just before the new ones, with
  // the same term. By induction (Log Matching), everything before it then
  // matches the leader too.
  if (prev > log_.last_index()) {
    // Our log is too short. Ask for everything after our last entry.
    resp->set_success(false);
    resp->set_conflict_index(log_.last_index() + 1);
    send(msg.from(), std::move(reply));
    return;
  }
  if (log_.term_at(prev) != ae.prev_log_term()) {
    // Same index, different term. Point the leader at the start of our
    // conflicting term so it can skip all of it in one round trip.
    resp->set_success(false);
    resp->set_conflict_term(log_.term_at(prev));
    resp->set_conflict_index(log_.first_index_of_term_at(prev));
    send(msg.from(), std::move(reply));
    return;
  }

  for (const auto& e : ae.entries()) {
    if (e.index() <= log_.last_index()) {
      // Already have this exact entry (e.g. a duplicated or re-sent message).
      // Skipping instead of truncating matters: a stale, shorter message
      // must never cut off entries we got from a newer one.
      if (log_.term_at(e.index()) == e.term()) continue;
      // A conflict: our entry came from a leader that was deposed before it
      // committed. Committed entries can never conflict.
      assert(e.index() > commit_index_ && "would truncate a committed entry");
      log_.truncate_from(e.index());
    }
    log_.append(e);
    queue_persist(e);
  }

  // "Index of last new entry", not our last index: anything we hold past the
  // leader's entries hasn't been checked against the leader's log yet.
  const LogIndex last_new = prev + static_cast<LogIndex>(ae.entries_size());
  if (ae.leader_commit() > commit_index_) {
    commit_index_ = std::max(commit_index_, std::min(ae.leader_commit(), last_new));
  }

  resp->set_success(true);
  resp->set_match_index(last_new);
  send(msg.from(), std::move(reply));
}

void RaftCore::handle_append_entries_resp(const pb::RaftMessage& msg) {
  if (role_ != Role::kLeader || msg.term() != term_) return;
  auto it = progress_.find(msg.from());
  if (it == progress_.end()) return;
  Progress& pr = it->second;
  const auto& resp = msg.append_entries_resp();
  pr.in_flight = false;

  // Any reply in our term, success or not, means this follower still
  // accepted us as leader when it answered.
  reads_.ack(msg.from(), resp.read_ctx());
  release_confirmed_reads();

  if (resp.success()) {
    // Responses can arrive late or twice; match only ever moves forward.
    if (resp.match_index() > pr.match) {
      pr.match = resp.match_index();
      maybe_commit();
    }
    pr.next = std::max(pr.next, pr.match + 1);
    if (pr.next <= log_.last_index()) send_append(msg.from(), /*force=*/false);
    return;
  }

  // Rejected: move `next` back using the follower's hint, then retry.
  LogIndex next = resp.conflict_index();
  if (resp.conflict_term() != 0) {
    // If we also have entries of the conflicting term, the follower's log
    // probably agrees with ours up to our last entry of that term.
    if (auto last = log_.last_index_of_term(resp.conflict_term())) next = *last + 1;
  }
  next = std::max<LogIndex>(std::min(next, log_.last_index() + 1), 1);
  if (next <= pr.match) {
    // The follower is missing entries it once acknowledged. With durable
    // storage this only happens for a stale rejection that arrived out of
    // order; with the in-memory storage we have until week 5, it also
    // happens when a follower restarts empty. Either way, trust the hint and
    // start over from there. This is safe: lowering match can't un-commit
    // anything, because commit_index never moves back.
    pr.match = next - 1;
  }
  pr.next = next;
  send_append(msg.from(), /*force=*/false);
}

void RaftCore::send_append(NodeId peer, bool force) {
  Progress& pr = progress_.at(peer);
  if (pr.in_flight && !force) return;

  const LogIndex prev = pr.next - 1;
  pb::RaftMessage m;
  auto* ae = m.mutable_append_entries();
  ae->set_prev_log_index(prev);
  ae->set_prev_log_term(log_.term_at(prev));
  for (auto& e : log_.slice(pr.next, config_.max_entries_per_append)) {
    *ae->add_entries() = std::move(e);
  }
  ae->set_leader_commit(commit_index_);
  ae->set_read_ctx(reads_.last_ctx());
  pr.in_flight = ae->entries_size() > 0;
  send(peer, std::move(m));
}

// Heartbeat: every follower gets an AppendEntries, with entries if it is
// behind. This doubles as the retransmit for anything lost.
void RaftCore::broadcast_append() {
  for (NodeId peer : config_.peers) send_append(peer, /*force=*/true);
}

void RaftCore::maybe_commit() {
  // The highest index stored on a majority: sort everyone's match index
  // (ours is our last index) descending and take the quorum-th one.
  std::vector<LogIndex> matches{log_.last_index()};
  for (const auto& [_, pr] : progress_) matches.push_back(pr.match);
  std::sort(matches.begin(), matches.end(), std::greater<>());
  const LogIndex n = matches[quorum() - 1];

  // Only an entry from the current term may be committed by counting
  // (§5.4.2, Figure 8). An older-term entry on a majority can still be
  // overwritten by a future leader that never had it. Once a current-term
  // entry commits, everything before it is committed too.
  if (n > commit_index_ && log_.term_at(n) == term_) commit_index_ = n;

  if (!reads_waiting_.empty() && committed_in_current_term()) {
    std::vector<uint64_t> ids;
    ids.swap(reads_waiting_);
    start_reads(ids);
  }
}

// ---------------------------------------------------------------- helpers

void RaftCore::append_entry(pb::EntryType type, std::string data) {
  pb::LogEntry e;
  e.set_term(term_);
  e.set_index(log_.last_index() + 1);
  e.set_type(type);
  e.set_data(std::move(data));
  log_.append(e);
  queue_persist(e);
}

void RaftCore::queue_persist(const pb::LogEntry& e) {
  auto& q = pending_.entries;
  while (!q.empty() && q.back().index() >= e.index()) q.pop_back();
  q.push_back(e);
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
         !pending_.messages.empty() || commit_index_ > applied_ ||
         !pending_.reads_ready.empty() || !pending_.reads_failed.empty();
}

Ready RaftCore::ready() {
  Ready rd = std::move(pending_);
  pending_ = Ready{};
  HardState hs{term_, voted_for_};
  if (hs != persisted_) {
    rd.hard_state = hs;
    persisted_ = hs;
  }
  for (LogIndex i = applied_ + 1; i <= commit_index_; ++i) rd.committed_entries.push_back(log_.at(i));
  applied_ = commit_index_;
  return rd;
}

}  // namespace ledger::raft
