#include "raft/raft_core.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <functional>

namespace ledger::raft {

const char* role_name(Role r) {
  switch (r) {
    case Role::kFollower:
      return "follower";
    case Role::kPreCandidate:
      return "pre-candidate";
    case Role::kCandidate:
      return "candidate";
    case Role::kLeader:
      return "leader";
  }
  return "?";
}

RaftCore::RaftCore(RaftConfig config, HardState hard_state, std::vector<pb::LogEntry> log,
                   std::optional<Snapshot> snapshot)
    : config_(std::move(config)),
      rng_(config_.seed),
      term_(hard_state.term),
      voted_for_(hard_state.voted_for),
      log_(initial_log(snapshot, std::move(log))),
      snapshot_(std::move(snapshot)),
      persisted_(hard_state) {
  if (snapshot_) {
    // A snapshot only ever holds committed, applied state.
    commit_index_ = snapshot_->index;
    applied_ = snapshot_->index;
  }
  reset_election_timer();
}

RaftLog RaftCore::initial_log(const std::optional<Snapshot>& snap, std::vector<pb::LogEntry> log) {
  if (!snap) return RaftLog(std::move(log));
  // Keep only the entries that continue right after the snapshot.
  std::vector<pb::LogEntry> after;
  for (auto& e : log) {
    if (e.index() <= snap->index) continue;
    if (e.index() != snap->index + after.size() + 1) break;
    after.push_back(std::move(e));
  }
  return RaftLog(snap->index, snap->term, std::move(after));
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
  // No word from a leader for a whole timeout, so start an election (with
  // pre-vote: first ask whether we could win one). A candidate whose election
  // split also lands here and retries with a fresh random timeout.
  if (++election_elapsed_ >= election_timeout_) {
    if (config_.pre_vote) {
      become_pre_candidate();
    } else {
      become_candidate();
    }
  }
}

void RaftCore::step(const pb::RaftMessage& msg) {
  // Rule for all servers (Figure 2): a higher term means our term is stale.
  // Step down before handling the message. An AppendEntries can only come
  // from the leader of that term, so we learn the leader too.
  //
  // Except pre-vote traffic: a pre-vote request carries the term its sender
  // WOULD use, and a granted answer echoes it. Neither means a newer term
  // exists, so stepping down for them would cause exactly the disruption
  // pre-vote exists to prevent. (A refusal carries the refuser's real term.)
  const bool pre_vote_term =
      (msg.has_request_vote() && msg.request_vote().pre_vote()) ||
      (msg.has_request_vote_resp() && msg.request_vote_resp().pre_vote() &&
       msg.request_vote_resp().granted());
  if (msg.term() > term_ && !pre_vote_term) {
    NodeId leader = msg.has_append_entries() || msg.has_install_snapshot() ? msg.from() : kNoNode;
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
    case pb::RaftMessage::kInstallSnapshot:
      handle_install_snapshot(msg);
      break;
    case pb::RaftMessage::kInstallSnapshotResp:
      handle_install_snapshot_resp(msg);
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

void RaftCore::become_pre_candidate() {
  // Nothing durable changes: no new term, no vote. If nobody answers (we're
  // cut off), we just time out and ask again, at the same term.
  role_ = Role::kPreCandidate;
  leader_ = kNoNode;
  votes_ = {config_.id};
  reset_election_timer();

  if (votes_.size() >= quorum()) {  // single-node cluster
    become_candidate();
    return;
  }
  for (NodeId peer : config_.peers) {
    pb::RaftMessage m;
    auto* rv = m.mutable_request_vote();
    rv->set_last_log_index(log_.last_index());
    rv->set_last_log_term(log_.last_term());
    rv->set_pre_vote(true);
    send(peer, std::move(m), term_ + 1);
  }
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
  if (rv.pre_vote()) {
    // "Would you vote for me in term msg.term()?" Yes only if that term is
    // newer than ours, its log is up to date, and we haven't heard from a
    // leader for a minimum election timeout. The last condition is the point:
    // while a leader is alive and reaching us, nobody can start an election.
    // Answering changes nothing: no term, no vote, no timer reset.
    const bool leader_alive =
        role_ == Role::kLeader ||
        (leader_ != kNoNode && election_elapsed_ < config_.election_timeout_min);
    const bool grant = msg.term() > term_ && !leader_alive &&
                       log_.is_up_to_date(rv.last_log_index(), rv.last_log_term());
    pb::RaftMessage reply;
    reply.mutable_request_vote_resp()->set_granted(grant);
    reply.mutable_request_vote_resp()->set_pre_vote(true);
    // A yes echoes the proposed term; a no carries ours, which makes a
    // pre-candidate behind on terms catch up.
    send(msg.from(), std::move(reply), grant ? msg.term() : 0);
    return;
  }
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
  if (msg.request_vote_resp().pre_vote()) {
    // Yes votes echo the term we proposed (ours + 1). A no with a higher
    // term already made us a follower in step().
    if (role_ != Role::kPreCandidate || msg.term() != term_ + 1) return;
    if (!msg.request_vote_resp().granted()) return;
    votes_.insert(msg.from());
    if (votes_.size() >= quorum()) become_candidate();  // now a real election
    return;
  }
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

  if (prev < log_.snapshot_index()) {
    // Entries our snapshot already covers. Those are committed, so they
    // match the leader's; say how far we agree and let it send the rest.
    resp->set_success(true);
    resp->set_match_index(log_.snapshot_index());
    send(msg.from(), std::move(reply));
    return;
  }

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
  if (pr.next <= log_.snapshot_index()) {
    // The entry before `next` was compacted away: we can't prove the logs
    // match there, and we no longer have the entries. Send the snapshot.
    send_snapshot(peer, force);
    return;
  }
  pr.snapshotting = false;
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

void RaftCore::send_snapshot(NodeId peer, bool force) {
  Progress& pr = progress_.at(peer);
  assert(snapshot_ && "log compacted without a snapshot");
  if (!pr.snapshotting || pr.snap_index != snapshot_->index) {
    // Start (or restart with a newer snapshot) from the beginning.
    pr.snapshotting = true;
    pr.snap_index = snapshot_->index;
    pr.snap_offset = 0;
  } else if (pr.in_flight && !force) {
    return;  // one chunk in flight at a time; the heartbeat resends a lost one
  }
  const std::string& data = snapshot_->data;
  const uint64_t off = std::min<uint64_t>(pr.snap_offset, data.size());
  const size_t n = std::min<uint64_t>(config_.snapshot_chunk_bytes, data.size() - off);

  pb::RaftMessage m;
  auto* is = m.mutable_install_snapshot();
  is->set_last_index(snapshot_->index);
  is->set_last_term(snapshot_->term);
  is->set_offset(off);
  is->set_data(data.substr(off, n));
  is->set_done(off + n == data.size());
  pr.in_flight = true;
  send(peer, std::move(m));
}

void RaftCore::handle_install_snapshot(const pb::RaftMessage& msg) {
  const auto& is = msg.install_snapshot();
  pb::RaftMessage reply;
  auto* resp = reply.mutable_install_snapshot_resp();
  resp->set_last_index(is.last_index());

  if (msg.term() < term_) {  // a deposed leader; our term makes it step down
    send(msg.from(), std::move(reply));
    return;
  }
  become_follower(msg.term(), msg.from());

  if (is.last_index() <= commit_index_) {
    // We already have everything it covers (committed entries match).
    incoming_.reset();
    resp->set_done(true);
    send(msg.from(), std::move(reply));
    return;
  }
  if (is.offset() == 0) {
    incoming_ = Snapshot{is.last_index(), is.last_term(), ""};
  } else if (!incoming_ || incoming_->index != is.last_index() ||
             incoming_->data.size() != is.offset()) {
    // A chunk that doesn't continue what we have (an earlier one was lost,
    // or this is another snapshot): tell the leader where to resume.
    bool same = incoming_ && incoming_->index == is.last_index();
    resp->set_bytes_received(same ? incoming_->data.size() : 0);
    send(msg.from(), std::move(reply));
    return;
  }
  incoming_->data += is.data();
  resp->set_bytes_received(incoming_->data.size());
  if (is.done()) {
    install(std::move(*incoming_));
    incoming_.reset();
    resp->set_done(true);
  }
  send(msg.from(), std::move(reply));
}

void RaftCore::install(Snapshot s) {
  // §7: if our log has the snapshot's last entry (same index and term), the
  // entries after it may still be good: keep them. Otherwise our log is
  // stale or conflicting, and the snapshot replaces all of it.
  std::vector<pb::LogEntry> keep;
  if (s.index <= log_.last_index() && s.index >= log_.snapshot_index() &&
      log_.term_at(s.index) == s.term) {
    keep = log_.slice(s.index + 1, SIZE_MAX);
  }
  log_ = RaftLog(s.index, s.term, keep);
  commit_index_ = std::max(commit_index_, s.index);
  applied_ = s.index;  // the host applies it by installing the snapshot
  // The host replaces its stored log with the snapshot + these entries.
  pending_.entries = std::move(keep);
  pending_.snapshot = s;
  snapshot_ = std::move(s);
}

void RaftCore::handle_install_snapshot_resp(const pb::RaftMessage& msg) {
  if (role_ != Role::kLeader || msg.term() != term_) return;
  auto it = progress_.find(msg.from());
  if (it == progress_.end()) return;
  Progress& pr = it->second;
  const auto& resp = msg.install_snapshot_resp();
  pr.in_flight = false;

  if (resp.done()) {
    pr.snapshotting = false;
    if (resp.last_index() > pr.match) {
      pr.match = resp.last_index();
      maybe_commit();
    }
    pr.next = std::max(pr.next, pr.match + 1);
    if (pr.next <= log_.last_index()) send_append(msg.from(), /*force=*/false);
    return;
  }
  if (!pr.snapshotting || resp.last_index() != pr.snap_index) return;  // about an older one
  pr.snap_offset = resp.bytes_received();
  send_snapshot(msg.from(), /*force=*/false);
}

void RaftCore::compact(LogIndex index, std::string data) {
  assert(index <= applied_ && "can't snapshot entries the host hasn't applied");
  if (index == 0 || (snapshot_ && index <= snapshot_->index)) return;
  snapshot_ = Snapshot{index, log_.term_at(index), std::move(data)};
  const LogIndex through = index > config_.compaction_margin ? index - config_.compaction_margin : 0;
  if (through > log_.snapshot_index()) log_.compact_through(through);
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

void RaftCore::send(NodeId to, pb::RaftMessage msg, Term term) {
  msg.set_from(config_.id);
  msg.set_to(to);
  msg.set_term(term != 0 ? term : term_);
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
  return HardState{term_, voted_for_} != persisted_ || pending_.snapshot ||
         !pending_.entries.empty() ||
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
