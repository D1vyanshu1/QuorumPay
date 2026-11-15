#include "server/client_service.h"

#include <utility>

#include "common/hex.h"
#include "common/logger.h"
#include "ledger/transaction.h"

namespace ledger::server {

ClientService::ClientService(raft::RaftCore& raft, const ledger::StateMachine& ledger, Reply reply)
    : raft_(raft), ledger_(ledger), reply_(std::move(reply)) {}

bool ClientService::handle(uint64_t conn_id, const pb::Envelope& req) {
  switch (req.msg_case()) {
    case pb::Envelope::kSubmitTx:
      submit(conn_id, req.submit_tx());
      return true;
    case pb::Envelope::kGetAccount:
      read(conn_id, req, req.get_account().stale());
      return true;
    case pb::Envelope::kGetHistory:
      read(conn_id, req, req.get_history().stale());
      return true;
    case pb::Envelope::kGetTx:
      read(conn_id, req, req.get_tx().stale());
      return true;
    case pb::Envelope::kReadReq:
      read(conn_id, req, /*stale=*/false);
      return true;
    default:
      return false;
  }
}

// ---------------------------------------------------------------- submits

void ClientService::submit(uint64_t conn_id, const pb::SubmitTx& req) {
  const ledger::Transaction t = ledger::Transaction::from_proto(req.tx());
  const std::string tx_id = t.tx_id();

  if (raft_.role() != raft::Role::kLeader) {
    reply_submit(conn_id, tx_id, pb::RESP_NOT_LEADER);
    return;
  }
  // A result that used up the nonce is final: no later entry can change it.
  // So a resend of a transaction that already took effect gets its original
  // result without touching the log. (Resending the same bytes gives the
  // same tx_id, because the signature is deterministic.)
  if (auto rec = ledger_.tx(tx_id); rec && consumes_nonce(rec->status)) {
    reply_submit(conn_id, tx_id, pb::RESP_OK, rec->index, rec->status);
    return;
  }
  TxStatus pre = ledger_.prevalidate(t, state_is_current());
  if (pre != TxStatus::kOk) {
    reply_submit(conn_id, tx_id, pb::RESP_REJECTED, 0, pre);
    return;
  }
  // Propose the re-serialized transaction, i.e. exactly what was validated.
  auto r = raft_.propose(t.to_proto().SerializeAsString());
  if (!r) {  // unreachable: checked leadership above
    reply_submit(conn_id, tx_id, pb::RESP_NOT_LEADER);
    return;
  }
  pending_[r->index] = PendingSubmit{conn_id, tx_id, r->term};
  log_debug("submit", {{"tx_id", to_hex(tx_id)}, {"index", r->index}, {"term", r->term}});
}

void ClientService::on_applied(const pb::LogEntry& entry) {
  auto it = pending_.find(entry.index());
  if (it == pending_.end()) return;
  PendingSubmit p = std::move(it->second);
  pending_.erase(it);

  if (entry.term() != p.term) {
    // Another leader's entry took this index: ours was overwritten before it
    // committed. It may still be in the log at another index, so the client
    // must resend rather than be told it failed.
    reply_submit(p.conn_id, p.tx_id, pb::RESP_RETRY);
    return;
  }
  // Our entry. Report what the ledger recorded for this tx_id rather than
  // this entry's own result: if this was a resend of a transaction that took
  // effect earlier, this entry failed BAD_NONCE but the record still holds
  // the original OK, which is the truth the client asked about.
  auto rec = ledger_.tx(p.tx_id);
  if (!rec) {  // unreachable: apply() records every transaction
    reply_submit(p.conn_id, p.tx_id, pb::RESP_RETRY);
    return;
  }
  reply_submit(p.conn_id, p.tx_id, pb::RESP_OK, rec->index, rec->status);
}

void ClientService::after_ready() {
  if (pending_.empty()) return;
  const bool leading = raft_.role() == raft::Role::kLeader;
  for (auto it = pending_.begin(); it != pending_.end();) {
    // A leader only appends in its own term, so a pending entry from an
    // earlier term means this node was deposed (maybe re-elected since). The
    // entry may or may not commit; only a resend can find out safely.
    if (!leading || it->second.term != raft_.term()) {
      reply_submit(it->second.conn_id, it->second.tx_id, pb::RESP_RETRY);
      it = pending_.erase(it);
    } else {
      ++it;
    }
  }
}

void ClientService::reply_submit(uint64_t conn_id, const std::string& tx_id, pb::RespCode code,
                                 LogIndex index, TxStatus status) {
  pb::Envelope resp;
  auto* r = resp.mutable_submit_tx_resp();
  r->set_code(code);
  r->set_tx_id(tx_id);
  if (code == pb::RESP_OK || code == pb::RESP_REJECTED) {
    r->set_tx_status(static_cast<uint32_t>(status));
    r->set_log_index(index);
  } else {
    r->set_leader_hint(raft_.leader());
  }
  reply_(conn_id, resp);
}

bool ClientService::state_is_current() const {
  const LogIndex c = raft_.commit_index();
  return c > 0 && raft_.log().term_at(c) == raft_.term() && ledger_.last_applied() >= c;
}

// ---------------------------------------------------------------- reads

void ClientService::read(uint64_t conn_id, const pb::Envelope& req, bool stale) {
  if (stale) {
    reply_(conn_id, answer(req));
    return;
  }
  const uint64_t id = ++next_read_id_;
  if (!raft_.read_index(id)) {
    reply_(conn_id, redirect(req, pb::RESP_NOT_LEADER));
    return;
  }
  reads_[id] = PendingRead{conn_id, req};
}

void ClientService::on_read(uint64_t read_id, const raft::ReadState* ok) {
  auto it = reads_.find(read_id);
  if (it == reads_.end()) return;
  PendingRead r = std::move(it->second);
  reads_.erase(it);
  if (!ok) {
    reply_(r.conn_id, redirect(r.req, pb::RESP_RETRY));
    return;
  }
  pb::Envelope resp = answer(r.req);
  if (resp.has_read_resp()) resp.mutable_read_resp()->set_read_index(ok->index);
  reply_(r.conn_id, resp);
}

pb::Envelope ClientService::answer(const pb::Envelope& req) const {
  pb::Envelope resp;
  const LogIndex applied = ledger_.last_applied();
  switch (req.msg_case()) {
    case pb::Envelope::kGetAccount: {
      auto* r = resp.mutable_account_resp();
      r->set_code(pb::RESP_OK);
      r->set_name(req.get_account().name());
      r->set_applied_index(applied);
      if (const auto* a = ledger_.account(req.get_account().name())) {
        r->set_found(true);
        r->set_balance(a->balance);
        r->set_next_nonce(a->next_nonce);
        r->set_public_key(a->public_key);
      }
      break;
    }
    case pb::Envelope::kGetHistory: {
      auto* r = resp.mutable_history_resp();
      r->set_code(pb::RESP_OK);
      r->set_applied_index(applied);
      if (const auto* a = ledger_.account(req.get_history().name())) {
        r->set_found(true);
        const auto& h = a->history;
        const size_t limit = req.get_history().limit();
        const size_t first = (limit > 0 && h.size() > limit) ? h.size() - limit : 0;
        for (size_t i = first; i < h.size(); ++i) {
          pb::HistoryItem* item = r->add_records();
          item->set_index(h[i].index);
          item->set_tx_id(h[i].tx_id);
          item->set_counterparty(h[i].counterparty);
          item->set_amount(h[i].amount);
          item->set_incoming(h[i].incoming);
        }
      }
      break;
    }
    case pb::Envelope::kGetTx: {
      auto* r = resp.mutable_tx_resp();
      r->set_code(pb::RESP_OK);
      r->set_applied_index(applied);
      const std::string& id = req.get_tx().tx_id();
      if (auto rec = ledger_.tx(id)) {
        r->set_found(true);
        r->set_log_index(rec->index);
        r->set_tx_status(static_cast<uint32_t>(rec->status));
      } else {
        for (const auto& [index, p] : pending_) {
          if (p.tx_id == id) {
            r->set_pending(true);
            r->set_log_index(index);
            break;
          }
        }
      }
      break;
    }
    case pb::Envelope::kReadReq: {
      auto* r = resp.mutable_read_resp();
      r->set_ok(true);
      r->set_applied_index(applied);
      r->set_state_hash(to_hex(ledger_.state_hash()));
      r->set_total_supply(ledger_.total_supply());
      break;
    }
    default:
      break;
  }
  return resp;
}

pb::Envelope ClientService::redirect(const pb::Envelope& req, pb::RespCode code) const {
  pb::Envelope resp;
  const NodeId hint = raft_.leader();
  switch (req.msg_case()) {
    case pb::Envelope::kGetAccount:
      resp.mutable_account_resp()->set_code(code);
      resp.mutable_account_resp()->set_leader_hint(hint);
      break;
    case pb::Envelope::kGetHistory:
      resp.mutable_history_resp()->set_code(code);
      resp.mutable_history_resp()->set_leader_hint(hint);
      break;
    case pb::Envelope::kGetTx:
      resp.mutable_tx_resp()->set_code(code);
      resp.mutable_tx_resp()->set_leader_hint(hint);
      break;
    case pb::Envelope::kReadReq:
      resp.mutable_read_resp()->set_error(code == pb::RESP_NOT_LEADER ? "not leader"
                                                                       : "leadership lost, retry");
      resp.mutable_read_resp()->set_leader_hint(hint);
      break;
    default:
      break;
  }
  return resp;
}

}  // namespace ledger::server
