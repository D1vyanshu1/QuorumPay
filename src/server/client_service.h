#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <string>

#include "common/types.h"
#include "envelope.pb.h"
#include "ledger/state_machine.h"
#include "raft/raft_core.h"

namespace ledger::server {

// Serves the ledger API on a node's client port: SubmitTx, GetAccount,
// GetHistory, GetTx (and the admin `read` test hook, which shares the
// linearizable read path).
//
// Like RaftCore it does no I/O: replies go out through the Reply callback, by
// client connection id, and the host drives it from its Ready loop:
//
//   handle(req)          a request arrived; then the host runs process_ready()
//   on_applied(entry)    after the ledger applied a committed entry
//   on_read(id, state)   for each of Ready's reads_ready / reads_failed
//   after_ready()        at the end of every Ready loop pass
//
// Submit lifecycle (project_details.md §6.7):
//   1. Not leader -> NOT_LEADER + hint.
//   2. Already decided (its tx_id has a final result) -> that result, at once.
//   3. Pre-check fails -> REJECTED, nothing proposed.
//   4. propose() -> remember pending[index] = (client, tx_id, term).
//   5. Entry at that index applied:
//        same term -> the transaction's result from the ledger's tx_index;
//        other term -> it was overwritten (its leader was deposed): RETRY.
//   6. This node loses leadership first -> RETRY. The entry may still commit
//      under the new leader; the client resends the same signed bytes and
//      step 2 or 5 gives it the one real result.
// So a transfer takes effect at most once, and every reply is its real result.
class ClientService {
 public:
  using Reply = std::function<void(uint64_t conn_id, const pb::Envelope& msg)>;

  ClientService(raft::RaftCore& raft, const ledger::StateMachine& ledger, Reply reply);

  // Returns false if `req` isn't a ledger request (the host treats it as admin).
  bool handle(uint64_t conn_id, const pb::Envelope& req);

  void on_applied(const pb::LogEntry& entry);
  // `ok` is null for a read that failed (its leader stepped down).
  void on_read(uint64_t read_id, const raft::ReadState* ok);
  void after_ready();

  size_t pending_submits() const { return pending_.size(); }
  size_t pending_reads() const { return reads_.size(); }

 private:
  struct PendingSubmit {
    uint64_t conn_id;
    std::string tx_id;
    Term term;
  };
  struct PendingRead {
    uint64_t conn_id;
    pb::Envelope req;
  };

  void submit(uint64_t conn_id, const pb::SubmitTx& req);
  // Answers `req` now from local state (stale), or after a ReadIndex round.
  void read(uint64_t conn_id, const pb::Envelope& req, bool stale);
  pb::Envelope answer(const pb::Envelope& req) const;
  // The reply to `req` when it can't be served here: NOT_LEADER or RETRY.
  pb::Envelope redirect(const pb::Envelope& req, pb::RespCode code) const;
  void reply_submit(uint64_t conn_id, const std::string& tx_id, pb::RespCode code,
                    LogIndex index = 0, TxStatus status = TxStatus::kOk);
  // True once this leader's state reflects every committed entry. A new
  // leader's commit index can lag until it commits an entry of its own term,
  // so until then only stateless checks may reject.
  bool state_is_current() const;

  raft::RaftCore& raft_;
  const ledger::StateMachine& ledger_;
  Reply reply_;

  std::map<LogIndex, PendingSubmit> pending_;
  uint64_t next_read_id_ = 0;
  std::map<uint64_t, PendingRead> reads_;
};

}  // namespace ledger::server
