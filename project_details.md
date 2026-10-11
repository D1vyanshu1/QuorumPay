# Ledger — Project Plan

**A crash-fault-tolerant distributed payment ledger built on Raft consensus.**
C++17 · Raft (from scratch) · Ed25519 (libsodium) · Protobuf · epoll · 5-node cluster

Timeline: **7 weeks**. Main goal: **understand every part well enough to explain it in an interview**, not just get it working.

---

## Table of Contents

1. [What the System Does](#1-what-the-system-does)
2. [Scope: In, Stretch, Out](#2-scope-in-stretch-out)
3. [Architecture Overview](#3-architecture-overview)
4. [Directory Structure](#4-directory-structure)
5. [Tech Stack & Dependencies](#5-tech-stack--dependencies)
6. [Design Specification (all modules, gaps filled in)](#6-design-specification)
   - 6.1 Configuration & Genesis
   - 6.2 Wire Protocol & Messages
   - 6.3 Networking Layer
   - 6.4 Raft Consensus Engine
   - 6.5 Ledger State Machine (Transactions)
   - 6.6 Persistence & Recovery
   - 6.7 Client Interface & Request Lifecycle
   - 6.8 Reads (Consistency)
   - 6.9 Fault Injection & Testing Harness
   - 6.10 Observability
7. [Build Order (dependency graph)](#7-build-order)
8. [Week-by-Week Plan](#8-week-by-week-plan)
9. [The Three Demos](#9-the-three-demos)
10. [Benchmarks to Produce](#10-benchmarks-to-produce)
11. [Testing Strategy](#11-testing-strategy)
12. [Working Rules (how to build it well)](#12-working-rules)
13. [Interview Concepts Checklist](#13-interview-concepts-checklist)
14. [Resume Bullets](#14-resume-bullets)

---

## 1. What the System Does

- 5 node processes form a cluster. One is elected **leader** with Raft. The others are **followers**.
- Clients create accounts and send **signed transfers** (Ed25519) to any node. Followers redirect them to the leader.
- The leader adds each transaction to the **replicated log**. When a majority (3 of 5) has stored it durably, the entry is **committed**.
- Every node **applies committed entries in log order** to a deterministic **state machine** (accounts, balances, nonces). Same log + same rules = same state on every node.
- The cluster keeps working if **up to 2 nodes crash** or are partitioned away. Nodes that crash recover from their **write-ahead log + snapshots**.
- Correctness is shown by **fault injection** (kill, restart, partition), a **Jepsen-style checker**, and **balance-hash comparison** across nodes.

**Fault model:** crash-stop/crash-recover nodes, plus message loss, delay and partitions. **No Byzantine (malicious) nodes**: signatures stop forged *client* transactions, but nodes trust each other.

---

## 2. Scope: In, Stretch, Out

| Category | Items |
|---|---|
| **Core (must ship)** | epoll networking, protobuf messages, Raft election + replication + commit, pre-vote, Ed25519-signed transactions, nonces, genesis/faucet, write-ahead log with CRC, fsync with group commit, snapshots + log compaction + InstallSnapshot, CLI client with leader redirect, linearizable reads (ReadIndex), partition/kill fault injection, 3 demos, Jepsen-style checker, chaos test, latency/recovery benchmarks, structured logs, `status` command |
| **Stretch (only if ahead)** | Leader lease reads, terminal dashboard (Python `rich` or ncurses), REST API (cpp-httplib), transaction fees, 2-of-3 multisig, pipelined replication (AppendEntries without waiting), parallel leader fsync |
| **Out of scope (say so in docs)** | Cluster membership changes (fixed 5-node config), Byzantine fault tolerance, TLS between nodes, multi-machine deployment, sharding |

Saying in an interview *why* you scoped something out is as valuable as building it.

---

## 3. Architecture Overview

```
                 ┌──────────────────────── Node process ───────────────────────────┐
  ledger-cli ───▶│  Client port ──▶ ClientService ──┐                               │
  (signs txs)    │                                  ▼                               │
                 │  Peer port  ◀──▶ PeerManager ◀──▶ RaftCore (pure logic, no I/O)  │
  harness ──────▶│  (admin cmds)    + FaultFilter       │  Ready{persist, send,    │
  (kill/part.)   │                                      │        apply, snapshot}  │
                 │                                      ▼                           │
                 │                 Storage (MetaStore, WAL, SnapshotStore)          │
                 │                                      │ committed entries         │
                 │                                      ▼                           │
                 │                 StateMachine (accounts, nonces, history)         │
                 │                                                                  │
                 │  Everything runs on ONE event-loop thread (epoll + timerfd)      │
                 └──────────────────────────────────────────────────────────────────┘
```

**Key design decisions (and why):**

1. **RaftCore is pure logic with no I/O** (like etcd/raft). It takes inputs (`step(msg)`, `tick()`, `propose()`, `read_index()`) and returns a `Ready` batch: *what to persist, what messages to send, which entries to apply*. The node host does the I/O.
   *Why:* you can test Raft deterministically in a simulated network with a virtual clock and seeded randomness, before any sockets exist. This is the single most important decision for understanding and debugging Raft.
2. **Single-threaded event loop per node.** Networking, Raft and apply all run on one thread, so the core has no locks or data races. Signature checks (~50µs) and small fsyncs fit within this budget.
   *Why:* concurrency bugs inside Raft are very hard to debug. One thread keeps the reasoning simple, and you can explain the trade-off (throughput ceiling vs simplicity).
3. **Deterministic state machine.** No wall-clock time, randomness or hash-map iteration order in apply or hashing. Use `std::map` and sorted serialization.
4. **The leader appends a no-op entry when elected.** This lets it commit entries from earlier terms (Raft §5.4.2) and is required for ReadIndex.

---

## 4. Directory Structure

```
Ledger/
├── CMakeLists.txt                 # top-level build; options for tests/sanitizers
├── README.md                      # overview, quickstart, demo GIFs, results
├── project_details.md             # this plan
├── .gitignore                     # build/, data/, *.key, results/
├── .clang-format
│
├── proto/
│   ├── raft.proto                 # RequestVote, AppendEntries, InstallSnapshot, LogEntry
│   ├── ledger.proto               # Transaction, TxResult, Account (snapshot payload)
│   └── client.proto               # client + admin requests/responses, envelope
│
├── config/
│   ├── cluster.json               # 5 nodes: id, host, peer_port, client_port, data_dir
│   └── genesis.json               # chain_id, initial accounts (faucet) + pubkeys
│
├── src/
│   ├── common/                    # shared, no dependencies on other modules
│   │   ├── types.h                # NodeId, Term, LogIndex, aliases
│   │   ├── status.h               # Result/error type, TxStatus enum
│   │   ├── config.{h,cpp}         # load cluster.json / genesis.json
│   │   └── logger.{h,cpp}         # structured JSON-lines logger
│   │
│   ├── crypto/
│   │   ├── keys.{h,cpp}           # keypair gen, load/save keyfiles
│   │   ├── signer.{h,cpp}         # sign / verify (libsodium Ed25519)
│   │   └── hash.{h,cpp}           # SHA-256 helpers, CRC32
│   │
│   ├── ledger/                    # the replicated state machine (pure, deterministic)
│   │   ├── transaction.{h,cpp}    # Tx types + canonical signing bytes + tx_id
│   │   ├── account_store.{h,cpp}  # std::map<name, Account>
│   │   ├── state_machine.{h,cpp}  # apply(entry) -> TxResult; validate pipeline
│   │   └── state_hash.{h,cpp}     # deterministic balance hash
│   │
│   ├── raft/                      # pure consensus logic, NO sockets, NO files
│   │   ├── raft_core.{h,cpp}      # roles, elections, replication, commit
│   │   ├── raft_log.{h,cpp}       # in-memory log view (with snapshot offset)
│   │   ├── progress.{h,cpp}       # per-follower nextIndex/matchIndex
│   │   ├── read_index.{h,cpp}     # pending linearizable reads
│   │   └── ready.h                # Ready struct: to_persist/to_send/to_apply
│   │
│   ├── storage/
│   │   ├── meta_store.{h,cpp}     # currentTerm + votedFor (atomic write)
│   │   ├── wal.{h,cpp}            # segmented append-only log with CRC, truncate
│   │   ├── snapshot_store.{h,cpp} # write/read/list snapshots atomically
│   │   └── file_util.{h,cpp}      # fsync file+dir, atomic rename helpers
│   │
│   ├── net/
│   │   ├── event_loop.{h,cpp}     # epoll + timerfd wrapper
│   │   ├── connection.{h,cpp}     # non-blocking socket, read/write buffers
│   │   ├── framing.{h,cpp}        # 4-byte length prefix + protobuf
│   │   ├── listener.{h,cpp}       # accept()
│   │   ├── peer_manager.{h,cpp}   # outbound dial + reconnect w/ backoff
│   │   └── fault_filter.{h,cpp}   # blocked-peers list (partition simulation)
│   │
│   ├── server/
│   │   ├── node.{h,cpp}           # wires RaftCore + storage + net + state machine
│   │   ├── client_service.{h,cpp} # handles client requests, pending map
│   │   ├── admin_service.{h,cpp}  # block/unblock peer, status, snapshot-now
│   │   └── main.cpp               # ledger-node binary
│   │
│   └── client/
│       ├── client_lib.{h,cpp}     # connect, redirect, retry, timeouts
│       ├── keystore.{h,cpp}       # ~/.ledger/keys/<name>.key (0600)
│       └── main.cpp               # ledger-cli binary
│
├── tests/
│   ├── unit/                      # GoogleTest: crypto, codec, state machine, WAL...
│   ├── sim/                       # deterministic Raft simulation tests
│   │   ├── sim_network.{h,cpp}    # in-memory network: drop/delay/partition, seeded
│   │   └── raft_sim_test.cpp      # election safety, log matching, etc.
│   └── integration/               # spawn real processes (driven from tools/)
│
├── tools/                         # Python 3 tooling (harness, not part of the system)
│   ├── cluster.py                 # start/stop/kill/restart nodes, wipe data
│   ├── faults.py                  # partition/heal via admin port
│   ├── workload.py                # concurrent clients, records op history
│   ├── checker.py                 # Jepsen-style consistency checker
│   ├── chaos.py                   # random faults + workload for N minutes
│   ├── bench.py                   # latency/throughput/recovery experiments
│   ├── plot.py                    # matplotlib graphs -> docs/img/
│   ├── make_genesis.py            # builds genesis.json from faucet pubkey
│   └── demos/
│       ├── demo_double_spend.py
│       ├── demo_leader_failure.py
│       └── demo_partition.py
│
├── docs/
│   ├── design.md                  # architecture + decisions (written as you go)
│   ├── raft-notes.md              # your own notes on the paper + invariants
│   ├── protocol.md                # message formats, WAL format, snapshot format
│   ├── benchmarks.md              # methodology + results + graphs
│   └── img/
│
├── data/                          # runtime (gitignored): data/node-1/{meta,wal/,snap/}
└── results/                       # benchmark/chaos outputs (gitignored)
```

**Dependency rule (enforce in CMake):**
`common` ← `crypto` ← `ledger` ; `common` ← `raft` (no crypto needed) ; `storage`, `net` depend only on `common`/`crypto` ; `server` depends on everything ; `client` depends on `common`, `crypto`, `ledger`, `net`.
**`raft/` must never include `net/` or `storage/`.** That is what keeps it testable.

---

## 5. Tech Stack & Dependencies

| Need | Choice | Notes |
|---|---|---|
| Language | C++17, GCC/Clang | `-Wall -Wextra -Werror`, ASan/UBSan build option |
| Build | CMake ≥ 3.16 | `FetchContent` for GoogleTest, nlohmann/json |
| Serialization | Protocol Buffers (protobuf + protoc) | wire format only; **not** used for signing bytes |
| Crypto | libsodium | Ed25519 (`crypto_sign_*`), SHA-256 (`crypto_hash_sha256`) |
| Config | nlohmann/json (header-only) | cluster.json, genesis.json |
| Tests | GoogleTest | unit + sim tests |
| Tooling | Python 3 + matplotlib | harness, checker, plots |
| CRC32 | own implementation (table-based) or zlib `crc32` | |
| Stretch | cpp-httplib, `rich` (Python) | REST API, dashboard |

---

## 6. Design Specification

### 6.1 Configuration & Genesis

**`config/cluster.json`**: identical on every node.
```
{ "cluster_id": "ledger-dev",
  "nodes": [ {"id":1, "host":"127.0.0.1", "peer_port":7001, "client_port":8001, "data_dir":"data/node-1"}, ... x5 ],
  "raft": { "tick_ms":10, "election_ticks_min":15, "election_ticks_max":30, "heartbeat_ticks":5,
            "max_entries_per_append":64, "snapshot_every":1000, "fsync":"batch" } }
```
- Node is started with `ledger-node --config config/cluster.json --id 3`.
- Timing: tick = 10ms → election timeout 150–300ms (randomized per election), heartbeat 50ms.

**Where does money come from? (gap filled): Genesis + Faucet**
- `config/genesis.json` holds `chain_id` plus a list of initial accounts: `{name, public_key_hex, balance}`. It normally has a single `faucet` account with a large supply (e.g. 1,000,000,000 units).
- Every node loads the **same genesis** at startup to build the state at index 0, so it is deterministic.
- The faucet's secret key is generated by `ledger-cli keygen faucet`. `tools/make_genesis.py` writes the pubkey into genesis.json.
- No special "mint" operation: `ledger-cli fund <name> <amt>` is an ordinary transfer signed by the faucet. **Total supply is constant**, which the checker uses as an invariant.
- Amounts are `uint64` integer units. **Never use floating point for money.**

### 6.2 Wire Protocol & Messages

**Framing:** `[u32 length (big-endian)][protobuf bytes]`. Max frame 16MB (reject bigger).
**Envelope:** every frame is one `Envelope { oneof { ...all message types... } }` so a single parser handles all traffic.

**Handshake (first message on any peer connection):** `Hello {node_id, cluster_id}`. Close the connection if `cluster_id` doesn't match. This protects against misconfiguration.

**Raft messages (`raft.proto`):**
```
LogEntry          { term, index, type (NOOP | TX), payload (bytes: serialized Transaction) }

RequestVote       { term, candidate_id, last_log_index, last_log_term, pre_vote (bool) }
RequestVoteResp   { term, vote_granted, pre_vote (bool) }

AppendEntries     { term, leader_id, prev_log_index, prev_log_term,
                    entries[], leader_commit, read_ctx (u64, for ReadIndex heartbeats) }
AppendEntriesResp { term, success, match_index,
                    conflict_term, conflict_index,     // fast backtracking
                    read_ctx }

InstallSnapshot     { term, leader_id, last_included_index, last_included_term,
                      offset, data (chunk), done }
InstallSnapshotResp { term, bytes_received }
```
Original plan gap: `matchIndex` alone makes backtracking O(n) round-trips. `conflict_term/conflict_index` let the leader skip a whole conflicting term in one round trip (Raft paper §5.3, "optimization" paragraph).

**Client & admin messages (`client.proto`):**
```
SubmitTx      { tx }                       -> SubmitTxResp  { status, tx_id, log_index, leader_hint }
GetBalance    { account, linearizable }    -> BalanceResp   { status, balance, nonce, applied_index, leader_hint }
GetHistory    { account, limit }           -> HistoryResp   { status, records[], leader_hint }
GetTx         { tx_id }                    -> TxResp        { status (PENDING|OK|FAILED_<reason>|UNKNOWN), log_index }
GetStatus     {}                           -> StatusResp    { node_id, role, term, leader_id, commit_index, last_applied,
                                                              first_log_index, last_log_index, snapshot_index,
                                                              state_hash, peers[{id, match_index, connected, blocked}] }
Admin: BlockPeer{peer_id} / UnblockPeer{peer_id} / UnblockAll / TakeSnapshot -> AdminResp
```
`status` codes for responses: `OK, NOT_LEADER, TIMEOUT, REJECTED(reason), RETRY`.

### 6.3 Networking Layer

**Model:** one `epoll` event loop thread per node. Timers use `timerfd`, so the Raft tick is just another fd. All sockets are non-blocking.

**Connections between peers:** each node **dials every other node** for its *outgoing* RPCs and **accepts** inbound connections for incoming RPCs. That gives two TCP connections per pair, one per direction. A response travels back on the connection its request came in on.
*Why:* it avoids the "who dials whom" race and makes reconnection logic symmetric and simple.

**Connection lifecycle:**
- `Connection` owns a read buffer (accumulate until a full frame is available) and a write buffer (queue frames, flush on `EPOLLOUT`).
- Partial reads and writes are normal. Handle `EAGAIN`, `EINTR`, peer close and `ECONNRESET`.
- **Reconnect** with exponential backoff (50ms → 1s cap, plus jitter) when an outgoing connection drops. Raft itself tolerates lost messages, so no message is "retried" by the transport. Raft heartbeats and retransmissions handle it.
- **Back-pressure:** if a peer's write buffer exceeds a limit (e.g. 8MB), drop new non-essential messages to it. Raft will resend.

**Batching:** comes from Raft. On each heartbeat or new proposal, the leader sends a follower all entries from `nextIndex`, up to `max_entries_per_append`. Benchmark different batch sizes.

**FaultFilter:** a set of blocked peer IDs. Messages to or from a blocked peer are **silently dropped** at the transport, in both directions. The harness drives it through admin commands, so a partition = block on both sides.

### 6.4 Raft Consensus Engine

Read the **extended Raft paper (Ongaro & Ousterhout), especially Figure 2**. Figure 2 *is* the spec. Implement it rule by rule.

**State:**
- Persistent (must be on disk **before** responding): `currentTerm`, `votedFor`, `log[]`
- Volatile: `commitIndex`, `lastApplied`, `role`, `leaderId`, election timer
- Leader-only: `nextIndex[]`, `matchIndex[]` per follower

**Pure-core API (no I/O inside):**
```
tick()                         // called every 10ms
step(message)                  // any incoming Raft message
propose(tx_bytes) -> index|NOT_LEADER
read_index(request_id)         // start a linearizable read
ready() -> Ready { hard_state?, entries_to_append, truncate_from?, messages[],
                   committed_entries[], snapshot_to_send?, reads_ready[] }
advance(ready)                 // host tells core "I persisted/sent/applied this"
```
**Order inside the host for each Ready:** (1) persist hard_state + entries (fsync) → (2) send messages → (3) apply committed entries → (4) `advance()`.
Persisting before sending is what makes acknowledgments durable. Be able to explain this.

**Leader election:**
1. A follower times out (randomized 150–300ms, re-randomized each time) and becomes a **pre-candidate**. It sends `RequestVote{pre_vote=true, term=currentTerm+1}` **without** incrementing its own term.
2. Nodes grant a pre-vote only if (a) they haven't heard from a leader within the minimum election timeout, and (b) the candidate's log is at least as up to date as theirs. Pre-vote does not change their term or vote.
3. With a majority of pre-votes: become **candidate**, `currentTerm++`, vote for self, **persist**, then send real RequestVotes.
4. Grant a vote if `term >= currentTerm`, you haven't voted for someone else this term, and the candidate's log is up to date: `(lastLogTerm, lastLogIndex)` compared term first, then index.
5. On a majority, become **leader**. Immediately append a **no-op** entry and send AppendEntries (heartbeat) to all.
6. **Any message with a higher term → become follower at that term** (clear votedFor, persist). A message with a lower term → reject and reply with your term.

**Log replication:**
1. The leader appends the proposal to its log (persists) and sends AppendEntries to each follower from `nextIndex[f]`.
2. Follower: reject if `term < currentTerm`. Reset the election timer. Check that `prevLogIndex/prevLogTerm` match. If not, reply `success=false` with `conflict_term/conflict_index`.
3. On a match: for each new entry, if an existing entry conflicts (same index, different term), **truncate from there**, then append whatever is missing. **Never truncate committed entries**; assert this.
4. Follower sets `commitIndex = min(leader_commit, index of last new entry)`.
5. Leader on success: update `matchIndex`/`nextIndex`. On failure: use the conflict info to move `nextIndex` back.
6. **Commit rule:** the leader advances `commitIndex` to the highest N where a majority has `matchIndex >= N` **and `log[N].term == currentTerm`**. The second condition is the Figure 8 case; understand it deeply.
7. Every node applies `lastApplied+1 … commitIndex` in order to the state machine.

**Safety invariants (assert in sim tests):**
- **Election Safety:** at most one leader per term.
- **Leader Append-Only:** a leader never overwrites or deletes its own entries.
- **Log Matching:** if two logs have an entry with the same index and term, the logs are identical up to that index.
- **Leader Completeness:** a committed entry is present in the logs of all future leaders.
- **State Machine Safety:** no two nodes apply different entries at the same index.

**Snapshots in the core:** the log has an offset (`snapshot_index`, `snapshot_term`). When `nextIndex[f] <= snapshot_index`, the leader sends InstallSnapshot instead of AppendEntries.

### 6.5 Ledger State Machine (Transactions)

**Transaction types (gap filled: account creation needs its own type):**
```
Transaction {
  type:       CREATE_ACCOUNT | TRANSFER
  chain_id:   string           // prevents replay on another cluster
  sender:     string           // account name (CREATE_ACCOUNT: the new name)
  receiver:   string           // TRANSFER only
  amount:     uint64           // TRANSFER only, > 0
  nonce:      uint64           // sender's next expected nonce (CREATE_ACCOUNT: 0)
  public_key: bytes[32]        // CREATE_ACCOUNT only
  signature:  bytes[64]
}
```

**Account:** `{ name, public_key, balance (uint64), next_nonce (uint64), history[] }`. Stored in `std::map<std::string, Account>` (sorted, deterministic).

**Account names:** 3–32 chars, `[a-z0-9_]`. The first CREATE_ACCOUNT committed for a name wins. Later ones fail with `DUPLICATE_ACCOUNT`.

**CREATE_ACCOUNT** is signed by the **new key itself** (proof of possession). The account starts with balance 0 and next_nonce 1, because the create consumes nonce 0.

**Canonical signing bytes (important gap):** do **not** sign protobuf bytes. Protobuf serialization isn't guaranteed to be canonical. Sign a hand-written, fixed-layout encoding:
```
"LEDGER_TX_V1" || len+chain_id || u8 type || len+sender || len+receiver
              || u64be amount || u64be nonce || [32-byte pubkey if CREATE]
```
- `tx_id = SHA-256(canonical bytes || signature)`, used to look up a transaction's status.
- The domain-separation prefix and chain_id stop signatures being reused in another context.

**Validation pipeline at apply time** (runs on every node, on every committed TX entry, in order):

| Step | Check | On failure | Nonce consumed? |
|---|---|---|---|
| 1 | Well-formed (name rules, amount > 0, sender ≠ receiver, correct chain_id) | `MALFORMED` | No |
| 2 | Sender exists (TRANSFER) / name free (CREATE) | `UNKNOWN_ACCOUNT` / `DUPLICATE_ACCOUNT` | No |
| 3 | Signature valid against the sender's stored pubkey (or the embedded pubkey for CREATE) | `BAD_SIGNATURE` | **No** (otherwise anyone could burn someone else's nonces) |
| 4 | `nonce == next_nonce` | `BAD_NONCE` | No |
| 5 | Receiver exists | `UNKNOWN_RECEIVER` | **Yes** |
| 6 | `balance >= amount`; receiver balance won't overflow | `INSUFFICIENT_FUNDS` / `OVERFLOW` | **Yes** |
| 7 | Apply: debit, credit, `next_nonce++`, append to both histories | `OK` | Yes |

**Nonce rule (refined from the original):** a nonce is consumed only once the transaction is *proven to be from the sender* (valid signature + correct nonce). Business-logic failures then still consume it, the Ethereum-style choice that avoids nonce gaps. Rejections that aren't authenticated never touch state.

**Results:** every applied entry produces a `TxResult{tx_id, index, status}` stored in a `tx_index` map (tx_id → result). This map is part of the state and is included in snapshots. It drives `GetTx` and **exactly-once semantics** (see 6.7).

**Leader pre-validation (cheap filter, not authoritative):** before `propose()`, the leader checks well-formedness, signature, and `nonce >= next_nonce` against its *applied* state. It rejects obvious garbage early so the log doesn't fill with junk. **The apply-time pipeline remains the source of truth.** Be ready to explain why the pre-check alone isn't enough: two in-flight transactions with the same nonce both pass the pre-check, and only one survives at apply time.

**State hash:** `SHA-256` over accounts serialized in sorted order `(name, balance, next_nonce, pubkey)` **plus** `last_applied`. Exposed in `status`. Compare hashes only at the same applied index.

### 6.6 Persistence & Recovery

**Files per node (`data/node-N/`):**
```
meta                      # currentTerm, votedFor, CRC
wal/000001.log ...        # segmented WAL, 64MB per segment (configurable)
snap/snap-<term>-<index>.snap
```

**MetaStore (gap filled; the original missed this):** `currentTerm` and `votedFor` **must** be durable before replying to a RequestVote or sending your own. Otherwise a node could vote twice in one term after a crash.
Atomic update: write `meta.tmp` → `fsync(tmp)` → `rename(tmp, meta)` → `fsync(dir)`.

**WAL record format:**
```
[u32 length][u32 crc32][u8 record_type][u64 term][u64 index][payload bytes]
 crc32 covers everything after the crc field
```
- **Recovery:** scan segments in order and stop at the first record with a bad length or CRC (a torn write from a crash). Truncate the file at that offset.
- **Truncation (Raft conflict, gap filled):** keep an in-memory `index → (segment, offset)` map. On conflict, `ftruncate` at that entry's offset, delete later segments, then `fsync`. This is safe because only *uncommitted* entries are ever truncated.
- **Durability policy (configurable, benchmarked):**
  - `fsync=entry`: fsync after every record (slow, simplest)
  - `fsync=batch` (**default, group commit**): append all records produced in one event-loop iteration, then fsync **once** before sending any acknowledgments
  - `fsync=none`: unsafe; shows how much fsync costs

**Snapshots:**
- Taken when `last_applied - snapshot_index >= snapshot_every` (1000) or by admin command.
- Contents: `last_included_index`, `last_included_term`, all accounts (sorted), `tx_index`, history, then a SHA-256 checksum of the payload.
- Written atomically (tmp + fsync + rename + dir fsync). Keep the newest 2 snapshots.
- Taken synchronously on the event loop. The state is small, so this is fine; note copy-on-write as a known scaling limit.

**Log compaction:** after a snapshot at index S, delete WAL segments whose entries are all ≤ S. Keep a small margin (e.g. the last 100 entries) so slightly lagging followers don't need a full snapshot.

**Recovery on startup:**
1. Load `meta` (term, votedFor).
2. Load the newest valid snapshot (verify checksum; fall back to the older one if corrupt), or genesis if there is none.
3. Replay WAL entries after the snapshot index into the in-memory Raft log. **Do not apply them yet.** `commitIndex` is not persisted; it is relearned from the leader. Restore `lastApplied = snapshot_index`.
4. Start as a follower. The leader's `leader_commit` tells the node how far to apply.

**InstallSnapshot:** the leader sends the snapshot file in 1MB chunks (`offset`, `done`). The follower writes chunks to a temp file. On `done` it verifies the checksum, replaces its state machine, discards log entries ≤ last_included_index (keeping any later entries that match), and sets `commitIndex`/`lastApplied` to that index.

**Recovery time** (snapshot vs full replay) is a headline benchmark.

### 6.7 Client Interface & Request Lifecycle

**CLI commands (`ledger-cli`):**
```
keygen <name>                      # create keypair in ~/.ledger/keys/<name>.key
create_account <name>              # keygen if needed, submit signed CREATE_ACCOUNT
fund <name> <amount>               # transfer from faucet (needs faucet key locally)
transfer <from> <to> <amount>      # fetch nonce, sign, submit, wait for result
balance <account> [--stale]        # linearizable by default; --stale reads any node
history <account> [--limit N]
tx <tx_id>                         # status of a transaction
status [--node N | --all]          # role, term, leader, commit/applied index, hash
```
Global flags: `--config config/cluster.json`, `--node N` (starting node), `--timeout ms`.

**Leader redirect:** a follower replies `NOT_LEADER{leader_hint}`. The client reconnects to the hint. With no hint, it tries the nodes round-robin. Cap retries at about 5.

**Nonce source:** for `transfer`, the client first does a linearizable `balance` read to get `next_nonce`, then signs. The client keeps no nonce state between runs.

**Submit lifecycle (gap filled):**
1. The leader pre-validates, calls `propose()`, gets a log index, and stores `pending[index] = (connection, tx_id, term)`.
2. When the entry at that index is **applied**, the leader responds with the `TxResult` (OK / FAILED_reason).
3. If the leader loses leadership, or the entry at that index ends up with a different term (overwritten), it responds `RETRY` for every pending request it can no longer guarantee.
4. Client timeout → outcome **unknown**.

**Exactly-once via nonce + tx_id (gap filled):** on `RETRY` or a timeout, the client resubmits the **same signed bytes** to the (new) leader:
- If the original never committed → it commits now.
- If it *did* commit → the resubmission fails `BAD_NONCE` (nonce already used). The client then queries `tx <tx_id>` to learn the real outcome.

So **a transfer can never be applied twice**, even with retries during a failover. This is a strong interview point.

### 6.8 Reads (Consistency)

| Mode | How | Guarantee | Cost |
|---|---|---|---|
| **Linearizable (default): ReadIndex** | Leader records `readIndex = commitIndex`, confirms it is still leader with one heartbeat round to a majority (tagged by `read_ctx`), waits for `lastApplied >= readIndex`, then reads locally | Linearizable | 1 heartbeat RTT, no log write |
| **Stale (`--stale`)** | Any node answers from its local state and returns `applied_index` | May be stale | Local |
| **Lease (stretch)** | Leader serves locally while `now < last_majority_ack + election_timeout_min - drift` | Linearizable *if clocks behave* | Local |

ReadIndex requires the leader to have committed an entry in its current term, which the **no-op on election** provides. Followers handle linearizable reads by redirecting to the leader (forwarding ReadIndex is a stretch goal).

### 6.9 Fault Injection & Testing Harness

**Mechanisms:**
- **Kill:** `SIGKILL` the process (no clean shutdown; a real crash). `tools/cluster.py kill 3`.
- **Restart:** relaunch with the same `--id` and data dir, then measure the time until it catches up (applied index = leader commit).
- **Partition:** `faults.py partition 1,2 3,4,5` sends `BlockPeer` admin commands to both sides. `faults.py heal` sends `UnblockAll` to everyone.
- **Admin commands go over the client port**, so they still work during a partition. Only peer traffic is filtered.

**Workload generator (`workload.py`):** N concurrent clients doing random transfers between K accounts. Every operation is recorded in Jepsen style:
```
{client, op_id, type: invoke|ok|fail|info, tx_id, from, to, amount, nonce, t_start, t_end, result}
```
`info` = indeterminate (timeout). The transaction may or may not have committed.

**Checker (`checker.py`), run after the workload:**
1. Wait for all live nodes to reach the same `last_applied`. **All state hashes are equal.**
2. Fetch the committed log (via `history`/`tx` queries). **Every `ok` transaction appears exactly once. No `fail` transaction was applied.** `info` transactions may go either way.
3. **Independent model:** re-apply the committed transfers to the genesis state using a *separate Python implementation* of the rules. The final balances must equal the cluster's.
4. **Conservation:** the sum of all balances equals the genesis supply.
5. **No double spend:** for each (sender, nonce), at most one transaction applied.
6. **Durability:** every transaction acknowledged `ok` before a leader crash is still present afterward.

**Chaos test (`chaos.py`):** for 5 minutes, every 2–5s pick a random fault (kill a random node, kill the leader, random 2|3 partition, isolate the leader, restart dead nodes, heal). At most 2 nodes are down or isolated at a time. The workload runs throughout. At the end: heal everything, restart all nodes, wait, run the checker. Seed the RNG and log the seed so failures can be reproduced.

### 6.10 Observability

- **Structured logs**, JSON lines to `data/node-N/node.log`:
  `{"ts_ms":..., "node":3, "term":7, "role":"leader", "event":"commit", "index":1042, ...}`
  Events: `election_start, prevote_granted, vote_granted, became_leader, stepped_down, append_reject, commit, apply, snapshot_taken, snapshot_installed, wal_truncate, peer_connected, peer_disconnected, partition_set`.
- **`status`** command (6.2) is the main live view.
- **Timeline tool (cheap and very useful):** `tools/timeline.py` merges all nodes' logs by timestamp, so you can see an election or partition unfold. Use it in demos.
- **Stretch:** `tools/dashboard.py` (Python `rich`) polls `status --all` every 200ms and shows roles, terms, commit index, hashes, and blocked links.

---

## 7. Build Order

Build from **pure, testable pieces outward to I/O**. Each layer is tested before the next one depends on it.

```
 [1] CMake + common + config
        │
 [2] crypto (keys, sign/verify, sha256, crc32)
        │
 [3] ledger: transaction + canonical bytes + state machine   ← pure, unit-tested
        │
 [4] proto definitions + framing codec                       ← pure, unit-tested
        │
 [5] raft core: election  ──▶ [6] raft core: replication + commit + apply
        │                              │
        └──── both tested in [S] deterministic simulator (no sockets) ────┘
                                       │
 [7] net: event loop, connections, peer manager, fault filter
        │
 [8] server/node: wire core + net + in-memory storage + state machine
        │
 [9] client CLI: submit, redirect, retry, reads (ReadIndex)  ── first end-to-end demo
        │
[10] storage: meta + WAL + recovery  ──▶ [11] snapshots + compaction + InstallSnapshot
        │
[12] pre-vote, harness, demos, checker, chaos
        │
[13] benchmarks, timeline/dashboard, docs, polish
```

**Why this order:** Raft bugs are the hardest to debug. Building the core with no sockets means the first time you see a bug, it is reproducible with a seed rather than a flaky race. The networking layer then only has to move bytes. Persistence comes after a working in-memory cluster, so you can tell "Raft bug" apart from "disk bug".

---

## 8. Week-by-Week Plan

Each week lists **Learn → Build (in order) → Tests → Done when → Explain it**. Don't start the next week until "Done when" is true. Behind schedule? Drop stretch items, never tests.

### Week 1: Foundations: Build System, Crypto, State Machine, Codec

**Learn**
- libsodium docs: `crypto_sign_keypair`, `crypto_sign_detached`, `crypto_sign_verify_detached`, `crypto_hash_sha256`
- Protobuf C++ basics; why protobuf encoding isn't canonical
- Skim the Raft paper (§1–5) once, just for orientation. Deep reading starts in week 2.

**Build (in order)**
1. Repo skeleton (directory structure above), `.gitignore`, `.clang-format`, CMake with library targets per module + GoogleTest via FetchContent + an ASan/UBSan option.
2. `common/`: types, `Result`/status enums, JSON config loader (cluster.json, genesis.json), structured logger.
3. `crypto/`: key generation, save/load key files (hex), sign/verify, SHA-256, CRC32.
4. `ledger/transaction`: struct, canonical signing bytes, `tx_id`, name validation.
5. `ledger/state_machine`: genesis load, the full 7-step validation pipeline, apply, `tx_index`, history, state hash.
6. `proto/*.proto` (all messages from 6.2) + `net/framing` encode/decode (works on byte buffers; no sockets yet).
7. `ledger-cli keygen` + `tools/make_genesis.py`, so a real genesis.json exists.

**Tests**
- Sign → verify OK; tampering with any field → verify fails; a wrong key fails.
- State machine table tests: one test per row of the validation table, including nonce-consumption behavior.
- Double-spend at the state-machine level: two transactions with the same nonce → exactly one OK.
- Overflow, zero amount, self-transfer, duplicate account name.
- Determinism: apply the same transaction list on two instances → identical hash.
- Framing: round-trip, partial buffer (incomplete frame returns "need more"), oversize frame rejected.

**Done when:** `ctest` is green, the state machine handles every case in 6.5, and you can create a genesis file.

**Explain it:** why sign canonical bytes instead of protobuf; why some failures consume nonces and others don't; why the state machine must be deterministic.

---

### Week 2: Raft Core Part 1: Simulator + Leader Election

**Learn (most important reading week)**
- The Raft extended paper, §5.1–5.2 + **Figure 2** (print it and keep it beside you)
- Raft thesis §9.6 (pre-vote), for later
- Optional: the Raft visualization at raft.github.io; the "Students' Guide to Raft" (MIT 6.824 TA blog)
- Write `docs/raft-notes.md` in your own words before coding.

**Build (in order)**
1. `raft/ready.h`, the `RaftCore` skeleton with roles, term, votedFor, a tick-based election timer with an **injected RNG** (seeded).
2. In-memory `RaftLog` (with snapshot offset fields, unused for now).
3. RequestVote send/handle, up-to-date log comparison, vote counting, becoming leader, heartbeats (empty AppendEntries), the higher-term → step-down rule everywhere.
4. Leader appends a no-op on election.
5. **`tests/sim/sim_network`:** 5 `RaftCore` instances, virtual clock (calls `tick()`), message queue with seeded random delay/drop/duplication/reordering, `partition(groups)`, `crash(id)` (drop volatile state, keep "persisted" state), `restart(id)`. The simulator processes `Ready` the same way the real host will.
6. An invariant checker hook that runs after every step: **at most one leader per term**.

**Tests**
- A 5-node cluster elects exactly one leader within N ticks (across 1000 seeds).
- Kill the leader → a new leader with a higher term.
- 2|3 partition: the minority never elects a leader; the majority does.
- Message loss of 20–30%: still converges.
- Split votes resolve thanks to randomized timeouts.

**Done when:** election tests pass on 1000+ random seeds with no invariant violations.

**Explain it:** why the timeouts are randomized; why vote state must be persisted before replying; what goes wrong without the "up-to-date log" check.

---

### Week 3: Raft Core Part 2: Log Replication, Commit, Apply, ReadIndex

**Learn**
- Raft paper §5.3–5.4 thoroughly; **Figure 8** (why you only commit current-term entries by counting)
- Raft thesis §6.4 (ReadIndex)

**Build (in order)**
1. `propose()` on the leader; `Progress` per follower (`nextIndex`, `matchIndex`).
2. AppendEntries send with batching (`max_entries_per_append`); follower consistency check; truncate-on-conflict; `conflict_term/conflict_index` in rejections; leader backtracking using them.
3. The commit rule (majority `matchIndex` **and** current term); follower commit from `leader_commit`.
4. `committed_entries` in `Ready` → apply to the `StateMachine` (wire the real ledger state machine from week 1 into the simulator).
5. Pending-proposal tracking: report results; mark `RETRY` when leadership is lost or the entry is overwritten.
6. ReadIndex: `read_index()`, `read_ctx` on heartbeats, release the read once majority acks arrive and `lastApplied >= readIndex`.
7. Extend the invariant checker: **Log Matching**, **State Machine Safety** (apply-history per node must agree index by index), **Leader Completeness** (committed entries are present in every new leader's log).

**Tests**
- Random proposals + random crashes/partitions/drops over 1000 seeds → all invariants hold, and live nodes converge to the same state hash.
- A Figure 8 scenario reproduced as a hand-scripted test.
- A follower that is far behind with divergent entries gets repaired.
- Proposals to a deposed leader → `RETRY`, never silently lost or double-applied.
- ReadIndex on a partitioned (stale) leader never returns (and times out), while the new leader serves reads.

**Done when:** a randomized simulation passes with all 5 invariants checked, and the double-spend scenario (same nonce proposed twice under failover) yields exactly one apply.

**Explain it:** the Log Matching property and how prevLogIndex/prevLogTerm enforce it; the Figure 8 problem; why ReadIndex needs a heartbeat round and the no-op.

---

### Week 4: Real Networking + Node Binary + CLI (first end-to-end cluster)

**Learn**
- `man epoll`, `man timerfd_create`, non-blocking sockets, edge-triggered vs level-triggered (use **level-triggered**; it is simpler and less error-prone)
- Beej's Guide to Network Programming (refresher)

**Build (in order)**
1. `net/event_loop`: epoll wrapper, fd callbacks, timerfd for the 10ms tick.
2. `net/connection`: non-blocking read/write buffers; frame extraction via `framing`.
3. `net/listener` + `net/peer_manager`: accept inbound; dial outbound to all peers; `Hello` handshake; reconnect with backoff + jitter; write-buffer cap.
4. `net/fault_filter`: blocked-peers set checked on send and receive.
5. `server/node`: owns the event loop, `RaftCore`, the state machine, and **in-memory storage for now**. Runs the Ready loop (persist → send → apply → advance).
6. `server/client_service`: SubmitTx (pre-validate → propose → pending map), GetBalance (ReadIndex or stale), GetHistory, GetTx, GetStatus; `NOT_LEADER` with a hint.
7. `server/admin_service`: BlockPeer/UnblockPeer/UnblockAll.
8. `client/`: keystore, client_lib (redirect, retry with the same signed bytes, timeouts), all CLI commands.
9. `tools/cluster.py`: start/stop/kill/restart 5 nodes on localhost; `tools/faults.py`.

**Tests**
- Integration: start 5 nodes → leader elected → create accounts, fund, transfer → all 5 nodes show the same state hash.
- Kill the leader → the client retries → the transfer succeeds via the new leader; no double apply.
- A partition through admin commands behaves the way the simulator predicted.
- Run the sim test suite again under ASan/UBSan, and the node under ASan during the integration test.

**Done when:** you can run the full CLI flow against a live 5-node cluster, and killing the leader mid-transfer works correctly (state is in memory; a restarted node starts empty and catches up from the leader).

**Explain it:** why level-triggered epoll; handling partial reads/writes; why transport-level retries aren't needed with Raft; the exactly-once retry story.

---

### Week 5: Persistence: Meta, WAL, Recovery, Snapshots, Compaction, InstallSnapshot

**Learn**
- `man fsync`, `man rename`, `man ftruncate`; why you must also fsync the **directory** after a rename
- "Files are hard" (Dan Luu) and the "All File Systems Are Not Created Equal" paper (OSDI '14): skim for crash-consistency intuition
- Raft paper §7 (log compaction)

**Build (in order)**
1. `storage/file_util`: write-file-atomically, fsync file + dir.
2. `storage/meta_store`; wire it in so term/vote are persisted before any vote response or RequestVote send.
3. `storage/wal`: segmented append, CRC, index→offset map, truncate, scan/recover with torn-tail handling; `fsync` modes `entry|batch|none`.
4. Node startup recovery (6.6 steps 1–4). Replace in-memory storage.
5. `storage/snapshot_store` + state machine (de)serialization; automatic snapshots every 1000 entries; admin `TakeSnapshot`.
6. Log compaction (delete old segments, keep a margin); RaftLog offset handling.
7. InstallSnapshot send (chunked) and receive (temp file, verify, install). Add it to the simulator too (snapshot as bytes).

**Tests**
- WAL: write N records → reopen → same records. Corrupt or truncate the last record at every byte offset → recovery keeps exactly the valid prefix.
- Truncate-then-append works across segment boundaries.
- Meta: crash between write and rename → old or new value, never garbage.
- Integration: `kill -9` a follower under load → restart → catches up and the hash matches.
- `kill -9` **all 5** nodes → restart all → every committed transaction is still there.
- A follower down long enough for the leader to compact past its index → receives InstallSnapshot → hash matches.
- Simulator: crash/restart now preserves the persisted log; all invariants still hold.

**Done when:** any node (or all of them) can be SIGKILLed at any moment and recovers with no committed data lost.

**Explain it:** why term/vote must be durable; torn writes and CRC; group commit; why `commitIndex` doesn't need persisting; snapshot vs log replay trade-off.

---

### Week 6: Correctness Under Faults: Pre-Vote, Demos, Jepsen-Style Checker, Chaos

**Learn**
- Raft thesis §9.6 (pre-vote and disruptive servers)
- Jepsen basics: Kyle Kingsbury's talks/blog (jepsen.io); what "indeterminate" operations are; linearizability in one paragraph

**Build (in order)**
1. **Pre-vote** in RaftCore (+ simulator test: an isolated node rejoining doesn't bump terms or depose the leader).
2. `tools/workload.py` (concurrent clients, op history recording).
3. `tools/checker.py` with all 6 checks from 6.9, including the **independent Python model** of the transfer rules.
4. The three demo scripts (section 9), each printing a clear narrative + final hash comparison.
5. `tools/chaos.py`: seeded random faults for 5 minutes + workload + checker. Run it several times and fix every bug it finds. **Expect bugs here; that is the point.**
6. `tools/timeline.py` (merged log view) to debug chaos failures.

**Tests**
- All three demos pass reliably (run each 10×).
- Chaos runs ≥ 10 times with different seeds and no checker violation; record seeds and results in `results/`.

**Done when:** chaos + checker pass repeatedly; you have saved demo outputs and a list of bugs found and fixed (great interview stories, so keep a `docs/bugs-found.md`).

**Explain it:** what pre-vote fixes; what the checker proves and what it *doesn't* prove (it doesn't test linearizability of reads exhaustively, doesn't cover Byzantine faults, etc.).

---

### Week 7: Benchmarks, Observability, Documentation, Polish (+ buffer)

**Build (in order)**
1. `tools/bench.py` + `tools/plot.py`: all experiments in section 10, each repeated ≥ 3 times, with graphs saved to `docs/img/`.
2. Final structured-log event coverage; `status --all` output polished.
3. **Stretch, only if everything above is done:** dashboard (`rich`), then leader lease, then REST API.
4. `docs/design.md` (architecture, decisions, trade-offs, limitations), `docs/protocol.md`, `docs/benchmarks.md` (methodology: hardware, localhost caveats, fsync on the same disk for all nodes).
5. README: one-paragraph pitch, architecture diagram, quickstart (build → start cluster → run demo), results table, graphs, "what I'd do next".
6. Record terminal demos (asciinema), e.g. leader failure and partition healing.
7. Fill in the resume bullets with **your real measured numbers**.

**Done when:** a stranger can clone, build, run the cluster, run the demos, and read the results in under 10 minutes.

**Use remaining time as buffer.** Most likely, weeks 3 and 5 overrun; that is what this buffer is for.

---

## 9. The Three Demos

**Demo 1: Double-spend prevention**
- Setup: `alice` has 100.
- (a) The **same nonce**, two different signed transfers (80 → bob, 80 → carol), submitted **simultaneously to two different nodes**. Result: exactly one is OK; the other is `BAD_NONCE`. Alice = 20.
- (b) **Consecutive nonces**, 80 + 80. Result: the first is OK, the second is `INSUFFICIENT_FUNDS` (its nonce is still consumed).
- (c) **Replay** of an already-committed signed transaction → `BAD_NONCE`. A transaction with a tampered amount → `BAD_SIGNATURE`.
- Show: identical state hashes on all 5 nodes; conservation holds.

**Demo 2: Leader failure**
- Run a workload of continuous transfers. `SIGKILL` the leader.
- Show: a new leader elected (measure the time from kill → first new commit); clients retry and continue; **no acknowledged transaction lost; none applied twice**. Restart the old leader → it recovers from its WAL/snapshot, catches up, and its hash matches.

**Demo 3: Network partition**
- Partition `{1,2}` vs `{3,4,5}`, with the leader on the minority side (`{1,2}`).
- Show: the minority leader can't commit (its clients time out → `info`); linearizable reads on it don't return stale data; the majority elects a new leader and keeps committing. **Pre-vote** keeps the minority followers from inflating terms.
- Heal: the old leader sees the higher term and steps down; its uncommitted entries are overwritten; all 5 hashes converge; the checker passes.

---

## 10. Benchmarks to Produce

| # | Experiment | Vary | Measure | Graph |
|---|---|---|---|---|
| B1 | Commit latency vs load | concurrent clients 1, 4, 16, 64 | p50/p95/p99 latency, throughput (tx/s) | latency vs throughput curve |
| B2 | Batching | `max_entries_per_append` 1, 8, 64, 256 | throughput, p99 | bar chart |
| B3 | Durability cost | `fsync=entry` vs `batch` vs `none` | throughput, p99 | bar chart |
| B4 | Recovery time | log length 10k / 50k / 100k; with vs without snapshots | time from restart → caught up | line chart |
| B5 | Failover time | 50 leader kills | time from kill → new leader → first commit; distribution | histogram/CDF |
| B6 (opt.) | Pre-vote effect | rejoining isolated node, with/without pre-vote | # disruptive elections, term inflation | before/after |

**Methodology notes for docs:** all nodes run on one machine and share a disk, so the absolute numbers are not production numbers. Focus on **relative** comparisons. State the hardware, warm-up period, run length, and repetitions.

---

## 11. Testing Strategy

| Layer | Tool | What |
|---|---|---|
| Unit | GoogleTest | crypto, canonical encoding, framing, state machine rules, WAL/meta/snapshot files |
| Deterministic simulation | GoogleTest + `sim_network` | Raft safety invariants under random faults across 1000s of seeds. **This is where most Raft bugs are found.** |
| Integration | Python + real processes | end-to-end CLI flows, kill/restart, partitions |
| Chaos + checker | Python | long randomized runs validated by the independent model |
| Sanitizers | ASan/UBSan builds | run unit + sim + integration under sanitizers weekly |

Every bug found → first write a failing test (sim seed or unit test) → then fix.

---

## 12. Working Rules

1. **Figure 2 is law.** When unsure, re-read it rather than inventing behavior.
2. **Write the doc paragraph before the code.** For each module, add a short section to `docs/design.md` first. If you can't explain it, you're not ready to write it.
3. **Small commits** with meaningful messages, one logical step each. The git history becomes evidence of understanding.
4. **Assert invariants in code** (`assert`/`LEDGER_CHECK`): never truncating committed entries, commitIndex monotonic, term monotonic, etc.
5. **Never add threads** to the node without a written reason.
6. **Log every state transition** with term and index. Your future debugging self depends on it.
7. **Keep `docs/bugs-found.md`:** symptom, root cause, fix, test added. These are your best interview stories.
8. Weekly: re-read your own `raft-notes.md` and explain one component out loud as if in an interview.

---

## 13. Interview Concepts Checklist

You should be able to explain each of these **without notes** by the end:

- [ ] Why majority quorums guarantee overlap (and why 5 nodes tolerate 2 failures)
- [ ] Leader election, randomized timeouts, split votes
- [ ] Why `currentTerm`/`votedFor`/log must be durable before replying
- [ ] Log Matching property and the prevLogIndex/prevLogTerm check
- [ ] Figure 8: why leaders only commit current-term entries by counting replicas
- [ ] Leader no-op on election: why it's needed
- [ ] Pre-vote: the problem and the fix
- [ ] ReadIndex vs lease reads vs reads-through-log: guarantees and costs
- [ ] State machine replication and determinism requirements
- [ ] Canonical encoding for signatures; domain separation; replay protection with chain_id + nonce
- [ ] Nonce semantics: which failures consume nonces and why
- [ ] Exactly-once transfer semantics under client retries and failover
- [ ] WAL, CRC, torn writes, fsync, directory fsync, atomic rename
- [ ] Group commit: durability vs throughput
- [ ] Snapshots, log compaction, InstallSnapshot
- [ ] epoll level vs edge triggered; non-blocking I/O; back-pressure
- [ ] CAP: what your system chooses during a partition (CP: the minority is unavailable)
- [ ] What crash-fault tolerance does **not** cover (Byzantine) and what would change for BFT (3f+1, PBFT/HotStuff)
- [ ] Deterministic simulation testing and Jepsen-style checking: what each proves

---

## 14. Resume Bullets

Fill in X/Y/Z with **your measured numbers** in week 7.

```
Ledger — Raft-Based Distributed Payment Ledger                          C++17
• Built a crash-fault-tolerant payment ledger from scratch on a 5-node cluster
  using Raft (leader election, log replication, pre-vote, ReadIndex reads)
  with Ed25519-signed transfers and nonce-based exactly-once semantics
• Implemented an epoll-based event-driven network layer and a CRC-checked
  write-ahead log with group commit, snapshots, log compaction and
  InstallSnapshot; recovery in Xms with snapshots vs Yms with full replay
• Verified safety with a deterministic fault-injection simulator (N seeds) and
  a Jepsen-style checker under node crashes, leader failover and network
  partitions; zero double-spends, identical state hashes across all nodes
• Achieved p99 commit latency of Xms at Y tx/s with Z concurrent clients;
  batching improved throughput by A× and group commit by B× over per-entry fsync
```
