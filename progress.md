# QuorumPay: Progress

_Last updated: 2026-10-29_

This file shows **what is built, what is left, and how the working parts fit together today**.
The full design is in [`project_details.md`](project_details.md).

---

## 1. Overall progress

Counted against the **49 "Build" items** in the plan (§8 of `project_details.md`):

```
Overall   [████████████████░░░░░░░░░░░░░░]  ~54%   (≈26 of 49 build items)
```

```mermaid
pie showData
    title Plan build items
    "Done" : 26
    "Left" : 23
```

> By *effort* it's closer to **45%**: the Raft core and the ledger are done, but persistence (Week 5) and the fault-testing week (Week 6) haven't started.

### By plan week

| Week | Topic | Progress | Status |
|---|---|---|---|
| 1 | Foundations: crypto, transactions, state machine | `█████████░` 6.75 / 7 | ✅ crypto, transactions, state machine, keygen, genesis · client request messages (SubmitTx…) come in step 6 |
| 2 | Raft: leader election + simulator | `██████████` 6 / 6 | ✅ done |
| 3 | Raft: replication, commit, apply, ReadIndex | `█████████░` 6 / 7 | ✅ Raft core done · RETRY tracking moved to step 6 |
| 4 | Network + node + CLI | `████████░░` 7 / 9 | 🟨 network + node (persist → send → apply) done; client service + CLI left |
| 5 | Persistence: WAL, snapshots, recovery | `░░░░░░░░░░` 0 / 7 | ⬜ |
| 6 | Pre-vote, checker, demos, chaos | `░░░░░░░░░░` 0 / 6 | ⬜ |
| 7 | Benchmarks + docs | `█░░░░░░░░░` 0.5 / 7 | 🟨 README only |

---

## 2. Build order (bottom-up)

We build one layer at a time. Each step adds one idea and ends with something you can run.

```mermaid
flowchart BT
    S1["1. Network<br/>nodes can talk"]:::done
    S2["2. Leader election (simulator)<br/>agree on a leader"]:::done
    S3["3. Election on real cluster<br/>real processes pick a leader"]:::done
    S4["4. Log replication + commit + ReadIndex<br/>agree on a list of entries"]:::done
    S5["5. Ledger: crypto + tx + state machine<br/>what entries mean"]:::done
    S6["6. Client service + ledger-cli<br/>users send money"]:::next
    S7["7. Persistence: WAL + snapshots<br/>survive crashes"]:::todo
    S8["8. Pre-vote, checker, demos, chaos<br/>prove it's correct"]:::todo
    S9["9. Benchmarks + docs<br/>how fast is it"]:::todo
    S1 --> S2 --> S3 --> S4 --> S5 --> S6 --> S7 --> S8 --> S9

    classDef done fill:#2e7d32,color:#fff,stroke:#1b5e20
    classDef next fill:#f9a825,color:#000,stroke:#f57f17
    classDef todo fill:#eceff1,color:#37474f,stroke:#90a4ae
```

| Step | Plan week | Status |
|---|---|---|
| 1. Network | Week 4 (items 1–4) | ✅ |
| 2. Election in simulator | Week 2 | ✅ |
| 3. Election on real cluster | Week 4 (item 5, partly) | ✅ |
| 4. Log replication + commit + ReadIndex | Week 3 | ✅ |
| 5. Ledger | Week 1 (+ Week 3 item 4) | ✅ |
| 6. Client service + CLI | Week 4 (items 6, 8) + Week 3 item 5 (RETRY) | 🔜 **next** |
| 7. Persistence | Week 5 | ⬜ |
| 8. Correctness under faults | Week 6 | ⬜ |
| 9. Benchmarks + docs | Week 7 | ⬜ |

---

## 3. Module status

```mermaid
flowchart LR
    subgraph built["Built and tested"]
        common["common/<br/>config, logger, types"]
        net["net/<br/>event loop, framing,<br/>connections, peer mesh,<br/>fault filter"]
        raft["raft/<br/>RaftCore (election +<br/>replication + commit +<br/>ReadIndex), RaftLog,<br/>Progress, Ready"]
        server["server/node<br/>hosts Raft + ledger + admin"]
        admin["client/admin_main<br/>ledger-admin"]
        crypto["crypto/<br/>Ed25519, SHA-256, CRC32,<br/>key files"]
        ledger["ledger/<br/>transactions, accounts,<br/>state machine, state hash"]
    end
    subgraph partial["Partly built"]
        cli["client/ ledger-cli<br/>keygen ✅ · cluster commands ⬜"]
    end
    subgraph stubs["Stubs only (empty files)"]
        storage["storage/"]
        svc["server/client_service"]
    end
    common --> net --> server
    common --> raft --> server
    common --> crypto --> ledger --> server
    crypto -.-> storage

    style built fill:#e8f5e9,stroke:#2e7d32
    style partial fill:#fff8e1,stroke:#f9a825
    style stubs fill:#f5f5f5,stroke:#9e9e9e,stroke-dasharray: 5 5
```

| Module | Files | State |
|---|---|---|
| `common/` | `types.h`, `config` (+ genesis), `logger`, `clock.h`, `status.h`, `hex.h` | ✅ |
| `net/` | `event_loop`, `framing`, `connection`, `listener`, `peer_manager`, `fault_filter` | ✅ |
| `raft/` | `raft_core`, `raft_log`, `progress.h`, `read_index`, `ready.h` | ✅ election + replication + commit + ReadIndex |
| `server/` | `node`, `main` | ✅ runs Raft, loads genesis, applies entries to the ledger, `propose`/`read` test commands · `client_service`, `admin_service` are stubs |
| `client/` | `admin_main` (`ledger-admin`), `keystore`, `main` (`ledger-cli keygen`/`pubkey`) | ✅ · `client_lib` and the cluster commands come in step 6 |
| `crypto/` | `hash` (CRC32, SHA-256), `keys` (Ed25519 pairs, key files 0600), `signer` (sign/verify) | ✅ libsodium |
| `ledger/` | `transaction`, `encoding.h`, `account_store`, `state_machine`, `state_hash` | ✅ 7-step validation, nonces, tx_index, history, SHA-256 state hash |
| `storage/` | — | ⬜ stubs |
| `proto/` | `peer`, `client`, `envelope`, `raft`, `ledger` | ✅ (client SubmitTx/GetBalance messages come in step 6) |
| `tools/` | `cluster.py`, `make_genesis.py` | ✅ · faults/workload/checker/chaos/bench/plot/demos are stubs |

---

## 4. How it works today

### 4.1 The cluster

Five `ledger-node` processes on localhost form a **full mesh**. Each pair uses **two TCP connections**, one dialed in each
direction. `ledger-admin` and `tools/cluster.py` talk to each node's client port.

```mermaid
flowchart TB
    subgraph cluster["5-node cluster (localhost)"]
        N1(("Node 1<br/>LEADER<br/>term 1")):::leader
        N2(("Node 2<br/>follower"))
        N3(("Node 3<br/>follower"))
        N4(("Node 4<br/>follower"))
        N5(("Node 5<br/>follower"))
        N1 <--> N2
        N1 <--> N3
        N1 <--> N4
        N1 <--> N5
        N2 <--> N3
        N2 <--> N4
        N2 <--> N5
        N3 <--> N4
        N3 <--> N5
        N4 <--> N5
    end
    T["tools/cluster.py<br/>start / kill / partition / status"] --> A["ledger-admin"]
    A -. "client port 800N" .-> cluster

    classDef leader fill:#2e7d32,color:#fff
```

| Port | Used for |
|---|---|
| `700N` (peer port) | Hello handshake, Ping/Pong health probe, **Raft messages** |
| `800N` (client port) | Admin: `status`, `block`, `unblock`, `unblock-all`, `propose <text>` and `read` (test hooks) |

### 4.2 Inside one node

Everything runs on **one thread**: an epoll event loop. Raft is a **pure state machine** that never touches sockets or
files. The node feeds it inputs and carries out its outputs.

```
┌──────────────────────────── ledger-node (one thread) ───────────────────────────┐
│                                                                                 │
│   EventLoop (epoll + timerfd)                                                   │
│      │                                                                          │
│      ├── every 10ms ──▶ on_tick() ──▶ raft_.tick() ──────────┐                  │
│      │                         └──▶ peers_.tick() (redial)    │                  │
│      │                         └──▶ send Pings (every 50ms)   │                  │
│      │                                                        ▼                  │
│      ├── peer socket ──▶ PeerManager ──▶ Envelope            process_ready()     │
│      │                    + FaultFilter    ├─ raft ──▶ raft_.step() ─┐  1. persist│
│      │                                     ├─ ping ──▶ reply Pong    │  (memory) │
│      │                                     └─ pong ──▶ RTT/health    │  2. send  │
│      │                                                               │  3. apply │
│      │                                                               └─▶ (ledger)│
│      └── client socket ─▶ admin: status / block / propose / read ─▶ propose()   │
│                                         (reads wait in pending_reads_) read_index()│
│                                                                                 │
│   RaftCore (pure: no I/O)          In-memory "disk" (until step 7)              │
│   role, term, votedFor, log,       hard_state_ {term, votedFor}, log_           │
│   commitIndex, Progress per peer   ledger_: StateMachine (from genesis.json)     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 The Ready loop (the main design idea)

```mermaid
flowchart LR
    I1["tick()"] --> C
    I2["step(msg)"] --> C
    I3["propose(data)"] --> C
    C["RaftCore<br/>(pure logic)"] --> R["Ready<br/>• hard_state (term, vote)<br/>• new log entries<br/>• messages<br/>• committed entries<br/>• reads ready / failed"]
    R --> P["1. Persist<br/>(memory now, fsync in step 7)"]
    P --> S["2. Send<br/>(PeerManager)"]
    S --> A["3. Apply<br/>(ledger state machine)"]
    A --> RD["4. Answer reads<br/>(state is now ≥ readIndex)"]
```

**Why persist before send:** a vote must be saved *before* the candidate hears about it. Otherwise a crash and restart
could let the node vote twice in one term, and two leaders could be elected.

### 4.4 Raft roles

```mermaid
stateDiagram-v2
    [*] --> Follower: start
    Follower --> Candidate: election timeout<br/>(random 150–300ms)
    Candidate --> Candidate: split vote →<br/>timeout, term+1
    Candidate --> Leader: votes from majority (3 of 5)
    Candidate --> Follower: AppendEntries from<br/>leader of same term
    Leader --> Follower: sees higher term
    Candidate --> Follower: sees higher term
```

### 4.5 An election, message by message

```mermaid
sequenceDiagram
    participant N3 as Node 3
    participant N1 as Node 1
    participant N2 as Node 2
    participant N4 as Node 4
    participant N5 as Node 5
    Note over N3: no heartbeat for 150–300ms<br/>→ candidate, term 1, votes for self
    N3->>N1: RequestVote(term 1, lastLog)
    N3->>N2: RequestVote(term 1, lastLog)
    N3->>N4: RequestVote(term 1, lastLog)
    N3->>N5: RequestVote(term 1, lastLog)
    N1-->>N3: granted
    N2-->>N3: granted
    Note over N3: 3 of 5 votes → LEADER<br/>appends no-op entry
    N3->>N1: AppendEntries (heartbeat)
    N3->>N2: AppendEntries (heartbeat)
    N3->>N4: AppendEntries (heartbeat)
    N3->>N5: AppendEntries (heartbeat)
    Note over N1,N5: every 50ms: heartbeats reset<br/>followers' election timers
```

**A vote is granted only if:** the request's term is current, the voter hasn't voted for anyone else this term, **and** the
candidate's log is at least as up to date as the voter's (last term first, then length).

### 4.6 Replicating one entry

```mermaid
sequenceDiagram
    participant A as ledger-admin
    participant L as Leader
    participant F1 as Follower
    participant F2 as Follower
    participant F3 as Follower
    participant F4 as Follower
    A->>L: propose("payment")
    L->>L: append at index 5 (term 2), persist
    L-->>A: accepted (index 5, term 2)
    L->>F1: AppendEntries(prev 4/term 2, [5], commit 4)
    L->>F2: AppendEntries(prev 4/term 2, [5], commit 4)
    L->>F3: AppendEntries(...)
    L->>F4: AppendEntries(...)
    F1->>F1: prev matches → append, persist
    F1-->>L: success, match 5
    F2-->>L: success, match 5
    Note over L: leader + 2 followers = 3 of 5,<br/>entry is from current term → COMMIT 5, apply
    L->>F1: next AppendEntries / heartbeat carries commit 5
    Note over F1,F4: followers apply up to 5
```

**The rules that make it safe:**
- **Consistency check:** a follower accepts entries only if it has the entry just before them (`prev index + term`).
  By induction, the whole log before that point then matches the leader (**Log Matching**).
- **Conflicts:** an entry with the same index but a different term is an uncommitted leftover from a deposed leader.
  The follower truncates from there and takes the leader's entries.
- **Fast backtracking:** a rejection includes a hint (`conflict_term`, `conflict_index`), so the leader skips a
  whole conflicting term in one round trip instead of going back one entry at a time.
- **Commit rule (Figure 8):** the leader commits index N only if a majority stores it **and** `log[N].term ==
  currentTerm`. An older-term entry on a majority can still be overwritten, so it is committed only indirectly,
  once a current-term entry after it commits. That's why a new leader appends a no-op.
- **Follower commit:** `min(leader_commit, last new entry)`, never past what this message verified.

### 4.7 How a divergent log is repaired

```
Before heal (old leader 1 was cut off and appended entries it could never commit):

  index:      1   2   3   4   5   6
  node 1:    [1] [1] [1] [1] [1]          ← term-1 entries 3-5: uncommitted leftovers
  node 3:    [1] [1] [2] [2] [2] [2]      ← new leader (term 2), 3-6 committed

1. AppendEntries(prev=6/term 2)  → node 1: "I have no index 6"    → reject, conflict_index = 6
2. AppendEntries(prev=5/term 2)  → node 1: "my 5 has term 1"      → reject, conflict_term = 1,
                                                                     conflict_index = 1 (start of its term-1 run)
3. Leader's last term-1 entry is index 2, so the logs must agree up to 2 → resend from 3
4. AppendEntries(prev=2/term 1, [3,4,5,6]) → matches → node 1 truncates its 3-5, appends 3-6 → identical
```

### 4.8 Linearizable reads (ReadIndex)

A leader can't just answer from its own state: it may have been deposed without knowing it, and then its data is stale.

```mermaid
sequenceDiagram
    participant C as client
    participant L as Leader
    participant F1 as Follower
    participant F2 as Follower
    C->>L: read
    Note over L: 1. readIndex = commitIndex (= 62)<br/>(only once an entry of its own term is committed,<br/>otherwise its commitIndex may lag)
    L->>F1: AppendEntries(read_ctx = 7)
    L->>F2: AppendEntries(read_ctx = 7)
    F1-->>L: reply(read_ctx = 7)
    F2-->>L: reply(read_ctx = 7)
    Note over L: 2. leader + 2 = majority answered a message sent<br/>AFTER the read started → no newer leader existed
    Note over L: 3. wait until applied ≥ 62
    L-->>C: answer (state at index ≥ 62)
```

**Why each rule matters (each was tested by breaking it):**

| Rule | Without it |
|---|---|
| Confirm with a majority | A cut-off old leader answers with stale data |
| Count only replies to messages sent *after* the read started (`read_ctx`) | An old reply "confirms" leadership from before a new leader took over |
| New leader waits until its no-op commits | Its commit index may be behind writes the previous leader already committed |

A cut-off leader never gets the majority, so its reads hang. When it later sees a higher term it steps down and fails them, and the client retries on the new leader.

### 4.9 The ledger (what the log entries mean)

Each Raft log entry carries one **signed transaction**. Every node applies the committed entries in order to the same
**deterministic state machine**, built from the same `genesis.json`, so all nodes end up with identical balances.

```mermaid
flowchart LR
    W["ledger-cli<br/>(holds the secret key)"] -- "sign(signing_bytes)" --> T["Transaction<br/>type · chain_id · sender ·<br/>receiver · amount · nonce ·<br/>signature"]
    T -- "proposed as a<br/>log entry" --> R["Raft log<br/>(committed, ordered)"]
    R --> SM["StateMachine.apply()<br/>on every node"]
    SM --> H["SHA-256 state hash<br/>(same on all nodes)"]
```

**What gets signed:** a fixed byte layout, not protobuf bytes (protobuf isn't guaranteed to be byte-identical everywhere):
`"LEDGER_TX_V1" | chain_id | type | sender | receiver | amount | nonce | [public key]`, with length prefixes.
The domain tag, `chain_id` and `nonce` together stop a signature being reused elsewhere, on another ledger, or twice.

**Validation pipeline** (the first failing check decides the result):

```
 1. well-formed?  ──no──▶ MALFORMED            ┐
 2. sender exists? ─no──▶ UNKNOWN_ACCOUNT      │ nothing changes:
 3. signature ok?  ─no──▶ BAD_SIGNATURE        │ junk can't burn someone else's nonce
 4. nonce = next?  ─no──▶ BAD_NONCE            ┘ (this is what stops double spends + replays)
    ── sender proven: nonce is used up from here ──
 5. receiver exists? no─▶ UNKNOWN_RECEIVER     ┐ nonce consumed, no money moves:
 6. enough funds?  ─no──▶ INSUFFICIENT_FUNDS   ┘ this signed transfer can never be replayed
 7. move the money ─────▶ OK
```

**Exactly-once:** a client that retries the same signed bytes gets `BAD_NONCE` if the original already committed, and
the original `OK` stays on record under its `tx_id` (= SHA-256 of signing bytes + signature). A transfer can't apply twice.

**Determinism:** accounts are kept in a `std::map` (sorted), with no clock, randomness or unordered iteration, so the same
entries always produce the same state hash. Total supply never changes (only genesis creates money).

### 4.10 What happens on failures (verified by tests)

```mermaid
flowchart LR
    subgraph A["Leader crash"]
        a1["Leader 1 killed"] --> a2["Followers time out"] --> a3["New leader, term 2"] --> a4["Node 1 restarts<br/>→ follower of new leader"]
    end
    subgraph B["Partition {1,2} | {3,4,5}"]
        b1["Old leader in minority<br/>still thinks it leads (old term)<br/>but can't commit"] 
        b2["Majority elects new leader<br/>(higher term)"]
        b3["Heal → old leader sees<br/>higher term → steps down"]
        b1 --> b3
        b2 --> b3
    end
```

---

## 5. Tests

| Suite | Count | What it proves |
|---|---|---|
| `unit_tests` (network) | 26 | framing, connections, peer mesh, reconnect, fault filter, config |
| `unit_tests` (crypto + ledger) | 38 | CRC32; SHA-256 FIPS vectors; **Ed25519 RFC 8032 vector**, tamper/wrong-key rejection; key files (0600, no overwrite); genesis; signing bytes pinned byte-for-byte; **one test per validation-table row**, double spend, replay keeps original result, determinism, conservation |
| `unit_tests` (`RaftCore`) | 32 | votes, step-down, no-op, heartbeats; append/reject/truncate, conflict hints, backtracking, majority commit, **Figure 8**, follower commit bound; ReadIndex: majority ack, stale-reply ctx, new-leader wait, fail on step-down |
| `raft_sim_tests` | 15 | election (1000 seeds), crash, partition, loss/dup/reorder, split votes; **replication + commit**, crashed follower catches up, divergent log repaired, **all 5 safety properties under random crashes + partitions + proposals** (300 seeds); **stale leader never serves reads**; reads linearizable under random faults; **ledger on Raft: double spend under failover applies exactly once** (200 seeds); ledger consistent under random faults with real signed transfers (100 seeds) |
| `test_network.py` | 1 script | 5 real processes: mesh, kill/restart, partition/heal |
| `test_election.py` | 1 script | 5 real processes: election, failover, restart without disruption, partition, heal |
| `test_replication.py` | 1 script | 5 real processes: entries applied everywhere (same ledger state hash), follower catch-up, minority leader can't commit or answer reads, heal overwrites its entries |

```sh
cmake --build build -j && ctest --test-dir build      # 111 unit + sim tests (also pass under ASan/UBSan)
python3 tests/integration/test_network.py
python3 tests/integration/test_election.py
python3 tests/integration/test_replication.py
```

**Safety checks in the simulator** (run continuously, from Figure 3 of the Raft paper):

| Property | Meaning |
|---|---|
| Election Safety | at most one leader per term |
| Leader Append-Only | a leader never overwrites its own entries |
| Log Matching | same index + term ⇒ identical logs up to there |
| Leader Completeness | a new leader has every committed entry |
| State Machine Safety | no two nodes apply different entries at one index |
| Read Linearizability | a served read reflects every write committed before it started |
| Deterministic apply | every node has the same ledger state hash after applying the same index |
| No double spend (in tests) | no (sender, nonce) is applied successfully twice; total supply never changes |

**Proof that the checks work (mutation testing):** each bug below was planted on purpose. The test that caught it is listed.

| Planted bug | Caught by |
|---|---|
| Allow voting twice in a term | sim: Election Safety |
| Skip the up-to-date log check when voting | sim: Leader Completeness |
| Skip the prev-entry consistency check | sim: State Machine Safety |
| Remove the Figure 8 current-term rule | unit: `Figure8OldTermEntryNotCommittedByCounting` (random sim missed it) |
| Follower commits past verified entries | unit: `FollowerCommitsOnlyUpToVerifiedEntries` (random sim missed it) |
| Truncate even when an entry already matches | unit: `StaleShorterAppendDoesNotTruncate` |
| Serve reads without a confirmation round | sim: Read Linearizability + unit tests |
| Don't wait for the new leader's no-op to commit | sim: Read Linearizability + unit test |
| Count replies to messages sent before the read | unit: `ReplyToMessageSentBeforeReadDoesNotConfirmIt` (random sim missed it) |
| Ledger: no nonce check | unit: double spend / replay / row 4 tests · sim: "alice nonce 0 used twice" |
| Ledger: consume nonce before the signature check | unit: `Row3BadSignatureConsumesNoNonce` |
| Ledger: failed transfer doesn't consume its nonce | unit: rows 5–6, consecutive-spend, replay tests |
| Ledger: a retry overwrites the original OK | unit: `ReplayFailsAndKeepsOriginalResult` |
| Ledger: no chain_id check | unit: `Row1Malformed` |

**Bugs found so far:**

| Bug | How found | Fix |
|---|---|---|
| Restarted node deposed a healthy leader (peers' reconnect backoff meant it heard no heartbeats) | `test_election.py` | Redial a peer as soon as it says Hello (+ unit test) |
| Restarted follower never caught up: it lost acknowledged entries (in-memory storage), but the leader never sent below its old `match` | `test_replication.py` | Leader resets `match` when a rejection is at or below it (+ unit test). The real fix is persistence (step 7) |
| `raft_` initialized from members declared after it (undefined behavior) | Code review | Pass empty state explicitly |
| Test called `tick()` on a "crashed" null PeerManager | Unit test crash | Fixed in the test |

---

## 6. What's left

```mermaid
flowchart TB
    subgraph S6["Step 6: Clients (next)"]
        direction LR
        c1["client_service<br/>SubmitTx, GetBalance,<br/>GetHistory, GetTx"] --> c2["proposal tracking<br/>+ RETRY"] --> c3["ledger-cli<br/>create_account, fund,<br/>transfer, balance<br/>(redirect + retry)"]
    end
    subgraph S7["Step 7: Persistence"]
        direction LR
        p1["meta file<br/>(term, vote)"] --> p2["WAL + CRC"] --> p3["snapshots +<br/>InstallSnapshot"]
    end
    S6 --> S7 --> S8["Step 8: pre-vote, checker,<br/>3 demos, chaos"] --> S9["Step 9: benchmarks + docs"]
```

**Known gaps in the current code:**
- Storage is in memory, so a restarted node forgets its vote and its log (fixed in step 7).
- Proposals aren't tracked yet: the `propose` test command answers "accepted", not "committed". Tracking + RETRY come with the client service (step 6).
- There's no way yet to send a signed transfer to the live cluster (the `ledger-cli` cluster commands are step 6), and the repo's `config/genesis.json` has no accounts until `make_genesis.py` is run.
- `OVERFLOW` (validation step 6) can't actually happen, because genesis ensures total supply fits in uint64; it's checked anyway.
- No pre-vote yet: a node that can't hear the leader can still disrupt it with a higher term (fixed in step 8).
- An old leader cut off in a minority doesn't step down by itself (harmless; check-quorum could fix it).
