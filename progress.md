# QuorumPay: Progress

_Last updated: 2026-10-27_

This file shows **what is built, what is left, and how the working parts fit together today**.
The full design is in [`project_details.md`](project_details.md).

---

## 1. Overall progress

Counted against the **49 "Build" items** in the plan (§8 of `project_details.md`):

```
Overall   [██████████░░░░░░░░░░░░░░░░░░░░]  ~31%   (≈15 of 49 build items)
```

```mermaid
pie showData
    title Plan build items
    "Done" : 15
    "Left" : 34
```

> By *effort* it's closer to **25%**, because the heaviest weeks (3, 5, 6) haven't started.

### By plan week

| Week | Topic | Progress | Status |
|---|---|---|---|
| 1 | Foundations: crypto, transactions, state machine | `██░░░░░░░░` 2 / 7 | 🟨 skeleton + framing only |
| 2 | Raft: leader election + simulator | `██████████` 6 / 6 | ✅ done |
| 3 | Raft: replication, commit, apply, ReadIndex | `░░░░░░░░░░` 0 / 7 | ⬜ next |
| 4 | Network + node + CLI | `███████░░░` 6.5 / 9 | 🟨 network + Raft-in-node done; client service + CLI left |
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
    S4["4. Log replication + commit<br/>agree on a list of entries"]:::next
    S5["5. Ledger: crypto + tx + state machine<br/>what entries mean"]:::todo
    S6["6. Client service + ledger-cli<br/>users send money"]:::todo
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
| 4. Log replication + commit | Week 3 | 🔜 **next** |
| 5. Ledger (needs `libsodium-dev`) | Week 1 | ⬜ |
| 6. Client service + CLI | Week 4 (items 6, 8) | ⬜ |
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
        raft["raft/<br/>RaftCore (election),<br/>RaftLog, Ready"]
        server["server/node<br/>hosts Raft + admin"]
        admin["client/admin_main<br/>ledger-admin"]
    end
    subgraph stubs["Stubs only (empty files)"]
        crypto["crypto/"]
        ledger["ledger/"]
        storage["storage/"]
        cli["client/ ledger-cli"]
        svc["server/client_service"]
    end
    common --> net --> server
    common --> raft --> server
    common -.-> crypto -.-> ledger
    crypto -.-> storage

    style built fill:#e8f5e9,stroke:#2e7d32
    style stubs fill:#f5f5f5,stroke:#9e9e9e,stroke-dasharray: 5 5
```

| Module | Files | State |
|---|---|---|
| `common/` | `types.h`, `config`, `logger`, `clock.h` | ✅ (`status.h` is a stub) |
| `net/` | `event_loop`, `framing`, `connection`, `listener`, `peer_manager`, `fault_filter` | ✅ |
| `raft/` | `raft_core`, `raft_log`, `ready.h` | ✅ election only · `progress`, `read_index` are stubs |
| `server/` | `node`, `main` | ✅ runs Raft · `client_service`, `admin_service` are stubs |
| `client/` | `admin_main` (`ledger-admin`) | ✅ · `client_lib`, `keystore`, `ledger-cli` are stubs |
| `crypto/`, `ledger/`, `storage/` | — | ⬜ stubs |
| `proto/` | `peer`, `client`, `envelope`, `raft` | ✅ · `ledger.proto` is empty |

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
| `800N` (client port) | Admin: `status`, `block`, `unblock`, `unblock-all` |

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
│      │                                                               └──▶ via    │
│      │                                                                 PeerManager│
│      └── client socket ─▶ admin: status / block / unblock                       │
│                                                                                 │
│   RaftCore (pure: no I/O)          In-memory "disk" (until step 7)              │
│   role, term, votedFor, log        hard_state_ {term, votedFor}, log_           │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 The Ready loop (the main design idea)

```mermaid
flowchart LR
    I1["tick()"] --> C
    I2["step(msg)"] --> C
    C["RaftCore<br/>(pure logic)"] --> R["Ready<br/>• hard_state (term, vote)<br/>• new log entries<br/>• messages"]
    R --> P["1. Persist<br/>(memory now, fsync in step 7)"]
    P --> S["2. Send<br/>(PeerManager)"]
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

### 4.6 What happens on failures (verified by tests)

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
| `unit_tests` (`RaftCore`) | 13 | vote rules, up-to-date log check, step-down, no-op, heartbeats |
| `raft_sim_tests` | 7 | one leader across **1000 seeds**; crash; partition; 25% loss + duplicates + reordering; split votes; safety under random crashes |
| `test_network.py` | 1 script | 5 real processes: mesh, kill/restart, partition/heal |
| `test_election.py` | 1 script | 5 real processes: election, failover, restart without disruption, partition, heal |

```sh
cmake --build build -j && ctest --test-dir build      # 46 unit + sim tests
python3 tests/integration/test_network.py
python3 tests/integration/test_election.py
```

**Bugs found so far:**

| Bug | How found | Fix |
|---|---|---|
| Restarted node deposed a healthy leader (peers' reconnect backoff meant it heard no heartbeats) | `test_election.py` | Redial a peer as soon as it says Hello (+ unit test) |
| `raft_` initialized from members declared after it (undefined behavior) | Code review | Pass empty state explicitly |
| Test called `tick()` on a "crashed" null PeerManager | Unit test crash | Fixed in the test |

---

## 6. What's left

```mermaid
flowchart TB
    subgraph S4["Step 4: Log replication (next)"]
        direction LR
        r1["propose()"] --> r2["AppendEntries with entries<br/>+ consistency check"] --> r3["commit on majority<br/>(Figure 8 rule)"] --> r4["apply in order"]
    end
    subgraph S5["Step 5: Ledger"]
        direction LR
        l1["Ed25519 keys + signatures"] --> l2["transactions + nonces"] --> l3["7-step validation"] --> l4["state hash"]
    end
    subgraph S6["Step 6: Clients"]
        direction LR
        c1["client_service"] --> c2["ledger-cli<br/>(redirect + retry)"]
    end
    subgraph S7["Step 7: Persistence"]
        direction LR
        p1["meta file<br/>(term, vote)"] --> p2["WAL + CRC"] --> p3["snapshots +<br/>InstallSnapshot"]
    end
    S4 --> S5 --> S6 --> S7 --> S8["Step 8: pre-vote, checker,<br/>3 demos, chaos"] --> S9["Step 9: benchmarks + docs"]
```

**Known gaps in the current code:**
- Storage is in memory, so a restarted node forgets its vote (fixed in step 7).
- No pre-vote yet: a node that can't hear the leader can still disrupt it with a higher term (fixed in step 8).
- An old leader cut off in a minority doesn't step down by itself (harmless; check-quorum could fix it).
- `libsodium-dev` isn't installed yet (needed in step 5).
