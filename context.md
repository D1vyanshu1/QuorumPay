# QuorumPay — Session Context

Running log of work sessions, so any future session (or person) can pick up where we left off.
**Newest session at the bottom.** The "Current State" section is always kept up to date.

---

## Current State (keep this updated)

**Project:** Crash-fault-tolerant payment ledger on 5 nodes using Raft (written from scratch), C++17.
Course assignment: **Topic 24, "Peer-to-Peer Payment Ledger"** in `../Project_topics.pdf`.
Full design + 7-week plan: `project_details.md`.

**Done**
- Networking layer (built first, by choice, before the plan's Week 1 items):
  - `src/net/event_loop` — single-threaded epoll (level-triggered) + timerfd
  - `src/net/framing` — `[u32 big-endian length][protobuf]` frames, 16MB cap
  - `src/net/connection` — non-blocking socket, read/write buffers, partial I/O, 8MB back-pressure
  - `src/net/listener` — accept loop, SO_REUSEADDR
  - `src/net/peer_manager` — full mesh (2 TCP conns per pair), Hello handshake, reconnect with exponential backoff + jitter
  - `src/net/fault_filter` — blocked-peer set to simulate partitions
  - `src/server/node` — Ping/Pong liveness + RTT, admin port (status/block/unblock)
  - `src/client/admin_main` → `ledger-admin` binary
  - `tools/cluster.py` (start/stop/kill/restart/status/partition/heal), `tests/integration/test_network.py`
- Tests: 25 unit tests pass (also under ASan/UBSan); 5-process integration test passes.

- **Full project skeleton** (Session 3): every planned file now exists as a stub (a header with a purpose comment and the week it gets built in). New CMake targets `ledger_crypto`, `ledger_ledger`, `ledger_raft`, `ledger_storage`, `ledger-cli` and `raft_sim_tests` enforce the dependency rule. The build is clean.

- **Raft leader election (Week 2)**: `src/raft/{raft_core,raft_log,ready.h}` and `proto/raft.proto`. Elections, votes, heartbeats, step-down on a higher term, and the no-op entry on election. A pure core with no I/O, using the Ready pattern.
- **Deterministic simulator**: `tests/sim/sim_network` (`SimCluster`). Virtual clock, seeded delay/drop/duplicate/reorder, partition/heal, crash/restart from a simulated disk, and an Election Safety checker that runs after every step.
- **Step 3 done: election on the real cluster.** `ledger-node` hosts `RaftCore` (Raft messages go in `Envelope.raft`), storage is in memory, and role changes are logged as `raft_state`. `status` shows role/term/leader, and `tests/integration/test_election.py` passes.
- **Step 4a done: log replication** (plan Week 3 items 1–4 and 7): propose, Progress, AppendEntries consistency check + truncation + fast backtracking, the commit rule (Figure 8), committed entries in Ready, and node apply (running FNV hash). The sim checks all 5 Raft safety properties. `propose` is an admin test command.
- **Step 4b done: ReadIndex** (Week 3 item 6). The Raft core for Weeks 2–3 is complete except RETRY tracking (moved to step 6).
- Tests: 71 unit+sim pass (also under ASan/UBSan). Earlier line: 62 unit+sim pass (also under ASan/UBSan, ~3.5 min), plus 3 integration scripts (network, election, replication). Earlier line: 46 unit+sim, 2 integration scripts. Earlier line: 45 pass. That is 25 network + 13 `RaftCore` unit (`tests/unit/raft_core_test.cpp`) + 7 sim (up to 1000 seeds each).

**Not done yet** (stubs exist; real code still to write)
- Week 1 items: `crypto/` (Ed25519, SHA-256), `ledger/` (transactions, state machine, 7-step validation)
- Raft (Weeks 2–3), persistence (Week 5), demos/checker (Week 6), benchmarks (Week 7)

**Open items**
- `.gitignore` now lists `context.md`, but the file is **already tracked**, so git will keep tracking it. To really ignore it: `git rm --cached context.md` (the file stays on disk). Otherwise remove that line from `.gitignore`.
- The skeleton + README were committed (`9a120a2`). The Raft election work is uncommitted.
- The user removed the README Roadmap section, but the table of contents still links to `#roadmap` (a dead link).
- protobuf must be installed system-wide: `sudo apt install -y build-essential cmake protobuf-compiler libprotobuf-dev`

**Build order (agreed in Session 3, bottom-up):** 1 network ✅ → 2 election in the sim ✅ → 3 election on the real cluster ✅ → 4 log replication + commit with opaque entries (plan Week 3) → 5 ledger: crypto/tx/state machine (Week 1; needs libsodium) → 6 client service + ledger-cli (rest of Week 4) → 7 persistence (Week 5) → 8 pre-vote, checker, demos, chaos (Week 6) → 9 benchmarks + docs (Week 7).

**Next step:** step 5, the ledger (plan Week 1): `common/status.h` (Result + TxStatus), genesis loader, `crypto/` (Ed25519 keys/sign/verify via libsodium, SHA-256, CRC32), `ledger/transaction` (canonical signing bytes, tx_id, name rules), `ledger/state_machine` (7-step validation, nonces, tx_index, history, state hash), `ledger.proto`, `ledger-cli keygen` + `tools/make_genesis.py`, then put the real state machine into the sim and the node in place of the hash. **Blocker: the user must run `sudo apt install -y libsodium-dev`.**

**Quick commands** (from `QuorumPay/`)
```sh
cmake -S . -B build && cmake --build build -j && ctest --test-dir build
tools/cluster.py start | status | kill 3 | restart 3 | partition 1,2 3,4,5 | heal | stop
python3 tests/integration/test_network.py
```

---

## Session History

### Session 1 — 2026-10-14 → 2026-10-16

**Discussed**
- Explained the project and the plan's first phase (Week 1: build system, crypto, state machine, codec).
- Read `Project_topics.pdf`: it's a list of 55 topics; ours is Topic 24. The plan covers every Topic 24 requirement (4–5 nodes, signed transfers, nonces, Raft, deterministic apply, persistence + snapshots, 3 demos, balance hashes, latency/recovery metrics).
- User decided to **start with the networking layer** instead of Week 1. That's fine because `net/` depends only on `common/`.

**Decisions**
- Use protobuf from the start (an `Envelope` with a `oneof`), not a temporary hand-made format.
- Two TCP connections per node pair (one per direction); replies go back on the incoming connection.
- No transport-level retries: Raft handles message loss.
- Admin commands (`status`, `block`, `unblock`, `unblock-all`) go over the client port via a C++ `ledger-admin` tool, so Python doesn't need protobuf.

**Built:** everything listed under "Done" above. Also `CMakeLists.txt` (FetchContent for nlohmann/json + GoogleTest, `LEDGER_SANITIZE` option), `config/cluster.json`, `proto/{peer,client,envelope}.proto`, `.gitignore`, `.clang-format`, and README build/run instructions.

**Bugs found:** one, in a test (the fixture called `tick()` on a "crashed" null PeerManager). Fixed in the test; the network code was correct.

**Explained to user:** each component and its concepts: why single-threaded, epoll level- vs edge-triggered, TCP as a byte stream (framing), non-blocking I/O, partial writes, MSG_NOSIGNAL, TCP_NODELAY, back-pressure, shared_ptr/weak_ptr lifetimes, the handshake, backoff + jitter, no transport retries, simulating partitions, RTT with only the sender's clock, failure detection by timeout, signalfd.

**User preference:** after writing code, explain what each part does and the concepts behind it (for interviews).

### Session 2 — 2026-10-19

**Discussed**
- User asked for a folder-by-folder walkthrough of everything built so far, with diagrams. Explained (in chat, with ASCII diagrams): the repo layout, the CMake library layering (proto/common → net → binaries/tests), what each file does, how the components fit together inside one node, the 5-node topology (2 TCP connections per pair), the life of one Ping/Pong, the Hello handshake, the reconnect state machine, how partitions are injected, and what each test covers.
- No code changed. Found a stray line at the end of `README.md` (see Open items).
- User hit git's "Please tell me who you are" error on commit. Cause: no `user.name`/`user.email` set in git config (global or repo). Explained that git stamps every commit with an author identity and refuses to commit without one; fix is `git config --global user.name/user.email` (user runs it themselves).

### Session 3 — 2026-10-27

**Discussed**
- Resumed the project. Checked git: the network layer was committed on 2026-10-19 (`7245c5f`), so the "not committed" open item was out of date and has been removed.
- Deleted the stray last line of `README.md` (the "build the model, move it to th..." text).
- Found that `context.md` was added to `.gitignore` after it was already committed. `.gitignore` only affects untracked files, so this line does nothing on its own (see Open items).
- **Built the full project skeleton**, following §4 of `project_details.md`. A one-off generator script created only missing files, so no existing file was touched. Stubs: `src/{crypto,ledger,raft,storage}/*`, `src/common/status.h`, `src/server/{client,admin}_service`, `src/client/{client_lib,keystore,main}`, `proto/{raft,ledger}.proto`, `config/genesis.json`, `tests/unit/{crypto,state_machine,wal}_test.cpp`, `tests/sim/*`, `tools/{faults,workload,checker,chaos,bench,plot,make_genesis}.py`, `tools/demos/*`, `docs/*.md`.
- Kept our existing deviations from the plan's layout: `proto/peer.proto` + `envelope.proto`, and `src/client/admin_main.cpp`.
- CMake now has one static library per module. Link edges follow the dependency rule; raft links only common + proto. libsodium is **not** added yet (that comes when crypto is implemented).
- Verified: the build is clean with `-Werror`, and 25/25 tests pass.

- **Rewrote `README.md`** as a GitHub showcase for the whole project. It has badges, a features table, an architecture diagram, Mermaid diagrams (the mesh and a payment sequence), design decisions, the validation pipeline, persistence, build/usage, demos, testing, benchmarks, the structure and references. It describes the complete planned system, plus a **Roadmap checklist** that shows honestly what is done so far. It contains **no invented benchmark numbers**; real results go in `docs/benchmarks.md` later.
- README notes: it lists `libsodium-dev` as a prerequisite (CMake doesn't need it yet). The `ledger-cli` commands in it are planned, not yet working.
- **Built Raft leader election (Week 2 of the plan).**
  - `RaftCore` API: `tick()`, `step(msg)`, `has_ready()`, `ready()`. `Ready` = {hard_state if changed, new entries, messages}; the host persists, then sends. `advance()` from the plan is not needed yet (`ready()` hands over and clears its contents); it comes back with apply in Week 3.
  - Timing in ticks: 10ms per tick, election timeout 15–30 ticks, heartbeat every 5. The RNG is injected via `RaftConfig::seed`.
  - Heartbeats are empty AppendEntries (prev_log_index = 0, which always matches). Followers reply success; the full log-matching check is Week 3.
  - Pre-vote is deferred to Week 6 as planned (the proto field `pre_vote` is reserved).
  - Known behavior, tested on purpose: an old leader cut off in a minority partition still believes it is leader in its old term until it hears a higher term. This is harmless (it can't commit). Check-quorum would fix it.
- Sim tests: one leader across 1000 seeds and stable afterwards; a new higher-term leader after a crash; the minority never elects; convergence with 25% loss + 10% duplicates + reordering; fixed timeouts → split votes forever (why timeouts are randomized); restart keeps term/vote; election safety under random crashes.
- **Mutation check:** temporarily allowing double voting made the checker report "two leaders in term 1". This proves the invariant check works. The code was then restored.
- Small fix during the build: a `-Werror=dangling-else` error from an `if` around `EXPECT_EQ` (the gtest macro expands to an if/else); fixed by adding braces.

- User asked for overall project progress. Estimate given: about 25–30% of the plan's core scope. Done: the network layer (most of Week 4), the skeleton, Raft election (Week 2). Not started: Week 1 crypto/ledger, Week 3 replication, Week 5 persistence, most of Weeks 6–7.
- User asked for progress against the plan's own checklist. Item-by-item count: about 15 of the 49 "Build" items done (~30% by count, ~25% by effort, because Weeks 3/5/6 are the heaviest). Week 2 is fully done. Week 1: only the skeleton/build system, part of common (no Result/status, no genesis loader), and framing. Week 4: items 1–4, 7, 9 done; 5 partial (node runs Ping/Pong, not Raft); 6 and 8 not done. Missing from Week 2's "Learn": `docs/raft-notes.md`, written by the user.
- User pointed out that Week 1 (foundations) was skipped. Explained why: in Session 1 the user chose networking first, and then I recommended Raft election by following context.md's "next step" without checking it against the plan (my oversight). **Decision: do Week 1 next, before Week 3**, because Week 3 step 4 needs the real state machine.
- Blocker: libsodium is not installed. The user must run `sudo apt install -y libsodium-dev`.
- Explained what comes after the network layer in the plan. Networking is Week 4 items 1–4. Then come Week 4 items 5–9 (node runs Raft, client service, admin, CLI, tooling), which depend on Weeks 1–3; then Week 5 persistence, Week 6 fault tolerance/demos, Week 7 benchmarks/docs.
- User asked for a step-by-step order that builds on the network layer and is easy to follow. **Proposed bottom-up order** (each layer adds one idea and ends with something you can run or see):
  1 network ✅ → 2 leader election (sim) ✅ → 3 election on the real cluster (Raft inside ledger-node instead of Ping/Pong) → 4 log replication + commit, using opaque byte entries (Week 3) → 5 ledger: crypto, transactions, state machine (Week 1) → 6 client service + ledger-cli: end-to-end payments (rest of Week 4) → 7 persistence (Week 5) → 8 pre-vote, checker, demos, chaos (Week 6) → 9 benchmarks + docs (Week 7).
  This changes the earlier "Week 1 next" suggestion: libsodium isn't needed until step 5. Waiting for the user to confirm.
- User agreed to the bottom-up build order (see Current State).
- **Step 3: Raft on the real cluster.**
  - `proto/envelope.proto`: `RaftMessage raft = 4`. `client.proto` StatusResp: role, term, leader, last_log_index (shown by `ledger-admin` JSON and `cluster.py status`).
  - `server/node`: `make_raft_config()` (real nodes seed from `random_device`), and `process_ready()` persists to the in-memory `hard_state_`/`log_` and then sends each message via `peers_.send`. Incoming Raft messages are checked: `from` must match the handshake identity. Ping/Pong is kept as a network-health probe (RTT/reachability for status and test_network).
  - Known gap until step 7: storage is in memory, so a restarted node forgets its vote. A double vote is possible in theory.
  - **Bug caught in review:** `raft_` was initialized from `hard_state_`/`log_`, which are declared after it (C++ constructs members in declaration order, so this read uninitialized members = UB). Now it passes empty values.
  - **Bug found by the integration test:** restarting a killed node deposed a healthy leader (term 2→4). Cause: peers' outbound connections to the dead node were in reconnect backoff (up to 1s), so the restarted node could send but heard no heartbeats, timed out, and its higher-term RequestVotes deposed the leader. **Fix:** `PeerManager::handle_hello` redials a peer immediately when it says Hello. Unit test `RedialsAtOnceWhenRestartedPeerSaysHello` (fails without the fix: 819ms vs < 300ms). test_election now asserts that a restart causes no election. Pre-vote (step 8) is the full fix for a node that truly can't hear the leader.

- Created **`progress.md`**: overall/per-week progress (~15 of 49 build items, ~31%), build-order flowchart, module status, and diagrams of the current system (cluster mesh, node internals, the Ready loop, the Raft role state machine, an election sequence, failure scenarios), plus the test table, bugs found, what's left, and known gaps. Mermaid diagrams render on GitHub. **Keep it updated after each step.**

### Session 4 — 2026-10-29

**Step 4a: Raft log replication** (plan Week 3 items 1–4, 7; item 5 RETRY moved to step 6; item 6 ReadIndex is 4b).
- `raft_log`: `truncate_from`, `slice`, `first_index_of_term_at`, `last_index_of_term`. `progress.h`: `Progress{match, next, in_flight}`; at most one entry batch in flight per follower, and the heartbeat force-resends (this covers loss).
- `ready.h`: `committed_entries`. **`entries` semantics: they replace stored entries from `entries.front().index()` on** (the host truncates, then appends). Host order: persist → send → apply. No `advance()`: `ready()` hands over everything and sets applied = commit.
- `raft_core`: `propose()` → `ProposeResult{index, term}`; `send_append`/`broadcast_append` (the heartbeat now carries entries); follower AE handling (too short → conflict_index = last+1; term mismatch → conflict_term + first index of that run; skip matching entries, never truncate on a stale/duplicate message; assert never truncate committed; commit = min(leader_commit, last NEW entry)); leader response handling (match only grows normally; backtrack via hints); `maybe_commit` (quorum-th largest match + current-term check = Figure 8). The no-op on election commits earlier-term leftovers.
- Sim: apply stage, `propose`/`propose_on`, `run_until_converged`, persisted-log truncation, and checks for **all 5 properties** (Election Safety, Leader Append-Only, Log Matching over disks every tick, Leader Completeness when a new leader appears, State Machine Safety via a global applied history). New tests: replicate+commit (200 seeds), crashed follower catches up, deposed leader's divergent log repaired, all invariants under random crash/partition/proposals (300 seeds).
- Unit tests (+12): append/commit, too-short reject, term hint, overwrite suffix, stale-shorter append doesn't truncate, follower commit bound, backtracking, propose only on leader, majority commit, **Figure 8 scripted**, single-node commit, follower that lost its log.
- **Mutation testing** (each bug planted, then reverted): the sim caught double voting, a missing up-to-date check (Leader Completeness) and a missing prev-term check (State Machine Safety). **The random sim MISSED the Figure 8 rule removal and the follower-commit-bound bug.** These are now caught by targeted unit tests. The lesson: random testing needs hand-scripted tests for rare interleavings, as the plan said.
- Node: truncate+append persist, `apply()` = chained FNV-1a hash over (index, term, data), status shows commit/applied/hash, admin `propose <text>` (answers "accepted" with index/term, or "not leader" + hint). `cluster.py propose`, `leader()`, and a richer status line. New `tests/integration/test_replication.py` passes.
- **Bug found by the integration test:** a restarted follower (in-memory storage → empty log) never caught up, because the leader kept its old `match` and never sent below it. The root cause is that Raft assumes acknowledged entries are durable. Fix: when a rejection's hint is ≤ match, reset match = hint−1 (safe: commit_index never decreases). Unit test added. The real fix is persistence (step 7).
- Fixed `-Werror=dangling-else` again (an if around an EXPECT_ macro).
- ASan/UBSan Debug build: 62/62 pass (asserts on).
- `progress.md` updated: ~21/49 build items (~42%); new diagrams for replicating one entry and log repair; safety-property and mutation tables.

**Step 4b: ReadIndex** (2026-10-29)
- `raft.proto`: `read_ctx` on AppendEntries and its response (echoed by followers). `ready.h`: `ReadState{request_id, index}`, `reads_ready`, `reads_failed`; host step 4 = serve reads after apply.
- `raft/read_index`: `ReadIndexTracker` (add → ctx; ack(from, ctx) counts only for reads with ctx <= the acked ctx; pop_confirmed(quorum) in order; take_all). Every AppendEntries carries the latest ctx, and any same-term reply (success or not) acks.
- `raft_core`: `read_index(id)` (false if not leader). If no entry of the current term is committed yet, reads wait in `reads_waiting_` until `maybe_commit` sees a current-term commit. `start_reads` registers at commit_index and broadcasts immediately. Step-down fails all pending/waiting reads.
- Sim: `read()`, `reads()`; reads are served after apply; a new **Read Linearizability** check (served state ≥ the highest committed index when the read started); crash fails that node's pending reads. Tests: stale leader never serves reads (200 seeds); reads linearizable under random faults (300 seeds; ~3.5k served, ~3k failed safely).
- Unit tests (+7): follower refuses, majority ack after start, reply to a pre-read message doesn't confirm, new leader waits for its no-op, reads fail on step-down, follower echoes ctx, single node serves immediately.
- **Mutations:** no confirmation round → caught by the sim + unit tests; no current-term wait → caught by the sim + unit test; counting pre-read replies → **missed by the random sim** (a partition drops in-flight messages and delays are under an election timeout, so a stale reply can't arrive after a new leader commits), caught by the unit test.
- Node: async read path (`start_read`/`finish_read`, `pending_reads_` id → client connection id, looked up again on completion). Admin `read` command; `cluster.py read`; test_replication checks that the leader read reflects the writes, that a cut-off old leader never answers, and that the new leader answers.
- All 3 integration tests pass; ASan 71/71. progress.md: ~22/49 (~45%), Week 3 6/7, new ReadIndex section + mutation rows.

