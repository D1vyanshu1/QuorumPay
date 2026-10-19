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

**Not done yet**
- Week 1 items: `crypto/` (Ed25519, SHA-256), `ledger/` (transactions, state machine, 7-step validation)
- Raft (Weeks 2–3), persistence (Week 5), demos/checker (Week 6), benchmarks (Week 7)

**Open items**
- Code is **not committed to git** yet (check `git status`).
- `README.md` ends with a stray line ("build the model, move it to th..."), probably typed by accident. Delete it before committing.
- protobuf must be installed system-wide: `sudo apt install -y build-essential cmake protobuf-compiler libprotobuf-dev`

**Next step:** Week 2: Raft leader election as a pure `RaftCore` (no I/O) tested in a deterministic simulator; then swap Ping/Pong for Raft messages on the existing network layer.

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
