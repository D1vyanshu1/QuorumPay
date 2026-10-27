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

**Not done yet** (stubs exist; real code still to write)
- Week 1 items: `crypto/` (Ed25519, SHA-256), `ledger/` (transactions, state machine, 7-step validation)
- Raft (Weeks 2–3), persistence (Week 5), demos/checker (Week 6), benchmarks (Week 7)

**Open items**
- `.gitignore` now lists `context.md`, but the file is **already tracked**, so git will keep tracking it. To really ignore it: `git rm --cached context.md` (the file stays on disk). Otherwise remove that line from `.gitignore`.
- Uncommitted: the README fix, the `.gitignore` change, and the whole skeleton.
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
