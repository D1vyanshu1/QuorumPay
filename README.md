Ledger: A Crash-Fault-Tolerant Distributed Payment Ledger
Built on Raft consensus | C++17 | Ed25519 signatures | 5-node cluster

See `project_details.md` for the full design and plan.

## Status

Networking layer done: epoll event loop, length-prefixed protobuf framing,
full peer mesh with handshake and reconnect/backoff, partition injection.
Nodes currently exchange Ping/Pong; Raft comes next.

## Build

```sh
sudo apt install -y build-essential cmake protobuf-compiler libprotobuf-dev
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Sanitizer build: `cmake -S . -B build-asan -DLEDGER_SANITIZE=ON -DCMAKE_BUILD_TYPE=Debug`.

## Run a local cluster

```sh
tools/cluster.py start               # 5 nodes, logs in data/node-N/node.log
tools/cluster.py status              # which peers each node can reach + RTT
tools/cluster.py kill 3              # crash a node
tools/cluster.py restart 3
tools/cluster.py partition 1,2 3,4,5
tools/cluster.py heal
tools/cluster.py stop

python3 tests/integration/test_network.py   # automated end-to-end check
```
 build the model, move it to th build the model, move it to th