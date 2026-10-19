// Three PeerManagers in one process sharing one EventLoop, talking over real
// loopback sockets.

#include "net/peer_manager.h"

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <functional>
#include <memory>
#include <vector>

#include "common/clock.h"

using namespace ledger;
using namespace ledger::net;

namespace {

uint16_t free_port() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
  socklen_t len = sizeof addr;
  ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
  ::close(fd);
  return ntohs(addr.sin_port);
}

ClusterConfig make_config(int n) {
  ClusterConfig c;
  c.cluster_id = "test";
  for (int i = 1; i <= n; ++i) {
    NodeConfig nc;
    nc.id = static_cast<NodeId>(i);
    nc.host = "127.0.0.1";
    nc.peer_port = free_port();
    nc.client_port = free_port();
    c.nodes.push_back(nc);
  }
  return c;
}

struct Received {
  NodeId at;
  NodeId from;
  uint64_t seq;
};

class PeerManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_ = make_config(3);
    for (NodeId id = 1; id <= 3; ++id) {
      filters_.push_back(std::make_unique<FaultFilter>());
      pms_.push_back(std::make_unique<PeerManager>(loop_, config_, id, *filters_.back()));
      pms_.back()->set_message_handler(
          [this, id](NodeId from, const pb::Envelope& msg, Connection&) {
            if (msg.has_ping()) received_.push_back({id, from, msg.ping().seq()});
          });
    }
    for (auto& pm : pms_) pm->start();
    loop_.add_periodic_timer(10, [this] {
      for (auto& pm : pms_) {
        if (pm) pm->tick(now_ms());  // null while a test has "crashed" that node
      }
      if (step_) step_();
    });
  }

  PeerManager& pm(NodeId id) { return *pms_[id - 1]; }
  FaultFilter& filter(NodeId id) { return *filters_[id - 1]; }

  bool fully_connected() {
    for (NodeId a = 1; a <= 3; ++a)
      for (NodeId b = 1; b <= 3; ++b)
        if (a != b && !pm(a).is_connected(b)) return false;
    return true;
  }

  // Runs the loop until `done()` is true (checked every tick) or timeout.
  bool run_until(std::function<bool()> done, uint64_t timeout_ms = 3000) {
    uint64_t deadline = now_ms() + timeout_ms;
    bool ok = false;
    step_ = [&] {
      if (done()) {
        ok = true;
        loop_.stop();
      } else if (now_ms() > deadline) {
        loop_.stop();
      }
    };
    loop_.run();
    step_ = nullptr;
    return ok;
  }

  static pb::Envelope ping(uint64_t seq) {
    pb::Envelope e;
    e.mutable_ping()->set_seq(seq);
    return e;
  }

  ClusterConfig config_;
  EventLoop loop_;
  std::vector<std::unique_ptr<FaultFilter>> filters_;
  std::vector<std::unique_ptr<PeerManager>> pms_;
  std::vector<Received> received_;
  std::function<void()> step_;
};

}  // namespace

TEST_F(PeerManagerTest, FormsFullMeshAndDeliversWithSenderIdentity) {
  ASSERT_TRUE(run_until([this] { return fully_connected(); }));

  EXPECT_TRUE(pm(1).send(2, ping(42)));
  EXPECT_TRUE(pm(3).send(2, ping(43)));
  ASSERT_TRUE(run_until([this] { return received_.size() == 2; }));

  for (const auto& r : received_) {
    EXPECT_EQ(r.at, 2u);
    // The receiver knows who sent it from the handshake, not from the message.
    EXPECT_EQ(r.from, r.seq == 42 ? 1u : 3u);
  }
}

TEST_F(PeerManagerTest, BlockedPeerTrafficIsDroppedBothWays) {
  ASSERT_TRUE(run_until([this] { return fully_connected(); }));

  filter(1).block(2);
  EXPECT_FALSE(pm(1).send(2, ping(1)));  // outgoing dropped at the sender
  EXPECT_TRUE(pm(2).send(1, ping(2)));   // node 2 doesn't know; it sends...
  EXPECT_TRUE(pm(3).send(1, ping(3)));   // ...and 3 -> 1 is unaffected

  run_until([] { return false; }, 300);  // let everything that will arrive, arrive
  ASSERT_EQ(received_.size(), 1u);       // ...but node 1 drops what came from 2
  EXPECT_EQ(received_[0].at, 1u);
  EXPECT_EQ(received_[0].from, 3u);

  filter(1).unblock(2);
  EXPECT_TRUE(pm(1).send(2, ping(4)));
  ASSERT_TRUE(run_until([this] { return received_.size() == 2; }));
  EXPECT_EQ(received_[1].from, 1u);
}

TEST_F(PeerManagerTest, ReconnectsAfterPeerRestart) {
  ASSERT_TRUE(run_until([this] { return fully_connected(); }));

  // "Crash" node 3: destroying its PeerManager closes all its sockets.
  pms_[2].reset();
  ASSERT_TRUE(run_until([this] { return !pm(1).is_connected(3) && !pm(2).is_connected(3); }));

  // "Restart" it on the same ports.
  pms_[2] = std::make_unique<PeerManager>(loop_, config_, 3, filter(3));
  pms_[2]->set_message_handler([this](NodeId from, const pb::Envelope& msg, Connection&) {
    if (msg.has_ping()) received_.push_back({3, from, msg.ping().seq()});
  });
  pms_[2]->start();
  ASSERT_TRUE(run_until([this] { return fully_connected(); }));

  EXPECT_TRUE(pm(1).send(3, ping(7)));
  ASSERT_TRUE(run_until([this] { return received_.size() == 1; }));
  EXPECT_EQ(received_[0].from, 1u);
}

TEST_F(PeerManagerTest, WrongClusterIdIsRejected) {
  ASSERT_TRUE(run_until([this] { return fully_connected(); }));

  // A node from another cluster claiming to be node 1.
  ClusterConfig other = config_;
  other.cluster_id = "other";
  other.nodes[0].peer_port = free_port();
  FaultFilter f;
  PeerManager intruder(loop_, other, 1, f);
  intruder.set_message_handler([](NodeId, const pb::Envelope&, Connection&) {});
  intruder.start();

  // Its connections are accepted at the TCP level but closed as soon as the
  // Hello is checked. Keep sending for a while: nothing may get through.
  run_until(
      [&] {
        intruder.send(2, ping(99));
        intruder.send(3, ping(99));
        return false;
      },
      500);
  EXPECT_TRUE(received_.empty());
}
