// Real sockets on 127.0.0.1, driven by a real EventLoop in this thread.

#include "net/connection.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "net/event_loop.h"
#include "net/listener.h"

using namespace ledger::net;

namespace {

// Stops the loop after `ms` so a broken test fails instead of hanging.
void add_deadline(EventLoop& loop, uint32_t ms, bool* timed_out) {
  auto ticks = std::make_shared<uint32_t>(0);
  loop.add_periodic_timer(10, [&loop, ticks, ms, timed_out] {
    if (++*ticks * 10 >= ms) {
      *timed_out = true;
      loop.stop();
    }
  });
}

}  // namespace

TEST(Connection, EchoSmallAndLargeFrames) {
  EventLoop loop;
  std::vector<std::shared_ptr<Connection>> server_conns;

  // Server: echo every frame back.
  Listener listener(loop, "127.0.0.1", 0, [&](int fd, std::string remote) {
    auto c = Connection::from_accepted(loop, fd, std::move(remote));
    c->set_frame_handler([](Connection& self, std::string_view p) { self.send_frame(p); });
    server_conns.push_back(c);
    c->start();
  });

  // 2MB is far bigger than a socket buffer, so it is guaranteed to be sent
  // and received in many partial chunks.
  const std::vector<std::string> sent = {"a", std::string(2 * 1024 * 1024, 'z'), "", "last"};
  std::vector<std::string> received;
  bool connected = false;

  std::string err;
  auto client = Connection::dial(loop, "127.0.0.1", listener.port(), &err);
  ASSERT_NE(client, nullptr) << err;
  client->set_connected_handler([&](Connection&) { connected = true; });
  client->set_frame_handler([&](Connection&, std::string_view p) {
    received.emplace_back(p);
    if (received.size() == sent.size()) loop.stop();
  });
  client->start();
  // Queued before the connect completes; must still arrive, in order.
  for (const auto& s : sent) ASSERT_TRUE(client->send_frame(s));

  bool timed_out = false;
  add_deadline(loop, 5000, &timed_out);
  loop.run();

  ASSERT_FALSE(timed_out);
  EXPECT_TRUE(connected);
  EXPECT_EQ(received, sent);
}

TEST(Connection, DialToClosedPortReportsClose) {
  EventLoop loop;
  uint16_t port;
  {
    // Grab a free port, then release it so nothing is listening there.
    Listener tmp(loop, "127.0.0.1", 0, [](int, std::string) {});
    port = tmp.port();
  }

  std::string err;
  auto conn = Connection::dial(loop, "127.0.0.1", port, &err);
  if (conn == nullptr) {
    SUCCEED() << "refused immediately: " << err;  // also a valid outcome
    return;
  }
  bool connected = false;
  bool closed = false;
  conn->set_connected_handler([&](Connection&) { connected = true; });
  conn->set_close_handler([&](Connection&) {
    closed = true;
    loop.stop();
  });
  conn->start();

  bool timed_out = false;
  add_deadline(loop, 2000, &timed_out);
  loop.run();

  EXPECT_FALSE(timed_out);
  EXPECT_TRUE(closed);
  EXPECT_FALSE(connected);
  EXPECT_NE(conn->close_reason().find("connect failed"), std::string::npos);
}

TEST(Connection, PeerCloseIsDetected) {
  EventLoop loop;
  std::shared_ptr<Connection> server_side;
  Listener listener(loop, "127.0.0.1", 0, [&](int fd, std::string remote) {
    server_side = Connection::from_accepted(loop, fd, std::move(remote));
    server_side->start();
    server_side->close("test closes it");  // hang up immediately
  });

  std::string err;
  auto client = Connection::dial(loop, "127.0.0.1", listener.port(), &err);
  ASSERT_NE(client, nullptr) << err;
  bool closed = false;
  client->set_close_handler([&](Connection&) {
    closed = true;
    loop.stop();
  });
  client->start();

  bool timed_out = false;
  add_deadline(loop, 2000, &timed_out);
  loop.run();

  EXPECT_FALSE(timed_out);
  EXPECT_TRUE(closed);
  EXPECT_FALSE(client->send_frame("x"));  // sending on a closed connection is refused
}

TEST(Connection, CloseHandlerMayDropLastReference) {
  // The owner commonly erases its shared_ptr inside the close handler; the
  // connection must survive until close() returns.
  EventLoop loop;
  Listener listener(loop, "127.0.0.1", 0, [](int fd, std::string) { ::close(fd); });

  std::string err;
  std::shared_ptr<Connection> owner = Connection::dial(loop, "127.0.0.1", listener.port(), &err);
  ASSERT_NE(owner, nullptr) << err;
  owner->set_close_handler([&](Connection&) {
    owner.reset();
    loop.stop();
  });
  owner->start();

  bool timed_out = false;
  add_deadline(loop, 2000, &timed_out);
  loop.run();
  EXPECT_FALSE(timed_out);
  EXPECT_EQ(owner, nullptr);
}
