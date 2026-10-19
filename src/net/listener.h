#pragma once

#include <cstdint>
#include <functional>
#include <string>

#include "net/event_loop.h"

namespace ledger::net {

// A listening TCP socket. Each accepted (non-blocking) socket is passed to the
// accept handler, which takes ownership of the fd.
class Listener {
 public:
  using AcceptHandler = std::function<void(int fd, std::string remote)>;

  // Throws std::system_error if the address can't be bound (e.g. port in use).
  // Port 0 picks a free port (used by tests); see port().
  Listener(EventLoop& loop, const std::string& host, uint16_t port, AcceptHandler on_accept);
  ~Listener();
  Listener(const Listener&) = delete;
  Listener& operator=(const Listener&) = delete;

  uint16_t port() const { return port_; }

 private:
  void handle_readable();

  EventLoop& loop_;
  int fd_ = -1;
  uint16_t port_ = 0;
  AcceptHandler on_accept_;
};

}  // namespace ledger::net
