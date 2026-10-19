#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

namespace ledger::net {

// Single-threaded reactor built on epoll (level-triggered) + timerfd.
//
// Everything a node does (accepting, reading, writing, the periodic tick)
// is a callback run from run() on ONE thread, so no code in the node needs
// locks. A callback must never block: sockets are non-blocking and slow work
// is not allowed inside callbacks.
//
// Callbacks may freely add or remove fds (including their own) while running.
class EventLoop {
 public:
  using Callback = std::function<void()>;

  EventLoop();
  ~EventLoop();
  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  // Watch `fd`. on_readable runs when it has data (or an error/hangup);
  // on_writable runs only while writable interest is enabled. The loop does
  // not own the fd: the caller closes it after remove_fd().
  void add_fd(int fd, Callback on_readable, Callback on_writable);

  // Enable EPOLLOUT only while there is queued data. In level-triggered mode
  // a socket is almost always writable, so leaving it on busy-loops.
  void set_writable_interest(int fd, bool enabled);

  void remove_fd(int fd);

  // Calls `cb` every `interval_ms`, backed by a timerfd so the timer is just
  // another fd in the same epoll set.
  void add_periodic_timer(uint32_t interval_ms, Callback cb);

  // Blocks dispatching events until stop() is called (from a callback).
  void run();
  void stop();

 private:
  struct Handler {
    int fd = -1;
    uint64_t id = 0;  // unique per registration; see run()
    bool want_write = false;
    Callback on_readable;
    Callback on_writable;
  };

  int epfd_ = -1;
  bool running_ = false;
  uint64_t next_id_ = 1;
  std::unordered_map<int, std::shared_ptr<Handler>> by_fd_;
  std::unordered_map<uint64_t, std::shared_ptr<Handler>> by_id_;
  std::vector<int> timer_fds_;
};

}  // namespace ledger::net
