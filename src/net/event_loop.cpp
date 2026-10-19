#include "net/event_loop.h"

#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <stdexcept>
#include <system_error>

namespace ledger::net {

namespace {

[[noreturn]] void throw_errno(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

}  // namespace

EventLoop::EventLoop() {
  epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
  if (epfd_ < 0) throw_errno("epoll_create1");
}

EventLoop::~EventLoop() {
  for (int tfd : timer_fds_) ::close(tfd);
  if (epfd_ >= 0) ::close(epfd_);
}

void EventLoop::add_fd(int fd, Callback on_readable, Callback on_writable) {
  auto h = std::make_shared<Handler>();
  h->fd = fd;
  h->id = next_id_++;
  h->on_readable = std::move(on_readable);
  h->on_writable = std::move(on_writable);

  epoll_event ev{};
  ev.events = EPOLLIN;
  ev.data.u64 = h->id;
  if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) throw_errno("epoll_ctl(ADD)");

  by_fd_[fd] = h;
  by_id_[h->id] = h;
}

void EventLoop::set_writable_interest(int fd, bool enabled) {
  auto it = by_fd_.find(fd);
  if (it == by_fd_.end()) return;
  Handler& h = *it->second;
  if (h.want_write == enabled) return;

  epoll_event ev{};
  ev.events = EPOLLIN;
  if (enabled) ev.events |= EPOLLOUT;
  ev.data.u64 = h.id;
  if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) < 0) throw_errno("epoll_ctl(MOD)");
  h.want_write = enabled;
}

void EventLoop::remove_fd(int fd) {
  auto it = by_fd_.find(fd);
  if (it == by_fd_.end()) return;
  ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
  by_id_.erase(it->second->id);
  by_fd_.erase(it);
}

void EventLoop::add_periodic_timer(uint32_t interval_ms, Callback cb) {
  if (interval_ms == 0) throw std::invalid_argument("timer interval must be > 0");

  int tfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (tfd < 0) throw_errno("timerfd_create");

  itimerspec spec{};
  spec.it_interval.tv_sec = interval_ms / 1000;
  spec.it_interval.tv_nsec = static_cast<long>(interval_ms % 1000) * 1'000'000L;
  spec.it_value = spec.it_interval;  // first expiry after one interval
  if (::timerfd_settime(tfd, 0, &spec, nullptr) < 0) {
    ::close(tfd);
    throw_errno("timerfd_settime");
  }
  timer_fds_.push_back(tfd);

  add_fd(
      tfd,
      [tfd, cb = std::move(cb)] {
        // Reading clears the readiness. `expirations` > 1 means the loop was
        // busy and missed ticks; we still run cb once. For Raft that only
        // makes timeouts a bit longer, which is the safe direction.
        uint64_t expirations = 0;
        if (::read(tfd, &expirations, sizeof expirations) != sizeof expirations) return;
        cb();
      },
      nullptr);
}

void EventLoop::run() {
  running_ = true;
  std::array<epoll_event, 64> events{};

  while (running_) {
    int n = ::epoll_wait(epfd_, events.data(), static_cast<int>(events.size()), -1);
    if (n < 0) {
      if (errno == EINTR) continue;
      throw_errno("epoll_wait");
    }

    for (int i = 0; i < n; ++i) {
      // We look handlers up by registration id, not fd. If an earlier callback
      // in this batch closed fd 7 and a new socket reused number 7, an event
      // meant for the old socket would otherwise be delivered to the new one.
      uint64_t id = events[i].data.u64;
      uint32_t ev = events[i].events;

      auto it = by_id_.find(id);
      if (it == by_id_.end()) continue;  // removed earlier in this batch
      std::shared_ptr<Handler> h = it->second;  // keep alive during callbacks

      // Errors and hangups go to the read path: read() then returns 0 or an
      // error, and the owner closes the connection.
      if ((ev & (EPOLLIN | EPOLLERR | EPOLLHUP)) && h->on_readable) h->on_readable();

      if ((ev & (EPOLLOUT | EPOLLERR)) && h->on_writable && by_id_.count(id)) {
        h->on_writable();
      }
    }
  }
}

void EventLoop::stop() { running_ = false; }

}  // namespace ledger::net
