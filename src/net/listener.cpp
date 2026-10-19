#include "net/listener.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <system_error>

#include "common/logger.h"

namespace ledger::net {

namespace {

std::string format_addr(const sockaddr_storage& ss) {
  char host[INET6_ADDRSTRLEN] = "?";
  uint16_t port = 0;
  if (ss.ss_family == AF_INET) {
    const auto* a = reinterpret_cast<const sockaddr_in*>(&ss);
    ::inet_ntop(AF_INET, &a->sin_addr, host, sizeof host);
    port = ntohs(a->sin_port);
  } else if (ss.ss_family == AF_INET6) {
    const auto* a = reinterpret_cast<const sockaddr_in6*>(&ss);
    ::inet_ntop(AF_INET6, &a->sin6_addr, host, sizeof host);
    port = ntohs(a->sin6_port);
  }
  return std::string(host) + ":" + std::to_string(port);
}

}  // namespace

Listener::Listener(EventLoop& loop, const std::string& host, uint16_t port,
                   AcceptHandler on_accept)
    : loop_(loop), on_accept_(std::move(on_accept)) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;
  addrinfo* res = nullptr;
  std::string port_str = std::to_string(port);
  int rc = ::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
  if (rc != 0) {
    throw std::system_error(EINVAL, std::generic_category(),
                            std::string("getaddrinfo: ") + ::gai_strerror(rc));
  }

  fd_ = ::socket(res->ai_family, res->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                 res->ai_protocol);
  if (fd_ < 0) {
    int err = errno;
    ::freeaddrinfo(res);
    throw std::system_error(err, std::generic_category(), "socket");
  }

  // SO_REUSEADDR lets a restarted node bind its port again right away, even
  // though connections from the killed process are still in TIME_WAIT.
  int one = 1;
  ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

  if (::bind(fd_, res->ai_addr, res->ai_addrlen) < 0 || ::listen(fd_, 128) < 0) {
    int err = errno;
    ::freeaddrinfo(res);
    ::close(fd_);
    throw std::system_error(err, std::generic_category(),
                            "bind/listen " + host + ":" + port_str);
  }
  ::freeaddrinfo(res);

  sockaddr_storage bound{};
  socklen_t len = sizeof bound;
  ::getsockname(fd_, reinterpret_cast<sockaddr*>(&bound), &len);
  port_ = bound.ss_family == AF_INET6 ? ntohs(reinterpret_cast<sockaddr_in6*>(&bound)->sin6_port)
                                      : ntohs(reinterpret_cast<sockaddr_in*>(&bound)->sin_port);

  loop_.add_fd(fd_, [this] { handle_readable(); }, nullptr);
}

Listener::~Listener() {
  if (fd_ >= 0) {
    loop_.remove_fd(fd_);
    ::close(fd_);
  }
}

void Listener::handle_readable() {
  // Several clients may have connected since the last wake-up: accept them all.
  while (true) {
    sockaddr_storage addr{};
    socklen_t len = sizeof addr;
    int fd = ::accept4(fd_, reinterpret_cast<sockaddr*>(&addr), &len,
                       SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0) {
      if (errno == EINTR || errno == ECONNABORTED) continue;
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        // e.g. EMFILE (out of fds). Stop for now; level-triggered epoll
        // will call us again.
        log_warn("accept_failed", {{"error", std::strerror(errno)}});
      }
      return;
    }
    on_accept_(fd, format_addr(addr));
  }
}

}  // namespace ledger::net
