#include "net/connection.h"

#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "net/framing.h"

namespace ledger::net {

namespace {

constexpr size_t kReadChunk = 64 * 1024;

// Disable Nagle's algorithm. Nagle delays small writes hoping to batch them,
// which adds latency to exactly the small request/response messages Raft
// sends. We batch at the application level instead.
void set_nodelay(int fd) {
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}

uint64_t next_connection_id() {
  static uint64_t next = 1;
  return next++;
}

}  // namespace

Connection::Connection(EventLoop& loop, int fd, State state, std::string remote)
    : loop_(loop), fd_(fd), state_(state), id_(next_connection_id()), remote_(std::move(remote)) {}

Connection::~Connection() {
  // Destruction without close() (e.g. owner shutting down): release the fd
  // quietly, without running handlers.
  if (fd_ >= 0) {
    if (registered_) loop_.remove_fd(fd_);
    ::close(fd_);
  }
}

std::shared_ptr<Connection> Connection::from_accepted(EventLoop& loop, int fd,
                                                      std::string remote) {
  set_nodelay(fd);
  return std::shared_ptr<Connection>(
      new Connection(loop, fd, State::kConnected, std::move(remote)));
}

std::shared_ptr<Connection> Connection::dial(EventLoop& loop, const std::string& host,
                                             uint16_t port, std::string* error) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  std::string port_str = std::to_string(port);

  // Note: getaddrinfo blocks on DNS. Fine for numeric IPs and /etc/hosts
  // names like "localhost", which is all this project uses.
  int rc = ::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
  if (rc != 0) {
    if (error) *error = ::gai_strerror(rc);
    return nullptr;
  }

  int fd = -1;
  int last_errno = 0;
  for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
    fd = ::socket(ai->ai_family, ai->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC, ai->ai_protocol);
    if (fd < 0) {
      last_errno = errno;
      continue;
    }
    // Non-blocking connect usually returns EINPROGRESS: the handshake
    // continues in the kernel and epoll reports the socket writable when done.
    if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0 || errno == EINPROGRESS) break;
    last_errno = errno;
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(res);

  if (fd < 0) {
    if (error) *error = std::strerror(last_errno);
    return nullptr;
  }
  set_nodelay(fd);
  // Even if connect() succeeded instantly we stay in kConnecting; the socket
  // is writable right away, so finish_connect() runs on the next loop pass.
  return std::shared_ptr<Connection>(
      new Connection(loop, fd, State::kConnecting, host + ":" + port_str));
}

void Connection::start() {
  if (registered_ || state_ == State::kClosed) return;
  std::weak_ptr<Connection> weak = shared_from_this();
  // The loop holds only weak pointers: the owner decides the lifetime.
  loop_.add_fd(
      fd_,
      [weak] {
        if (auto self = weak.lock()) self->handle_readable();
      },
      [weak] {
        if (auto self = weak.lock()) self->handle_writable();
      });
  registered_ = true;
  update_write_interest();
}

bool Connection::send_frame(std::string_view payload) {
  if (state_ == State::kClosed) return false;
  if (pending_write_bytes() + kFrameHeaderSize + payload.size() > kMaxWriteBuffer) return false;

  // flush() can fail and close us, and the close handler may drop the owner's
  // last reference. Hold one ourselves until this function returns.
  auto self = shared_from_this();

  append_frame(write_buf_, payload);
  if (state_ == State::kConnected && registered_) flush();  // try right away
  return true;
}

void Connection::close(const std::string& reason) {
  if (state_ == State::kClosed) return;
  auto self = shared_from_this();  // the close handler may drop the owner's reference

  state_ = State::kClosed;
  close_reason_ = reason;
  if (registered_) {
    loop_.remove_fd(fd_);
    registered_ = false;
  }
  ::close(fd_);
  fd_ = -1;

  if (on_close_) {
    Handler h = std::move(on_close_);
    on_close_ = nullptr;
    h(*this);
  }
}

bool Connection::finish_connect() {
  // A finished non-blocking connect reports its result through SO_ERROR.
  int err = 0;
  socklen_t len = sizeof err;
  if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len) < 0) err = errno;
  if (err != 0) {
    close(std::string("connect failed: ") + std::strerror(err));
    return false;
  }
  state_ = State::kConnected;
  auto self = shared_from_this();
  if (on_connected_) on_connected_(*this);
  return state_ == State::kConnected;
}

void Connection::handle_readable() {
  if (state_ == State::kConnecting && !finish_connect()) return;
  if (state_ == State::kClosed) return;

  // One recv() per wake-up. Level-triggered epoll wakes us again if more data
  // is waiting, and other sockets get a turn in between.
  char buf[kReadChunk];
  ssize_t n = ::recv(fd_, buf, sizeof buf, 0);
  if (n == 0) {
    close("peer closed connection");
    return;
  }
  if (n < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return;
    close(std::string("recv: ") + std::strerror(errno));
    return;
  }
  read_buf_.append(buf, static_cast<size_t>(n));

  auto self = shared_from_this();  // a frame handler may close us
  size_t offset = 0;
  while (state_ != State::kClosed) {
    DecodeResult r = decode_frame(std::string_view(read_buf_).substr(offset));
    if (r.status == DecodeStatus::kNeedMore) break;
    if (r.status == DecodeStatus::kTooLarge) {
      close("frame too large");
      return;
    }
    offset += r.consumed;
    // r.payload points into read_buf_, which isn't modified until the loop ends.
    if (on_frame_) on_frame_(*this, r.payload);
  }
  // Drop the consumed frames once per read (not once per frame), keeping any
  // trailing partial frame for next time.
  if (state_ != State::kClosed) read_buf_.erase(0, offset);
}

void Connection::handle_writable() {
  if (state_ == State::kConnecting && !finish_connect()) return;
  if (state_ == State::kClosed) return;
  flush();
}

void Connection::flush() {
  while (write_offset_ < write_buf_.size()) {
    // MSG_NOSIGNAL: writing to a socket the peer closed would otherwise raise
    // SIGPIPE and kill the whole process. We get EPIPE instead.
    ssize_t n = ::send(fd_, write_buf_.data() + write_offset_, write_buf_.size() - write_offset_,
                       MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // kernel buffer full
      close(std::string("send: ") + std::strerror(errno));
      return;
    }
    write_offset_ += static_cast<size_t>(n);
  }

  if (write_offset_ == write_buf_.size()) {
    write_buf_.clear();
    write_offset_ = 0;
  } else if (write_offset_ > 1024 * 1024) {
    // Reclaim space occasionally instead of shifting the buffer on every send.
    write_buf_.erase(0, write_offset_);
    write_offset_ = 0;
  }
  update_write_interest();
}

void Connection::update_write_interest() {
  if (!registered_) return;
  bool want = state_ == State::kConnecting || pending_write_bytes() > 0;
  loop_.set_writable_interest(fd_, want);
}

}  // namespace ledger::net
