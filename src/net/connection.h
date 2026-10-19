#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "common/types.h"
#include "net/event_loop.h"

namespace ledger::net {

// One non-blocking TCP socket that sends and receives whole frames.
//
// Read side: bytes from recv() pile up in read_buf_ until decode_frame() finds
// complete frames, which are handed to the frame handler one by one.
// Write side: send_frame() appends to write_buf_; bytes go out as fast as the
// kernel accepts them. Partial writes are normal and the rest is sent when
// epoll reports the socket writable again.
//
// Always owned through shared_ptr. Handlers must not capture a shared_ptr to
// their own connection (that would be a reference cycle).
class Connection : public std::enable_shared_from_this<Connection> {
 public:
  using FrameHandler = std::function<void(Connection&, std::string_view payload)>;
  using Handler = std::function<void(Connection&)>;

  // Back-pressure limit. If a peer stops reading, we drop new frames instead
  // of growing memory forever; Raft resends anything that matters.
  static constexpr size_t kMaxWriteBuffer = 8 * 1024 * 1024;

  // Wraps a socket returned by accept().
  static std::shared_ptr<Connection> from_accepted(EventLoop& loop, int fd, std::string remote);

  // Starts a non-blocking connect(). Returns nullptr (and sets *error) if it
  // fails immediately. Otherwise the result is known later: on_connected or
  // on_close fires.
  static std::shared_ptr<Connection> dial(EventLoop& loop, const std::string& host,
                                          uint16_t port, std::string* error);

  ~Connection();
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  // Set handlers first, then start() to register with the event loop.
  void set_frame_handler(FrameHandler h) { on_frame_ = std::move(h); }
  void set_connected_handler(Handler h) { on_connected_ = std::move(h); }
  void set_close_handler(Handler h) { on_close_ = std::move(h); }
  void start();

  // Queues one frame. Frames queued while still connecting are sent once the
  // connection is established. Returns false if the frame was dropped (closed
  // or write buffer full).
  bool send_frame(std::string_view payload);

  // Idempotent. Runs the close handler exactly once.
  void close(const std::string& reason);

  bool is_connected() const { return state_ == State::kConnected; }
  bool is_closed() const { return state_ == State::kClosed; }
  uint64_t id() const { return id_; }
  const std::string& remote() const { return remote_; }
  const std::string& close_reason() const { return close_reason_; }
  size_t pending_write_bytes() const { return write_buf_.size() - write_offset_; }

  // Which cluster node is on the other end (kNoNode until the Hello arrives).
  NodeId peer_id() const { return peer_id_; }
  void set_peer_id(NodeId id) { peer_id_ = id; }

 private:
  enum class State { kConnecting, kConnected, kClosed };

  Connection(EventLoop& loop, int fd, State state, std::string remote);

  void handle_readable();
  void handle_writable();
  bool finish_connect();  // false if the connect failed (connection now closed)
  void flush();
  void update_write_interest();

  EventLoop& loop_;
  int fd_;
  State state_;
  bool registered_ = false;
  uint64_t id_;
  std::string remote_;
  NodeId peer_id_ = kNoNode;
  std::string close_reason_;

  std::string read_buf_;
  std::string write_buf_;
  size_t write_offset_ = 0;  // bytes of write_buf_ already sent

  FrameHandler on_frame_;
  Handler on_connected_;
  Handler on_close_;
};

}  // namespace ledger::net
