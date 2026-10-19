#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace ledger::net {

// TCP is a byte stream with no message boundaries: one read() can return half
// a message, or three and a half. Every message is therefore sent as
//
//   [ u32 payload length, big-endian ][ payload bytes ]
//
// Pure functions on byte buffers, no sockets, so they're easy to unit-test.

constexpr size_t kFrameHeaderSize = 4;
constexpr size_t kMaxFrameSize = 16 * 1024 * 1024;  // 16 MB

// Appends one encoded frame to `out`. Throws std::length_error if the payload
// is bigger than kMaxFrameSize.
void append_frame(std::string& out, std::string_view payload);

enum class DecodeStatus {
  kFrame,     // a complete frame is at the start of the buffer
  kNeedMore,  // not enough bytes yet; wait for the next read
  kTooLarge,  // length header exceeds kMaxFrameSize: protocol error, close
};

struct DecodeResult {
  DecodeStatus status;
  std::string_view payload;  // valid only if status == kFrame; points into the input
  size_t consumed = 0;       // bytes to drop from the input (header + payload)
};

// Looks for one frame at the start of `buffer`. Never consumes partial frames.
DecodeResult decode_frame(std::string_view buffer);

}  // namespace ledger::net
