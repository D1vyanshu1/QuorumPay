#include "net/framing.h"

#include <cstdint>
#include <stdexcept>

namespace ledger::net {

void append_frame(std::string& out, std::string_view payload) {
  if (payload.size() > kMaxFrameSize) throw std::length_error("frame payload too large");

  // Big-endian ("network byte order") so the format doesn't depend on the CPU.
  auto len = static_cast<uint32_t>(payload.size());
  char header[kFrameHeaderSize] = {
      static_cast<char>((len >> 24) & 0xff),
      static_cast<char>((len >> 16) & 0xff),
      static_cast<char>((len >> 8) & 0xff),
      static_cast<char>(len & 0xff),
  };
  out.append(header, kFrameHeaderSize);
  out.append(payload.data(), payload.size());
}

DecodeResult decode_frame(std::string_view buffer) {
  if (buffer.size() < kFrameHeaderSize) return {DecodeStatus::kNeedMore, {}, 0};

  auto b = [&](size_t i) { return static_cast<uint32_t>(static_cast<uint8_t>(buffer[i])); };
  uint32_t len = (b(0) << 24) | (b(1) << 16) | (b(2) << 8) | b(3);

  // Checked before waiting for the body: a corrupt or hostile header must not
  // make us buffer gigabytes waiting for a frame that will never be valid.
  if (len > kMaxFrameSize) return {DecodeStatus::kTooLarge, {}, 0};

  if (buffer.size() < kFrameHeaderSize + len) return {DecodeStatus::kNeedMore, {}, 0};

  return {DecodeStatus::kFrame, buffer.substr(kFrameHeaderSize, len), kFrameHeaderSize + len};
}

}  // namespace ledger::net
