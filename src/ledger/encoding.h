#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace ledger::ledger {

// Fixed-layout big-endian encoding for bytes that get signed or hashed. Every
// node and client must produce identical bytes, so nothing here may depend
// on the platform, the compiler, or a serialization library's choices.

inline void put_u32(std::string& out, uint32_t v) {
  for (int shift = 24; shift >= 0; shift -= 8) out.push_back(static_cast<char>(v >> shift));
}

inline void put_u64(std::string& out, uint64_t v) {
  for (int shift = 56; shift >= 0; shift -= 8) out.push_back(static_cast<char>(v >> shift));
}

// Length-prefixed, so concatenated strings can't be split another way.
inline void put_str(std::string& out, std::string_view s) {
  put_u32(out, static_cast<uint32_t>(s.size()));
  out.append(s);
}

}  // namespace ledger::ledger
