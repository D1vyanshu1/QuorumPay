#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace ledger {

// Lowercase hex encoding of raw bytes (keys, signatures, hashes in JSON and
// on the command line).
inline std::string to_hex(std::string_view bytes) {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string out;
  out.reserve(bytes.size() * 2);
  for (unsigned char b : bytes) {
    out.push_back(kDigits[b >> 4]);
    out.push_back(kDigits[b & 0xf]);
  }
  return out;
}

// Decodes hex (either case). nullopt on odd length or a non-hex character.
inline std::optional<std::string> from_hex(std::string_view hex) {
  auto nibble = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
  };
  if (hex.size() % 2 != 0) return std::nullopt;
  std::string out;
  out.reserve(hex.size() / 2);
  for (size_t i = 0; i < hex.size(); i += 2) {
    int hi = nibble(hex[i]), lo = nibble(hex[i + 1]);
    if (hi < 0 || lo < 0) return std::nullopt;
    out.push_back(static_cast<char>((hi << 4) | lo));
  }
  return out;
}

}  // namespace ledger
