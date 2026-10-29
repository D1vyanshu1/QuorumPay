#include "crypto/hash.h"

#include <sodium.h>

#include <array>

#include "crypto/keys.h"

namespace ledger::crypto {

namespace {

// Byte-at-a-time lookup table for the reflected polynomial 0xEDB88320,
// built once at compile time.
constexpr std::array<uint32_t, 256> make_crc_table() {
  std::array<uint32_t, 256> t{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k) c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
    t[i] = c;
  }
  return t;
}

constexpr std::array<uint32_t, 256> kCrcTable = make_crc_table();

}  // namespace

uint32_t crc32(std::string_view data, uint32_t crc) {
  crc = ~crc;
  for (unsigned char b : data) crc = kCrcTable[(crc ^ b) & 0xff] ^ (crc >> 8);
  return ~crc;
}

std::string sha256(std::string_view data) {
  ensure_sodium();
  std::string out(kSha256Size, '\0');
  crypto_hash_sha256(reinterpret_cast<unsigned char*>(out.data()),
                     reinterpret_cast<const unsigned char*>(data.data()), data.size());
  return out;
}

}  // namespace ledger::crypto
