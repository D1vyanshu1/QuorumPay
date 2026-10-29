#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace ledger::crypto {

constexpr size_t kSha256Size = 32;

// CRC-32 (IEEE 802.3, the zlib/PNG one). Detects accidental corruption, such
// as a torn write at the end of the WAL after a crash. It is NOT a security
// hash: anyone can forge data with a chosen CRC. Use SHA-256 for that.
//
// Pass the previous result as `crc` to checksum data in pieces.
uint32_t crc32(std::string_view data, uint32_t crc = 0);

// SHA-256 (libsodium). Returns 32 raw bytes. Used for transaction ids and the
// state hash: two different inputs giving the same hash is infeasible.
std::string sha256(std::string_view data);

}  // namespace ledger::crypto
