#pragma once

#include <chrono>
#include <cstdint>

namespace ledger {

// Monotonic time for measuring intervals (timeouts, backoff, RTT).
// Never jumps backwards, unlike wall-clock time, but it's only meaningful on
// this machine: never compare these values across nodes.
inline uint64_t now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

inline uint64_t now_us() {
  using namespace std::chrono;
  return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

}  // namespace ledger
