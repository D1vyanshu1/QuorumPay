#pragma once

#include <cstdint>

namespace ledger {

// Node IDs come from cluster.json and start at 1. 0 means "unknown / none".
using NodeId = uint32_t;
constexpr NodeId kNoNode = 0;

// Used by the Raft core (week 2+).
using Term = uint64_t;
using LogIndex = uint64_t;

}  // namespace ledger
