#pragma once

#include <nlohmann/json.hpp>
#include <string_view>

#include "common/types.h"

namespace ledger {

enum class LogLevel { kDebug = 0, kInfo = 1, kWarn = 2, kError = 3 };

// Structured logger: one JSON object per line on stderr, e.g.
//   {"ts_ms":1760000000000,"node":2,"level":"info","event":"peer_connected","peer":3}
// One line per event makes logs from 5 nodes easy to merge, grep and parse.
// The node is single-threaded, so there is no locking.
class Logger {
 public:
  static void set_node(NodeId id);
  static void set_level(LogLevel level);
  static bool enabled(LogLevel level);
  static void log(LogLevel level, std::string_view event, const nlohmann::json& fields);
};

// "debug" | "info" | "warn" | "error". Returns false if unknown.
bool parse_log_level(std::string_view name, LogLevel* out);

inline void log_debug(std::string_view event,
                      const nlohmann::json& fields = nlohmann::json::object()) {
  Logger::log(LogLevel::kDebug, event, fields);
}
inline void log_info(std::string_view event,
                     const nlohmann::json& fields = nlohmann::json::object()) {
  Logger::log(LogLevel::kInfo, event, fields);
}
inline void log_warn(std::string_view event,
                     const nlohmann::json& fields = nlohmann::json::object()) {
  Logger::log(LogLevel::kWarn, event, fields);
}
inline void log_error(std::string_view event,
                      const nlohmann::json& fields = nlohmann::json::object()) {
  Logger::log(LogLevel::kError, event, fields);
}

}  // namespace ledger
