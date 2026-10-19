#include "common/logger.h"

#include <chrono>
#include <cstdio>
#include <string>

namespace ledger {

namespace {

NodeId g_node = kNoNode;
LogLevel g_level = LogLevel::kInfo;

const char* level_name(LogLevel level) {
  switch (level) {
    case LogLevel::kDebug: return "debug";
    case LogLevel::kInfo: return "info";
    case LogLevel::kWarn: return "warn";
    case LogLevel::kError: return "error";
  }
  return "?";
}

}  // namespace

void Logger::set_node(NodeId id) { g_node = id; }
void Logger::set_level(LogLevel level) { g_level = level; }
bool Logger::enabled(LogLevel level) { return level >= g_level; }

void Logger::log(LogLevel level, std::string_view event, const nlohmann::json& fields) {
  if (!enabled(level)) return;

  // Wall-clock time here (not steady_clock) so lines from different nodes can
  // be lined up on one timeline. Logging is the only place wall time is used.
  auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();

  // Built by hand to keep a fixed field order: ts, node, level, event, extras.
  std::string line = "{\"ts_ms\":" + std::to_string(ts);
  if (g_node != kNoNode) line += ",\"node\":" + std::to_string(g_node);
  line += ",\"level\":\"";
  line += level_name(level);
  line += "\",\"event\":" + nlohmann::json(std::string(event)).dump();
  if (fields.is_object()) {
    for (const auto& item : fields.items()) {
      line += "," + nlohmann::json(item.key()).dump() + ":" + item.value().dump();
    }
  }
  line += "}\n";

  // stderr is unbuffered, so the line survives even if the process is
  // SIGKILLed right after (important for crash tests).
  std::fwrite(line.data(), 1, line.size(), stderr);
}

bool parse_log_level(std::string_view name, LogLevel* out) {
  if (name == "debug") *out = LogLevel::kDebug;
  else if (name == "info") *out = LogLevel::kInfo;
  else if (name == "warn") *out = LogLevel::kWarn;
  else if (name == "error") *out = LogLevel::kError;
  else return false;
  return true;
}

}  // namespace ledger
