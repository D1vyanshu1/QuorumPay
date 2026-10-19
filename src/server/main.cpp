// ledger-node: one node of the cluster.
//
//   ledger-node --config config/cluster.json --id 3 [--log-level debug]

#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

#include "common/config.h"
#include "common/logger.h"
#include "server/node.h"

namespace {

void usage() {
  std::cerr << "usage: ledger-node --config <cluster.json> --id <node-id> "
               "[--log-level debug|info|warn|error]\n";
}

}  // namespace

int main(int argc, char** argv) {
  std::string config_path = "config/cluster.json";
  long id = 0;
  std::string level = "info";

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (i + 1 >= argc) {
      usage();
      return 2;
    }
    if (arg == "--config") config_path = argv[++i];
    else if (arg == "--id") id = std::strtol(argv[++i], nullptr, 10);
    else if (arg == "--log-level") level = argv[++i];
    else {
      usage();
      return 2;
    }
  }

  ledger::LogLevel log_level;
  if (id <= 0 || !ledger::parse_log_level(level, &log_level)) {
    usage();
    return 2;
  }
  ledger::Logger::set_level(log_level);
  ledger::Logger::set_node(static_cast<ledger::NodeId>(id));

  try {
    auto config = ledger::ClusterConfig::load_file(config_path);
    ledger::Node node(std::move(config), static_cast<ledger::NodeId>(id));
    node.run();
  } catch (const std::exception& e) {
    ledger::log_error("fatal", {{"error", e.what()}});
    return 1;
  }
  return 0;
}
