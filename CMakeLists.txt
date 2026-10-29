cmake_minimum_required(VERSION 3.16)
project(Ledger LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

option(LEDGER_BUILD_TESTS "Build unit tests" ON)
option(LEDGER_SANITIZE "Build with AddressSanitizer + UndefinedBehaviorSanitizer" OFF)

if(LEDGER_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

# Strict warnings for our own code only (generated protobuf code is excluded).
add_library(ledger_warnings INTERFACE)
target_compile_options(ledger_warnings INTERFACE -Wall -Wextra -Werror)

# ---------------------------------------------------------------- dependencies
include(FetchContent)
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

FetchContent_Declare(nlohmann_json
  URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz)
FetchContent_MakeAvailable(nlohmann_json)

find_package(Protobuf REQUIRED)

# libsodium ships a pkg-config file, not a CMake package.
find_package(PkgConfig REQUIRED)
pkg_check_modules(SODIUM REQUIRED IMPORTED_TARGET libsodium)

# ---------------------------------------------------------------- proto
# Generated headers land in the build dir as e.g. "envelope.pb.h".
set(LEDGER_PROTOS
  proto/peer.proto
  proto/client.proto
  proto/raft.proto
  proto/ledger.proto
  proto/envelope.proto)
protobuf_generate_cpp(LEDGER_PROTO_SRCS LEDGER_PROTO_HDRS ${LEDGER_PROTOS})

add_library(ledger_proto STATIC ${LEDGER_PROTO_SRCS} ${LEDGER_PROTO_HDRS})
target_include_directories(ledger_proto SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(ledger_proto PUBLIC protobuf::libprotobuf)

# ---------------------------------------------------------------- common
add_library(ledger_common STATIC
  src/common/config.cpp
  src/common/logger.cpp)
target_include_directories(ledger_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(ledger_common
  PUBLIC nlohmann_json::nlohmann_json
  PRIVATE ledger_warnings)

# ---------------------------------------------------------------- net
add_library(ledger_net STATIC
  src/net/event_loop.cpp
  src/net/framing.cpp
  src/net/connection.cpp
  src/net/listener.cpp
  src/net/fault_filter.cpp
  src/net/peer_manager.cpp)
target_link_libraries(ledger_net
  PUBLIC ledger_common ledger_proto
  PRIVATE ledger_warnings)

# ---------------------------------------------------------------- crypto
# Dependency rule: common <- crypto <- ledger;  common <- raft (raft never sees
# net/ or storage/);  storage depends only on common/crypto.
add_library(ledger_crypto STATIC
  src/crypto/keys.cpp
  src/crypto/signer.cpp
  src/crypto/hash.cpp)
target_link_libraries(ledger_crypto
  PUBLIC ledger_common
  PRIVATE PkgConfig::SODIUM ledger_warnings)

# ---------------------------------------------------------------- ledger
add_library(ledger_ledger STATIC
  src/ledger/transaction.cpp
  src/ledger/account_store.cpp
  src/ledger/state_machine.cpp
  src/ledger/state_hash.cpp)
target_link_libraries(ledger_ledger
  PUBLIC ledger_crypto ledger_proto
  PRIVATE ledger_warnings)

# ---------------------------------------------------------------- raft
add_library(ledger_raft STATIC
  src/raft/raft_core.cpp
  src/raft/raft_log.cpp
  src/raft/progress.cpp
  src/raft/read_index.cpp)
target_link_libraries(ledger_raft
  PUBLIC ledger_common ledger_proto
  PRIVATE ledger_warnings)

# ---------------------------------------------------------------- storage
add_library(ledger_storage STATIC
  src/storage/meta_store.cpp
  src/storage/wal.cpp
  src/storage/snapshot_store.cpp
  src/storage/file_util.cpp)
target_link_libraries(ledger_storage
  PUBLIC ledger_crypto ledger_proto
  PRIVATE ledger_warnings)

# ---------------------------------------------------------------- binaries
add_executable(ledger-node
  src/server/main.cpp
  src/server/node.cpp
  src/server/client_service.cpp
  src/server/admin_service.cpp)
target_link_libraries(ledger-node PRIVATE
  ledger_net ledger_raft ledger_storage ledger_ledger ledger_warnings)

add_executable(ledger-cli
  src/client/main.cpp
  src/client/client_lib.cpp
  src/client/keystore.cpp)
target_link_libraries(ledger-cli PRIVATE ledger_net ledger_ledger ledger_warnings)

add_executable(ledger-admin src/client/admin_main.cpp)
target_link_libraries(ledger-admin PRIVATE ledger_net ledger_warnings)

# ---------------------------------------------------------------- tests
if(LEDGER_BUILD_TESTS)
  FetchContent_Declare(googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  enable_testing()
  include(GoogleTest)

  add_executable(unit_tests
    tests/unit/framing_test.cpp
    tests/unit/config_test.cpp
    tests/unit/fault_filter_test.cpp
    tests/unit/connection_test.cpp
    tests/unit/peer_manager_test.cpp
    tests/unit/crypto_test.cpp
    tests/unit/state_machine_test.cpp
    tests/unit/wal_test.cpp
    tests/unit/raft_core_test.cpp
    tests/unit/genesis_test.cpp
    tests/unit/transaction_test.cpp)
  target_link_libraries(unit_tests PRIVATE
    ledger_net ledger_ledger ledger_storage ledger_raft GTest::gtest_main ledger_warnings)
  target_compile_definitions(unit_tests PRIVATE LEDGER_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
  gtest_discover_tests(unit_tests)

  # Deterministic Raft simulation: Raft + the real ledger state machine, but
  # never net/ or storage/ (no sockets, no files).
  add_executable(raft_sim_tests
    tests/sim/sim_network.cpp
    tests/sim/raft_sim_test.cpp)
  target_include_directories(raft_sim_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
  target_link_libraries(raft_sim_tests PRIVATE
    ledger_raft ledger_ledger GTest::gtest_main ledger_warnings)
  gtest_discover_tests(raft_sim_tests)
endif()
