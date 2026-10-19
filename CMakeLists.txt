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

# ---------------------------------------------------------------- proto
# Generated headers land in the build dir as e.g. "envelope.pb.h".
set(LEDGER_PROTOS
  proto/peer.proto
  proto/client.proto
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

# ---------------------------------------------------------------- binaries
add_executable(ledger-node
  src/server/main.cpp
  src/server/node.cpp)
target_link_libraries(ledger-node PRIVATE ledger_net ledger_warnings)

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
    tests/unit/peer_manager_test.cpp)
  target_link_libraries(unit_tests PRIVATE ledger_net GTest::gtest_main ledger_warnings)
  target_compile_definitions(unit_tests PRIVATE LEDGER_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
  gtest_discover_tests(unit_tests)
endif()
