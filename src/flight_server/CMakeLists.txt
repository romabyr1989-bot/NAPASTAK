# napastak_flight_server — Apache Arrow Flight bridge for NAPASTAK.
#
# Builds a small C++ binary that translates Arrow Flight gRPC requests
# into HTTP calls against a running gateway. Heavy dependencies are
# isolated to this directory; the rest of the project stays pure C.
#
# Build this target separately:
#     cmake -S src/flight_server -B src/flight_server/build
#     cmake --build src/flight_server/build -j
#
# Or via the project Makefile shortcut:
#     make flight
#
# Requirements (typical install):
#   macOS:  brew install apache-arrow grpc nlohmann-json
#   Debian: apt install libarrow-flight-dev libgrpc++-dev nlohmann-json3-dev libcurl4-openssl-dev cmake

cmake_minimum_required(VERSION 3.16)
project(napastak_flight_server CXX)

set(CMAKE_CXX_STANDARD 20)        # Arrow 24.x uses std::log2p1 (C++20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# ── Dependencies ─────────────────────────────────────────────
# All discovered via find_package; the user is expected to have them
# installed via the system package manager (brew / apt / yum / nix).

find_package(Arrow         REQUIRED)
find_package(ArrowFlight   REQUIRED)
find_package(CURL          REQUIRED)
find_package(nlohmann_json REQUIRED)

# Try Arrow's bundled gRPC first; fall back to the standalone gRPC package
# which Homebrew ships separately on macOS.
find_package(gRPC QUIET)

# ── Target ───────────────────────────────────────────────────
add_executable(napastak_flight_server
    main.cc
    flight_service.cc
    http_client.cc
    colbatch_to_arrow.cc
)

target_include_directories(napastak_flight_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(napastak_flight_server PRIVATE
    Arrow::arrow_shared
    ArrowFlight::arrow_flight_shared
    CURL::libcurl
    nlohmann_json::nlohmann_json
)

if(gRPC_FOUND)
    target_link_libraries(napastak_flight_server PRIVATE gRPC::grpc++)
endif()

# Verbose output during initial bring-up
target_compile_options(napastak_flight_server PRIVATE -Wall -Wextra -Wpedantic)

# Install rule (so `cmake --install` works for downstream packagers)
install(TARGETS napastak_flight_server DESTINATION bin)
