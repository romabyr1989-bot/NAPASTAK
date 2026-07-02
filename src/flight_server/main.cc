/* main.cc — dfo_flight_server entry point.
 *
 * Bootstraps the gRPC server on a TCP port and serves Flight requests via
 * DfoFlightService. All logs go to stderr — Flight clients communicate over
 * the gRPC port; nothing else should write to stdout.
 *
 * Run:
 *   dfo_flight_server --gateway http://localhost:8080 \
 *                     --port 8815 --api-key dfo_xxx
 */
#include <arrow/flight/server.h>
#include <arrow/util/logging.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

#include "flight_service.h"

namespace flight = arrow::flight;

namespace {

struct CliConfig {
    std::string gateway = "http://localhost:8080";
    std::string api_key;
    int         port    = 8815;
    std::string host    = "0.0.0.0";
};

void print_usage(const char* prog) {
    std::cerr <<
        "Usage: " << prog << " [--gateway URL] [--api-key TOKEN]\n"
        "                 [--host HOST] [--port PORT]\n"
        "\n"
        "Defaults: --gateway http://localhost:8080  --host 0.0.0.0  --port 8815\n";
}

bool parse_args(int argc, char** argv, CliConfig& c) {
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help")    { print_usage(argv[0]); return false; }
        else if (arg == "--gateway" && i + 1 < argc) c.gateway = argv[++i];
        else if (arg == "--api-key" && i + 1 < argc) c.api_key = argv[++i];
        else if (arg == "--port"    && i + 1 < argc) c.port    = std::atoi(argv[++i]);
        else if (arg == "--host"    && i + 1 < argc) c.host    = argv[++i];
        else { std::cerr << "unknown arg: " << arg << "\n"; print_usage(argv[0]); return false; }
    }
    return true;
}

std::unique_ptr<flight::FlightServerBase> g_server;

/* Обработчик SIGINT/SIGTERM: инициирует graceful-остановку gRPC-сервера,
 * после чего блокирующий Serve() в main возвращает управление. */
void on_signal(int sig) {
    (void)sig;
    if (g_server) {
        std::cerr << "[flight] shutdown signal received\n";
        auto _ = g_server->Shutdown();
    }
}

}  // namespace

int main(int argc, char** argv) {
    CliConfig cfg;
    if (!parse_args(argc, argv, cfg)) return 1;

    auto loc_result = flight::Location::ForGrpcTcp(cfg.host, cfg.port);
    if (!loc_result.ok()) {
        std::cerr << "[flight] cannot construct location: "
                  << loc_result.status().ToString() << "\n";
        return 1;
    }
    flight::Location location = loc_result.ValueOrDie();
    flight::FlightServerOptions options(location);
    auto service = std::make_unique<dfo::DfoFlightService>(cfg.gateway, cfg.api_key);

    auto init_status = service->Init(options);
    if (!init_status.ok()) {
        std::cerr << "[flight] Init failed: " << init_status.ToString() << "\n";
        return 1;
    }
    g_server.reset(service.release());

    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);
    std::signal(SIGPIPE, SIG_IGN);

    std::cerr << "[flight] dfo_flight_server listening on grpc://"
              << cfg.host << ":" << cfg.port
              << "  (gateway=" << cfg.gateway << ")\n";

    auto serve_status = g_server->Serve();
    if (!serve_status.ok()) {
        std::cerr << "[flight] Serve failed: " << serve_status.ToString() << "\n";
        return 1;
    }
    std::cerr << "[flight] stopped\n";
    return 0;
}
