/* dfo_mcp_server — Model Context Protocol server for DataFlow OS.
 *
 * Transport: JSON-RPC over stdio (one message per line, separator '\n').
 *   stdin  → requests from the MCP client (Claude Desktop, Cursor, …)
 *   stdout → responses (NEVER write anything else here — protocol only)
 *   stderr → logs
 *
 * Modes:
 *   --gateway URL --api-key TOKEN  (HTTP mode — calls into running gateway)
 *   --data-dir DIR                 (embedded mode — placeholder, not yet wired)
 *
 * Example client config (~/Library/Application Support/Claude/claude_desktop_config.json):
 *   {
 *     "mcpServers": {
 *       "dataflow-os": {
 *         "command": "/path/to/dfo_mcp_server",
 *         "args": ["--gateway", "http://localhost:8080", "--api-key", "<JWT>"]
 *       }
 *     }
 *   }
 */
#include "mcp.h"
#include "../../lib/core/log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

/* Глобальное состояние сервера: режим, URL шлюза, токен, путь к данным. */
McpState g_mcp;

/* Печатает справку по аргументам командной строки в stderr. */
static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [--gateway URL --api-key TOKEN] | [--data-dir DIR]\n"
        "\n"
        "Options:\n"
        "  --gateway URL    DataFlow OS gateway URL (e.g. http://localhost:8080)\n"
        "  --api-key TOKEN  JWT bearer token for the gateway\n"
        "  --data-dir DIR   Embedded mode (not implemented yet)\n",
        prog);
}

/* Разбирает argv в g_mcp. По умолчанию HTTP-режим на localhost:8080.
 * Возвращает ненулевое значение при ошибке или запросе справки (нужно выйти). */
static int parse_args(int argc, char **argv) {
    g_mcp.mode = MCP_MODE_HTTP;
    strncpy(g_mcp.gateway_url, "http://localhost:8080", sizeof(g_mcp.gateway_url) - 1);
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--gateway") == 0 && i + 1 < argc) {
            strncpy(g_mcp.gateway_url, argv[++i], sizeof(g_mcp.gateway_url) - 1);
        } else if (strcmp(argv[i], "--api-key") == 0 && i + 1 < argc) {
            strncpy(g_mcp.api_key, argv[++i], sizeof(g_mcp.api_key) - 1);
        } else if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            g_mcp.mode = MCP_MODE_EMBEDDED;
            strncpy(g_mcp.data_dir, argv[++i], sizeof(g_mcp.data_dir) - 1);
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]); return 1;
        } else {
            fprintf(stderr, "unknown arg: %s\n", argv[i]);
            usage(argv[0]); return 1;
        }
    }
    if (g_mcp.mode == MCP_MODE_EMBEDDED) {
        fprintf(stderr, "embedded mode is not yet implemented; "
                        "pass --gateway URL --api-key TOKEN instead\n");
        return 1;
    }
    return 0;
}

/* Флаг штатного завершения, выставляется обработчиком сигналов;
 * sig_atomic_t/volatile — для безопасного чтения из основного цикла. */
static volatile sig_atomic_t g_shutdown = 0;
static void on_sig(int s) { (void)s; g_shutdown = 1; }

int main(int argc, char **argv) {
    if (parse_args(argc, argv)) return 1;

    /* CRITICAL: stdout is the protocol channel. Logs go to stderr. */
    log_init(&g_log, stderr, LOG_INFO, 0);
    setvbuf(stdout, NULL, _IOLBF, 0);   /* line-buffered protocol output */

    signal(SIGINT,  on_sig);
    signal(SIGTERM, on_sig);
    signal(SIGPIPE, SIG_IGN);

    LOG_INFO("dfo_mcp_server starting — gateway=%s mode=%s",
             g_mcp.gateway_url,
             g_mcp.mode == MCP_MODE_HTTP ? "http" : "embedded");

    char *line = malloc(MCP_LINE_BUF);
    if (!line) { LOG_ERROR("oom"); return 1; }

    /* Основной цикл: по одному JSON-RPC сообщению на строку из stdin. */
    while (!g_shutdown && fgets(line, MCP_LINE_BUF, stdin)) {
        size_t n = strlen(line);
        /* Срезаем завершающие \n и \r (учитывая CRLF). */
        while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r')) line[--n] = '\0';
        if (n == 0) continue;  /* пустые строки игнорируем */
        mcp_handle_message(line);
    }

    free(line);
    LOG_INFO("dfo_mcp_server stopped");
    return 0;
}
