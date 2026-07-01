/* mcp.h — shared types/decls for dfo_mcp_server.
 *
 * Implements the Model Context Protocol (https://modelcontextprotocol.io)
 * over JSON-RPC on stdio. Exposes NAPASTAK as a tool provider for AI
 * agents (Claude Desktop, Cursor, Continue, Cline, …).
 */
#pragma once
#include "../../lib/core/arena.h"
#include "../../lib/core/json.h"
#include <stdbool.h>

#define MCP_PROTOCOL_VERSION "2024-11-05"
#define MCP_SERVER_NAME      "dataflow-os"
#define MCP_SERVER_VERSION   "0.1.0"

#define MCP_LINE_BUF (256 * 1024)   /* one JSON-RPC message ≤ 256 KB */
#define MCP_RESULT_BUF (64 * 1024)  /* tool result text ≤ 64 KB */

/* Режим работы: HTTP — проксирование запросов в gateway по сети;
 * EMBEDDED — прямой доступ к данным без сетевого слоя (заготовка). */
typedef enum { MCP_MODE_HTTP, MCP_MODE_EMBEDDED } McpMode;

/* Конфигурация и состояние одного экземпляра MCP-сервера. */
typedef struct {
    McpMode mode;
    char    gateway_url[512];   /* e.g. http://localhost:8080 */
    char    api_key[256];       /* JWT bearer token (sent as Authorization) */
    char    data_dir[512];      /* unused for now (embedded mode placeholder) */
    char    client_name[128];   /* "Claude Desktop", "Cursor", … */
    int     initialized;
} McpState;

/* Глобальное состояние сервера (единственный экземпляр на процесс). */
extern McpState g_mcp;

/* ── Dispatch (main.c) ── */
void mcp_handle_message(const char *json);
void mcp_send_raw(const char *json);
void mcp_send_error(JVal *id, int code, const char *msg);
void mcp_send_result_obj(JVal *id, const char *result_json_obj);

/* ── Handshake / discovery (main.c) ── */
void mcp_handle_initialize(JVal *id, JVal *params, Arena *a);
void mcp_handle_tools_list(JVal *id, JVal *params, Arena *a);
void mcp_handle_tools_call(JVal *id, JVal *params, Arena *a);

/* ── JSON helpers ── */
/* Re-emit a JVal as JSON into a JBuf — used to echo back the request id
 * which can be either a number or a string per JSON-RPC spec. */
void mcp_jb_emit_jval(JBuf *jb, JVal *v);

/* ── HTTP client (http_client.c) ── */
/* Результат одного HTTP-запроса к gateway. */
typedef struct {
    int   status;          /* HTTP status, -1 on transport error */
    char *body;            /* arena-owned */
    size_t body_len;
    char  err[256];
} McpHttpResp;

int mcp_http_get (const char *url_base, const char *path,
                  const char *bearer, McpHttpResp *out, Arena *a);
int mcp_http_post(const char *url_base, const char *path,
                  const char *bearer, const char *body,
                  const char *content_type, McpHttpResp *out, Arena *a);
int mcp_http_delete(const char *url_base, const char *path,
                    const char *bearer, McpHttpResp *out, Arena *a);

/* ── Tools (tools.c) ──
 * Every tool fills `out` (text for the MCP `content` field) and sets
 * *is_err to non-zero if the call failed. Functions never throw or exit. */
void tool_query          (JVal *args, char *out, size_t cap, int *is_err, Arena *a);
void tool_list_tables    (JVal *args, char *out, size_t cap, int *is_err, Arena *a);
void tool_describe_table (JVal *args, char *out, size_t cap, int *is_err, Arena *a);
void tool_ingest_csv     (JVal *args, char *out, size_t cap, int *is_err, Arena *a);
void tool_ingest_url     (JVal *args, char *out, size_t cap, int *is_err, Arena *a);
void tool_list_pipelines (JVal *args, char *out, size_t cap, int *is_err, Arena *a);
void tool_run_pipeline   (JVal *args, char *out, size_t cap, int *is_err, Arena *a);
void tool_create_pipeline(JVal *args, char *out, size_t cap, int *is_err, Arena *a);
void tool_get_metrics    (JVal *args, char *out, size_t cap, int *is_err, Arena *a);
void tool_analyze        (JVal *args, char *out, size_t cap, int *is_err, Arena *a);
