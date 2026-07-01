/* dispatch.c — JSON-RPC routing, response framing, initialize/tools-list. */
#include "mcp.h"
#include "../../lib/core/log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Wire output ──────────────────────────────────────────────── */
void mcp_send_raw(const char *json) {
    fputs(json, stdout);
    fputc('\n', stdout);
    fflush(stdout);
}

/* Emit a JVal id (string / number / null) as JSON into a JBuf.
 * JSON-RPC requires the response id to mirror the request id verbatim. */
void mcp_jb_emit_jval(JBuf *jb, JVal *v) {
    if (!v) { jb_null(jb); return; }
    switch (v->type) {
        case JV_NULL:   jb_null(jb); break;
        case JV_BOOL:   jb_bool(jb, v->b); break;
        case JV_NUMBER: {
            /* ints come through as doubles; emit without trailing zeros */
            double d = v->n;
            if (d == (long long)d) jb_int(jb, (long long)d);
            else                   jb_double(jb, d);
            break;
        }
        case JV_STRING: jb_strn(jb, v->s, v->len); break;
        case JV_ARRAY:
            jb_arr_begin(jb);
            for (size_t i = 0; i < v->nitems; i++) mcp_jb_emit_jval(jb, v->items[i]);
            jb_arr_end(jb);
            break;
        case JV_OBJECT:
            jb_obj_begin(jb);
            for (size_t i = 0; i < v->nkeys; i++) {
                jb_key(jb, v->keys[i]);
                mcp_jb_emit_jval(jb, v->vals[i]);
            }
            jb_obj_end(jb);
            break;
        default: jb_null(jb); break;
    }
}

/* Сформировать и отправить JSON-RPC error-ответ с заданными code и message;
 * id зеркалит id запроса (или null). Использует временную арену. */
void mcp_send_error(JVal *id, int code, const char *msg) {
    Arena *a = arena_create(2048);
    JBuf b; jb_init(&b, a, 1024);
    jb_obj_begin(&b);
        jb_key(&b, "jsonrpc"); jb_str(&b, "2.0");
        jb_key(&b, "id");      mcp_jb_emit_jval(&b, id);
        jb_key(&b, "error");
        jb_obj_begin(&b);
            jb_key(&b, "code");    jb_int(&b, code);
            jb_key(&b, "message"); jb_str(&b, msg);
        jb_obj_end(&b);
    jb_obj_end(&b);
    mcp_send_raw(jb_done(&b));
    arena_destroy(a);
}

/* ── Dispatch ─────────────────────────────────────────────────── */
/* Точка входа для одного входящего JSON-RPC сообщения: парсит, проверяет
 * method/id и маршрутизирует на соответствующий обработчик. Уведомления
 * (без id) при неизвестном методе игнорируются. */
void mcp_handle_message(const char *json) {
    Arena *a = arena_create(64 * 1024);
    JVal *req = json_parse(a, json, strlen(json));
    if (!req || req->type == JV_ERROR || req->type != JV_OBJECT) {
        mcp_send_error(NULL, -32700, "Parse error");
        arena_destroy(a);
        return;
    }

    JVal *id     = json_get(req, "id");
    JVal *method = json_get(req, "method");
    JVal *params = json_get(req, "params");

    if (!method || method->type != JV_STRING) {
        mcp_send_error(id, -32600, "Invalid Request: missing method");
        arena_destroy(a);
        return;
    }

    /* json_str gives a NUL-terminated copy when needed; for fixed-size compare we use strncmp via len */
    const char *m   = method->s;
    size_t      mlen = method->len;

    #define METHOD_IS(name) (mlen == sizeof(name) - 1 && memcmp(m, name, mlen) == 0)

    if      (METHOD_IS("initialize"))      mcp_handle_initialize(id, params, a);
    else if (METHOD_IS("initialized"))     g_mcp.initialized = 1;          /* notification */
    else if (METHOD_IS("notifications/initialized")) g_mcp.initialized = 1; /* alt name */
    else if (METHOD_IS("tools/list"))      mcp_handle_tools_list(id, params, a);
    else if (METHOD_IS("tools/call"))      mcp_handle_tools_call(id, params, a);
    else if (METHOD_IS("ping"))            {
        JBuf b; jb_init(&b, a, 256);
        jb_obj_begin(&b);
            jb_key(&b, "jsonrpc"); jb_str(&b, "2.0");
            jb_key(&b, "id");      mcp_jb_emit_jval(&b, id);
            jb_key(&b, "result");  jb_obj_begin(&b); jb_obj_end(&b);
        jb_obj_end(&b);
        mcp_send_raw(jb_done(&b));
    }
    else if (METHOD_IS("shutdown")) {
        JBuf b; jb_init(&b, a, 256);
        jb_obj_begin(&b);
            jb_key(&b, "jsonrpc"); jb_str(&b, "2.0");
            jb_key(&b, "id");      mcp_jb_emit_jval(&b, id);
            jb_key(&b, "result");  jb_obj_begin(&b); jb_obj_end(&b);
        jb_obj_end(&b);
        mcp_send_raw(jb_done(&b));
        arena_destroy(a);
        exit(0);
    }
    else {
        if (id) mcp_send_error(id, -32601, "Method not found");
        /* notifications without id: silently ignored */
    }

    #undef METHOD_IS
    arena_destroy(a);
}

/* ── initialize handshake ─────────────────────────────────────── */
void mcp_handle_initialize(JVal *id, JVal *params, Arena *a) {
    if (params && params->type == JV_OBJECT) {
        JVal *ci = json_get(params, "clientInfo");
        if (ci && ci->type == JV_OBJECT) {
            const char *cn = json_str(json_get(ci, "name"), NULL);
            if (cn) strncpy(g_mcp.client_name, cn, sizeof(g_mcp.client_name) - 1);
        }
    }
    LOG_INFO("MCP client connected: %s",
             g_mcp.client_name[0] ? g_mcp.client_name : "(unknown)");

    JBuf b; jb_init(&b, a, 1024);
    jb_obj_begin(&b);
        jb_key(&b, "jsonrpc"); jb_str(&b, "2.0");
        jb_key(&b, "id");      mcp_jb_emit_jval(&b, id);
        jb_key(&b, "result");
        jb_obj_begin(&b);
            jb_key(&b, "protocolVersion"); jb_str(&b, MCP_PROTOCOL_VERSION);
            jb_key(&b, "capabilities");
            jb_obj_begin(&b);
                jb_key(&b, "tools");
                jb_obj_begin(&b);
                    jb_key(&b, "listChanged"); jb_bool(&b, false);
                jb_obj_end(&b);
            jb_obj_end(&b);
            jb_key(&b, "serverInfo");
            jb_obj_begin(&b);
                jb_key(&b, "name");    jb_str(&b, MCP_SERVER_NAME);
                jb_key(&b, "version"); jb_str(&b, MCP_SERVER_VERSION);
            jb_obj_end(&b);
        jb_obj_end(&b);
    jb_obj_end(&b);
    mcp_send_raw(jb_done(&b));
}

/* ── tools/list ───────────────────────────────────────────────── */
/* Helper to emit a single tool descriptor.
 * `props_json` is a raw JSON object literal of the inputSchema.properties,
 * `required` is a comma-separated list of required field names (or ""). */
static void emit_tool(JBuf *b, const char *name, const char *desc,
                      const char *props_json, const char *required_csv) {
    jb_obj_begin(b);
        jb_key(b, "name");        jb_str(b, name);
        jb_key(b, "description"); jb_str(b, desc);
        jb_key(b, "inputSchema");
        jb_obj_begin(b);
            jb_key(b, "type"); jb_str(b, "object");
            jb_key(b, "properties");
            jb_raw(b, props_json);
            jb_key(b, "required");
            jb_arr_begin(b);
            if (required_csv && *required_csv) {
                const char *p = required_csv;
                while (*p) {
                    const char *e = strchr(p, ',');
                    size_t n = e ? (size_t)(e - p) : strlen(p);
                    jb_strn(b, p, n);
                    if (!e) break;
                    p = e + 1;
                }
            }
            jb_arr_end(b);
        jb_obj_end(b);
    jb_obj_end(b);
}

void mcp_handle_tools_list(JVal *id, JVal *params, Arena *a) {
    (void)params;
    JBuf b; jb_init(&b, a, 16 * 1024);
    jb_obj_begin(&b);
    jb_key(&b, "jsonrpc"); jb_str(&b, "2.0");
    jb_key(&b, "id");      mcp_jb_emit_jval(&b, id);
    jb_key(&b, "result");
    jb_obj_begin(&b);
    jb_key(&b, "tools");
    jb_arr_begin(&b);

    emit_tool(&b, "query",
        "Execute SQL on NAPASTAK. Supports SELECT/INSERT/UPDATE/DELETE/"
        "BEGIN/COMMIT/ROLLBACK and aggregates with GROUP BY, JOIN, window functions. "
        "Returns rows + columns + duration_ms. RBAC applies based on the configured API key.",
        "{\"sql\":{\"type\":\"string\",\"description\":\"SQL statement to execute\"}}",
        "sql");

    emit_tool(&b, "list_tables",
        "List all tables in NAPASTAK with metadata (row count, columns, source).",
        "{}", "");

    emit_tool(&b, "describe_table",
        "Show the schema of a table plus a few sample rows. Returns column names, "
        "types, and a small preview to help compose queries.",
        "{\"table\":{\"type\":\"string\",\"description\":\"Table name\"},"
        "\"sample_rows\":{\"type\":\"integer\",\"description\":\"Rows to preview (default 5, max 50)\"}}",
        "table");

    emit_tool(&b, "ingest_csv",
        "Upload CSV text into a table (creates the table if it does not exist). "
        "First line is treated as the header.",
        "{\"table\":{\"type\":\"string\",\"description\":\"Target table name\"},"
        "\"csv_data\":{\"type\":\"string\",\"description\":\"CSV content (header + rows)\"}}",
        "table,csv_data");

    emit_tool(&b, "ingest_url",
        "Fetch CSV/JSON/Parquet from a URL via the gateway pipeline runner and "
        "ingest into a table. Useful for one-shot imports.",
        "{\"table\":{\"type\":\"string\",\"description\":\"Target table name\"},"
        "\"url\":{\"type\":\"string\",\"description\":\"HTTPS URL of the source file\"},"
        "\"format\":{\"type\":\"string\",\"description\":\"csv | json | parquet (default csv)\"}}",
        "table,url");

    emit_tool(&b, "list_pipelines",
        "List all configured pipelines with their cron schedule and last-run status.",
        "{}", "");

    emit_tool(&b, "run_pipeline",
        "Trigger a pipeline run by ID immediately, ignoring its cron schedule.",
        "{\"pipeline_id\":{\"type\":\"string\",\"description\":\"Pipeline UUID\"}}",
        "pipeline_id");

    emit_tool(&b, "create_pipeline",
        "Create a new pipeline. Steps are JSON describing connector_type and target_table.",
        "{\"name\":{\"type\":\"string\"},"
        "\"cron\":{\"type\":\"string\",\"description\":\"Cron expression or empty for manual\"},"
        "\"steps\":{\"type\":\"array\",\"description\":\"Pipeline steps\"}}",
        "name,steps");

    emit_tool(&b, "get_metrics",
        "Return current process metrics: uptime, total rows ingested, queries run, "
        "average latencies. No parameters.",
        "{}", "");

    emit_tool(&b, "analyze",
        "Compute basic profile of a table — row count, column null %, min/max for "
        "numeric columns. Helps the agent decide which columns are useful.",
        "{\"table\":{\"type\":\"string\",\"description\":\"Table name\"}}",
        "table");

    jb_arr_end(&b);
    jb_obj_end(&b);
    jb_obj_end(&b);
    mcp_send_raw(jb_done(&b));
}

/* ── tools/call ───────────────────────────────────────────────── */
/* Диспетчеризация вызова инструмента по полю name: подбирает tool_*-функцию,
 * собирает её текстовый вывод в content[0].text и проставляет isError. */
void mcp_handle_tools_call(JVal *id, JVal *params, Arena *a) {
    if (!params || params->type != JV_OBJECT) {
        mcp_send_error(id, -32602, "Invalid params");
        return;
    }
    JVal *name = json_get(params, "name");
    JVal *args = json_get(params, "arguments");
    if (!name || name->type != JV_STRING) {
        mcp_send_error(id, -32602, "Missing 'name'");
        return;
    }

    char *out = arena_alloc(a, MCP_RESULT_BUF);
    out[0] = '\0';
    int is_err = 0;
    const char *n = name->s;
    size_t nlen = name->len;

    #define NAME_IS(s) (nlen == sizeof(s) - 1 && memcmp(n, s, nlen) == 0)
    if      (NAME_IS("query"))            tool_query          (args, out, MCP_RESULT_BUF, &is_err, a);
    else if (NAME_IS("list_tables"))      tool_list_tables    (args, out, MCP_RESULT_BUF, &is_err, a);
    else if (NAME_IS("describe_table"))   tool_describe_table (args, out, MCP_RESULT_BUF, &is_err, a);
    else if (NAME_IS("ingest_csv"))       tool_ingest_csv     (args, out, MCP_RESULT_BUF, &is_err, a);
    else if (NAME_IS("ingest_url"))       tool_ingest_url     (args, out, MCP_RESULT_BUF, &is_err, a);
    else if (NAME_IS("list_pipelines"))   tool_list_pipelines (args, out, MCP_RESULT_BUF, &is_err, a);
    else if (NAME_IS("run_pipeline"))     tool_run_pipeline   (args, out, MCP_RESULT_BUF, &is_err, a);
    else if (NAME_IS("create_pipeline"))  tool_create_pipeline(args, out, MCP_RESULT_BUF, &is_err, a);
    else if (NAME_IS("get_metrics"))      tool_get_metrics    (args, out, MCP_RESULT_BUF, &is_err, a);
    else if (NAME_IS("analyze"))          tool_analyze        (args, out, MCP_RESULT_BUF, &is_err, a);
    else {
        snprintf(out, MCP_RESULT_BUF, "Unknown tool: %.*s", (int)nlen, n);
        is_err = 1;
    }
    #undef NAME_IS

    JBuf b; jb_init(&b, a, strlen(out) + 1024);
    jb_obj_begin(&b);
        jb_key(&b, "jsonrpc"); jb_str(&b, "2.0");
        jb_key(&b, "id");      mcp_jb_emit_jval(&b, id);
        jb_key(&b, "result");
        jb_obj_begin(&b);
            jb_key(&b, "content");
            jb_arr_begin(&b);
                jb_obj_begin(&b);
                    jb_key(&b, "type"); jb_str(&b, "text");
                    jb_key(&b, "text"); jb_str(&b, out);
                jb_obj_end(&b);
            jb_arr_end(&b);
            jb_key(&b, "isError"); jb_bool(&b, is_err != 0);
        jb_obj_end(&b);
    jb_obj_end(&b);
    mcp_send_raw(jb_done(&b));
}
