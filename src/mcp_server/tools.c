/* tools.c — MCP tool implementations.
 *
 * Every tool reads its arguments from a JVal*, calls the gateway over HTTP,
 * and formats a human-readable text response. AI agents work better with
 * Markdown-style text than with raw JSON, so we render tables and prose. */
#include "mcp.h"
#include "../../lib/core/log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

/* ── Small helpers ─────────────────────────────────────────────── */

static void put_err(char *out, size_t cap, int *is_err, const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    vsnprintf(out, cap, fmt, ap);
    va_end(ap);
    *is_err = 1;
}

/* HTTP-error → text */
static int handle_http_failure(const McpHttpResp *r, char *out, size_t cap, int *is_err) {
    if (r->status < 0) { put_err(out, cap, is_err, "Network error: %s", r->err); return 1; }
    if (r->status >= 400) {
        put_err(out, cap, is_err, "Gateway returned HTTP %d:\n%.500s",
                r->status, r->body ? r->body : "");
        return 1;
    }
    return 0;
}

/* Append a printf-style chunk to out, advancing *off.
 * Silently stops once the buffer is full so callers can ignore truncation. */
static void append(char *out, size_t cap, size_t *off, const char *fmt, ...) {
    if (*off >= cap) return;
    va_list ap; va_start(ap, fmt);
    int n = vsnprintf(out + *off, cap - *off, fmt, ap);
    va_end(ap);
    if (n > 0) *off += (size_t)n;
}

/* JSON-escape a string into a stack buffer, returning the buffer pointer.
 * Used to build outgoing request bodies. */
static char *json_escape_into(const char *s, char *dst, size_t cap) {
    size_t o = 0;
    for (size_t i = 0; s[i] && o + 6 < cap; i++) {
        unsigned char c = (unsigned char)s[i];
        if      (c == '"')  { dst[o++] = '\\'; dst[o++] = '"'; }
        else if (c == '\\') { dst[o++] = '\\'; dst[o++] = '\\'; }
        else if (c == '\n') { dst[o++] = '\\'; dst[o++] = 'n'; }
        else if (c == '\r') { dst[o++] = '\\'; dst[o++] = 'r'; }
        else if (c == '\t') { dst[o++] = '\\'; dst[o++] = 't'; }
        else if (c < 0x20)  o += (size_t)snprintf(dst + o, cap - o, "\\u%04x", c);
        else                dst[o++] = (char)c;
    }
    dst[o] = '\0';
    return dst;
}

/* Format a JVal as a short string for table cells. */
static void cell_to_str(JVal *v, char *out, size_t cap) {
    if (!v) { snprintf(out, cap, ""); return; }
    switch (v->type) {
        case JV_NULL:   snprintf(out, cap, "NULL"); break;
        case JV_BOOL:   snprintf(out, cap, v->b ? "true" : "false"); break;
        case JV_NUMBER: {
            double d = v->n;
            if (d == (long long)d) snprintf(out, cap, "%lld", (long long)d);
            else                   snprintf(out, cap, "%g", d);
            break;
        }
        case JV_STRING: snprintf(out, cap, "%.*s", (int)v->len, v->s); break;
        default:        snprintf(out, cap, "(complex)"); break;
    }
}

/* ── tool: query ──────────────────────────────────────────────── */
/* Run arbitrary SQL via the gateway and render the result as a Markdown
 * table (SELECT) or a short status line (DML / non-tabular response). */
void tool_query(JVal *args, char *out, size_t cap, int *is_err, Arena *a) {
    const char *sql = json_str(json_get(args, "sql"), NULL);
    if (!sql) { put_err(out, cap, is_err, "Missing 'sql' parameter"); return; }

    /* Build {"sql":"…"} body with proper escaping */
    size_t sql_len = strlen(sql);
    char *esc = arena_alloc(a, sql_len * 6 + 1);
    json_escape_into(sql, esc, sql_len * 6 + 1);
    char *body = arena_alloc(a, strlen(esc) + 32);
    sprintf(body, "{\"sql\":\"%s\"}", esc);

    McpHttpResp r = {0};
    if (mcp_http_post(g_mcp.gateway_url, "/api/tables/query",
                       g_mcp.api_key, body, "application/json", &r, a) < 0) {
        put_err(out, cap, is_err, "Network error: %s", r.err); return;
    }
    if (handle_http_failure(&r, out, cap, is_err)) return;

    /* Parse: {"columns":["a","b"],"rows":[[1,"x"],...],"duration_ms":12} */
    JVal *resp = json_parse(a, r.body, r.body_len);
    if (!resp || resp->type != JV_OBJECT) {
        put_err(out, cap, is_err, "Invalid gateway response (not JSON):\n%.300s", r.body);
        return;
    }
    JVal *cols = json_get(resp, "columns");
    JVal *rows = json_get(resp, "rows");
    long long dur = json_int(json_get(resp, "duration_ms"), 0);

    size_t off = 0;
    if (cols && rows && cols->type == JV_ARRAY && rows->type == JV_ARRAY) {
        size_t ncols = cols->nitems;
        size_t nrows = rows->nitems;
        append(out, cap, &off, "Query executed in %lldms — %zu row%s\n\n",
               dur, nrows, nrows == 1 ? "" : "s");
        if (ncols == 0) { append(out, cap, &off, "(no columns)\n"); return; }

        /* Header row */
        append(out, cap, &off, "|");
        for (size_t i = 0; i < ncols; i++) {
            char nm[64]; cell_to_str(cols->items[i], nm, sizeof(nm));
            append(out, cap, &off, " %s |", nm);
        }
        append(out, cap, &off, "\n|");
        for (size_t i = 0; i < ncols; i++) append(out, cap, &off, "----|");
        append(out, cap, &off, "\n");

        /* Body — cap rendering at 200 rows to keep response small */
        size_t shown = nrows > 200 ? 200 : nrows;
        for (size_t r2 = 0; r2 < shown; r2++) {
            JVal *row = rows->items[r2];
            if (!row || row->type != JV_ARRAY) continue;
            append(out, cap, &off, "|");
            for (size_t c = 0; c < ncols; c++) {
                char buf[256];
                cell_to_str(c < row->nitems ? row->items[c] : NULL, buf, sizeof(buf));
                append(out, cap, &off, " %s |", buf);
            }
            append(out, cap, &off, "\n");
        }
        if (nrows > shown)
            append(out, cap, &off, "\n(showing %zu of %zu rows)\n", shown, nrows);
    } else {
        /* DML or non-tabular response — just echo it */
        long long deleted = json_int(json_get(resp, "deleted"), -1);
        long long updated = json_int(json_get(resp, "updated"), -1);
        long long inserted = json_int(json_get(resp, "inserted"), -1);
        if      (deleted  >= 0) append(out, cap, &off, "Deleted %lld rows in %lldms.", deleted, dur);
        else if (updated  >= 0) append(out, cap, &off, "Updated %lld rows in %lldms.", updated, dur);
        else if (inserted >= 0) append(out, cap, &off, "Inserted %lld rows in %lldms.", inserted, dur);
        else                    append(out, cap, &off, "OK in %lldms.\n%.500s", dur, r.body);
    }
}

/* ── tool: list_tables ────────────────────────────────────────── */
void tool_list_tables(JVal *args, char *out, size_t cap, int *is_err, Arena *a) {
    (void)args;
    McpHttpResp r = {0};
    if (mcp_http_get(g_mcp.gateway_url, "/api/tables", g_mcp.api_key, &r, a) < 0) {
        put_err(out, cap, is_err, "Network error: %s", r.err); return;
    }
    if (handle_http_failure(&r, out, cap, is_err)) return;

    JVal *list = json_parse(a, r.body, r.body_len);
    if (!list || list->type != JV_ARRAY) {
        put_err(out, cap, is_err, "Invalid response: %.200s", r.body); return;
    }
    size_t off = 0;
    if (list->nitems == 0) {
        append(out, cap, &off,
               "No tables exist yet. Use the 'ingest_csv' tool or run a pipeline to create one.\n");
        return;
    }
    append(out, cap, &off, "Found %zu table%s:\n\n",
           list->nitems, list->nitems == 1 ? "" : "s");
    append(out, cap, &off, "| Name | Rows | Columns | Source |\n|------|------|---------|--------|\n");
    for (size_t i = 0; i < list->nitems; i++) {
        JVal *t = list->items[i];
        const char *name   = json_str(json_get(t, "name"), "?");
        long long   rows   = json_int(json_get(t, "rows"), 0);
        const char *source = json_str(json_get(t, "source"), "ingest");
        JVal *cols = json_get(t, "columns");
        size_t ncols = (cols && cols->type == JV_ARRAY) ? cols->nitems : 0;
        append(out, cap, &off, "| %s | %lld | %zu | %s |\n", name, rows, ncols, source);
    }
}

/* ── tool: describe_table ─────────────────────────────────────── */
void tool_describe_table(JVal *args, char *out, size_t cap, int *is_err, Arena *a) {
    const char *table = json_str(json_get(args, "table"), NULL);
    if (!table) { put_err(out, cap, is_err, "Missing 'table' parameter"); return; }
    long long sample = json_int(json_get(args, "sample_rows"), 5);
    if (sample < 0) sample = 0; if (sample > 50) sample = 50; /* clamp to [0,50] */

    /* Fetch /api/tables/<name>/info for schema */
    char path[512];
    snprintf(path, sizeof(path), "/api/tables/%s/info", table);
    McpHttpResp r = {0};
    if (mcp_http_get(g_mcp.gateway_url, path, g_mcp.api_key, &r, a) < 0) {
        put_err(out, cap, is_err, "Network error: %s", r.err); return;
    }
    /* Some gateway versions don't expose /info — fall back to /api/tables and filter */
    JVal *info = NULL;
    if (r.status == 200) {
        info = json_parse(a, r.body, r.body_len);
    } else {
        McpHttpResp r2 = {0};
        if (mcp_http_get(g_mcp.gateway_url, "/api/tables", g_mcp.api_key, &r2, a) < 0) {
            put_err(out, cap, is_err, "Network error: %s", r2.err); return;
        }
        if (handle_http_failure(&r2, out, cap, is_err)) return;
        JVal *list = json_parse(a, r2.body, r2.body_len);
        if (list && list->type == JV_ARRAY) {
            for (size_t i = 0; i < list->nitems; i++) {
                if (strcmp(json_str(json_get(list->items[i], "name"), ""), table) == 0) {
                    info = list->items[i]; break;
                }
            }
        }
    }
    if (!info) { put_err(out, cap, is_err, "Table '%s' not found", table); return; }

    size_t off = 0;
    long long rows = json_int(json_get(info, "rows"), 0);
    append(out, cap, &off, "Table: %s (%lld rows)\n\nSchema:\n", table, rows);
    JVal *cols = json_get(info, "columns");
    if (cols && cols->type == JV_ARRAY) {
        for (size_t i = 0; i < cols->nitems; i++) {
            JVal *c = cols->items[i];
            const char *cn = json_str(json_get(c, "name"), "?");
            const char *ct = json_str(json_get(c, "type"), "?");
            int nullable = (int)json_int(json_get(c, "nullable"), 1);
            append(out, cap, &off, "  - %-20s %-8s %s\n", cn, ct,
                   nullable ? "NULL" : "NOT NULL");
        }
    }

    /* Sample rows via SELECT */
    if (sample > 0) {
        char esc[256]; json_escape_into(table, esc, sizeof(esc));
        char body[512];
        snprintf(body, sizeof(body),
                 "{\"sql\":\"SELECT * FROM %s LIMIT %lld\"}", esc, sample);
        McpHttpResp rs = {0};
        if (mcp_http_post(g_mcp.gateway_url, "/api/tables/query",
                           g_mcp.api_key, body, "application/json", &rs, a) == 0
            && rs.status == 200) {
            append(out, cap, &off, "\nSample rows:\n");
            JVal *resp = json_parse(a, rs.body, rs.body_len);
            JVal *cnames = json_get(resp, "columns");
            JVal *rrows  = json_get(resp, "rows");
            if (cnames && rrows && cnames->type == JV_ARRAY && rrows->type == JV_ARRAY) {
                size_t nc = cnames->nitems;
                append(out, cap, &off, "|");
                for (size_t i = 0; i < nc; i++) {
                    char nm[64]; cell_to_str(cnames->items[i], nm, sizeof(nm));
                    append(out, cap, &off, " %s |", nm);
                }
                append(out, cap, &off, "\n");
                for (size_t r2 = 0; r2 < rrows->nitems; r2++) {
                    JVal *row = rrows->items[r2];
                    if (!row || row->type != JV_ARRAY) continue;
                    append(out, cap, &off, "|");
                    for (size_t c = 0; c < nc; c++) {
                        char buf[256];
                        cell_to_str(c < row->nitems ? row->items[c] : NULL, buf, sizeof(buf));
                        append(out, cap, &off, " %s |", buf);
                    }
                    append(out, cap, &off, "\n");
                }
            }
        }
    }
}

/* ── tool: ingest_csv ─────────────────────────────────────────── */
void tool_ingest_csv(JVal *args, char *out, size_t cap, int *is_err, Arena *a) {
    const char *table = json_str(json_get(args, "table"), NULL);
    const char *csv   = json_str(json_get(args, "csv_data"), NULL);
    if (!table || !csv) {
        put_err(out, cap, is_err, "Missing 'table' or 'csv_data'"); return;
    }
    char path[512];
    snprintf(path, sizeof(path), "/api/ingest/csv?table=%s", table);
    McpHttpResp r = {0};
    if (mcp_http_post(g_mcp.gateway_url, path, g_mcp.api_key,
                       csv, "text/csv", &r, a) < 0) {
        put_err(out, cap, is_err, "Network error: %s", r.err); return;
    }
    if (handle_http_failure(&r, out, cap, is_err)) return;

    JVal *resp = json_parse(a, r.body, r.body_len);
    long long rows = json_int(json_get(resp, "rows_loaded"),
                     json_int(json_get(resp, "rows"), -1));
    if (rows >= 0)
        snprintf(out, cap, "Ingested %lld rows into '%s'.", rows, table);
    else
        snprintf(out, cap, "Ingest OK.\n%.500s", r.body);
}

/* ── tool: ingest_url ─────────────────────────────────────────── */
void tool_ingest_url(JVal *args, char *out, size_t cap, int *is_err, Arena *a) {
    const char *table = json_str(json_get(args, "table"), NULL);
    const char *url   = json_str(json_get(args, "url"),   NULL);
    const char *fmt   = json_str(json_get(args, "format"), "csv");
    if (!table || !url) { put_err(out, cap, is_err, "Missing 'table' or 'url'"); return; }

    /* Build a one-shot pipeline payload (cron empty → not scheduled, run once below) */
    char et[256], eu[1024], ef[64];
    json_escape_into(table, et, sizeof(et));
    json_escape_into(url,   eu, sizeof(eu));
    json_escape_into(fmt,   ef, sizeof(ef));
    char body[2048];
    snprintf(body, sizeof(body),
        "{\"name\":\"mcp_ingest_%s\",\"cron\":\"\",\"steps\":["
        "{\"connector_type\":\"json_http\",\"connector_config\":"
        "{\"url\":\"%s\",\"method\":\"GET\",\"data_path\":\"\",\"page_type\":\"none\"},"
        "\"target_table\":\"%s\"}]}", et, eu, et);
    (void)ef; /* format is informational; the actual connector is json_http */

    McpHttpResp r = {0};
    if (mcp_http_post(g_mcp.gateway_url, "/api/pipelines",
                       g_mcp.api_key, body, "application/json", &r, a) < 0) {
        put_err(out, cap, is_err, "Network error: %s", r.err); return;
    }
    if (handle_http_failure(&r, out, cap, is_err)) return;

    /* Trigger the pipeline */
    JVal *resp = json_parse(a, r.body, r.body_len);
    const char *pid = json_str(json_get(resp, "id"), NULL);
    if (!pid) { snprintf(out, cap, "Created pipeline but no id returned: %.200s", r.body); return; }
    char run_path[256]; snprintf(run_path, sizeof(run_path), "/api/pipelines/%s/run", pid);
    McpHttpResp r2 = {0};
    mcp_http_post(g_mcp.gateway_url, run_path, g_mcp.api_key, "{}",
                   "application/json", &r2, a);
    snprintf(out, cap, "Pipeline %s created and started for table '%s' (URL: %s).",
             pid, table, url);
}

/* ── tool: list_pipelines ─────────────────────────────────────── */
void tool_list_pipelines(JVal *args, char *out, size_t cap, int *is_err, Arena *a) {
    (void)args;
    McpHttpResp r = {0};
    if (mcp_http_get(g_mcp.gateway_url, "/api/pipelines", g_mcp.api_key, &r, a) < 0) {
        put_err(out, cap, is_err, "Network error: %s", r.err); return;
    }
    if (handle_http_failure(&r, out, cap, is_err)) return;
    JVal *list = json_parse(a, r.body, r.body_len);
    if (!list || list->type != JV_ARRAY) {
        put_err(out, cap, is_err, "Invalid response"); return;
    }
    size_t off = 0;
    if (list->nitems == 0) { append(out, cap, &off, "No pipelines configured.\n"); return; }
    append(out, cap, &off, "%zu pipeline%s:\n\n",
           list->nitems, list->nitems == 1 ? "" : "s");
    append(out, cap, &off, "| ID | Name | Cron | Last run |\n|----|------|------|----------|\n");
    for (size_t i = 0; i < list->nitems; i++) {
        JVal *p = list->items[i];
        append(out, cap, &off, "| %s | %s | %s | %lld |\n",
               json_str(json_get(p, "id"),   "?"),
               json_str(json_get(p, "name"), "?"),
               json_str(json_get(p, "cron"), ""),
               json_int(json_get(p, "last_run"), 0));
    }
}

/* ── tool: run_pipeline ───────────────────────────────────────── */
void tool_run_pipeline(JVal *args, char *out, size_t cap, int *is_err, Arena *a) {
    const char *pid = json_str(json_get(args, "pipeline_id"), NULL);
    if (!pid) { put_err(out, cap, is_err, "Missing 'pipeline_id'"); return; }
    char path[512]; snprintf(path, sizeof(path), "/api/pipelines/%s/run", pid);
    McpHttpResp r = {0};
    if (mcp_http_post(g_mcp.gateway_url, path, g_mcp.api_key, "{}",
                       "application/json", &r, a) < 0) {
        put_err(out, cap, is_err, "Network error: %s", r.err); return;
    }
    if (handle_http_failure(&r, out, cap, is_err)) return;
    snprintf(out, cap, "Pipeline %s triggered.", pid);
}

/* ── tool: create_pipeline ────────────────────────────────────── */
void tool_create_pipeline(JVal *args, char *out, size_t cap, int *is_err, Arena *a) {
    /* The gateway accepts the same JSON shape we receive — just forward it. */
    if (!args || args->type != JV_OBJECT) {
        put_err(out, cap, is_err, "Arguments must be an object"); return;
    }
    JBuf b; jb_init(&b, a, 4096);
    mcp_jb_emit_jval(&b, args);
    const char *body = jb_done(&b);

    McpHttpResp r = {0};
    if (mcp_http_post(g_mcp.gateway_url, "/api/pipelines",
                       g_mcp.api_key, body, "application/json", &r, a) < 0) {
        put_err(out, cap, is_err, "Network error: %s", r.err); return;
    }
    if (handle_http_failure(&r, out, cap, is_err)) return;
    JVal *resp = json_parse(a, r.body, r.body_len);
    const char *pid = json_str(json_get(resp, "id"), NULL);
    if (pid) snprintf(out, cap, "Pipeline created with id: %s", pid);
    else     snprintf(out, cap, "Pipeline created.\n%.300s", r.body);
}

/* ── tool: get_metrics ────────────────────────────────────────── */
void tool_get_metrics(JVal *args, char *out, size_t cap, int *is_err, Arena *a) {
    (void)args;
    McpHttpResp r = {0};
    if (mcp_http_get(g_mcp.gateway_url, "/api/metrics", g_mcp.api_key, &r, a) < 0) {
        put_err(out, cap, is_err, "Network error: %s", r.err); return;
    }
    if (handle_http_failure(&r, out, cap, is_err)) return;
    JVal *m = json_parse(a, r.body, r.body_len);
    if (!m) { put_err(out, cap, is_err, "Invalid response"); return; }

    long long uptime  = json_int(json_get(m, "uptime"), 0);
    long long rows    = json_int(json_get(m, "total_rows"), 0);
    long long queries = json_int(json_get(m, "total_queries"), 0);
    long long pruns   = json_int(json_get(m, "total_pipelines_run"), 0);
    double    qlat    = json_dbl(json_get(m, "avg_query_latency_ms"), 0);
    double    plat    = json_dbl(json_get(m, "avg_pipeline_latency_ms"), 0);
    snprintf(out, cap,
        "NAPASTAK metrics:\n"
        "- Uptime: %lldh %lldm\n"
        "- Total rows ingested: %lld\n"
        "- Queries run: %lld\n"
        "- Pipeline runs: %lld\n"
        "- Avg query latency: %.1f ms\n"
        "- Avg pipeline latency: %.1f ms\n",
        uptime / 3600, (uptime % 3600) / 60, rows, queries, pruns, qlat, plat);
}

/* ── tool: analyze ────────────────────────────────────────────── */
/* Build a per-column profile: row count plus distinct/min/max per column,
 * issuing one aggregate query per column against the gateway. */
void tool_analyze(JVal *args, char *out, size_t cap, int *is_err, Arena *a) {
    const char *table = json_str(json_get(args, "table"), NULL);
    if (!table) { put_err(out, cap, is_err, "Missing 'table'"); return; }

    /* COUNT(*) */
    char esc[256]; json_escape_into(table, esc, sizeof(esc));
    char body[512];
    snprintf(body, sizeof(body),
             "{\"sql\":\"SELECT COUNT(*) AS n FROM %s\"}", esc);
    McpHttpResp r = {0};
    if (mcp_http_post(g_mcp.gateway_url, "/api/tables/query",
                       g_mcp.api_key, body, "application/json", &r, a) < 0) {
        put_err(out, cap, is_err, "Network error: %s", r.err); return;
    }
    if (handle_http_failure(&r, out, cap, is_err)) return;

    JVal *resp = json_parse(a, r.body, r.body_len);
    long long n = 0;
    JVal *rrs = json_get(resp, "rows");
    if (rrs && rrs->type == JV_ARRAY && rrs->nitems > 0) {
        JVal *first = rrs->items[0];
        if (first && first->type == JV_ARRAY && first->nitems > 0)
            n = json_int(first->items[0], 0);
    }

    /* Get column list via /api/tables list endpoint */
    McpHttpResp r2 = {0};
    if (mcp_http_get(g_mcp.gateway_url, "/api/tables", g_mcp.api_key, &r2, a) < 0) {
        put_err(out, cap, is_err, "Network error: %s", r2.err); return;
    }
    JVal *list = json_parse(a, r2.body, r2.body_len);
    JVal *info = NULL;
    if (list && list->type == JV_ARRAY) {
        for (size_t i = 0; i < list->nitems; i++) {
            if (strcmp(json_str(json_get(list->items[i], "name"), ""), table) == 0) {
                info = list->items[i]; break;
            }
        }
    }

    size_t off = 0;
    append(out, cap, &off, "Profile of '%s': %lld rows\n\n", table, n);
    if (!info) { append(out, cap, &off, "(table metadata not available)\n"); return; }

    JVal *cols = json_get(info, "columns");
    if (!cols || cols->type != JV_ARRAY) return;

    append(out, cap, &off, "| Column | Type | Distinct (sample) | Min | Max |\n");
    append(out, cap, &off, "|--------|------|--------------------|-----|-----|\n");
    /* Cap profiling at 20 columns to keep response bounded */
    size_t shown = cols->nitems > 20 ? 20 : cols->nitems;
    for (size_t i = 0; i < shown; i++) {
        JVal *c = cols->items[i];
        const char *cn = json_str(json_get(c, "name"), "?");
        const char *ct = json_str(json_get(c, "type"), "?");
        char eq[1024];
        snprintf(eq, sizeof(eq),
                 "{\"sql\":\"SELECT COUNT(DISTINCT %s) AS d, MIN(%s) AS mn, MAX(%s) AS mx FROM %s\"}",
                 cn, cn, cn, esc);
        McpHttpResp rc = {0};
        if (mcp_http_post(g_mcp.gateway_url, "/api/tables/query",
                           g_mcp.api_key, eq, "application/json", &rc, a) == 0
            && rc.status == 200) {
            JVal *pr = json_parse(a, rc.body, rc.body_len);
            JVal *prr = json_get(pr, "rows");
            char d[64] = "?", mn[64] = "?", mx[64] = "?";
            if (prr && prr->type == JV_ARRAY && prr->nitems > 0
                && prr->items[0] && prr->items[0]->type == JV_ARRAY
                && prr->items[0]->nitems >= 3) {
                cell_to_str(prr->items[0]->items[0], d,  sizeof(d));
                cell_to_str(prr->items[0]->items[1], mn, sizeof(mn));
                cell_to_str(prr->items[0]->items[2], mx, sizeof(mx));
            }
            append(out, cap, &off, "| %s | %s | %s | %s | %s |\n", cn, ct, d, mn, mx);
        } else {
            append(out, cap, &off, "| %s | %s | (skip) | | |\n", cn, ct);
        }
    }
    if (cols->nitems > shown)
        append(out, cap, &off, "\n(profiled %zu of %zu columns)\n", shown, cols->nitems);
}
