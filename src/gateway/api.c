/* api.c — HTTP/pgwire gateway: маршруты REST API, встроенный SQL-исполнитель
 * (eval_func/eval_val/exec_stmt с join/group/window), эмуляция pg-каталога,
 * запуск шагов пайплайнов (connector/sink/python/scala/scd2/match) и
 * обвязка auth/RBAC/audit/matview. Точка регистрации — api_register_routes(). */

/* Expose BSD/Darwin extensions (mkdtemp) that the global _POSIX_C_SOURCE
 * otherwise hides on macOS. No-op on Linux/glibc (covered by _GNU_SOURCE). */
#define _DARWIN_C_SOURCE
#include "app.h"
#include "../../lib/core/json.h"
#include "../../lib/core/log.h"
#include "../../lib/sql_parser/sql.h"
#include "../../lib/qengine/qengine.h"   /* qe_word_similarity / qe_normalize_* (also SQL fns) */
#include "../../lib/yaml/yaml_loader.h"
#include "../../lib/yaml/yaml_template.h"
#include "../../lib/pgwire/pgwire.h"
#include "../../lib/storage/storage.h"
#include "../../lib/storage/compress.h"
#include "../../lib/storage/txn.h"
#include "../../lib/connector/connector.h"
#include "../../lib/core/threadpool.h"   /* tp_submit — async pipeline execution */
#include "../../lib/auth/auth.h"
#include "../../lib/auth/rbac.h"
#include "../../lib/auth/audit.h"
#include "../../lib/matview/matview.h"
#include "../../lib/sql_template/sql_template.h"
#include <sqlite3.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <math.h>
#include <dirent.h>
/* MD5 for SCD2 row-hash change detection (scd2_hash_col). -lcrypto is already
 * in LDFLAGS. OPENSSL_SUPPRESS_DEPRECATED silences OpenSSL 3's deprecation of
 * the one-shot MD5() under -Wall (we only need a fast non-cryptographic digest
 * for change detection, not a security primitive). */
#define OPENSSL_SUPPRESS_DEPRECATED
#include <openssl/md5.h>

/* NULL contract sentinel: a cell equal to this string is a true SQL NULL.
 * Canonically defined in lib/storage/storage.h; this fallback guarantees api.c
 * compiles even if include ordering hides that define. Value MUST stay in sync. */
#ifndef DFO_NULL_SENTINEL
#define DFO_NULL_SENTINEL "\x01\x02__DFO_NULL__"
#endif

/* Thread-local: txn_id active during exec_stmt call (0 = auto-commit) */
static _Thread_local TxnId g_txn_current = 0;

/* ── Transaction commit callback: applies one buffered entry to a real table ── */
static int apply_txn_entry(const char *tname, TxnOpType op,
                            ColBatch *batch,
                            int64_t orig_offset,
                            const char *new_csv, size_t csv_len,
                            void *ud) {
    (void)ud;
    pthread_mutex_lock(&g_app.tables_mu);
    Table *t = hm_get(&g_app.tables, tname);
    pthread_mutex_unlock(&g_app.tables_mu);
    if (!t) { LOG_ERROR("txn commit: table '%s' not found", tname); return -1; }
    switch (op) {
        case TXN_OP_INSERT: return table_append(t, batch);
        case TXN_OP_DELETE: return table_delete(t, orig_offset);
        case TXN_OP_UPDATE: return table_update(t, orig_offset, new_csv, csv_len);
    }
    return -1;
}

/* ── RBAC helpers ── */

/* Returns true if access is allowed; writes 403 and returns false on denial. */
static bool check_table_access(HttpReq *req, HttpResp *resp,
                                const char *table_name, RbacAction action)
{
    if (!g_app.rbac_enabled || !g_app.rbac) return true;
    if (!g_app.auth_enabled) return true;
    if (rbac_check(g_app.rbac, &req->auth, action, table_name) == 0) return true;
    http_resp_error(resp, 403,
        arena_sprintf(req->arena, "access denied to table '%s' (role=%d)",
                      table_name, (int)req->auth.role));
    return false;
}

/* Подмешивает row-level-security фильтр в WHERE запроса: парсит rls_sql как
 * условие и объединяет с существующим WHERE через AND (или ставит как есть). */
static void apply_rls_to_select(SelectStmt *sel, const char *rls_sql, Arena *a)
{
    if (!rls_sql || !*rls_sql) return;
    char wrapper[1280];
    snprintf(wrapper, sizeof(wrapper), "SELECT 1 WHERE %s", rls_sql);
    Stmt *tmp = sql_parse(a, wrapper, strlen(wrapper));
    if (!tmp || tmp->error || tmp->type != STMT_SELECT || !tmp->select.where) {
        LOG_WARN("RLS: failed to parse filter: %s", rls_sql);
        return;
    }
    Expr *rls_expr = tmp->select.where;
    if (!sel->where) {
        sel->where = rls_expr;
    } else {
        Expr *combined = arena_calloc(a, sizeof(Expr));
        combined->type  = EXPR_BINOP;
        combined->op    = OP_AND;
        combined->left  = sel->where;
        combined->right = rls_expr;
        sel->where = combined;
    }
}

/* Проверяет READ-доступ ко всем таблицам SELECT (включая CTE) и применяет RLS;
 * при отказе пишет 403 и возвращает false. */
static bool check_select_access(HttpReq *req, HttpResp *resp, SelectStmt *sel)
{
    for (int i = 0; i < sel->nfrom; i++) {
        const char *t = sel->from[i].table;
        if (!t || !*t) continue;
        if (!check_table_access(req, resp, t, ACTION_TABLE_READ)) return false;
        const char *rls = rbac_row_filter(g_app.rbac, &req->auth, t, req->arena);
        if (rls && *rls) {
            apply_rls_to_select(sel, rls, req->arena);
            LOG_INFO("RLS: applied filter on '%s' for user '%s'", t, req->auth.user_id);
        }
    }
    for (int i = 0; i < sel->nctes; i++) {
        if (sel->ctes[i].body && !check_select_access(req, resp, sel->ctes[i].body))
            return false;
    }
    return true;
}

/* ── Table name validation ── */
static bool valid_table_name(const char *s) {
    if (!s || !*s) return false;
    for (const char *p = s; *p; p++) {
        char c = *p;
        if (!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_')) return false;
    }
    return strlen(s) < 128;
}

/* ── Static file serving ── */
static void h_static_file(HttpReq *req, HttpResp *resp,
                          const char *path, const char *ct) {
    (void)req;
    FILE *f = fopen(path, "rb");
    if (!f) { http_resp_error(resp, 404, "file not found"); return; }
    fseek(f, 0, SEEK_END); long sz = ftell(f); rewind(f);
    char *buf = arena_alloc(req->arena, (size_t)sz + 1);
    if (!buf || fread(buf, 1, (size_t)sz, f) != (size_t)sz) {
        fclose(f); http_resp_error(resp, 500, "read error"); return;
    }
    fclose(f);
    buf[sz] = '\0';
    resp->status = 200; resp->content_type = ct;
    resp->body = buf; resp->body_len = (size_t)sz;
}
static void h_ui_html(HttpReq *r, HttpResp *resp) { h_static_file(r, resp, "ui/index.html", "text/html"); }
static void h_ui_css (HttpReq *r, HttpResp *resp) { h_static_file(r, resp, "ui/style.css",  "text/css"); }
static void h_ui_js  (HttpReq *r, HttpResp *resp) { h_static_file(r, resp, "ui/app.js",     "application/javascript"); }

/* ── CSV helpers ── */
static char detect_delim(const char *line, size_t n) {
    int commas = 0, semis = 0;
    for (size_t i = 0; i < n; i++) {
        if (line[i] == ',') commas++;
        else if (line[i] == ';') semis++;
    }
    return (semis > commas) ? ';' : ',';
}

/* Делит строку по разделителю in-place (заменяя delim на '\0'); out[] получает
 * указатели на поля, *nout — их число. Кавычки не учитываются. */
static void split_line_simple(char *line, char delim, char **out, int max_cols, int *nout) {
    int n = 0; char *p = line; out[n++] = p;
    while (*p && n < max_cols) {
        if (*p == delim) { *p = '\0'; out[n++] = p + 1; }
        p++;
    }
    *nout = n;
}

/* RFC-4180 CSV field splitter (strict). Mirrors split_line_ex() in the csv
 * connector — that one is static there, so the strict logic is duplicated here
 * to keep the HTTP CSV ingest path on the same NULL/quoting contract:
 *   - honours double-quoted fields, collapsing "" -> " inside them;
 *   - rejects an unterminated quote, a stray '"' in an unquoted field, and any
 *     garbage after a closing quote (sets *err and returns NULL);
 * Field-count enforcement (vs. header) is the caller's job. */
static char **split_line_rfc4180(Arena *a, const char *line, char delim, int *n_out, int *err) {
    if (err) *err = 0;
    int cap=16; char **parts=arena_alloc(a,(size_t)cap*sizeof(char*)); int n=0;
    const char *p = line;
    while (1) {
        bool in_quote = (*p == '"');
        char *field;
        if (in_quote) {
            p++; /* skip opening quote */
            char *buf = arena_alloc(a, strlen(p) + 1);
            size_t bi = 0; int closed = 0;
            while (*p) {
                if (*p == '"') {
                    if (*(p+1) == '"') { buf[bi++] = '"'; p += 2; continue; }
                    p++; closed = 1; break;
                }
                buf[bi++] = *p++;
            }
            if (!closed) { if (err) *err = 1; return NULL; }
            buf[bi] = '\0';
            if (*p && *p != delim && *p != '\r' && *p != '\n') { if (err) *err = 1; return NULL; }
            field = buf;
        } else {
            const char *start = p;
            while (*p && *p!=delim && *p!='\r' && *p!='\n') {
                if (*p == '"') { if (err) *err = 1; return NULL; }
                p++;
            }
            field = arena_strndup(a, start, (size_t)(p - start));
        }
        if (n==cap){cap*=2;char**nb=arena_alloc(a,(size_t)cap*sizeof(char*));memcpy(nb,parts,(size_t)n*sizeof(char*));parts=nb;}
        parts[n++]=field;
        if (*p==delim) p++; else break;
    }
    *n_out=n; return parts;
}

/* ═══════════════════════════════════════════════════════
   QUERY ENGINE — full SQL executor
   ═══════════════════════════════════════════════════════ */

#define MAX_JOIN_TABLES 32
#define MAX_RS_ROWS     100000

/* ── Scalar value ── */
typedef struct {
    bool        is_null;
    bool        is_bool;
    bool        is_num;
    bool        b;
    double      num;
    const char *str;
} Val;

static Val vnull(void)               { Val v={0}; v.is_null=true; return v; }
static Val vbool(bool b)             { Val v={0}; v.is_bool=true; v.b=b; return v; }
static Val vnum(double n)            { Val v={0}; v.is_num=true; v.num=n; return v; }
static Val vstr_s(const char *s)     { Val v={0}; v.str=s; return v; }  /* no alloc */

static bool vt(Val v) {  /* truthy */
    if (v.is_null) return false;
    if (v.is_bool) return v.b;
    if (v.is_num)  return v.num != 0.0;
    return v.str && *v.str;
}

static bool pnum(const char *s, double *o) {
    if (!s || !*s) return false;
    const char *p = s;
    while (*p == ' ' || *p == '\t') p++;
    if (!*p) return false;
    char *end = NULL;
    double val = strtod(p, &end);
    if (!end || end == p) return false;
    while (*end == ' ' || *end == '\t') end++;
    if (*end) return false;
    if (o) *o = val;
    return true;
}

/* Shortest decimal string that round-trips the double EXACTLY. %.10g (10 sig
 * figs) silently rounds money over ~1e8 (123456789.99 -> 123456790), which is
 * unacceptable for a bank's control sums; this tries increasing precision and
 * stops at the shortest that parses back to the same bits — so simple values
 * stay clean (0.1 -> "0.1") while large money keeps every kopeck. */
static const char *dbl_fmt(Arena *a, double d) {
    /* 15 significant digits (DBL_DIG): exact to the kopeck for any realistic
     * money value (balances up to ~1e13 with cents) and clean output —
     * "123456789.99" not "123456790" (the old %.10g) and not the float-noise
     * "...10000002" that a full round-trip (%.17g) would expose on sums. */
    char buf[40];
    snprintf(buf, sizeof(buf), "%.15g", d);
    return arena_strdup(a, buf);
}

/* True if s is a valid JSON number literal — so it can be emitted unquoted
 * (jb_raw) for INT64/DOUBLE columns, preserving exact precision (no double
 * round-trip). Rejects anything that would produce invalid JSON. */
static bool is_json_number(const char *s) {
    if (!s || !*s) return false;
    const char *p = s; if (*p=='-') p++;
    bool dig=false;
    while (*p>='0'&&*p<='9') { p++; dig=true; }
    if (*p=='.') { p++; while (*p>='0'&&*p<='9') { p++; dig=true; } }
    if (*p=='e'||*p=='E') { p++; if (*p=='-'||*p=='+') p++; bool e=false; while (*p>='0'&&*p<='9'){p++;e=true;} if(!e) return false; }
    return dig && *p=='\0';
}

static const char *val_str(Val v, Arena *a) {
    if (v.is_null)  return "";
    if (v.str)      return v.str;
    if (v.is_bool)  return v.b ? "true" : "false";
    if (v.is_num) {
        double n = v.num;
        if (n == (int64_t)n && fabs(n) < 1e15)
            return arena_sprintf(a, "%lld", (long long)(int64_t)n);
        return dbl_fmt(a, n);
    }
    return "";
}

/* Materialize a Val into a stored result/table cell. Unlike val_str (which
 * renders NULL as "" for display/concat contexts), this preserves the NULL
 * contract: an is_null Val becomes the sentinel so that SQL-produced NULLs
 * (LEFT JOIN unmatched, NULLIF, COALESCE-all-null, …) round-trip identically
 * to connector-produced NULLs through EXPR_COL / IS NULL / COUNT and reach the
 * sink as a recognizable NULL rather than an empty string. */
static const char *val_cell(Val v, Arena *a) {
    if (v.is_null) return DFO_NULL_SENTINEL;
    return val_str(v, a);
}

/* ── Exact integer / decimal helpers (avoid binary-float precision loss) ──
 * A bank's account numbers are 20 digits — larger than 2^53 — so comparing them
 * through a double (the old pnum path) made DISTINCT accounts compare equal
 * (…001 == …002) and returned another customer's rows. And binary float makes
 * 8.20 - 8.10 == 0.0999999999999996, which breaks kopeck reconciliation. These
 * helpers keep integer comparisons and simple decimal add/sub/mul exact,
 * falling back to double only for exponents / oversized values. */

/* optional sign then ≥1 digits, nothing else */
static bool is_int_str(const char *s) {
    if (!s || !*s) return false;
    const char *p = s;
    if (*p == '+' || *p == '-') p++;
    if (!*p) return false;
    for (; *p; p++) if (*p < '0' || *p > '9') return false;
    return true;
}

/* Exact sign/leading-zero-aware compare of two integer strings, any length.
 * Returns <0 / 0 / >0. Assumes is_int_str(a) && is_int_str(b). */
static int int_str_cmp(const char *a, const char *b) {
    int sa = 1, sb = 1;
    if (*a == '+') a++; else if (*a == '-') { sa = -1; a++; }
    if (*b == '+') b++; else if (*b == '-') { sb = -1; b++; }
    while (a[0] == '0' && a[1]) a++;   /* strip leading zeros, keep last digit */
    while (b[0] == '0' && b[1]) b++;
    bool za = (a[0] == '0' && !a[1]);
    bool zb = (b[0] == '0' && !b[1]);
    if (za && zb) return 0;            /* +0 == -0 */
    if (za) return sb > 0 ? -1 : 1;    /* 0 vs ±nonzero */
    if (zb) return sa > 0 ?  1 : -1;
    if (sa != sb) return sa < sb ? -1 : 1;
    size_t la = strlen(a), lb = strlen(b);
    int mag = (la != lb) ? (la < lb ? -1 : 1) : strcmp(a, b);
    return sa > 0 ? mag : -mag;
}

/* Fixed-point decimal: value = mant / 10^scale. */
typedef struct { __int128 mant; int scale; bool ok; } Dec;

static Dec dec_parse(const char *s) {
    Dec d = { 0, 0, false };
    if (!s || !*s) return d;
    const char *p = s;
    while (*p == ' ' || *p == '\t') p++;
    int sign = 1;
    if (*p == '+') p++; else if (*p == '-') { sign = -1; p++; }
    __int128 m = 0; int scale = 0, ndig = 0; bool dot = false;
    for (; *p; p++) {
        if (*p == '.') { if (dot) return d; dot = true; continue; }
        if (*p < '0' || *p > '9') return d;   /* exponent / junk → not exact */
        if (++ndig > 30) return d;            /* keep within __int128 range */
        m = m * 10 + (*p - '0');
        if (dot) scale++;
    }
    if (!ndig) return d;
    d.mant = sign * m; d.scale = scale; d.ok = true;
    return d;
}

/* op: '+', '-', '*'. Returns false → caller falls back to double. */
static bool dec_arith(Dec a, Dec b, char op, Dec *out) {
    if (op == '*') {
        if (a.scale + b.scale > 30) return false;
        out->mant = a.mant * b.mant; out->scale = a.scale + b.scale; out->ok = true;
        return true;
    }
    int s = a.scale > b.scale ? a.scale : b.scale;
    if (s > 30) return false;
    __int128 ma = a.mant, mb = b.mant;
    for (int i = a.scale; i < s; i++) ma *= 10;
    for (int i = b.scale; i < s; i++) mb *= 10;
    out->mant = (op == '+') ? ma + mb : ma - mb;
    out->scale = s; out->ok = true;
    return true;
}

/* Render a Dec as the shortest exact decimal string (trailing zeros trimmed). */
static const char *dec_str(Arena *a, Dec d) {
    __int128 m = d.mant; int scale = d.scale;
    bool neg = m < 0; if (neg) m = -m;
    char digits[48]; int n = 0;                    /* least-significant first */
    if (m == 0) digits[n++] = '0';
    while (m > 0) { digits[n++] = (char)('0' + (int)(m % 10)); m /= 10; }
    while (n <= scale) digits[n++] = '0';          /* guarantee an integer part */
    char buf[64]; int o = 0;
    if (neg) buf[o++] = '-';
    int intdigits = n - scale;
    for (int i = 0; i < intdigits; i++) buf[o++] = digits[n - 1 - i];
    if (scale > 0) {
        int fracend = 0;                           /* trim trailing (low-order) zeros */
        while (fracend < scale && digits[fracend] == '0') fracend++;
        if (fracend < scale) {
            buf[o++] = '.';
            for (int i = scale - 1; i >= fracend; i--) buf[o++] = digits[i];
        }
    }
    buf[o] = '\0';
    if (buf[0] == '-' && buf[1] == '0' && buf[2] == '\0') return "0";  /* -0 → 0 */
    return arena_strdup(a, buf);
}

/* Трёхстороннее сравнение двух Val для ORDER BY / <,>,BETWEEN и join-условий.
 * NULL меньше любого не-NULL. Возвращает <0 / 0 / >0. */
static int vcmp(Val a, Val b) {
    if (a.is_null && b.is_null) return 0;
    if (a.is_null) return -1;
    if (b.is_null) return  1;
    /* Exact path first: when both carry a source string, compare without a
     * lossy double round-trip (fixes ordering/joins on 20-digit account ids). */
    if (a.str && b.str) {
        if (is_int_str(a.str) && is_int_str(b.str)) return int_str_cmp(a.str, b.str);
        double na, nb;
        if (pnum(a.str, &na) && pnum(b.str, &nb)) {
            if (na < nb) return -1;
            if (na > nb) return  1;
            return 0;
        }
        return strcmp(a.str, b.str);
    }
    if (a.is_num && b.is_num) {
        if (a.num < b.num) return -1;
        if (a.num > b.num) return  1;
        return 0;
    }
    const char *sa = a.str ? a.str : (a.is_bool ? (a.b?"true":"false") : "");
    const char *sb = b.str ? b.str : (b.is_bool ? (b.b?"true":"false") : "");
    return strcmp(sa, sb);
}

/* Равенство для =, IN, CASE, JOIN ON. NULL никогда не равен ничему (в т.ч.
 * NULL=NULL → false, как в SQL). Точный путь для строк — до сравнения через
 * double, чтобы длинные целые id не «слипались». */
static bool veq(Val a, Val b) {
    if (a.is_null || b.is_null) return false;
    /* Exact path first: distinct large integer ids must not collide via double. */
    if (a.str && b.str) {
        if (!strcmp(a.str, b.str)) return true;
        if (is_int_str(a.str) && is_int_str(b.str)) return int_str_cmp(a.str, b.str) == 0;
        double na, nb;
        if (pnum(a.str, &na) && pnum(b.str, &nb)) return na == nb;
        return false;
    }
    if (a.is_num && b.is_num)   return a.num == b.num;
    if (a.is_bool && b.is_bool) return a.b == b.b;
    return false;
}

/* ── LIKE matching ── */
static bool like_r(const char *s, const char *p) {
    if (!*p) return !*s;
    if (*p == '%') {
        while (*p == '%') p++;
        if (!*p) return true;
        for (; *s; s++) if (like_r(s, p)) return true;
        return like_r(s, p);
    }
    if (!*s) return false;
    if (*p == '_' || *s == *p) return like_r(s+1, p+1);
    return false;
}

static bool like_match(const char *str, const char *pat, bool icase, Arena *a) {
    if (!str || !pat) return false;
    if (!icase) return like_r(str, pat);
    /* case-insensitive: lower both */
    char *ls = arena_strdup(a, str);
    char *lp = arena_strdup(a, pat);
    for (char *p=ls; *p; p++) if(*p>='A'&&*p<='Z') *p+=32;
    for (char *p=lp; *p; p++) if(*p>='A'&&*p<='Z') *p+=32;
    return like_r(ls, lp);
}

/* ── Join context — N rows from N tables ── */
typedef struct {
    int         n;
    Schema     *schemas[MAX_JOIN_TABLES];
    char      **rows[MAX_JOIN_TABLES];   /* NULL = outer-join null row */
    const char *tnames[MAX_JOIN_TABLES];
    const char *aliases[MAX_JOIN_TABLES];
} JoinCtx;

/* Разрешает ссылку на столбец col (опц. с квалификатором tbl — имя или алиас
 * таблицы) в текущей строке join-контекста. Возвращает значение ячейки или NULL
 * (столбец не найден / строка таблицы — outer-join NULL). */
static const char *jctx_col(JoinCtx *ctx, const char *tbl, const char *col) {
    if (!ctx || !col) return NULL;
    for (int t = 0; t < ctx->n; t++) {
        if (!ctx->rows[t] || !ctx->schemas[t]) continue;
        if (tbl && *tbl) {
            bool match = (ctx->tnames[t]  && !strcasecmp(tbl, ctx->tnames[t])) ||
                         (ctx->aliases[t] && !strcasecmp(tbl, ctx->aliases[t]));
            if (!match) continue;
        }
        Schema *sc = ctx->schemas[t];
        for (int c = 0; c < sc->ncols; c++) {
            if (!strcasecmp(sc->cols[c].name, col))
                return ctx->rows[t][c];
        }
    }
    return NULL;
}

/* ── ResultSet ── */
typedef struct {
    char  **cells;   /* ncols strings */
    char  **skeys;   /* nskeys sort-key strings (pre-evaluated) */
} OutRow;

typedef struct {
    OutRow     *rows;
    int         nrows, cap;
    char      **col_names;
    int         ncols;
    int         nskeys;
    ColType    *col_types;   /* logical type per output column → typed JSON output */
    bool        truncated;   /* результат обрезан потолком MAX_RS_ROWS — данные
                              * НЕполные; потребители обязаны либо показать это
                              * (REST), либо отказаться (SCD2/sink/transform). */
} RS;

static RS *rs_new(Arena *a, int ncols, char **col_names, int nskeys) {
    RS *rs = arena_calloc(a, sizeof(RS));
    rs->ncols = ncols;
    rs->nskeys = nskeys;
    rs->col_names = col_names;
    rs->cap = 64;
    rs->rows = arena_alloc(a, (size_t)rs->cap * sizeof(OutRow));
    /* Default every output column to TEXT; the executor upgrades column-refs and
     * aggregates to their logical type for typed rendering. TEXT = string output
     * (current behaviour) so partial inference never regresses. */
    rs->col_types = arena_alloc(a, (size_t)(ncols>0?ncols:1) * sizeof(ColType));
    for (int i=0;i<ncols;i++) rs->col_types[i] = COL_TEXT;
    return rs;
}

static void rs_add(RS *rs, Arena *a, char **cells, char **skeys) {
    if (rs->nrows >= MAX_RS_ROWS) {
        /* Потолок достигнут: строка отброшена. Раньше это происходило МОЛЧА —
         * SELECT «успешно» возвращал ровно 100000 строк, и потребители
         * (sink/transform/SCD2/REST) не могли отличить полный результат от
         * усечённого. Теперь усечение всегда помечено. */
        rs->truncated = true;
        return;
    }
    if (rs->nrows == rs->cap) {
        int nc = rs->cap * 2;
        OutRow *nb = arena_alloc(a, (size_t)nc * sizeof(OutRow));
        memcpy(nb, rs->rows, (size_t)rs->nrows * sizeof(OutRow));
        rs->rows = nb; rs->cap = nc;
    }
    rs->rows[rs->nrows].cells = cells;
    rs->rows[rs->nrows].skeys = skeys;
    rs->nrows++;
}

/* ── Table data ── */
typedef struct {
    Schema     *schema;
    char     ***rows;   /* rows[i] is char*[ncols] */
    int         nrows;
    const char *tname;
    const char *alias;
} TblData;

/* ── Virtual table registry (CTEs + derived tables) ── */
typedef struct { const char *name; RS *rs; Schema *schema; } VTEntry;
typedef struct { VTEntry e[64]; int n; } VTReg;

static void vt_add(VTReg *vt, const char *name, RS *rs, Schema *schema) {
    if (vt->n < 64) { vt->e[vt->n].name=name; vt->e[vt->n].rs=rs; vt->e[vt->n].schema=schema; vt->n++; }
}

static bool vt_get(VTReg *vt, const char *name, RS **rs_out, Schema **sc_out) {
    if (!vt) return false;
    for (int i=0;i<vt->n;i++) {
        if (!strcasecmp(vt->e[i].name, name)) {
            if (rs_out) *rs_out = vt->e[i].rs;
            if (sc_out) *sc_out = vt->e[i].schema;
            return true;
        }
    }
    return false;
}

/* forward decls */
static Val  eval_val(Expr *e, JoinCtx *ctx, Arena *a);
static RS  *exec_stmt(Arena *a, const Stmt *s, VTReg *vt);
static bool expr_has_agg(Expr *e);
static bool expr_has_window(Expr *e);
static Expr *find_first_agg(Expr *e);
static bool expr_find_agg_args(Expr *e, const char *fn, Expr **call_args, int nargs);
static void apply_windows(Arena *a, RS *rs, const SelectStmt *sel);

/* ── GrpAcc forward declaration for thread-local group context ── */
typedef struct GrpAcc_s {
    char   *key;
    double *sums, *mins, *maxs;
    int    *cnts;
    int     total_cnt;
    bool   *min_set, *max_set;
    char  **first_str;
    /* Actual MIN/MAX value per column (string), so MIN/MAX over TEXT returns the
     * real value, not a numeric 0. Compared numerically when both look numeric,
     * else lexically. */
    char  **min_str, **max_str;
    /* DISTINCT aggregates (COUNT/SUM/AVG DISTINCT): per-select-column set of
     * already-seen value strings, so a repeated value is counted/summed once. */
    char ***dvals;
    int    *dcnt, *dcap;
} GrpAcc;

/* Thread-local group context: lets eval_val resolve agg funcs during group emission */
static __thread GrpAcc           *tl_grp = NULL;
static __thread const SelectStmt *tl_sel = NULL;
/* Aggregates referenced ONLY in HAVING/ORDER BY (not the SELECT list) get
 * accumulator slots appended after the select slots. tl_xagg exposes them to
 * eval_func during group emission. */
static __thread Expr            **tl_xagg  = NULL;
static __thread int               tl_nxagg = 0;

/* Read aggregate fn's accumulated value from group slot s. */
static Val agg_slot_value(GrpAcc *g, int s, const char *fn) {
    if (!strcasecmp(fn,"COUNT")) return vnum((double)g->cnts[s]);
    if (!strcasecmp(fn,"SUM"))   return vnum(g->sums[s]);
    if (!strcasecmp(fn,"AVG"))   return vnum(g->cnts[s] ? g->sums[s]/g->cnts[s] : 0.0);
    if (!strcasecmp(fn,"MIN")||!strcasecmp(fn,"MAX")) {
        bool set = !strcasecmp(fn,"MIN") ? g->min_set[s] : g->max_set[s];
        if (!set) return vnull();
        const char *sv = !strcasecmp(fn,"MIN") ? g->min_str[s] : g->max_str[s];
        Val r = vstr_s(sv); double n;          /* real value (numeric or text) */
        if (pnum(sv,&n)) { r.is_num=true; r.num=n; }
        return r;
    }
    return vnull();
}
/* Thread-local outer join context: for correlated subqueries */
static __thread JoinCtx          *tl_outer_ctx = NULL;

/* Thread-local window function pre-computed values (set during apply_windows re-eval) */
typedef struct { Expr *win_expr; char **values; } WinValEntry;
static __thread WinValEntry *tl_win_vals    = NULL;
static __thread int          tl_nwin_vals   = 0;
static __thread int          tl_win_cur_row = -1;

/* Thread-local execution error: set when exec_select hits an unresolvable
 * source table (no such table). Lets callers distinguish a real failure from a
 * legitimately empty result. Cleared at the top of each top-level exec_stmt.
 * NOTE: a missing table is an ERROR; an existing table with 0 rows is NOT. */
static __thread char tl_exec_error[256] = {0};

/* ── Expression evaluator ── */

static bool is_agg_name(const char *fn) {
    return !strcasecmp(fn,"COUNT") || !strcasecmp(fn,"SUM") ||
           !strcasecmp(fn,"AVG")   || !strcasecmp(fn,"MIN") || !strcasecmp(fn,"MAX");
}

/* ── Fuzzy-match primitives for eval_func (Match step). Ported from the
 *    qengine.c implementations so the gateway query engine — which is this
 *    file's eval_func, NOT qengine.c — can score candidate pairs. ── */
static int api_levenshtein(const char *x, const char *y, Arena *a) {
    size_t lx = strlen(x), ly = strlen(y);
    if (lx == 0) return (int)ly;
    if (ly == 0) return (int)lx;
    int *prev = arena_alloc(a, (ly + 1) * sizeof(int));
    int *cur  = arena_alloc(a, (ly + 1) * sizeof(int));
    for (size_t j = 0; j <= ly; j++) prev[j] = (int)j;
    for (size_t i = 1; i <= lx; i++) {
        cur[0] = (int)i;
        for (size_t j = 1; j <= ly; j++) {
            int cost = (x[i-1] == y[j-1]) ? 0 : 1;
            int del = prev[j] + 1, ins = cur[j-1] + 1, sub = prev[j-1] + cost;
            int m = del < ins ? del : ins;
            cur[j] = m < sub ? m : sub;
        }
        int *t = prev; prev = cur; cur = t;
    }
    return prev[ly];
}

static double api_jaro_winkler(const char *s1, const char *s2, Arena *a) {
    size_t l1 = strlen(s1), l2 = strlen(s2);
    if (l1 == 0 || l2 == 0) return (l1 == l2) ? 1.0 : 0.0;
    size_t maxl = l1 > l2 ? l1 : l2;
    long win = (long)(maxl / 2) - 1; if (win < 0) win = 0;
    char *m1 = arena_calloc(a, l1);
    char *m2 = arena_calloc(a, l2);
    size_t matches = 0;
    for (size_t i = 0; i < l1; i++) {
        long lo = (long)i - win; if (lo < 0) lo = 0;
        long hi = (long)i + win; if (hi >= (long)l2) hi = (long)l2 - 1;
        for (long j = lo; j <= hi; j++)
            if (!m2[j] && s1[i] == s2[j]) { m1[i] = 1; m2[j] = 1; matches++; break; }
    }
    if (matches == 0) return 0.0;
    size_t t = 0, k = 0;
    for (size_t i = 0; i < l1; i++) {
        if (!m1[i]) continue;
        while (!m2[k]) k++;
        if (s1[i] != s2[k]) t++;
        k++;
    }
    double mm = (double)matches, tt = (double)t / 2.0;
    double jaro = (mm/(double)l1 + mm/(double)l2 + (mm - tt)/mm) / 3.0;
    size_t pref = 0;
    for (size_t i = 0; i < l1 && i < l2 && i < 4; i++) {
        if (s1[i] == s2[i]) pref++; else break;
    }
    return jaro + (double)pref * 0.1 * (1.0 - jaro);
}

/* ── UTF-8 helpers for length()/substr() ──
 * length() and substr() must operate on UTF-8 code points, not raw bytes, so
 * multibyte text (Cyrillic / diacritics in ФИО) is counted and sliced
 * correctly. We don't decode into code points; instead we walk the byte string
 * tracking code-point boundaries (lead bytes), which keeps a clean byte slice
 * for the output. Mirrors the decode logic in qengine.c's qe_utf8_decode but
 * stays local to api.c per the change scope. */

/* Number of bytes in the UTF-8 sequence starting at lead byte c (1..4); a
 * stray/continuation byte counts as 1 so latin1/garbage degrades to bytes. */
static size_t u8_seq_len(unsigned char c) {
    if (c < 0x80)              return 1;
    else if ((c & 0xE0)==0xC0) return 2;
    else if ((c & 0xF0)==0xE0) return 3;
    else if ((c & 0xF8)==0xF0) return 4;
    return 1;
}

/* Count UTF-8 code points in s. Pure ASCII == strlen, so latin text is
 * unaffected. */
static size_t u8_cplen(const char *s) {
    size_t n = 0, i = 0, blen = s ? strlen(s) : 0;
    while (i < blen) {
        size_t l = u8_seq_len((unsigned char)s[i]);
        if (i + l > blen) l = 1;             /* truncated tail */
        /* validate continuation bytes; if bad, treat lead as 1 byte */
        for (size_t k = 1; k < l; k++) {
            if (((unsigned char)s[i+k] & 0xC0) != 0x80) { l = 1; break; }
        }
        i += l; n++;
    }
    return n;
}

/* Byte offset of the cp-th (0-based) code point in s; returns strlen(s) if cp
 * is past the end. */
static size_t u8_byte_at(const char *s, size_t cp) {
    size_t i = 0, n = 0, blen = s ? strlen(s) : 0;
    while (i < blen && n < cp) {
        size_t l = u8_seq_len((unsigned char)s[i]);
        if (i + l > blen) l = 1;
        for (size_t k = 1; k < l; k++) {
            if (((unsigned char)s[i+k] & 0xC0) != 0x80) { l = 1; break; }
        }
        i += l; n++;
    }
    return i;
}

/* substr by code points, 1-based start, len in code points (len<0 → to end). */
static char *u8_substr(Arena *a, const char *s, int start1, long len, bool have_len) {
    int start0 = start1 - 1;
    if (start0 < 0) start0 = 0;
    size_t total = u8_cplen(s);
    if ((size_t)start0 >= total) return arena_strdup(a, "");
    long avail = (long)(total - (size_t)start0);
    long take = (!have_len || len < 0 || len > avail) ? avail : len;
    if (take < 0) take = 0;
    size_t bstart = u8_byte_at(s, (size_t)start0);
    size_t bend   = u8_byte_at(s, (size_t)start0 + (size_t)take);
    return arena_strndup(a, s + bstart, bend - bstart);
}

/* Вычисляет вызов встроенной SQL-функции fn(args...) в контексте строки ctx.
 * Диспетчеризует по имени: скалярные (coalesce/nullif/round/upper/substr/...),
 * строковые/UTF-8, фаззи-скоринг для Match-шага (levenshtein/jaro_winkler/
 * word_similarity) и агрегаты (COUNT/SUM/AVG/MIN/MAX) — последние при активном
 * tl_grp читаются из аккумулятора группы. Неизвестная функция → NULL. */
static Val eval_func(const char *fn, Expr **args, int nargs, JoinCtx *ctx, Arena *a) {
    /* scalar functions */
    if (!strcasecmp(fn,"coalesce") || !strcasecmp(fn,"ifnull") || !strcasecmp(fn,"nvl")) {
        for (int i=0;i<nargs;i++) { Val v=eval_val(args[i],ctx,a); if(!v.is_null) return v; }
        return vnull();
    }
    if (!strcasecmp(fn,"nullif") && nargs==2) {
        Val a0=eval_val(args[0],ctx,a), a1=eval_val(args[1],ctx,a);
        if (veq(a0,a1)) return vnull();
        return a0;
    }
    if (!strcasecmp(fn,"if") || !strcasecmp(fn,"iif")) {
        if (nargs < 2) return vnull();
        Val cond = eval_val(args[0],ctx,a);
        return eval_val(vt(cond)?args[1]:args[2<nargs?2:1], ctx, a);
    }
    if (!strcasecmp(fn,"greatest")) {
        Val best=vnull();
        for (int i=0;i<nargs;i++) { Val v=eval_val(args[i],ctx,a); if(best.is_null||vcmp(v,best)>0) best=v; }
        return best;
    }
    if (!strcasecmp(fn,"least")) {
        Val best=vnull();
        for (int i=0;i<nargs;i++) { Val v=eval_val(args[i],ctx,a); if(best.is_null||vcmp(v,best)<0) best=v; }
        return best;
    }
    if (nargs == 0 && !strcasecmp(fn,"now"))  return vnum((double)time(NULL));
    if (nargs == 0 && !strcasecmp(fn,"random")) return vnum((double)rand()/(double)RAND_MAX);
    if (nargs == 0 && !strcasecmp(fn,"pi"))   return vnum(3.14159265358979323846);
    if (nargs == 0 && !strcasecmp(fn,"current_timestamp")) return vnum((double)time(NULL));

    if (nargs >= 1) {
        Val a0 = eval_val(args[0],ctx,a);
        const char *s0 = val_str(a0, a);
        double n0 = a0.num;
        if (!a0.is_num) pnum(s0, &n0);

        if (!strcasecmp(fn,"abs")) {
            if (a0.is_null) return vnull();
            if (a0.is_num) return vnum(fabs(a0.num));
            double n; if (pnum(s0,&n)) return vnum(fabs(n)); return vnull();
        }
        if (!strcasecmp(fn,"ceil") || !strcasecmp(fn,"ceiling")) {
            if (a0.is_null) return vnull(); return vnum(ceil(n0));
        }
        if (!strcasecmp(fn,"floor")) { if (a0.is_null) return vnull(); return vnum(floor(n0)); }
        if (!strcasecmp(fn,"round") && nargs==1) { if (a0.is_null) return vnull(); return vnum(round(n0)); }
        if (!strcasecmp(fn,"sqrt"))  { if (a0.is_null) return vnull(); return vnum(sqrt(n0 > 0 ? n0 : 0)); }
        if (!strcasecmp(fn,"exp"))   { if (a0.is_null) return vnull(); return vnum(exp(n0)); }
        if (!strcasecmp(fn,"ln")   || !strcasecmp(fn,"log"))  {
            if (a0.is_null) return vnull(); return vnum(n0 > 0 ? log(n0) : 0);
        }
        if (!strcasecmp(fn,"log10")) { if (a0.is_null) return vnull(); return vnum(n0 > 0 ? log10(n0) : 0); }
        if (!strcasecmp(fn,"sign"))  { if (a0.is_null) return vnull(); return vnum(n0>0?1:n0<0?-1:0); }
        if (!strcasecmp(fn,"upper") || !strcasecmp(fn,"ucase")) {
            if (a0.is_null) return vnull();
            char *out=arena_strdup(a,s0);
            for(char*p=out;*p;p++) if(*p>='a'&&*p<='z') *p-=32;
            return vstr_s(out);
        }
        if (!strcasecmp(fn,"lower") || !strcasecmp(fn,"lcase")) {
            if (a0.is_null) return vnull();
            char *out=arena_strdup(a,s0);
            for(char*p=out;*p;p++) if(*p>='A'&&*p<='Z') *p+=32;
            return vstr_s(out);
        }
        if (!strcasecmp(fn,"normalize_inn")) {
            if (a0.is_null) return vnull();
            return vstr_s((char *)qe_normalize_inn(s0, a));
        }
        if (!strcasecmp(fn,"normalize_name")) {
            if (a0.is_null) return vnull();
            return vstr_s((char *)qe_normalize_name(s0, a));
        }
        if (!strcasecmp(fn,"length") || !strcasecmp(fn,"len") || !strcasecmp(fn,"char_length")) {
            /* count UTF-8 code points, not bytes (ASCII == strlen) */
            if (a0.is_null) return vnull(); return vnum((double)u8_cplen(s0));
        }
        if (!strcasecmp(fn,"ltrim")) {
            if (a0.is_null) return vnull();
            while (*s0 == ' ' || *s0 == '\t') s0++;
            return vstr_s(arena_strdup(a, s0));
        }
        if (!strcasecmp(fn,"rtrim")) {
            if (a0.is_null) return vnull();
            char *out = arena_strdup(a,s0);
            size_t l = strlen(out);
            while (l>0 && (out[l-1]==' '||out[l-1]=='\t')) out[--l]='\0';
            return vstr_s(out);
        }
        if (!strcasecmp(fn,"trim")) {
            if (a0.is_null) return vnull();
            while (*s0==' '||*s0=='\t') s0++;
            char *out=arena_strdup(a,s0);
            size_t l=strlen(out);
            while(l>0&&(out[l-1]==' '||out[l-1]=='\t')) out[--l]='\0';
            return vstr_s(out);
        }
        if (!strcasecmp(fn,"reverse")) {
            if (a0.is_null) return vnull();
            size_t l=strlen(s0); char *out=arena_strdup(a,s0);
            for(size_t i=0;i<l/2;i++){char t=out[i];out[i]=out[l-1-i];out[l-1-i]=t;}
            return vstr_s(out);
        }
        if (!strcasecmp(fn,"md5") || !strcasecmp(fn,"sha1") || !strcasecmp(fn,"hash")) {
            /* stub: return the string */
            return a0;
        }
        if (!strcasecmp(fn,"to_char") || !strcasecmp(fn,"str") || !strcasecmp(fn,"cast_text")) {
            return vstr_s(val_str(a0, a));
        }
        if (!strcasecmp(fn,"to_number") || !strcasecmp(fn,"to_float")) {
            double n; if (pnum(s0,&n)) return vnum(n); return vnull();
        }
        if (!strcasecmp(fn,"to_int") || !strcasecmp(fn,"int") || !strcasecmp(fn,"integer")) {
            double n; if (pnum(s0,&n)) return vnum(floor(n)); return vnull();
        }
        if (!strcasecmp(fn,"bool") || !strcasecmp(fn,"to_bool")) {
            if (a0.is_null) return vnull();
            return vbool(!strcasecmp(s0,"true")||!strcmp(s0,"1"));
        }
        if (!strcasecmp(fn,"not")) { return vbool(!vt(a0)); }
        if (!strcasecmp(fn,"is_null") || !strcasecmp(fn,"isnull")) { return vbool(a0.is_null); }
        if (!strcasecmp(fn,"is_not_null") || !strcasecmp(fn,"notnull")) { return vbool(!a0.is_null); }

        if (nargs >= 2) {
            Val a1 = eval_val(args[1],ctx,a);
            const char *s1 = val_str(a1, a);
            double n1 = a1.num; if (!a1.is_num) pnum(s1, &n1);

            if (!strcasecmp(fn,"round")) {
                if (a0.is_null) return vnull();
                int dec = (int)n1;
                double factor = pow(10.0, (double)dec);
                return vnum(round(n0 * factor) / factor);
            }
            /* substr/substring/mid: UTF-8 code-point aware, 1-based start.
             * 2-arg form substr(str,start) = start..end of string; 3-arg form
             * substr(str,start,len) takes `len` code points. ASCII behaves as
             * before; multibyte (Cyrillic) no longer splits a code point. */
            if (!strcasecmp(fn,"substr") || !strcasecmp(fn,"substring") || !strcasecmp(fn,"mid")) {
                if (a0.is_null) return vnull();
                bool have_len = (nargs >= 3);
                long len = 0;
                if (have_len) {
                    Val a2 = eval_val(args[2],ctx,a);
                    if (a2.is_null) len = 0; else len = (long)a2.num;
                }
                return vstr_s(u8_substr(a, s0, (int)n1, len, have_len));
            }
            if (!strcasecmp(fn,"power") || !strcasecmp(fn,"pow")) {
                if (a0.is_null) return vnull(); return vnum(pow(n0,n1));
            }
            if (!strcasecmp(fn,"mod"))  { if (a0.is_null) return vnull(); return vnum(n1!=0?fmod(n0,n1):0); }
            if (!strcasecmp(fn,"left")) {
                if (a0.is_null) return vnull();
                int k=(int)n1; if(k<0) k=0;
                size_t sl=strlen(s0); if((size_t)k>sl) k=(int)sl;
                return vstr_s(arena_strndup(a,s0,(size_t)k));
            }
            if (!strcasecmp(fn,"right")) {
                if (a0.is_null) return vnull();
                int k=(int)n1; if(k<0) k=0;
                size_t sl=strlen(s0); if((size_t)k>sl) k=(int)sl;
                return vstr_s(arena_strdup(a, s0+sl-k));
            }
            if (!strcasecmp(fn,"repeat")) {
                if (a0.is_null) return vnull();
                int k=(int)n1; if(k<0) k=0; if(k>1000) k=1000;
                size_t sl=strlen(s0);
                char *out=arena_alloc(a, sl*(size_t)k+1);
                out[0]='\0';
                for(int i=0;i<k;i++) memcpy(out+sl*(size_t)i, s0, sl);
                out[sl*(size_t)k]='\0';
                return vstr_s(out);
            }
            if (!strcasecmp(fn,"lpad") || !strcasecmp(fn,"rpad")) {
                if (a0.is_null) return vnull();
                int k=(int)n1; if(k<0)k=0; if(k>10000)k=10000;
                const char *pad = (nargs>=3) ? val_str(eval_val(args[2],ctx,a),a) : " ";
                if(!pad||!*pad) pad=" ";
                size_t sl=strlen(s0), pl=strlen(pad);
                if((int)sl>=(int)k) return vstr_s(arena_strndup(a,s0,(size_t)k));
                int need=k-(int)sl;
                char *out=arena_alloc(a,(size_t)k+1);
                if(!strcasecmp(fn,"lpad")){
                    int pos=0;
                    while(pos<need){int take=(int)pl<need-pos?(int)pl:need-pos;memcpy(out+pos,pad,(size_t)take);pos+=take;}
                    memcpy(out+need,s0,sl); out[k]='\0';
                } else {
                    memcpy(out,s0,sl);
                    int pos=(int)sl;
                    while(pos<k){int take=(int)pl<k-pos?(int)pl:k-pos;memcpy(out+pos,pad,(size_t)take);pos+=take;}
                    out[k]='\0';
                }
                return vstr_s(out);
            }
            if (!strcasecmp(fn,"instr") || !strcasecmp(fn,"strpos") || !strcasecmp(fn,"locate")) {
                if (a0.is_null || a1.is_null) return vnull();
                const char *pos = strstr(s0, s1);
                return vnum(pos ? (double)(pos-s0+1) : 0.0);
            }
            if (!strcasecmp(fn,"split_part")) {
                if (a0.is_null||a1.is_null) return vnull();
                int idx = (nargs>=3)?(int)eval_val(args[2],ctx,a).num:1;
                if(idx<1) return vnull();
                char *tmp=arena_strdup(a,s0);
                char *tok=strtok(tmp,s1); int i=1;
                while(tok&&i<idx){tok=strtok(NULL,s1);i++;}
                return tok?vstr_s(arena_strdup(a,tok)):vnull();
            }
            /* fuzzy match (MDM Match step) */
            if (!strcasecmp(fn,"levenshtein")) {
                if (a0.is_null||a1.is_null) return vnull();
                return vnum((double)api_levenshtein(s0, s1, a));
            }
            if (!strcasecmp(fn,"jaro_winkler")) {
                if (a0.is_null||a1.is_null) return vnull();
                return vnum(api_jaro_winkler(s0, s1, a));
            }
            if (!strcasecmp(fn,"word_similarity")) {
                if (a0.is_null||a1.is_null) return vnull();
                return vnum(qe_word_similarity(s0, s1, a));
            }

            if (nargs >= 3) {
                Val a2 = eval_val(args[2],ctx,a);
                const char *s2 = val_str(a2, a);
                /* substr/substring/mid handled UTF-8-aware in the nargs>=2 block above */
                if (!strcasecmp(fn,"replace")) {
                    if (a0.is_null) return vnull();
                    if (!s1||!*s1) return a0;
                    size_t fl=strlen(s1), rl=strlen(s2);
                    /* count occurrences */
                    int cnt=0; const char *p=s0;
                    while((p=strstr(p,s1))){cnt++;p+=fl;}
                    size_t outsz=strlen(s0)+(size_t)cnt*(rl>fl?rl-fl:0)+1;
                    char *out=arena_alloc(a,outsz); char *op=out;
                    const char *src=s0;
                    while(*src){
                        const char *found=strstr(src,s1);
                        if(!found){size_t rem=strlen(src);memcpy(op,src,rem);op+=rem;break;}
                        memcpy(op,src,(size_t)(found-src)); op+=found-src;
                        memcpy(op,s2,rl); op+=rl; src=found+fl;
                    }
                    *op='\0';
                    return vstr_s(out);
                }
            }
        }

        /* cast */
        if (!strcasecmp(fn,"cast") && nargs==2) {
            Val a1=eval_val(args[1],ctx,a);
            const char *tname=a1.str?a1.str:"";
            if (!strncasecmp(tname,"int",3)||!strncasecmp(tname,"big",3)||!strncasecmp(tname,"sma",3)) {
                double n; if(pnum(s0,&n)) return vnum(floor(n)); return vnull();
            }
            if (!strncasecmp(tname,"float",5)||!strncasecmp(tname,"double",6)||!strncasecmp(tname,"num",3)||!strncasecmp(tname,"dec",3)||!strncasecmp(tname,"real",4)) {
                double n; if(pnum(s0,&n)) return vnum(n); return vnull();
            }
            if (!strncasecmp(tname,"bool",4)) {
                return vbool(!strcasecmp(s0,"true")||!strcmp(s0,"1"));
            }
            /* text/varchar/char: return as string */
            return vstr_s(s0);
        }
    }

    /* concat (variadic) */
    if (!strcasecmp(fn,"concat") || !strcasecmp(fn,"concat_ws")) {
        const char *sep="";
        int start=0;
        if (!strcasecmp(fn,"concat_ws") && nargs>0) {
            Val sv=eval_val(args[0],ctx,a);
            sep=val_str(sv,a); start=1;
        }
        size_t total=0;
        for(int i=start;i<nargs;i++){ Val v=eval_val(args[i],ctx,a); if(!v.is_null) total+=strlen(val_str(v,a)); }
        total += (nargs>start+1) ? (size_t)(nargs-start-1)*strlen(sep) : 0;
        char *out=arena_alloc(a,total+1); out[0]='\0';
        for(int i=start;i<nargs;i++){
            Val v=eval_val(args[i],ctx,a);
            if(v.is_null) continue;
            if(i>start && sep[0]) strcat(out,sep);
            strcat(out,val_str(v,a));
        }
        return vstr_s(out);
    }

    /* aggregate functions: return accumulated group value if inside group emission */
    if (is_agg_name(fn)) {
        if (tl_grp && tl_sel) {
            GrpAcc *g = tl_grp;
            const SelectStmt *sel = tl_sel;
            /* match an accumulator slot by argument STRUCTURE: first the SELECT
             * list, then the HAVING/ORDER-only extra slots (so HAVING SUM(x)>N
             * with x not selected still resolves to a real accumulator). */
            for (int i=0; i<sel->nselect; i++) {
                Expr *se=sel->select_list[i];
                Expr *sb=(se->type==EXPR_ALIAS)?se->expr:se;
                if (expr_find_agg_args(sb, fn, args, nargs)) return agg_slot_value(g, i, fn);
            }
            for (int e=0; e<tl_nxagg; e++)
                if (expr_find_agg_args(tl_xagg[e], fn, args, nargs))
                    return agg_slot_value(g, sel->nselect + e, fn);
            /* fallback: total count for COUNT(*) */
            if (!strcasecmp(fn,"COUNT")) return vnum((double)g->total_cnt);
        }
        /* scalar context (no group) */
        if (!strcasecmp(fn,"COUNT")) return vnum(0);
        if (!strcasecmp(fn,"SUM") || !strcasecmp(fn,"AVG")) return vnum(0);
        if (!strcasecmp(fn,"MIN") || !strcasecmp(fn,"MAX")) return vnull();
    }

    /* exists: check if subquery returned rows; expose outer ctx for correlated queries */
    if (!strcasecmp(fn,"exists") && nargs==1) {
        Expr *sub_e = args[0];
        if (sub_e && sub_e->type == EXPR_SUBQUERY && sub_e->subq) {
            JoinCtx *prev_outer = tl_outer_ctx;
            if (ctx) tl_outer_ctx = ctx;
            RS *sub_rs = exec_stmt(a, sub_e->subq, NULL);
            tl_outer_ctx = prev_outer;
            return vbool(sub_rs && sub_rs->nrows > 0);
        }
    }

    return vnull();
}

/* Рекурсивно вычисляет выражение AST e в контексте строки ctx → Val.
 * Ядро исполнителя: литералы, ссылки на столбцы (с NULL-контрактом и падением
 * в tl_outer_ctx для коррелированных подзапросов), функции (eval_func), CASE,
 * подзапросы (скалярные и IN), оконные плейсхолдеры, унарные/бинарные операции
 * (с точной decimal-арифметикой, LIKE/BETWEEN/IN, коротким замыканием AND/OR). */
static Val eval_val(Expr *e, JoinCtx *ctx, Arena *a) {
    if (!e) return vnull();
    switch (e->type) {
    case EXPR_LITERAL_INT:   { Val v = vnum((double)e->ival); if (e->litstr) v.str = e->litstr; return v; }  /* carry exact digits so veq/vcmp compare big ints losslessly */
    case EXPR_LITERAL_FLOAT: return vnum(e->fval);
    case EXPR_LITERAL_STR:   return vstr_s(e->sval);
    case EXPR_LITERAL_BOOL:  return vbool(e->bval);
    case EXPR_LITERAL_NULL:  return vnull();
    case EXPR_ALIAS:         return eval_val(e->expr, ctx, a);
    case EXPR_STAR:          return vstr_s("*");
    case EXPR_COL: {
        if (!ctx) return vnull();
        const char *s = jctx_col(ctx, e->table, e->name);
        /* correlated subquery: fall back to outer context if not found */
        if (!s && tl_outer_ctx) s = jctx_col(tl_outer_ctx, e->table, e->name);
        /* Treat NULL and any sub-page (non-pointer) cell as SQL NULL: a real
         * string is never in the first page, so this turns a corrupt/legacy
         * native-numeric cell into NULL instead of SIGSEGV-ing on strcmp below. */
        if (!s || (uintptr_t)s < 0x1000) return vnull();
        /* NULL contract: a cell carrying the sentinel is a true SQL NULL.
         * Map it back to a NULL-Val so IS NULL / COUNT(col) / COALESCE and
         * projection treat it as NULL rather than leaking the literal text. */
        if (!strcmp(s, DFO_NULL_SENTINEL)) return vnull();
        Val v = vstr_s(s);
        double n;
        if (pnum(s, &n)) { v.is_num=true; v.num=n; }
        if (!strcasecmp(s,"true"))  { v.is_bool=true; v.b=true; }
        if (!strcasecmp(s,"false")) { v.is_bool=true; v.b=false; }
        return v;
    }
    case EXPR_FUNC:
        return eval_func(e->func_name, e->args, e->nargs, ctx, a);
    case EXPR_CASE: {
        if (e->case_op) {
            /* simple CASE x WHEN v THEN ... */
            Val x = eval_val(e->case_op, ctx, a);
            for (int i=0;i<e->nwhens;i++) {
                Val w = eval_val(e->whens[i], ctx, a);
                if (veq(x, w)) return eval_val(e->thens[i], ctx, a);
            }
        } else {
            /* searched CASE WHEN cond THEN ... */
            for (int i=0;i<e->nwhens;i++) {
                Val w = eval_val(e->whens[i], ctx, a);
                if (vt(w)) return eval_val(e->thens[i], ctx, a);
            }
        }
        if (e->else_expr) return eval_val(e->else_expr, ctx, a);
        return vnull();
    }
    case EXPR_SUBQUERY: {
        /* scalar correlated subquery */
        if (!e->subq) return vnull();
        JoinCtx *prev_outer = tl_outer_ctx;
        if (ctx) tl_outer_ctx = ctx;
        RS *sub = exec_stmt(a, e->subq, NULL);
        tl_outer_ctx = prev_outer;
        if (!sub || sub->nrows==0) return vnull();
        const char *v = sub->rows[0].cells ? sub->rows[0].cells[0] : NULL;
        if (!v) return vnull();
        Val rv = vstr_s(v);
        double n; if (pnum(v,&n)) { rv.is_num=true; rv.num=n; }
        return rv;
    }
    case EXPR_LIST: return vnull(); /* lists not evaluated directly */
    case EXPR_WINDOW:
        /* placeholder: actual value injected by apply_windows re-evaluation pass */
        if (tl_win_vals && tl_win_cur_row >= 0) {
            for (int _i = 0; _i < tl_nwin_vals; _i++) {
                if (tl_win_vals[_i].win_expr == e) {
                    const char *_v = tl_win_vals[_i].values[tl_win_cur_row];
                    if (!_v) return vnull();
                    Val _rv = vstr_s(_v);
                    double _n; if (pnum(_v, &_n)) { _rv.is_num=true; _rv.num=_n; }
                    return _rv;
                }
            }
        }
        return vnull();
    case EXPR_UNOP: {
        if (e->op == OP_IS_NULL) {
            Val inner = eval_val(e->left, ctx, a);
            return vbool(inner.is_null);
        }
        if (e->op == OP_IS_NOT_NULL) {
            Val inner = eval_val(e->left, ctx, a);
            return vbool(!inner.is_null);
        }
        if (e->op == OP_NOT) {
            Val inner = eval_val(e->left, ctx, a);
            return vbool(!vt(inner));
        }
        if (e->op == OP_SUB) {
            Val inner = eval_val(e->left, ctx, a);
            if (inner.is_num) return vnum(-inner.num);
            double n; if (pnum(inner.str,&n)) return vnum(-n);
        }
        return vnull();
    }
    case EXPR_BINOP: {
        /* short-circuit for AND/OR */
        if (e->op == OP_AND) {
            Val L = eval_val(e->left, ctx, a);
            if (!vt(L)) return vbool(false);
            Val R = eval_val(e->right, ctx, a);
            return vbool(vt(R));
        }
        if (e->op == OP_OR) {
            Val L = eval_val(e->left, ctx, a);
            if (vt(L)) return vbool(true);
            Val R = eval_val(e->right, ctx, a);
            return vbool(vt(R));
        }

        Val L = eval_val(e->left, ctx, a);

        /* IN / NOT IN */
        if (e->op == OP_IN || e->op == OP_NOT_IN) {
            bool found = false;
            Expr *rhs = e->right;
            if (rhs && rhs->type == EXPR_LIST) {
                for (int i=0;i<rhs->nitems;i++) {
                    Val iv = eval_val(rhs->items[i], ctx, a);
                    if (veq(L, iv)) { found=true; break; }
                }
            } else if (rhs && rhs->type == EXPR_SUBQUERY && rhs->subq) {
                JoinCtx *prev_outer = tl_outer_ctx;
                if (ctx) tl_outer_ctx = ctx;
                RS *sub = exec_stmt(a, rhs->subq, NULL);
                tl_outer_ctx = prev_outer;
                if (sub) {
                    for (int i=0;i<sub->nrows;i++) {
                        if (!sub->rows[i].cells) continue;
                        Val sv = vstr_s(sub->rows[i].cells[0]);
                        double n; if (pnum(sv.str,&n)) { sv.is_num=true; sv.num=n; }
                        if (veq(L, sv)) { found=true; break; }
                    }
                }
            }
            return vbool(e->op==OP_IN ? found : !found);
        }

        Val R = eval_val(e->right, ctx, a);
        double lv=0, rv=0;
        bool ln = L.is_num || pnum(L.str, &lv);
        bool rn = R.is_num || pnum(R.str, &rv);
        if (L.is_num) lv = L.num;
        if (R.is_num) rv = R.num;
        bool num_ok = ln && rn;

        /* Exact decimal arithmetic for money/large ids: computes 8.20-8.10 as
         * 0.10 (not 0.0999999999999996) and keeps 20-digit sums exact. Falls
         * back to the double paths below for exponents / oversized values or a
         * string-concat OP_ADD (num_ok == false). Integer results stay numeric
         * so 0 remains falsy and further arithmetic chains cleanly. */
        if (num_ok && (e->op == OP_ADD || e->op == OP_SUB || e->op == OP_MUL)) {
            Dec dl = dec_parse(val_str(L, a)), drr = dec_parse(val_str(R, a)), res;
            char dop = e->op == OP_ADD ? '+' : e->op == OP_SUB ? '-' : '*';
            if (dl.ok && drr.ok && dec_arith(dl, drr, dop, &res)) {
                const char *rs = dec_str(a, res);
                if (is_int_str(rs)) {
                    const char *q = rs + (rs[0] == '-' || rs[0] == '+' ? 1 : 0);
                    if (strlen(q) <= 15) return vnum((double)strtoll(rs, NULL, 10));
                }
                return vstr_s(rs);
            }
        }

        switch (e->op) {
        case OP_EQ:  return vbool(veq(L,R));
        case OP_NE:  return vbool(!veq(L,R));
        case OP_LT:  return vbool(vcmp(L,R) <  0);
        case OP_LE:  return vbool(vcmp(L,R) <= 0);
        case OP_GT:  return vbool(vcmp(L,R) >  0);
        case OP_GE:  return vbool(vcmp(L,R) >= 0);
        case OP_ADD:
            if (!num_ok) {
                /* string concat fallback */
                const char *sl=val_str(L,a), *sr=val_str(R,a);
                size_t tl=strlen(sl)+strlen(sr)+1;
                char *out=arena_alloc(a,tl); strcpy(out,sl); strcat(out,sr);
                return vstr_s(out);
            }
            if (L.is_num&&R.is_num&&L.num==(int64_t)L.num&&R.num==(int64_t)R.num)
                return vnum(L.num+R.num);
            return vnum(lv+rv);
        case OP_SUB: return num_ok ? vnum(lv-rv) : vnull();
        case OP_MUL: return num_ok ? vnum(lv*rv) : vnull();
        case OP_DIV: return num_ok ? (rv!=0?vnum(lv/rv):vnull()) : vnull();
        case OP_MOD: return num_ok ? (rv!=0?vnum(fmod(lv,rv)):vnull()) : vnull();
        case OP_CONCAT: {
            /* ANSI/PostgreSQL: NULL || x = NULL. val_str() flattens NULL to "",
             * so without this guard `||` silently returned a non-NULL result on
             * NULL operands (T5) — inconsistent with concat(), which ignores NULL.
             * concat() keeps its skip-NULL semantics; only the || operator is
             * NULL-propagating per the standard. */
            if (L.is_null || R.is_null) return vnull();
            const char *sl=val_str(L,a), *sr=val_str(R,a);
            size_t tl=strlen(sl)+strlen(sr)+1;
            char *out=arena_alloc(a,tl); strcpy(out,sl); strcat(out,sr);
            return vstr_s(out);
        }
        case OP_LIKE:     { if(L.is_null||R.is_null) return vbool(false); return vbool(like_match(val_str(L,a),val_str(R,a),false,a)); }
        case OP_ILIKE:    { if(L.is_null||R.is_null) return vbool(false); return vbool(like_match(val_str(L,a),val_str(R,a),true,a)); }
        case OP_NOT_LIKE: { if(L.is_null||R.is_null) return vbool(false); return vbool(!like_match(val_str(L,a),val_str(R,a),false,a)); }
        case OP_NOT_ILIKE:{ if(L.is_null||R.is_null) return vbool(false); return vbool(!like_match(val_str(L,a),val_str(R,a),true,a)); }
        case OP_BETWEEN: {
            if (L.is_null) return vbool(false);
            Val lo=eval_val(e->right->left,ctx,a);
            Val hi=eval_val(e->right->right,ctx,a);
            return vbool(vcmp(L,lo)>=0 && vcmp(L,hi)<=0);
        }
        case OP_NOT_BETWEEN: {
            if (L.is_null) return vbool(false);
            Val lo=eval_val(e->right->left,ctx,a);
            Val hi=eval_val(e->right->right,ctx,a);
            return vbool(vcmp(L,lo)<0 || vcmp(L,hi)>0);
        }
        default: return vnull();
        }
    }
    default: return vnull();
    }
}

/* ── Column name inference ── */
static const char *expr_name(Expr *e, int pos, Arena *a) {
    if (!e) return arena_sprintf(a,"col%d",pos+1);
    if (e->type == EXPR_ALIAS)   return e->alias;
    if (e->type == EXPR_COL)     return e->name;
    if (e->type == EXPR_FUNC)    return e->func_name;
    if (e->type == EXPR_WINDOW)  return e->func_name ? e->func_name : arena_sprintf(a,"col%d",pos+1);
    if (e->type == EXPR_STAR)  return "*";
    if (e->type == EXPR_LITERAL_INT)   return arena_sprintf(a,"%lld",(long long)e->ival);
    if (e->type == EXPR_LITERAL_FLOAT) return dbl_fmt(a, e->fval);
    if (e->type == EXPR_LITERAL_STR)   return e->sval;
    return arena_sprintf(a,"col%d",pos+1);
}

/* ── Index predicate hint ── */
typedef struct {
    const char *col;   /* column name to look up         */
    int64_t     lo;    /* range low  (inclusive)          */
    int64_t     hi;    /* range high (inclusive)          */
    bool        valid; /* true if this hint is usable     */
} IdxHint;

/* Walk WHERE AST for simple "col op int_literal" patterns */
static void extract_idx_hint_r(Expr *e, IdxHint *out) {
    if (!e || out->valid) return;
    if (e->type == EXPR_BINOP) {
        if (e->op == OP_AND) {
            extract_idx_hint_r(e->left, out);
            extract_idx_hint_r(e->right, out);
            return;
        }
        /* col = int */
        if (e->op == OP_EQ &&
            e->left  && e->left->type  == EXPR_COL &&
            e->right && e->right->type == EXPR_LITERAL_INT) {
            out->col = e->left->name;
            out->lo  = out->hi = e->right->ival;
            out->valid = true;
            return;
        }
        /* int = col (commuted) */
        if (e->op == OP_EQ &&
            e->left  && e->left->type  == EXPR_LITERAL_INT &&
            e->right && e->right->type == EXPR_COL) {
            out->col = e->right->name;
            out->lo  = out->hi = e->left->ival;
            out->valid = true;
            return;
        }
        /* col BETWEEN lo AND hi  (parser packs lo/hi as AND subtree) */
        if (e->op == OP_BETWEEN &&
            e->left  && e->left->type  == EXPR_COL  &&
            e->right && e->right->type == EXPR_BINOP &&
            e->right->left  && e->right->left->type  == EXPR_LITERAL_INT &&
            e->right->right && e->right->right->type == EXPR_LITERAL_INT) {
            out->col = e->left->name;
            out->lo  = e->right->left->ival;
            out->hi  = e->right->right->ival;
            out->valid = true;
        }
    }
}
static IdxHint extract_idx_hint(Expr *where) {
    IdxHint h = {0};
    extract_idx_hint_r(where, &h);
    return h;
}

/* Parse one WAL record from *wf at current position into a row array */
static char **parse_wal_row(Arena *a, FILE *wf, int ncols) {
    uint32_t l = 0;
    if (fread(&l, 4, 1, wf) != 1) return NULL;
    if (l == 0 || l > 262144) return NULL;
    char *line = arena_alloc(a, (size_t)l + 1);
    if (fread(line, 1, l, wf) != l) return NULL;
    line[l] = '\0';
    size_t rl = strlen(line);
    while (rl > 0 && (line[rl-1]=='\n'||line[rl-1]=='\r')) line[--rl]='\0';
    char *vals[MAX_COLS]={0}; int nv=0;
    split_line_simple(line, ',', vals, MAX_COLS, &nv);
    char **row = arena_alloc(a, (size_t)ncols * sizeof(char*));
    for (int i = 0; i < ncols; i++) row[i] = (i<nv&&vals[i]) ? vals[i] : "";
    return row;
}

/* ── Load table rows ── */
/* Материализует все живые строки таблицы tname в out (TblData).
 * Источник по приоритету: виртуальный реестр (CTE/derived) → catalog schema.
 * Читает WAL: при годном IdxHint и B-tree по INT64-столбцу — точечный доступ
 * по offset'ам (btree_range), иначе полный скан в два прохода (сбор tombstone'ов
 * DELETE/UPDATE, затем выдача выживших с применением UPDATE). Поддерживает
 * сжатые батчи (version-байт 0x01). Возвращает 0, либо -1 если схемы нет
 * (несуществующая таблица — вызывающий помечает tl_exec_error). */
static int load_tbl(Arena *a, const char *tname, const char *alias,
                    VTReg *vt_reg, TblData *out, const IdxHint *ih) {
    memset(out, 0, sizeof(*out));
    out->tname = tname; out->alias = alias;

    /* Check virtual registry first (CTEs / derived tables) */
    RS *vrs = NULL; Schema *vsc = NULL;
    if (vt_reg && vt_get(vt_reg, tname, &vrs, &vsc) && vrs) {
        out->schema = vsc;
        out->nrows  = vrs->nrows;
        out->rows   = arena_alloc(a, (size_t)vrs->nrows * sizeof(char**));
        for (int i=0; i<vrs->nrows; i++) out->rows[i] = vrs->rows[i].cells;
        return 0;
    }

    if (catalog_get_schema(g_app.catalog, tname, &out->schema, a) != 0 || !out->schema)
        return -1;

    int ncols = out->schema->ncols;
    char wal_path[1024];
    snprintf(wal_path, sizeof(wal_path), "%s/%s/wal.bin", g_app.data_dir, tname);

    /* ── Index-accelerated path ── */
    if (ih && ih->valid && ih->col) {
        /* find col_idx in schema */
        int ci = -1;
        for (int i = 0; i < ncols; i++)
            if (out->schema->cols[i].name &&
                strcasecmp(out->schema->cols[i].name, ih->col) == 0 &&
                out->schema->cols[i].type == COL_INT64) { ci = i; break; }

        if (ci >= 0) {
            /* look for open B-tree handle in g_app.tables */
            pthread_mutex_lock(&g_app.tables_mu);
            Table *t = hm_get(&g_app.tables, tname);
            BTree *bt = t ? table_get_index(t, ci) : NULL;
            pthread_mutex_unlock(&g_app.tables_mu);

            if (bt) {
                /* allocate offsets on heap (can be large) */
                int64_t *offs = malloc((size_t)BT_MAX_OFFSETS * sizeof(int64_t));
                int noffs = 0;
                btree_range(bt, ih->lo, ih->hi, offs, &noffs);

                int cap = noffs > 0 ? noffs : 1;
                char ***rows = arena_alloc(a, (size_t)cap * sizeof(char**));
                int n = 0;

                FILE *wf = fopen(wal_path, "rb");
                if (wf) {
                    for (int oi = 0; oi < noffs; oi++) {
                        if (fseeko(wf, (off_t)offs[oi], SEEK_SET) != 0) continue;
                        char **row = parse_wal_row(a, wf, ncols);
                        if (row) rows[n++] = row;
                    }
                    fclose(wf);
                }
                free(offs);
                out->rows = rows; out->nrows = n;
                return 0;
            }
        }
    }

    /* ── Full scan with tombstone support ── */
    FILE *wf = fopen(wal_path, "rb");
    if (!wf) { out->nrows=0; return 0; }

    /* Pass 1: collect tombstones */
    typedef struct { int64_t orig_off; uint8_t op; char *new_csv; size_t new_len; } Tombstone;
    int tb_cap=16, ntb=0;
    Tombstone *tbs = arena_alloc(a, (size_t)tb_cap * sizeof(Tombstone));
    {
        int64_t file_off=0;
        char tbuf[262144];
        while (1) {
            uint32_t l=0;
            if (fread(&l,1,4,wf)!=4) break;
            if (l==0||l>sizeof(tbuf)-1) { fseek(wf,(long)l,SEEK_CUR); file_off+=4+(int64_t)l; continue; }
            if (fread(tbuf,1,l,wf)!=l) break;
            file_off += 4+(int64_t)l;
            uint8_t op=(uint8_t)tbuf[0];
            if ((op==WAL_OP_DELETE||op==WAL_OP_UPDATE) && l>=9) {
                int64_t orig=0;
                for(int b=0;b<8;b++) orig=(orig<<8)|((uint8_t)tbuf[1+b]);
                if(ntb==tb_cap){tb_cap*=2;Tombstone*nb=arena_alloc(a,(size_t)tb_cap*sizeof(Tombstone));memcpy(nb,tbs,(size_t)ntb*sizeof(Tombstone));tbs=nb;}
                tbs[ntb].orig_off=orig; tbs[ntb].op=op;
                tbs[ntb].new_csv=NULL; tbs[ntb].new_len=0;
                if (op==WAL_OP_UPDATE && l>9) {
                    size_t csv_len=l-9;
                    char *nc=arena_alloc(a,csv_len+1);
                    memcpy(nc,tbuf+9,csv_len); nc[csv_len]='\0';
                    tbs[ntb].new_csv=nc; tbs[ntb].new_len=csv_len;
                }
                ntb++;
            }
        }
        rewind(wf);
    }

    /* Pass 2: yield rows with tombstones applied */
    int cap=128, n=0;
    char ***rows = arena_alloc(a, (size_t)cap * sizeof(char**));
    int64_t file_off=0;
    while (1) {
        uint32_t l=0;
        if (fread(&l,1,4,wf)!=4) break;
        int64_t rec_off=file_off; file_off+=4+(int64_t)l;
        if (l==0||l>16777216) { fseek(wf,(long)l,SEEK_CUR); continue; } /* max 16 MiB record */
        char *line = arena_alloc(a,(size_t)l+1);
        if (fread(line,1,l,wf)!=l) break;
        line[l]='\0';

        uint8_t op=(uint8_t)(unsigned char)line[0];
        if (op==WAL_OP_DELETE||op==WAL_OP_UPDATE) continue; /* skip tombstone records */

        /* Compressed batch record (version byte 0x01).
         * Skip the entire batch if it's been tombstoned by DML —
         * the DML walker emits replacement uncompressed CSV records
         * for survivors, so all rows are still represented elsewhere. */
        if (op == 0x01 && l > 1) {
            bool batch_dead = false;
            for (int ti = 0; ti < ntb; ti++) {
                if (tbs[ti].orig_off == rec_off && tbs[ti].op == WAL_OP_DELETE) {
                    batch_dead = true; break;
                }
            }
            if (batch_dead) continue;
            CompressedBatch *cb = compressed_batch_deserialize(line+1, (size_t)(l-1), a);
            if (cb) {
                ColBatch *batch = decompress_batch(cb, a);
                if (batch) {
                    for (int r = 0; r < batch->nrows; r++) {
                        /* Convert batch row to char** cells */
                        char **row = arena_alloc(a, (size_t)ncols * sizeof(char*));
                        for (int c = 0; c < ncols; c++) {
                            if (c >= batch->ncols) { row[c] = DFO_NULL_SENTINEL; continue; }
                            if (batch->null_bitmap[c]) {
                                int bi=r/8; if (batch->null_bitmap[c][bi] & (1u<<(r%8))) { row[c]=DFO_NULL_SENTINEL; continue; }
                            }
                            ColType t = (batch->schema && c < batch->schema->ncols) ? batch->schema->cols[c].type : COL_TEXT;
                            switch (t) {
                                case COL_INT64: row[c]=arena_sprintf(a,"%lld",(long long)((int64_t*)batch->values[c])[r]); break;
                                case COL_DOUBLE: row[c]=dbl_fmt(a, ((double*)batch->values[c])[r]); break;
                                case COL_BOOL:  row[c]=((int64_t*)batch->values[c])[r]?"true":"false"; break;
                                case COL_TEXT:  {
                                    char *cv=((char**)batch->values[c])[r];
                                    /* Robustness: in a legacy/corrupt batch a COL_INT64 value (a tiny
                                     * integer like a kafka_offset) can sit under a COL_TEXT slot; deref-ing
                                     * it as a string would SIGSEGV the whole gateway (DoS). A real arena/heap
                                     * string pointer is never in the first page — treat sub-page values as NULL. */
                                    row[c]=((uintptr_t)cv < 0x1000) ? DFO_NULL_SENTINEL : cv;
                                    break;
                                }
                                default:        row[c]=DFO_NULL_SENTINEL; break;
                            }
                        }
                        if(n==cap){cap*=2;char***nb=arena_alloc(a,(size_t)cap*sizeof(char**));memcpy(nb,rows,(size_t)n*sizeof(char**));rows=nb;}
                        rows[n++]=row;
                    }
                }
            }
            continue;
        }

        size_t rl=strlen(line);
        while(rl>0&&(line[rl-1]=='\n'||line[rl-1]=='\r')) line[--rl]='\0';

        int printable=0;
        for (size_t ci=0;ci<rl;ci++) if ((unsigned char)line[ci]>=0x20) printable++;
        if (printable < 2) continue;

        /* Check tombstones */
        bool dead=false; char *upd_csv=NULL; size_t upd_len=0;
        for(int ti=0;ti<ntb;ti++) {
            if(tbs[ti].orig_off==rec_off) {
                dead=true;
                if(tbs[ti].op==WAL_OP_UPDATE) { upd_csv=tbs[ti].new_csv; upd_len=tbs[ti].new_len; }
                break;
            }
        }
        if(dead && !upd_csv) continue; /* deleted */
        if(upd_csv) {
            /* apply update: parse new CSV */
            char *uline=arena_alloc(a,upd_len+1);
            memcpy(uline,upd_csv,upd_len); uline[upd_len]='\0';
            size_t ul=strlen(uline);
            while(ul>0&&(uline[ul-1]=='\n'||uline[ul-1]=='\r')) uline[--ul]='\0';
            char *vals[MAX_COLS]={0}; int nv=0;
            split_line_simple(uline,',',vals,MAX_COLS,&nv);
            char **row=arena_alloc(a,(size_t)ncols*sizeof(char*));
            for(int i=0;i<ncols;i++) row[i]=(i<nv&&vals[i])?vals[i]:"";
            if(n==cap){cap*=2;char***nb=arena_alloc(a,(size_t)cap*sizeof(char**));memcpy(nb,rows,(size_t)n*sizeof(char**));rows=nb;}
            rows[n++]=row;
            continue;
        }

        char *vals[MAX_COLS]={0}; int nv=0;
        split_line_simple(line,',',vals,MAX_COLS,&nv);
        char **row=arena_alloc(a,(size_t)ncols*sizeof(char*));
        for(int i=0;i<ncols;i++) row[i]=(i<nv&&vals[i])?vals[i]:"";

        if(n==cap){cap*=2;char***nb=arena_alloc(a,(size_t)cap*sizeof(char**));memcpy(nb,rows,(size_t)n*sizeof(char**));rows=nb;}
        rows[n++]=row;
    }
    fclose(wf);
    out->rows=rows; out->nrows=n;
    return 0;
}

/* ── Execution state passed through the recursive join ── */
typedef struct {
    Arena            *a;
    const SelectStmt *sel;
    VTReg            *vt;
    RS               *rs;
    bool              do_agg;
    bool              implicit_agg;  /* agg with no GROUP BY */
    GrpAcc           *grps;
    int               ngrps, cap_grps;
    Expr            **xagg;   /* aggregates only in HAVING/ORDER BY → extra accumulator slots */
    int               nxagg;
    /* Ранняя пагинация: LIMIT/OFFSET применяются при эмиссии строк (collect_row),
     * а не финальным срезом. Без этого OFFSET дальше MAX_RS_ROWS невозможен —
     * rs обрезается на потолке ещё до среза (страничная выгрузка sink молча
     * теряла всё сверх 100k строк). Включается только для простых запросов. */
    bool              early_page;
    int64_t           skip_left;   /* сколько строк ещё пропустить (OFFSET) */
    int64_t           take_left;   /* сколько строк ещё отдать (LIMIT), -1 = без лимита */
    /* Потоковый скан: строки приходят из короткоживущей арены сканера, поэтому
     * star-проекция обязана копировать ячейки в арену запроса (не-star путь
     * копирует всегда через eval_val/val_cell). */
    bool              stream_rows;
    /* Арена для ПРОМЕЖУТОЧНЫХ вычислений строки (WHERE, ключи GROUP BY,
     * аргументы агрегатов, val_cell/val_str перед финальным strdup). При
     * потоковом скане указывает на арену порции и умирает вместе с ней —
     * иначе аллоцирующие выражения (UPPER(), ILIKE, col/100 в GROUP BY)
     * копили бы O(строк таблицы) в арене запроса и возвращали OOM, который
     * стриминг призван устранить. NULL — обычный путь (всё в a). */
    Arena            *eval_a;
} ExecState;

/* Арена для промежуточных вычислений текущей строки. */
#define ST_EVAL_A(st) ((st)->eval_a ? (st)->eval_a : (st)->a)


/* Находит группу по её key (значения GROUP BY, склеенные через \x1F) либо
 * создаёт новую с обнулёнными аккумуляторами на nselect+nxagg слотов. */
static GrpAcc *find_or_create_grp(ExecState *st, const char *key) {
    for (int i=0;i<st->ngrps;i++) if(!strcmp(st->grps[i].key,key)) return &st->grps[i];
    if (st->ngrps==st->cap_grps) {
        int nc=st->cap_grps*2;
        GrpAcc *nb=arena_alloc(st->a,(size_t)nc*sizeof(GrpAcc));
        memcpy(nb,st->grps,(size_t)st->ngrps*sizeof(GrpAcc));
        st->grps=nb; st->cap_grps=nc;
    }
    GrpAcc *g=&st->grps[st->ngrps++];
    memset(g,0,sizeof(*g));
    g->key=arena_strdup(st->a,key);
    int nc=st->sel->nselect + st->nxagg;   /* select slots + HAVING/ORDER extra-agg slots */
    g->sums     =arena_calloc(st->a,(size_t)nc*sizeof(double));
    g->mins     =arena_alloc (st->a,(size_t)nc*sizeof(double)); memset(g->mins,0,(size_t)nc*sizeof(double));
    g->maxs     =arena_alloc (st->a,(size_t)nc*sizeof(double)); memset(g->maxs,0,(size_t)nc*sizeof(double));
    g->cnts     =arena_calloc(st->a,(size_t)nc*sizeof(int));
    g->min_set  =arena_calloc(st->a,(size_t)nc*sizeof(bool));
    g->max_set  =arena_calloc(st->a,(size_t)nc*sizeof(bool));
    g->first_str=arena_calloc(st->a,(size_t)nc*sizeof(char*));
    g->min_str  =arena_calloc(st->a,(size_t)nc*sizeof(char*));
    g->max_str  =arena_calloc(st->a,(size_t)nc*sizeof(char*));
    g->dvals    =arena_calloc(st->a,(size_t)nc*sizeof(char**));
    g->dcnt     =arena_calloc(st->a,(size_t)nc*sizeof(int));
    g->dcap     =arena_calloc(st->a,(size_t)nc*sizeof(int));
    return g;
}

/* For a DISTINCT aggregate on select-column s: returns true if value `sv` is new
 * (and records it), false if already seen — so the caller counts/sums it once. */
static bool agg_distinct_first_seen(ExecState *st, GrpAcc *g, int s, const char *sv) {
    for (int k=0;k<g->dcnt[s];k++) if(!strcmp(g->dvals[s][k],sv)) return false;
    if (g->dcnt[s]==g->dcap[s]) {
        int nc=g->dcap[s]?g->dcap[s]*2:8;
        char **nb=arena_alloc(st->a,(size_t)nc*sizeof(char*));
        if(g->dcnt[s]) memcpy(nb,g->dvals[s],(size_t)g->dcnt[s]*sizeof(char*));
        g->dvals[s]=nb; g->dcap[s]=nc;
    }
    g->dvals[s][g->dcnt[s]++]=arena_strdup(st->a,sv);
    return true;
}

/* Accumulate one aggregate-function value (agg_e) into group slot s. */
/* MIN/MAX ordering: numeric when BOTH operands parse as numbers, else lexical. */
static int agg_val_cmp(const char *a, const char *b) {
    double na, nb;
    if (pnum(a,&na) && pnum(b,&nb)) return (na<nb)?-1:(na>nb)?1:0;
    return strcmp(a?a:"", b?b:"");
}

static void agg_accumulate(ExecState *st, GrpAcc *g, int s, Expr *agg_e, JoinCtx *ctx) {
    const char *fn=agg_e->func_name;
    bool is_star=(agg_e->nargs>0 && agg_e->args[0]->type==EXPR_STAR);
    bool distinct=agg_e->func_distinct;   /* COUNT/SUM/AVG(DISTINCT ...) */
    if (!strcasecmp(fn,"COUNT")) {
        if (is_star) g->cnts[s]++;
        else {
            Val v=eval_val(agg_e->nargs>0?agg_e->args[0]:NULL,ctx,ST_EVAL_A(st));
            if (!v.is_null && (!distinct || agg_distinct_first_seen(st,g,s,val_str(v,ST_EVAL_A(st)))))
                g->cnts[s]++;
        }
    } else {
        Val v=eval_val(agg_e->nargs>0?agg_e->args[0]:NULL,ctx,ST_EVAL_A(st));
        if (!v.is_null && (!distinct || agg_distinct_first_seen(st,g,s,val_str(v,ST_EVAL_A(st))))) {
            const char *sv=val_str(v,ST_EVAL_A(st));
            double n=v.num; if (!v.is_num) pnum(sv,&n);
            g->sums[s]+=n; g->cnts[s]++;
            if (!g->min_set[s]) {
                g->mins[s]=n; g->maxs[s]=n;
                g->min_str[s]=arena_strdup(st->a,sv); g->max_str[s]=arena_strdup(st->a,sv);
                g->min_set[s]=g->max_set[s]=true;
            } else {
                if (n<g->mins[s]) g->mins[s]=n;
                if (n>g->maxs[s]) g->maxs[s]=n;
                if (agg_val_cmp(sv,g->min_str[s])<0) g->min_str[s]=arena_strdup(st->a,sv);
                if (agg_val_cmp(sv,g->max_str[s])>0) g->max_str[s]=arena_strdup(st->a,sv);
            }
        }
    }
}

/* Обрабатывает одну входную строку в группе g: по каждому select-выражению
 * либо накапливает агрегат, либо (для не-агрегатных) запоминает первое значение;
 * плюс агрегаты из HAVING/ORDER BY в добавочных слотах. */
static void agg_row(ExecState *st, GrpAcc *g, JoinCtx *ctx) {
    g->total_cnt++;
    for (int s=0;s<st->sel->nselect;s++) {
        Expr *se=st->sel->select_list[s];
        Expr *base=(se->type==EXPR_ALIAS)?se->expr:se;
        Expr *agg_e = (base->type==EXPR_FUNC && base->func_name && is_agg_name(base->func_name))
                      ? base : find_first_agg(base);
        if (agg_e) agg_accumulate(st, g, s, agg_e, ctx);
        else if (!g->first_str[s]) {       /* non-agg: keep first value */
            Val v=eval_val(base,ctx,ST_EVAL_A(st));
            g->first_str[s]=arena_strdup(st->a,val_str(v,ST_EVAL_A(st)));
        }
    }
    /* aggregates appearing only in HAVING/ORDER BY → appended accumulator slots */
    for (int e=0;e<st->nxagg;e++)
        agg_accumulate(st, g, st->sel->nselect + e, st->xagg[e], ctx);
}

/* Финализирует агрегацию: по каждой накопленной группе формирует выходную
 * строку (агрегаты из аккумулятора, обёрнутые выражения — через eval_val с
 * активным tl_grp), применяет HAVING-фильтр и вычисляет sort-ключи ORDER BY,
 * после чего добавляет строку в результат. */
static void emit_groups(ExecState *st) {
    /* Build a synthetic schema for HAVING evaluation */
    int nc=st->sel->nselect;
    Schema *out_sc=arena_calloc(st->a,sizeof(Schema));
    out_sc->ncols=nc; out_sc->cols=arena_alloc(st->a,(size_t)nc*sizeof(ColDef));
    for(int i=0;i<nc;i++){
        out_sc->cols[i].name=expr_name(st->sel->select_list[i],i,st->a);
        out_sc->cols[i].type=COL_TEXT; out_sc->cols[i].nullable=true;
    }

    for (int gi=0;gi<st->ngrps;gi++) {
        GrpAcc *g=&st->grps[gi];

        /* expose group to eval_val so ROUND(AVG(x)) etc. work */
        tl_grp = g;
        tl_sel = st->sel;
        tl_xagg = st->xagg; tl_nxagg = st->nxagg;

        char **cells=arena_alloc(st->a,(size_t)nc*sizeof(char*));
        JoinCtx empty_ctx={0};
        for (int s=0;s<nc;s++) {
            Expr *se=st->sel->select_list[s];
            Expr *base=(se->type==EXPR_ALIAS)?se->expr:se;
            if (base->type==EXPR_FUNC && base->func_name && is_agg_name(base->func_name)) {
                /* pure aggregate at top level — read directly from accumulator */
                const char *fn=base->func_name;
                if (!strcasecmp(fn,"COUNT")) cells[s]=arena_sprintf(st->a,"%d",g->cnts[s]);
                else if (!strcasecmp(fn,"SUM")) cells[s]=dbl_fmt(st->a, g->sums[s]);
                else if (!strcasecmp(fn,"AVG")) cells[s]=g->cnts[s]?dbl_fmt(st->a, g->sums[s]/g->cnts[s]):"";
                else if (!strcasecmp(fn,"MIN")) cells[s]=g->min_set[s]?g->min_str[s]:"";
                else if (!strcasecmp(fn,"MAX")) cells[s]=g->max_set[s]?g->max_str[s]:"";
                else cells[s]="";
            } else if (expr_has_agg(base)) {
                /* expression wrapping aggregate (e.g. ROUND(AVG(x))) — eval with group ctx */
                Val v=eval_val(se,&empty_ctx,st->a);
                cells[s]=arena_strdup(st->a,val_cell(v,st->a));
            } else {
                cells[s]=g->first_str[s]?g->first_str[s]:"";
            }
        }

        /* HAVING — evaluated with group context active */
        if (st->sel->having) {
            JoinCtx hctx={0}; hctx.n=1;
            hctx.schemas[0]=out_sc; hctx.rows[0]=cells;
            hctx.tnames[0]=""; hctx.aliases[0]="";
            Val hv=eval_val(st->sel->having,&hctx,st->a);
            tl_grp=NULL; tl_sel=NULL;
            if (!vt(hv)) continue;
            tl_grp=g; tl_sel=st->sel;
        }

        /* sort keys */
        char **skeys=NULL;
        if (st->sel->norder>0) {
            JoinCtx sctx={0}; sctx.n=1;
            sctx.schemas[0]=out_sc; sctx.rows[0]=cells;
            skeys=arena_alloc(st->a,(size_t)st->sel->norder*sizeof(char*));
            for(int o=0;o<st->sel->norder;o++) {
                Val sv=eval_val(st->sel->order_by[o].expr,&sctx,st->a);
                skeys[o]=arena_strdup(st->a,val_str(sv,st->a));
            }
        }

        tl_grp=NULL; tl_sel=NULL;
        rs_add(st->rs, st->a, cells, skeys);
    }
}

/* Колбэк на каждую полную комбинацию строк join'а (вызывается из join_recurse):
 * применяет WHERE и либо роутит строку в группу (агрегация), либо проецирует
 * SELECT-список (с разворотом *) прямо в результат, попутно вычисляя ORDER BY-
 * ключи (с разрешением алиасов и позиционных ORDER BY). */
static void collect_row(ExecState *st, JoinCtx *ctx) {
    /* WHERE filter */
    if (st->sel->where) {
        Val w=eval_val(st->sel->where,ctx,ST_EVAL_A(st));
        if (!vt(w)) return;
    }

    if (st->do_agg) {
        /* Build group key */
        char kbuf[4096]; kbuf[0]='\0';
        if (!st->implicit_agg) {
            for (int g=0;g<st->sel->ngroup;g++) {
                Val gv=eval_val(st->sel->group_by[g],ctx,ST_EVAL_A(st));
                if (g) strncat(kbuf,"\x1F",sizeof(kbuf)-strlen(kbuf)-1);
                strncat(kbuf,val_str(gv,ST_EVAL_A(st)),sizeof(kbuf)-strlen(kbuf)-1);
            }
        }
        GrpAcc *grp=find_or_create_grp(st,kbuf);
        agg_row(st,grp,ctx);
        return;
    }

    /* Ранняя пагинация: пропустить OFFSET строк, отдать не более LIMIT.
     * Срабатывает ПОСЛЕ WHERE (отфильтрованные строки не тратят offset). */
    if (st->early_page) {
        if (st->take_left == 0) return;
        if (st->skip_left > 0) { st->skip_left--; return; }
        if (st->take_left > 0) st->take_left--;
    }

    /* Direct projection */
    int nc=st->sel->nselect;
    bool star=(nc==1 && st->sel->select_list[0]->type==EXPR_STAR);
    int out_cols=star?0:nc;
    if (star) {
        /* expand star: count all columns */
        for (int t=0;t<ctx->n;t++) {
            if (ctx->schemas[t]) out_cols+=ctx->schemas[t]->ncols;
        }
    }

    char **cells=arena_alloc(st->a,(size_t)out_cols*sizeof(char*));
    if (star) {
        int ci=0;
        for (int t=0;t<ctx->n;t++) {
            if (!ctx->schemas[t]||!ctx->rows[t]) {
                if (ctx->schemas[t]) for (int c=0;c<ctx->schemas[t]->ncols;c++) cells[ci++]="";
                continue;
            }
            for (int c=0;c<ctx->schemas[t]->ncols;c++) {
                const char *cv = ctx->rows[t][c] ? ctx->rows[t][c] : "";
                /* Потоковый скан: строка живёт в арене сканера до следующего
                 * батча — в RS обязана попасть копия. */
                cells[ci++] = st->stream_rows ? arena_strdup(st->a, cv) : (char *)cv;
            }
        }
    } else {
        for (int s=0;s<nc;s++) {
            Expr *se=st->sel->select_list[s];
            Expr *base=(se->type==EXPR_ALIAS)?se->expr:se;
            Val v=eval_val(base,ctx,ST_EVAL_A(st));
            cells[s]=arena_strdup(st->a,val_cell(v,ST_EVAL_A(st)));
        }
    }

    /* sort keys — resolve aliases against SELECT list before evaluating */
    char **skeys=NULL;
    if (st->sel->norder>0) {
        skeys=arena_alloc(st->a,(size_t)st->sel->norder*sizeof(char*));
        for(int o=0;o<st->sel->norder;o++) {
            Expr *oe=st->sel->order_by[o].expr;
            /* resolve SELECT alias: ORDER BY alias_name → eval the aliased expr */
            if (oe && oe->type==EXPR_COL && oe->name) {
                for(int s=0;s<st->sel->nselect;s++){
                    Expr *se=st->sel->select_list[s];
                    if (se->type==EXPR_ALIAS && se->alias && !strcasecmp(se->alias,oe->name)) {
                        oe=se->expr; break;
                    }
                }
            }
            /* positional ORDER BY: integer literal → use that SELECT column value */
            if (oe && oe->type==EXPR_LITERAL_INT && oe->ival>=1 && oe->ival<=st->sel->nselect) {
                Expr *se=st->sel->select_list[oe->ival-1];
                if (se->type==EXPR_ALIAS) se=se->expr;
                oe=se;
            }
            Val sv=eval_val(oe,ctx,ST_EVAL_A(st));
            skeys[o]=arena_strdup(st->a,val_str(sv,ST_EVAL_A(st)));
        }
    }

    rs_add(st->rs, st->a, cells, skeys);
}

/* Рекурсивный вложенно-цикловой join по таблицам tables[0..ntables). depth —
 * текущая таблица; ctx накапливает строку по одной из каждой. Для JOIN проверяет
 * условие ON, поддерживает INNER/LEFT/FULL/CROSS: при отсутствии совпадений для
 * LEFT/FULL добавляет строку с NULL-заполнением (force_nulls протягивает NULL по
 * оставшимся таблицам). На полной комбинации вызывает collect_row. */
static void join_recurse(ExecState *st, int depth, JoinCtx *ctx,
                         TblData *tables, int ntables, bool force_nulls) {
    if (force_nulls) {
        /* propagate NULLs for remaining tables */
        for (int i=depth;i<ntables;i++) {
            ctx->rows[i]=NULL; ctx->schemas[i]=tables[i].schema;
            ctx->tnames[i]=tables[i].tname; ctx->aliases[i]=tables[i].alias;
        }
        ctx->n=ntables;
        collect_row(st, ctx);
        return;
    }
    if (depth == ntables) {
        ctx->n = ntables;
        collect_row(st, ctx);
        return;
    }

    TblData *td=&tables[depth];
    ctx->schemas[depth]=td->schema;
    ctx->tnames[depth]=td->tname;
    ctx->aliases[depth]=td->alias;

    JoinType jtype=(depth==0)?JOIN_INNER:st->sel->from[depth].join_type;
    Expr *join_on=(depth==0)?NULL:st->sel->from[depth].on;

    if (jtype==JOIN_CROSS || (td->nrows==0 && jtype!=JOIN_LEFT && jtype!=JOIN_FULL)) {
        if (td->nrows==0 && jtype==JOIN_CROSS) return; /* CROSS JOIN with empty table = empty */
        if (td->nrows==0 && jtype==JOIN_LEFT && depth>0) {
            ctx->rows[depth]=NULL;
            join_recurse(st, depth+1, ctx, tables, ntables, false);
            return;
        }
    }

    bool any_match=false;
    for (int i=0;i<td->nrows;i++) {
        ctx->rows[depth]=td->rows[i];
        if (join_on) {
            ctx->n=depth+1;
            Val cv=eval_val(join_on,ctx,st->a);
            if (!vt(cv)) continue;
        }
        any_match=true;
        join_recurse(st, depth+1, ctx, tables, ntables, false);
    }

    if (!any_match && (jtype==JOIN_LEFT || (jtype==JOIN_FULL && depth==0))) {
        ctx->rows[depth]=NULL;
        join_recurse(st, depth+1, ctx, tables, ntables, jtype==JOIN_LEFT);
    }
}

/* ── Detect if any SELECT expression contains an aggregate ── */
static bool expr_has_agg(Expr *e) {
    if (!e) return false;
    Expr *b=(e->type==EXPR_ALIAS)?e->expr:e;
    if (b->type==EXPR_FUNC && b->func_name && is_agg_name(b->func_name)) return true;
    if (b->type==EXPR_FUNC) { for(int i=0;i<b->nargs;i++) if(expr_has_agg(b->args[i])) return true; return false; }
    if (b->type==EXPR_BINOP) return expr_has_agg(b->left)||expr_has_agg(b->right);
    if (b->type==EXPR_UNOP) return expr_has_agg(b->left);
    if (b->type==EXPR_CASE) {
        for(int i=0;i<b->nwhens;i++) if(expr_has_agg(b->whens[i])||expr_has_agg(b->thens[i])) return true;
        return expr_has_agg(b->else_expr);
    }
    return false;
}

/* Return first aggregate sub-expression found by DFS (NULL if none) */
static Expr *find_first_agg(Expr *e) {
    if (!e) return NULL;
    if (e->type==EXPR_ALIAS) return find_first_agg(e->expr);
    if (e->type==EXPR_FUNC && e->func_name) {
        if (is_agg_name(e->func_name)) return e;
        for (int i=0;i<e->nargs;i++) { Expr *r=find_first_agg(e->args[i]); if(r) return r; }
        return NULL;
    }
    if (e->type==EXPR_BINOP||e->type==EXPR_UNOP) {
        Expr *r=find_first_agg(e->left); if(r) return r;
        return find_first_agg(e->right);
    }
    if (e->type==EXPR_CASE) {
        for(int i=0;i<e->nwhens;i++) {
            Expr *r=find_first_agg(e->whens[i]); if(r) return r;
            r=find_first_agg(e->thens[i]); if(r) return r;
        }
        return find_first_agg(e->else_expr);
    }
    return NULL;
}

/* Return true if expr tree contains an agg function named fn with identical args (by ptr) */
/* Structural equality of two expression trees. Needed so an aggregate restated
 * in HAVING/ORDER BY (a separately-parsed Expr) resolves to the SELECT-list
 * accumulator slot: matching by pointer identity failed there, silently turning
 * HAVING SUM(x)>N into SUM=0 (empty/wrong result with no error). */
static bool expr_equal(const Expr *a, const Expr *b) {
    if (a==b) return true;
    if (!a || !b || a->type!=b->type) return false;
    switch (a->type) {
        case EXPR_COL:
            return ((!a->name)==(!b->name)) && (!a->name || !strcasecmp(a->name,b->name)) &&
                   ((!a->table)==(!b->table)) && (!a->table || !strcasecmp(a->table,b->table));
        case EXPR_STAR:
        case EXPR_LITERAL_NULL:  return true;
        case EXPR_LITERAL_INT:   return a->ival==b->ival;
        case EXPR_LITERAL_FLOAT: return a->fval==b->fval;
        case EXPR_LITERAL_STR:   return a->sval && b->sval && !strcmp(a->sval,b->sval);
        case EXPR_LITERAL_BOOL:  return a->bval==b->bval;
        case EXPR_FUNC:
            if ((!a->func_name)!=(!b->func_name)) return false;
            if (a->func_name && strcasecmp(a->func_name,b->func_name)) return false;
            if (a->func_distinct!=b->func_distinct || a->nargs!=b->nargs) return false;
            for (int i=0;i<a->nargs;i++) if(!expr_equal(a->args[i],b->args[i])) return false;
            return true;
        case EXPR_BINOP: case EXPR_UNOP:
            return a->op==b->op && expr_equal(a->left,b->left) && expr_equal(a->right,b->right);
        case EXPR_ALIAS:  return expr_equal(a->expr,b->expr);
        default: return false;
    }
}

static bool expr_find_agg_args(Expr *e, const char *fn, Expr **call_args, int nargs) {
    if (!e) return false;
    if (e->type==EXPR_ALIAS) return expr_find_agg_args(e->expr, fn, call_args, nargs);
    if (e->type==EXPR_FUNC && e->func_name) {
        if (!strcasecmp(e->func_name, fn) && is_agg_name(fn)) {
            /* match by argument STRUCTURE (not pointer): a HAVING/ORDER BY
             * aggregate is a separately-parsed tree, so SUM(amount) there must
             * still resolve to the SELECT-list SUM(amount) accumulator slot. */
            if (e->nargs == nargs) {
                bool ok=true;
                for(int i=0;i<nargs;i++) if(!expr_equal(e->args[i],call_args[i])){ok=false;break;}
                if (ok) return true;
            }
            /* COUNT(*) special: accept if both have EXPR_STAR first arg */
            if (nargs==1 && e->nargs==1 &&
                call_args[0]->type==EXPR_STAR && e->args[0]->type==EXPR_STAR) return true;
        }
        for (int i=0;i<e->nargs;i++) if(expr_find_agg_args(e->args[i],fn,call_args,nargs)) return true;
        return false;
    }
    if (e->type==EXPR_BINOP||e->type==EXPR_UNOP)
        return expr_find_agg_args(e->left,fn,call_args,nargs)||expr_find_agg_args(e->right,fn,call_args,nargs);
    if (e->type==EXPR_CASE) {
        for(int i=0;i<e->nwhens;i++)
            if(expr_find_agg_args(e->whens[i],fn,call_args,nargs)||
               expr_find_agg_args(e->thens[i],fn,call_args,nargs)) return true;
        return expr_find_agg_args(e->else_expr,fn,call_args,nargs);
    }
    return false;
}

/* Collect every distinct aggregate-function node in `e` into *list (dedup by
 * structure). Used to give HAVING/ORDER-BY-only aggregates accumulator slots. */
static void collect_aggs(Expr *e, Expr ***list, int *n, int *cap, Arena *a) {
    if (!e) return;
    if (e->type==EXPR_FUNC && e->func_name && is_agg_name(e->func_name)) {
        for (int i=0;i<*n;i++) if (expr_equal((*list)[i], e)) return;  /* already have it */
        if (*n==*cap) { int nc=*cap?*cap*2:4; Expr **nb=arena_alloc(a,(size_t)nc*sizeof(Expr*));
                        if(*n) memcpy(nb,*list,(size_t)*n*sizeof(Expr*)); *list=nb; *cap=nc; }
        (*list)[(*n)++]=e;
        return;  /* SQL aggregates do not nest */
    }
    switch (e->type) {
        case EXPR_ALIAS: collect_aggs(e->expr,list,n,cap,a); break;
        case EXPR_FUNC:  for(int i=0;i<e->nargs;i++) collect_aggs(e->args[i],list,n,cap,a); break;
        case EXPR_BINOP: case EXPR_UNOP:
            collect_aggs(e->left,list,n,cap,a); collect_aggs(e->right,list,n,cap,a); break;
        case EXPR_CASE:
            for(int i=0;i<e->nwhens;i++){ collect_aggs(e->whens[i],list,n,cap,a); collect_aggs(e->thens[i],list,n,cap,a); }
            collect_aggs(e->else_expr,list,n,cap,a); break;
        default: break;
    }
}

/* ── Build column names for a star expansion ── */
static void build_star_col_names(Arena *a, const SelectStmt *sel, TblData *tables, int ntables,
                                  char ***names_out, int *ncols_out) {
    int nc=0;
    for (int t=0;t<ntables;t++) if(tables[t].schema) nc+=tables[t].schema->ncols;
    char **names=arena_alloc(a,(size_t)nc*sizeof(char*));
    int ci=0;
    for (int t=0;t<ntables;t++) {
        if (!tables[t].schema) continue;
        for (int c=0;c<tables[t].schema->ncols;c++) {
            /* prefix with table alias if multiple tables */
            if (ntables>1 && (tables[t].alias||tables[t].tname)) {
                const char *pfx=tables[t].alias?tables[t].alias:tables[t].tname;
                names[ci++]=arena_sprintf(a,"%s.%s",pfx,tables[t].schema->cols[c].name);
            } else {
                names[ci++]=arena_strdup(a,tables[t].schema->cols[c].name);
            }
        }
    }
    *names_out=names; *ncols_out=nc;
    (void)sel;
}

/* Infer the logical output type of a select expression for typed JSON rendering:
 * a column ref -> its source column's catalog type; an aggregate -> COUNT:INT64,
 * SUM/AVG/MIN/MAX:DOUBLE; numeric/bool literals -> their type; else TEXT. */
static ColType infer_expr_type(Expr *e, TblData *tables, int ntables) {
    if (!e) return COL_TEXT;
    if (e->type==EXPR_ALIAS) return infer_expr_type(e->expr, tables, ntables);
    if (e->type==EXPR_COL) {
        for (int t=0;t<ntables;t++) {
            Schema *sc=tables[t].schema; if(!sc) continue;
            if (e->table && *e->table) {
                bool m=(tables[t].tname && !strcasecmp(e->table,tables[t].tname)) ||
                       (tables[t].alias && !strcasecmp(e->table,tables[t].alias));
                if(!m) continue;
            }
            for (int c=0;c<sc->ncols;c++)
                if (sc->cols[c].name && e->name && !strcasecmp(sc->cols[c].name,e->name))
                    return sc->cols[c].type;
        }
        return COL_TEXT;
    }
    if (e->type==EXPR_FUNC && e->func_name && is_agg_name(e->func_name)) {
        if (!strcasecmp(e->func_name,"COUNT")) return COL_INT64;
        if (!strcasecmp(e->func_name,"MIN")||!strcasecmp(e->func_name,"MAX"))
            return e->nargs>0 ? infer_expr_type(e->args[0], tables, ntables) : COL_TEXT;
        return COL_DOUBLE;  /* SUM/AVG */
    }
    if (e->type==EXPR_LITERAL_INT)   return COL_INT64;
    if (e->type==EXPR_LITERAL_FLOAT) return COL_DOUBLE;
    if (e->type==EXPR_LITERAL_BOOL)  return COL_BOOL;
    /* CAST(x AS <type>) — propagate the declared target type so a typed sink
     * (PG/Parquet) creates a typed column for it instead of TEXT (K2). The type
     * name is args[1] (a string literal). */
    if (e->type==EXPR_FUNC && e->func_name && !strcasecmp(e->func_name,"cast")
        && e->nargs==2 && e->args[1]) {
        const char *tn = e->args[1]->sval ? e->args[1]->sval : e->args[1]->name;
        if (tn) {
            if (!strncasecmp(tn,"int",3)||!strncasecmp(tn,"big",3)||!strncasecmp(tn,"sma",3)) return COL_INT64;
            if (!strncasecmp(tn,"float",5)||!strncasecmp(tn,"double",6)||!strncasecmp(tn,"num",3)
                ||!strncasecmp(tn,"dec",3)||!strncasecmp(tn,"real",4)) return COL_DOUBLE;
            if (!strncasecmp(tn,"bool",4)) return COL_BOOL;
        }
        return COL_TEXT;
    }
    return COL_TEXT;
}

/* ── Window function support ── */

static bool expr_has_window(Expr *e) {
    if (!e) return false;
    if (e->type == EXPR_WINDOW) return true;
    if (e->type == EXPR_ALIAS)  return expr_has_window(e->expr);
    if (e->type == EXPR_BINOP)  return expr_has_window(e->left) || expr_has_window(e->right);
    if (e->type == EXPR_UNOP)   return expr_has_window(e->left);
    if (e->type == EXPR_FUNC) {
        for (int i = 0; i < e->nargs; i++) if (expr_has_window(e->args[i])) return true;
    }
    if (e->type == EXPR_CASE) {
        if (expr_has_window(e->case_op)) return true;
        for (int i = 0; i < e->nwhens; i++)
            if (expr_has_window(e->whens[i]) || expr_has_window(e->thens[i])) return true;
        return expr_has_window(e->else_expr);
    }
    return false;
}

/* Collect all unique EXPR_WINDOW nodes from expression tree into out[]. */
static int collect_win_exprs(Expr *e, Expr **out, int cap, int n) {
    if (!e || n >= cap) return n;
    if (e->type == EXPR_WINDOW) {
        for (int i = 0; i < n; i++) if (out[i] == e) return n;
        out[n++] = e; return n;
    }
    if (e->type == EXPR_ALIAS)  return collect_win_exprs(e->expr, out, cap, n);
    if (e->type == EXPR_BINOP) { n = collect_win_exprs(e->left,out,cap,n); return collect_win_exprs(e->right,out,cap,n); }
    if (e->type == EXPR_UNOP)   return collect_win_exprs(e->left, out, cap, n);
    if (e->type == EXPR_FUNC) {
        for (int i = 0; i < e->nargs; i++) n = collect_win_exprs(e->args[i], out, cap, n);
    }
    if (e->type == EXPR_CASE) {
        n = collect_win_exprs(e->case_op, out, cap, n);
        for (int i = 0; i < e->nwhens; i++) {
            n = collect_win_exprs(e->whens[i], out, cap, n);
            n = collect_win_exprs(e->thens[i], out, cap, n);
        }
        n = collect_win_exprs(e->else_expr, out, cap, n);
    }
    return n;
}

/* Resolve frame start index (0-based position within sorted partition). */
static int win_frame_start(WindowSpec *ws, int pi, int plen) {
    (void)plen;
    switch (ws->frame_start.kind) {
    case WBOUND_UNBOUNDED_PREC: return 0;
    case WBOUND_N_PREC: { int v = pi - (int)ws->frame_start.n; return v < 0 ? 0 : v; }
    case WBOUND_N_FOLL: { int v = pi + (int)ws->frame_start.n; return v >= plen ? plen : v; }
    default:            return pi; /* CURRENT_ROW */
    }
}

/* Resolve frame end index. */
static int win_frame_end(WindowSpec *ws, int pi, int plen) {
    switch (ws->frame_end.kind) {
    case WBOUND_UNBOUNDED_FOLL: return plen - 1;
    case WBOUND_N_FOLL: { int v = pi + (int)ws->frame_end.n; return v >= plen ? plen-1 : v; }
    case WBOUND_N_PREC: { int v = pi - (int)ws->frame_end.n; return v < 0 ? 0 : v; }
    case WBOUND_UNBOUNDED_PREC: return 0;
    default:            return pi; /* CURRENT_ROW */
    }
}

/* Compare two rows by their per-key order-key arrays. Returns <0 / 0 / >0. */
static int win_ord_cmp(char **ak, char **bk, OrderItem *order_by, int norder) {
    for (int k = 0; k < norder; k++) {
        double na, nb;
        int c;
        if (pnum(ak[k], &na) && pnum(bk[k], &nb)) c = (na<nb)?-1:(na>nb?1:0);
        else c = strcmp(ak[k], bk[k]);
        if (order_by[k].desc) c = -c;
        if (c != 0) return c;
    }
    return 0;
}

/*
 * apply_windows — post-processing step that computes all window function values
 * for every row in rs, then re-evaluates expressions containing EXPR_WINDOW nodes
 * so that combined expressions like "revenue - LAG(...)" work correctly.
 */
static void apply_windows(Arena *a, RS *rs, const SelectStmt *sel) {
    if (rs->nrows == 0) return;

    /* Collect all unique EXPR_WINDOW nodes across the select list */
#define MAX_WIN_EXPRS 32
    Expr *win_exprs[MAX_WIN_EXPRS];
    int nwin = 0;
    for (int si = 0; si < sel->nselect; si++)
        nwin = collect_win_exprs(sel->select_list[si], win_exprs, MAX_WIN_EXPRS, nwin);
    if (nwin == 0) return;

    /* Build output schema: maps col_names → cells positions */
    Schema *out_sc = arena_calloc(a, sizeof(Schema));
    out_sc->ncols = rs->ncols;
    out_sc->cols  = arena_alloc(a, (size_t)rs->ncols * sizeof(ColDef));
    for (int i = 0; i < rs->ncols; i++) {
        out_sc->cols[i].name = rs->col_names[i];
        out_sc->cols[i].type = COL_TEXT;
    }

    /* Allocate per-window, per-row value storage */
    WinValEntry *wve = arena_alloc(a, (size_t)nwin * sizeof(WinValEntry));
    for (int wi = 0; wi < nwin; wi++) {
        wve[wi].win_expr = win_exprs[wi];
        wve[wi].values   = arena_alloc(a, (size_t)rs->nrows * sizeof(char*));
        for (int r = 0; r < rs->nrows; r++) wve[wi].values[r] = "";
    }

    /* Compute window function values for each EXPR_WINDOW node */
    for (int wi = 0; wi < nwin; wi++) {
        Expr      *e  = win_exprs[wi];
        WindowSpec *ws = e->win_spec;
        const char *fn = e->func_name ? e->func_name : "";

        /* Compute partition key string and per-order-key strings for every row */
        char  **part_keys    = arena_alloc(a, (size_t)rs->nrows * sizeof(char*));
        char ***row_ord_keys = NULL;
        if (ws->norder > 0)
            row_ord_keys = arena_alloc(a, (size_t)rs->nrows * sizeof(char**));

        for (int r = 0; r < rs->nrows; r++) {
            JoinCtx ctx = {0};
            ctx.n = 1; ctx.schemas[0] = out_sc;
            ctx.rows[0] = rs->rows[r].cells;
            ctx.tnames[0] = ""; ctx.aliases[0] = "";

            char kbuf[4096]; kbuf[0] = '\0';
            for (int k = 0; k < ws->npartition; k++) {
                Val v = eval_val(ws->partition_by[k], &ctx, a);
                if (k) strncat(kbuf, "\x01", sizeof(kbuf)-strlen(kbuf)-1);
                strncat(kbuf, val_str(v, a), sizeof(kbuf)-strlen(kbuf)-1);
            }
            part_keys[r] = arena_strdup(a, kbuf);

            if (ws->norder > 0) {
                row_ord_keys[r] = arena_alloc(a, (size_t)ws->norder * sizeof(char*));
                for (int k = 0; k < ws->norder; k++) {
                    Val v = eval_val(ws->order_by[k].expr, &ctx, a);
                    row_ord_keys[r][k] = arena_strdup(a, val_str(v, a));
                }
            }
        }

        /* Sort row indices by (partition_key, order_keys) — stable insertion sort */
        int *idx = arena_alloc(a, (size_t)rs->nrows * sizeof(int));
        for (int r = 0; r < rs->nrows; r++) idx[r] = r;
        for (int i = 1; i < rs->nrows; i++) {
            int ki = idx[i], j = i - 1;
            while (j >= 0) {
                int ai = idx[j];
                int c = strcmp(part_keys[ai], part_keys[ki]);
                if (c == 0 && ws->norder > 0)
                    c = win_ord_cmp(row_ord_keys[ai], row_ord_keys[ki], ws->order_by, ws->norder);
                if (c <= 0) break;
                idx[j+1] = idx[j]; j--;
            }
            idx[j+1] = ki;
        }

        /* Walk partitions and compute window values per function type */
        const char *prev_part = NULL;
        int part_start = 0;

        for (int i = 0; i <= rs->nrows; i++) {
            bool ep = (i == rs->nrows) || (!prev_part) ||
                      (strcmp(part_keys[idx[i]], prev_part) != 0);

            if (i > 0 && ep) {
                int plen = i - part_start;

                if (!strcasecmp(fn, "row_number")) {
                    for (int pi = 0; pi < plen; pi++)
                        wve[wi].values[idx[part_start+pi]] = arena_sprintf(a, "%d", pi+1);

                } else if (!strcasecmp(fn, "rank")) {
                    char **prev_ord = NULL;
                    int rank = 1;
                    for (int pi = 0; pi < plen; pi++) {
                        int ri = idx[part_start+pi];
                        if (pi == 0) {
                            prev_ord = ws->norder > 0 ? row_ord_keys[ri] : NULL;
                        } else {
                            bool same = true;
                            if (ws->norder > 0 && prev_ord)
                                for (int k=0; k<ws->norder && same; k++)
                                    if (strcmp(prev_ord[k], row_ord_keys[ri][k])) same=false;
                            if (!same) { rank = pi+1; prev_ord = ws->norder > 0 ? row_ord_keys[ri] : NULL; }
                        }
                        wve[wi].values[ri] = arena_sprintf(a, "%d", rank);
                    }

                } else if (!strcasecmp(fn, "dense_rank")) {
                    char **prev_ord = NULL;
                    int dr = 1;
                    for (int pi = 0; pi < plen; pi++) {
                        int ri = idx[part_start+pi];
                        if (pi == 0) {
                            prev_ord = ws->norder > 0 ? row_ord_keys[ri] : NULL;
                        } else {
                            bool same = true;
                            if (ws->norder > 0 && prev_ord)
                                for (int k=0; k<ws->norder && same; k++)
                                    if (strcmp(prev_ord[k], row_ord_keys[ri][k])) same=false;
                            if (!same) { dr++; prev_ord = ws->norder > 0 ? row_ord_keys[ri] : NULL; }
                        }
                        wve[wi].values[ri] = arena_sprintf(a, "%d", dr);
                    }

                } else if (!strcasecmp(fn, "lag") || !strcasecmp(fn, "lead")) {
                    int offset = 1;
                    const char *def_val = "0";
                    if (e->nargs >= 2) {
                        JoinCtx c0 = {0}; Val ov = eval_val(e->args[1], &c0, a);
                        if (ov.is_num) offset = (int)ov.num;
                    }
                    if (e->nargs >= 3) {
                        JoinCtx c0 = {0}; Val dv = eval_val(e->args[2], &c0, a);
                        def_val = arena_strdup(a, val_str(dv, a));
                    }
                    bool is_lead = !strcasecmp(fn, "lead");
                    for (int pi = 0; pi < plen; pi++) {
                        int ri  = idx[part_start+pi];
                        int spi = is_lead ? (pi + offset) : (pi - offset);
                        if (spi < 0 || spi >= plen) {
                            wve[wi].values[ri] = arena_strdup(a, def_val);
                        } else {
                            int sri = idx[part_start+spi];
                            JoinCtx ctx = {0}; ctx.n=1; ctx.schemas[0]=out_sc;
                            ctx.rows[0]=rs->rows[sri].cells; ctx.tnames[0]=""; ctx.aliases[0]="";
                            Val v = e->nargs > 0 ? eval_val(e->args[0], &ctx, a) : vnull();
                            wve[wi].values[ri] = arena_strdup(a, val_str(v, a));
                        }
                    }

                } else if (!strcasecmp(fn, "first_value") || !strcasecmp(fn, "last_value")) {
                    bool is_last = !strcasecmp(fn, "last_value");
                    for (int pi = 0; pi < plen; pi++) {
                        int ri  = idx[part_start+pi];
                        int f_s = ws->has_frame ? win_frame_start(ws,pi,plen) : 0;
                        int f_e = ws->has_frame ? win_frame_end(ws,pi,plen)   : plen-1;
                        int spi = is_last ? f_e : f_s;
                        int sri = idx[part_start+spi];
                        JoinCtx ctx = {0}; ctx.n=1; ctx.schemas[0]=out_sc;
                        ctx.rows[0]=rs->rows[sri].cells; ctx.tnames[0]=""; ctx.aliases[0]="";
                        Val v = e->nargs > 0 ? eval_val(e->args[0], &ctx, a) : vnull();
                        wve[wi].values[ri] = arena_strdup(a, val_str(v, a));
                    }

                } else {
                    /* Aggregate window: AVG / SUM / MIN / MAX / COUNT with frame */
                    for (int pi = 0; pi < plen; pi++) {
                        int ri  = idx[part_start+pi];
                        int f_s, f_e;
                        if (ws->has_frame) {
                            f_s = win_frame_start(ws, pi, plen);
                            f_e = win_frame_end(ws, pi, plen);
                        } else if (ws->norder > 0) {
                            f_s = 0; f_e = pi; /* default: UNBOUNDED PRECEDING to CURRENT ROW */
                        } else {
                            f_s = 0; f_e = plen - 1; /* no ORDER: entire partition */
                        }
                        double sum=0, cnt=0, minv=0, maxv=0;
                        bool min_set=false, max_set=false;
                        for (int fi = f_s; fi <= f_e; fi++) {
                            int fri = idx[part_start+fi];
                            JoinCtx ctx = {0}; ctx.n=1; ctx.schemas[0]=out_sc;
                            ctx.rows[0]=rs->rows[fri].cells; ctx.tnames[0]=""; ctx.aliases[0]="";
                            Val v = e->nargs > 0 ? eval_val(e->args[0], &ctx, a) : vnull();
                            if (!v.is_null) {
                                double nv = v.is_num ? v.num : 0;
                                if (!v.is_num) pnum(val_str(v, a), &nv);
                                sum += nv; cnt++;
                                if (!min_set || nv < minv) { minv=nv; min_set=true; }
                                if (!max_set || nv > maxv) { maxv=nv; max_set=true; }
                            }
                        }
                        const char *res;
                        if      (!strcasecmp(fn,"count")) res = arena_sprintf(a,"%.0f",cnt);
                        else if (!strcasecmp(fn,"sum"))   res = dbl_fmt(a, sum);
                        else if (!strcasecmp(fn,"avg"))   res = cnt>0 ? dbl_fmt(a, sum/cnt) : "";
                        else if (!strcasecmp(fn,"min"))   res = min_set ? dbl_fmt(a, minv) : "";
                        else if (!strcasecmp(fn,"max"))   res = max_set ? dbl_fmt(a, maxv) : "";
                        else res = "";
                        wve[wi].values[ri] = arena_strdup(a, res);
                    }
                }
            }

            if (i < rs->nrows) {
                if (!prev_part || strcmp(part_keys[idx[i]], prev_part) != 0) {
                    prev_part = part_keys[idx[i]];
                    part_start = i;
                }
            }
        }
    }

    /* Re-evaluate all select expressions that contain a window node,
       with tl_win_cur_row set so EXPR_WINDOW returns the pre-computed value. */
    tl_win_vals    = wve;
    tl_nwin_vals   = nwin;

    for (int si = 0; si < sel->nselect; si++) {
        if (!expr_has_window(sel->select_list[si])) continue;
        Expr *se   = sel->select_list[si];
        Expr *base = (se->type == EXPR_ALIAS) ? se->expr : se;
        for (int r = 0; r < rs->nrows; r++) {
            tl_win_cur_row = r;
            JoinCtx ctx = {0}; ctx.n=1; ctx.schemas[0]=out_sc;
            ctx.rows[0]=rs->rows[r].cells; ctx.tnames[0]=""; ctx.aliases[0]="";
            Val v = eval_val(base, &ctx, a);
            rs->rows[r].cells[si] = arena_strdup(a, val_cell(v, a));
        }
    }

    tl_win_vals    = NULL;
    tl_nwin_vals   = 0;
    tl_win_cur_row = -1;
}

/* ── Execute a SELECT statement ── */
/* Полный конвейер SELECT → RS. Порядок: выполнить CTE (в vt-реестр), загрузить
 * источники (таблицы через load_tbl / derived-подзапросы), определить нужду в
 * агрегации, вывести имена и логические типы столбцов, прогнать join_recurse
 * (→ WHERE/GROUP/проекция), затем emit_groups (агрегация), DISTINCT, оконные
 * функции (apply_windows), ORDER BY (стабильная сортировка) и LIMIT/OFFSET.
 * Несуществующий источник помечает tl_exec_error. */
/* ── Потоковый скан одиночной таблицы (без материализации в RAM) ──
 * Для SELECT из одной каталожной таблицы строки читаются из WAL порциями и
 * сразу эмитятся в collect_row; память — O(батча), а не O(таблицы): load_tbl
 * материализовал всю таблицу, и запрос по таблице в 56 млн строк (7.6 ГБ)
 * исчерпал бы RAM сервера. Семантика идентична load_tbl+join_recurse: тот же
 * порядок строк (порядок WAL), те же tombstone-правила; ORDER BY / DISTINCT /
 * оконные функции работают над RS как раньше (он ограничен MAX_RS_ROWS с
 * флагом truncated). Ускорения: целые сжатые батчи пропускаются по nrows из
 * заголовка (early_page без WHERE — страничная выгрузка sink не платит за
 * декомпрессию пропускаемого префикса); скан останавливается при исчерпании
 * LIMIT и при переполнении RS без агрегации (дальнейшие строки всё равно
 * отбрасываются). */
static void stream_tbl_scan(ExecState *st, TblData *td) {
    Arena *a = st->a;
    int ncols = td->schema->ncols;
    char wal_path[1024];
    snprintf(wal_path, sizeof(wal_path), "%s/%s/wal.bin", g_app.data_dir, td->tname);
    FILE *wf = fopen(wal_path, "rb");
    if (!wf) return;                          /* WAL нет — пустая таблица */

    /* Pass 1: tombstones (O(числа DML-операций) — живут в арене запроса). */
    typedef struct { int64_t orig_off; uint8_t op; char *new_csv; size_t new_len; } Tomb;
    int tb_cap = 16, ntb = 0;
    Tomb *tbs = arena_alloc(a, (size_t)tb_cap * sizeof(Tomb));
    {
        char tbuf[262144];
        while (1) {
            uint32_t l = 0;
            if (fread(&l, 1, 4, wf) != 4) break;
            if (l == 0 || l > sizeof(tbuf) - 1) { fseeko(wf, (off_t)l, SEEK_CUR); continue; }
            if (fread(tbuf, 1, l, wf) != l) break;
            uint8_t op = (uint8_t)tbuf[0];
            if ((op == WAL_OP_DELETE || op == WAL_OP_UPDATE) && l >= 9) {
                int64_t orig = 0;
                for (int b = 0; b < 8; b++) orig = (orig << 8) | ((uint8_t)tbuf[1 + b]);
                if (ntb == tb_cap) {
                    tb_cap *= 2;
                    Tomb *nb = arena_alloc(a, (size_t)tb_cap * sizeof(Tomb));
                    memcpy(nb, tbs, (size_t)ntb * sizeof(Tomb)); tbs = nb;
                }
                tbs[ntb].orig_off = orig; tbs[ntb].op = op;
                tbs[ntb].new_csv = NULL; tbs[ntb].new_len = 0;
                if (op == WAL_OP_UPDATE && l > 9) {
                    size_t cl = l - 9;
                    char *nc = arena_alloc(a, cl + 1);
                    memcpy(nc, tbuf + 9, cl); nc[cl] = '\0';
                    tbs[ntb].new_csv = nc; tbs[ntb].new_len = cl;
                }
                ntb++;
            }
        }
        rewind(wf);
    }

    JoinCtx ctx = {0};
    ctx.n = 1;
    ctx.schemas[0] = td->schema;
    ctx.tnames[0]  = td->tname ? td->tname : "";
    ctx.aliases[0] = td->alias ? td->alias : "";

    /* Пропуск строк до OFFSET без разбора допустим, только когда WHERE нет:
     * offset считается по строкам ПОСЛЕ фильтра. */
    bool fast_skip = st->early_page && st->sel->where == NULL;
    Arena *sa = arena_create(1 << 20);       /* арена текущей порции */
    st->eval_a = sa;                          /* промежуточные вычисления — в порцию */
    int64_t file_off = 0;
    unsigned nrec = 0;

    /* Стоп: LIMIT выбран, либо RS переполнен и агрегации нет (все дальнейшие
     * строки отбрасывались бы в rs_add — результат уже не изменится). */
    #define DFO_SCAN_DONE() \
        ((st->early_page && st->take_left == 0) || (st->rs->truncated && !st->do_agg))

    while (!DFO_SCAN_DONE()) {
        uint32_t l = 0;
        if (fread(&l, 1, 4, wf) != 4) break;
        int64_t rec_off = file_off; file_off += 4 + (int64_t)l;
        if (l == 0 || l > 16777216) { fseeko(wf, (off_t)l, SEEK_CUR); continue; }

        int op = fgetc(wf);
        if (op == EOF) break;
        if (op == WAL_OP_DELETE || op == WAL_OP_UPDATE) {  /* tombstone-запись */
            fseeko(wf, (off_t)(l - 1), SEEK_CUR); continue;
        }

        /* Судьба записи по tombstone известна до чтения тела (по rec_off). */
        bool dead = false; char *upd_csv = NULL; size_t upd_len = 0;
        for (int ti = 0; ti < ntb; ti++) {
            if (tbs[ti].orig_off == rec_off) {
                dead = true;
                if (tbs[ti].op == WAL_OP_UPDATE) { upd_csv = tbs[ti].new_csv; upd_len = tbs[ti].new_len; }
                break;
            }
        }

        if (op == 0x01 && l > 1) {                         /* сжатый батч */
            if (dead && !upd_csv) { fseeko(wf, (off_t)(l - 1), SEEK_CUR); continue; }
            if (l <= 14) {  /* короче заголовка: битая запись — 0 строк, как в load_tbl */
                fseeko(wf, (off_t)(l - 1), SEEK_CUR); continue;
            }
            char hdr[13];                                   /* "DCMB"+ver+ncols+nrows */
            if (fread(hdr, 1, 13, wf) != 13) break;
            uint32_t bn = 0; memcpy(&bn, hdr + 9, 4);
            /* fast-skip доверяет nrows только валидному заголовку; иначе полный
             * декод (битая запись даст NULL и 0 строк — паритет с load_tbl). */
            bool hdr_ok = memcmp(hdr, "DCMB", 4) == 0 && hdr[4] == 1 &&
                          bn > 0 && bn <= 16777216;
            if (fast_skip && hdr_ok && st->skip_left >= (int64_t)bn) {
                st->skip_left -= (int64_t)bn;               /* батч целиком до OFFSET */
                fseeko(wf, (off_t)(l - 14), SEEK_CUR);
                continue;
            }
            char *line = arena_alloc(sa, (size_t)l + 1);
            line[0] = (char)op; memcpy(line + 1, hdr, 13);
            if (fread(line + 14, 1, (size_t)l - 14, wf) != (size_t)l - 14) break;
            CompressedBatch *cb = compressed_batch_deserialize(line + 1, (size_t)(l - 1), sa);
            ColBatch *batch = cb ? decompress_batch(cb, sa) : NULL;
            if (batch) {
                for (int r = 0; r < batch->nrows && !DFO_SCAN_DONE(); r++) {
                    if (fast_skip && st->skip_left > 0) { st->skip_left--; continue; }
                    char **row = arena_alloc(sa, (size_t)ncols * sizeof(char *));
                    for (int c = 0; c < ncols; c++) {
                        if (c >= batch->ncols) { row[c] = (char *)DFO_NULL_SENTINEL; continue; }
                        if (batch->null_bitmap[c]) {
                            int bi = r / 8;
                            if (batch->null_bitmap[c][bi] & (1u << (r % 8))) { row[c] = (char *)DFO_NULL_SENTINEL; continue; }
                        }
                        ColType t = (batch->schema && c < batch->schema->ncols) ? batch->schema->cols[c].type : COL_TEXT;
                        switch (t) {
                            case COL_INT64:  row[c] = arena_sprintf(sa, "%lld", (long long)((int64_t *)batch->values[c])[r]); break;
                            case COL_DOUBLE: row[c] = (char *)dbl_fmt(sa, ((double *)batch->values[c])[r]); break;
                            case COL_BOOL:   row[c] = ((int64_t *)batch->values[c])[r] ? "true" : "false"; break;
                            case COL_TEXT: {
                                char *cv = ((char **)batch->values[c])[r];
                                /* см. load_tbl: защита от legacy-батча с числом под TEXT-слотом */
                                row[c] = ((uintptr_t)cv < 0x1000) ? (char *)DFO_NULL_SENTINEL : cv;
                                break;
                            }
                            default: row[c] = (char *)DFO_NULL_SENTINEL; break;
                        }
                    }
                    ctx.rows[0] = row;
                    collect_row(st, &ctx);
                }
            }
            arena_destroy(sa); sa = arena_create(1 << 20);  /* память O(батча) */
            st->eval_a = sa;
            continue;
        }

        /* несжатая CSV-запись */
        if (dead && !upd_csv) { fseeko(wf, (off_t)(l - 1), SEEK_CUR); continue; }
        char *line = arena_alloc(sa, (size_t)l + 1);
        line[0] = (char)op;
        if (l > 1 && fread(line + 1, 1, (size_t)l - 1, wf) != (size_t)l - 1) break;
        line[l] = '\0';

        /* printable-фильтр тела оригинала — ДО применения UPDATE (как в load_tbl:
         * непечатаемая запись отбрасывается, даже если на неё есть UPDATE). */
        {
            size_t rl0 = strlen(line);
            while (rl0 > 0 && (line[rl0-1]=='\n' || line[rl0-1]=='\r')) line[--rl0] = '\0';
            int pr0 = 0;
            for (size_t ci = 0; ci < rl0; ci++) if ((unsigned char)line[ci] >= 0x20) pr0++;
            if (pr0 < 2) {
                if (((++nrec) & 2047u) == 0) { arena_destroy(sa); sa = arena_create(1 << 20); st->eval_a = sa; }
                continue;
            }
        }
        char **row = NULL;
        if (upd_csv) {
            char *uline = arena_alloc(sa, upd_len + 1);
            memcpy(uline, upd_csv, upd_len); uline[upd_len] = '\0';
            size_t ul = strlen(uline);
            while (ul > 0 && (uline[ul-1]=='\n' || uline[ul-1]=='\r')) uline[--ul] = '\0';
            char *vals[MAX_COLS] = {0}; int nv = 0;
            split_line_simple(uline, ',', vals, MAX_COLS, &nv);
            row = arena_alloc(sa, (size_t)ncols * sizeof(char *));
            for (int i = 0; i < ncols; i++) row[i] = (i < nv && vals[i]) ? vals[i] : "";
        } else {
            char *vals[MAX_COLS] = {0}; int nv = 0;
            split_line_simple(line, ',', vals, MAX_COLS, &nv);
            row = arena_alloc(sa, (size_t)ncols * sizeof(char *));
            for (int i = 0; i < ncols; i++) row[i] = (i < nv && vals[i]) ? vals[i] : "";
        }
        if (row) {
            if (fast_skip && st->skip_left > 0) st->skip_left--;
            else { ctx.rows[0] = row; collect_row(st, &ctx); }
        }
        if (((++nrec) & 2047u) == 0) { arena_destroy(sa); sa = arena_create(1 << 20); st->eval_a = sa; }
    }
    #undef DFO_SCAN_DONE
    st->eval_a = NULL;                        /* HAVING/emit_groups после скана — в арене запроса */
    arena_destroy(sa);
    fclose(wf);
}

static RS *exec_select(Arena *a, const SelectStmt *sel, VTReg *parent_vt) {
    /* Setup VT registry — inherit parent + add CTEs */
    VTReg local_vt={0};
    if (parent_vt) { local_vt=*parent_vt; }
    VTReg *vt=&local_vt;

    /* Execute CTEs */
    for (int i=0;i<sel->nctes;i++) {
        CTE *cte=&sel->ctes[i];
        if (!cte->body) continue;
        Stmt tmp={.type=STMT_SELECT,.select=*cte->body};
        RS *cte_rs=exec_select(a,&tmp.select,vt);
        if (!cte_rs) continue;
        /* build schema for CTE */
        Schema *cte_sc=arena_calloc(a,sizeof(Schema));
        cte_sc->ncols=cte_rs->ncols; cte_sc->cols=arena_alloc(a,(size_t)cte_rs->ncols*sizeof(ColDef));
        for(int c=0;c<cte_rs->ncols;c++){cte_sc->cols[c].name=cte_rs->col_names[c];cte_sc->cols[c].type=COL_TEXT;}
        vt_add(vt,cte->name,cte_rs,cte_sc);
    }

    /* Load tables */
    int ntables=sel->nfrom;
    if (ntables==0) {
        /* SELECT without FROM */
        int nc=sel->nselect;
        char **names=arena_alloc(a,(size_t)nc*sizeof(char*));
        for(int i=0;i<nc;i++) names[i]=arena_strdup(a,expr_name(sel->select_list[i],i,a));
        RS *rs=rs_new(a,nc,names,sel->norder);
        char **cells=arena_alloc(a,(size_t)nc*sizeof(char*));
        JoinCtx ctx={0};
        for(int i=0;i<nc;i++){
            Val v=eval_val(sel->select_list[i],&ctx,a);
            cells[i]=arena_strdup(a,val_cell(v,a));
        }
        rs_add(rs,a,cells,NULL);
        return rs;
    }

    IdxHint ih = extract_idx_hint(sel->where);
    TblData *tables=arena_calloc(a,(size_t)ntables*sizeof(TblData));
    bool stream_scan = false;   /* единственная каталожная таблица → потоковый скан */
    for (int i=0;i<ntables;i++) {
        const char *tname=sel->from[i].table;
        const char *alias=sel->from[i].alias;
        if (sel->from[i].subquery) {
            /* derived table: execute subquery */
            RS *sub_rs=exec_stmt(a,sel->from[i].subquery,vt);
            if (!sub_rs) { tables[i].schema=arena_calloc(a,sizeof(Schema)); continue; }
            Schema *sub_sc=arena_calloc(a,sizeof(Schema));
            sub_sc->ncols=sub_rs->ncols;
            sub_sc->cols=arena_alloc(a,(size_t)sub_rs->ncols*sizeof(ColDef));
            for(int c=0;c<sub_rs->ncols;c++){
                sub_sc->cols[c].name=sub_rs->col_names[c];
                sub_sc->cols[c].type=COL_TEXT;
            }
            tables[i].schema=sub_sc;
            tables[i].nrows=sub_rs->nrows;
            tables[i].rows=arena_alloc(a,(size_t)sub_rs->nrows*sizeof(char**));
            for(int r=0;r<sub_rs->nrows;r++) tables[i].rows[r]=sub_rs->rows[r].cells;
            tables[i].tname=alias?alias:"_sub";
            tables[i].alias=alias;
        } else if (tname) {
            /* Потоковый скан: единственная таблица запроса, из каталога (не
             * CTE/derived) и без применимого индексного хинта — строки НЕ
             * материализуются, читаются из WAL в stream_tbl_scan ниже. */
            if (ntables == 1) {
                RS *vrs = NULL; Schema *vsc = NULL;
                bool is_vt = (vt && vt_get(vt, tname, &vrs, &vsc) && vrs);
                if (!is_vt &&
                    catalog_get_schema(g_app.catalog, tname, &tables[i].schema, a) == 0 &&
                    tables[i].schema) {
                    bool idx_usable = false;
                    if (ih.valid && ih.col) {
                        for (int c2 = 0; c2 < tables[i].schema->ncols; c2++) {
                            if (tables[i].schema->cols[c2].name &&
                                strcasecmp(tables[i].schema->cols[c2].name, ih.col) == 0 &&
                                tables[i].schema->cols[c2].type == COL_INT64) {
                                pthread_mutex_lock(&g_app.tables_mu);
                                Table *tt2 = hm_get(&g_app.tables, tname);
                                idx_usable = tt2 && table_get_index(tt2, c2) != NULL;
                                pthread_mutex_unlock(&g_app.tables_mu);
                                break;
                            }
                        }
                    }
                    if (!idx_usable) {
                        tables[i].tname = tname; tables[i].alias = alias;
                        tables[i].rows = NULL; tables[i].nrows = 0;
                        stream_scan = true;
                        continue;
                    }
                    /* есть рабочий B-tree по хинту → точечный load_tbl ниже */
                }
            }
            if (load_tbl(a,tname,alias,vt,&tables[i],&ih)!=0) {
                /* Unresolvable source table: NOT in the virtual registry
                 * (CTE/derived) and NOT in the catalog → "no such table". This
                 * is a real error, not a phantom empty result. Flag it so the
                 * caller (h_query / run paths) reports status=1 instead of a
                 * silent 0-row success. We still fill an empty schema so the
                 * rest of this function can unwind without dereferencing NULL.
                 * (An existing table with 0 rows takes the success path in
                 * load_tbl and never reaches here, so empty tables stay OK.) */
                if (!tl_exec_error[0])
                    snprintf(tl_exec_error, sizeof(tl_exec_error), "no such table: %s", tname);
                tables[i].schema=arena_calloc(a,sizeof(Schema));
                tables[i].tname=tname; tables[i].alias=alias;
            }
        }
    }

    /* Detect aggregation need */
    bool do_agg=sel->ngroup>0 || sel->having;
    if (!do_agg) {
        for(int i=0;i<sel->nselect;i++) if(expr_has_agg(sel->select_list[i])){do_agg=true;break;}
    }
    bool implicit_agg=(do_agg && sel->ngroup==0 && !sel->having);

    /* Determine output column names */
    bool is_star=(sel->nselect==1 && sel->select_list[0]->type==EXPR_STAR);
    int ncols;
    char **col_names;
    if (is_star) {
        build_star_col_names(a, sel, tables, ntables, &col_names, &ncols);
    } else {
        ncols=sel->nselect;
        col_names=arena_alloc(a,(size_t)ncols*sizeof(char*));
        for(int i=0;i<ncols;i++) col_names[i]=arena_strdup(a,expr_name(sel->select_list[i],i,a));
    }

    RS *rs=rs_new(a,ncols,col_names,sel->norder);
    /* Typed output: tag each output column with its logical type so h_query
     * renders numbers/bools as JSON numbers/bools (not strings). Star columns
     * map to source columns in order; explicit columns are inferred. */
    if (is_star) {
        int oc=0;
        for (int t=0;t<ntables && oc<ncols;t++) {
            Schema *sc=tables[t].schema; if(!sc) continue;
            for (int c=0;c<sc->ncols && oc<ncols;c++) rs->col_types[oc++]=sc->cols[c].type;
        }
    } else {
        for (int i=0;i<ncols;i++) rs->col_types[i]=infer_expr_type(sel->select_list[i], tables, ntables);
    }

    ExecState st={0};
    st.a=a; st.sel=sel; st.vt=vt; st.rs=rs;
    st.do_agg=do_agg; st.implicit_agg=implicit_agg;
    /* Ранняя пагинация допустима, только когда пост-обработка не меняет
     * состав/порядок строк (нет агрегации, DISTINCT, ORDER BY, оконных
     * функций) — тогда LIMIT/OFFSET на эмиссии эквивалентен финальному
     * срезу, но работает и за потолком MAX_RS_ROWS. */
    {
        bool early_win = false;
        for (int i = 0; i < sel->nselect && !early_win; i++)
            early_win = expr_has_window(sel->select_list[i]);
        st.early_page = !do_agg && !sel->distinct && sel->norder == 0 && !early_win
                        && (sel->offset > 0 || sel->limit >= 0);
        st.skip_left = sel->offset > 0 ? sel->offset : 0;
        st.take_left = sel->limit >= 0 ? sel->limit : -1;
    }
    /* Aggregates used only in HAVING/ORDER BY (not in the SELECT list) need their
     * own accumulator slots — e.g. HAVING SUM(x)>N where x is not selected. */
    if (do_agg) {
        Expr **xl=NULL; int xn=0, xc=0;
        collect_aggs(sel->having, &xl, &xn, &xc, a);
        for (int o=0;o<sel->norder;o++) collect_aggs(sel->order_by[o].expr, &xl, &xn, &xc, a);
        Expr **keep=arena_alloc(a,(size_t)(xn>0?xn:1)*sizeof(Expr*)); int kn=0;
        for (int i=0;i<xn;i++) {
            bool in_sel=false;
            for (int s=0;s<sel->nselect && !in_sel;s++) {
                Expr *se=sel->select_list[s]; Expr *sb=(se->type==EXPR_ALIAS)?se->expr:se;
                if (expr_find_agg_args(sb, xl[i]->func_name, xl[i]->args, xl[i]->nargs)) in_sel=true;
            }
            if (!in_sel) keep[kn++]=xl[i];
        }
        st.xagg=keep; st.nxagg=kn;
    }
    st.cap_grps=64;
    st.grps=arena_alloc(a,(size_t)st.cap_grps*sizeof(GrpAcc));

    if (stream_scan) {
        st.stream_rows = true;
        stream_tbl_scan(&st, &tables[0]);
    } else {
        JoinCtx ctx={0};
        join_recurse(&st, 0, &ctx, tables, ntables, false);
    }

    if (do_agg) emit_groups(&st);

    /* ── DISTINCT ── */
    if (sel->distinct && rs->nrows > 0) {
        /* simple O(n²) dedup */
        bool *keep=arena_calloc(a,(size_t)rs->nrows*sizeof(bool));
        for(int i=0;i<rs->nrows;i++){
            keep[i]=true;
            for(int j=0;j<i;j++){
                if(!keep[j]) continue;
                bool eq=true;
                for(int c=0;c<rs->ncols&&eq;c++){
                    const char *ca=rs->rows[i].cells?rs->rows[i].cells[c]:"";
                    const char *cb=rs->rows[j].cells?rs->rows[j].cells[c]:"";
                    if(strcmp(ca,cb)) eq=false;
                }
                if(eq){keep[i]=false;break;}
            }
        }
        int new_n=0;
        for(int i=0;i<rs->nrows;i++) if(keep[i]) rs->rows[new_n++]=rs->rows[i];
        rs->nrows=new_n;
    }

    /* ── Window functions ── */
    {
        bool has_win = false;
        for (int i = 0; i < sel->nselect && !has_win; i++)
            has_win = expr_has_window(sel->select_list[i]);
        if (has_win) apply_windows(a, rs, sel);
    }

    /* ── ORDER BY ── */
    if (sel->norder > 0 && rs->nrows > 1) {
        /* insertion sort for stability on small sets, otherwise bubble */
        for(int i=1;i<rs->nrows;i++){
            OutRow tmp_row=rs->rows[i];
            int j=i-1;
            while(j>=0){
                int cmp=0;
                for(int o=0;o<sel->norder&&cmp==0;o++){
                    const char *ka=rs->rows[j].skeys?rs->rows[j].skeys[o]:"";
                    const char *kb=tmp_row.skeys?tmp_row.skeys[o]:"";
                    double na,nb;
                    bool an=pnum(ka,&na),bn=pnum(kb,&nb);
                    if(an&&bn) cmp=(na<nb)?-1:(na>nb?1:0);
                    else cmp=strcmp(ka,kb);
                    if(sel->order_by[o].desc) cmp=-cmp;
                }
                if(cmp<=0) break;
                rs->rows[j+1]=rs->rows[j]; j--;
            }
            rs->rows[j+1]=tmp_row;
        }
    }

    /* ── LIMIT / OFFSET ── */
    int64_t off=sel->offset>0?sel->offset:0;
    int64_t lim=sel->limit;
    if (st.early_page) { off = 0; lim = -1; }  /* уже применено при эмиссии */
    if (off > 0 || lim >= 0) {
        int start=(int)off; if(start>rs->nrows) start=rs->nrows;
        int end_idx=rs->nrows;
        if(lim>=0 && start+lim<end_idx) end_idx=(int)(start+lim);
        int new_n=0;
        for(int i=start;i<end_idx;i++) rs->rows[new_n++]=rs->rows[i];
        rs->nrows=new_n;
    }

    if (rs->truncated)
        LOG_WARN("SELECT: результат усечён потолком %d строк — данные НЕполные "
                 "(добавьте фильтр или страничный LIMIT/OFFSET)", MAX_RS_ROWS);
    return rs;
}

/* ── Execute a statement (handles SET_OP) ── */
/* Точка входа исполнителя: диспетчер по типу Stmt. SELECT → exec_select;
 * SET_OP (UNION [ALL]/INTERSECT/EXCEPT) — рекурсивно исполняет обе стороны и
 * комбинирует строки; DELETE/UPDATE — скан WAL с оценкой WHERE, запись
 * tombstone'ов/обновлений (в txn-буфер при активной транзакции g_txn_current,
 * иначе прямо в таблицу; сжатые батчи маскируются и переписываются CSV) и
 * возврат 1-столбцового RS с числом затронутых строк. INSERT здесь не
 * реализован. Возвращает RS или NULL при ошибке. */
static RS *exec_stmt(Arena *a, const Stmt *s, VTReg *vt) {
    if (!s) return NULL;
    if (s->type == STMT_SELECT) return exec_select(a, &s->select, vt);
    if (s->type == STMT_SET_OP) {
        RS *L=exec_stmt(a,s->set_left,vt);
        RS *R=exec_stmt(a,s->set_right,vt);
        if (!L) return R;
        if (!R) return L;
        int nc=L->ncols;
        RS *out=rs_new(a,nc,L->col_names,0);

        if (s->set_op==SET_UNION_ALL) {
            for(int i=0;i<L->nrows;i++) rs_add(out,a,L->rows[i].cells,NULL);
            for(int i=0;i<R->nrows;i++) rs_add(out,a,R->rows[i].cells,NULL);
        } else if (s->set_op==SET_UNION) {
            /* deduplicate combined L ∪ R result */
            for(int i=0;i<L->nrows;i++){
                bool dup=false;
                for(int j=0;j<out->nrows&&!dup;j++){
                    bool eq=true;
                    for(int c=0;c<nc&&eq;c++){
                        const char *ca=out->rows[j].cells?out->rows[j].cells[c]:"";
                        const char *cb=L->rows[i].cells?L->rows[i].cells[c]:"";
                        if(strcmp(ca,cb)) eq=false;
                    }
                    if(eq) dup=true;
                }
                if(!dup) rs_add(out,a,L->rows[i].cells,NULL);
            }
            for(int i=0;i<R->nrows;i++){
                bool dup=false;
                for(int j=0;j<out->nrows&&!dup;j++){
                    bool eq=true;
                    for(int c=0;c<nc&&eq;c++){
                        const char *ca=out->rows[j].cells?out->rows[j].cells[c]:"";
                        const char *cb=R->rows[i].cells?R->rows[i].cells[c]:"";
                        if(strcmp(ca,cb)) eq=false;
                    }
                    if(eq) dup=true;
                }
                if(!dup) rs_add(out,a,R->rows[i].cells,NULL);
            }
        } else if (s->set_op==SET_INTERSECT) {
            for(int i=0;i<L->nrows;i++){
                for(int j=0;j<R->nrows;j++){
                    bool eq=true;
                    for(int c=0;c<nc&&eq;c++){
                        const char *ca=L->rows[i].cells?L->rows[i].cells[c]:"";
                        const char *cb=R->rows[j].cells?R->rows[j].cells[c]:"";
                        if(strcmp(ca,cb)) eq=false;
                    }
                    if(eq){rs_add(out,a,L->rows[i].cells,NULL);break;}
                }
            }
        } else { /* EXCEPT */
            for(int i=0;i<L->nrows;i++){
                bool found=false;
                for(int j=0;j<R->nrows&&!found;j++){
                    bool eq=true;
                    for(int c=0;c<nc&&eq;c++){
                        const char *ca=L->rows[i].cells?L->rows[i].cells[c]:"";
                        const char *cb=R->rows[j].cells?R->rows[j].cells[c]:"";
                        if(strcmp(ca,cb)) eq=false;
                    }
                    if(eq) found=true;
                }
                if(!found) rs_add(out,a,L->rows[i].cells,NULL);
            }
        }
        /* Усечение любой из сторон делает и комбинированный результат неполным. */
        out->truncated = out->truncated || L->truncated || R->truncated;
        return out;
    }

    /* ── DML: DELETE / UPDATE ── */
    if (s->type == STMT_DELETE || s->type == STMT_UPDATE) {
        const char *tname = s->dml.table;
        if (!tname || !*tname) return NULL;

        Schema *sc = NULL;
        if (catalog_get_schema(g_app.catalog, tname, &sc, a) != 0 || !sc) return NULL;
        int ncols = sc->ncols;

        char wal_path[1024], dir_path[1024];
        snprintf(dir_path, sizeof(dir_path), "%s/%s", g_app.data_dir, tname);
        snprintf(wal_path, sizeof(wal_path), "%s/wal.bin", dir_path);

        /* Get the open Table handle for writing tombstones */
        pthread_mutex_lock(&g_app.tables_mu);
        Table *tbl = hm_get(&g_app.tables, tname);
        pthread_mutex_unlock(&g_app.tables_mu);
        if (!tbl) return NULL;

        /* Scan WAL for matching rows.
         * Important: any DML on a compressed batch needs to (a) tombstone
         * the batch and (b) re-append survivors as uncompressed CSV. We
         * defer those writes into a queue and flush AFTER the read scan
         * finishes — otherwise our own appends extend the file end and
         * the same fread() loop would then process them again, causing
         * double-counting and ghost rows. */
        typedef struct DmlOp { int64_t tombstone_off; char *append_csv; size_t append_len; struct DmlOp *next; } DmlOp;
        DmlOp *pending_head = NULL, *pending_tail = NULL;
        #define PENDING_TOMBSTONE(off) do { \
            DmlOp *_o = arena_calloc(a, sizeof(DmlOp)); _o->tombstone_off = (off); _o->append_csv = NULL; \
            if (pending_tail) pending_tail->next = _o; else pending_head = _o; pending_tail = _o; } while (0)
        #define PENDING_APPEND(buf, len) do { \
            DmlOp *_o = arena_calloc(a, sizeof(DmlOp)); _o->tombstone_off = -1; \
            _o->append_csv = arena_alloc(a, (size_t)(len) + 1); \
            memcpy(_o->append_csv, (buf), (size_t)(len)); _o->append_csv[(size_t)(len)] = '\0'; \
            _o->append_len = (size_t)(len); \
            if (pending_tail) pending_tail->next = _o; else pending_head = _o; pending_tail = _o; } while (0)

        FILE *rf = fopen(wal_path, "rb");
        if (!rf) return NULL;
        /* Snapshot the WAL size up-front; ignore any bytes appended after */
        fseek(rf, 0, SEEK_END);
        int64_t wal_end = ftell(rf);
        rewind(rf);

        /* Set of already-tombstoned offsets so we don't double-process
         * batches that a previous DML statement masked. */
        int dead_cap = 16, ndead = 0;
        int64_t *dead_offs = arena_alloc(a, (size_t)dead_cap * sizeof(int64_t));

        /* Pre-pass: collect all existing WAL_OP_DELETE tombstones so the
         * main scan can skip them. (UPDATE-tombstones are also DELETE in
         * intent — the row is masked.) */
        {
            char tbuf[262144];
            int64_t off = 0;
            while (off < wal_end) {
                uint32_t l = 0;
                if (fread(&l, 1, 4, rf) != 4) break;
                int64_t this_off = off;
                off += 4 + (int64_t)l;
                if (l == 0 || l >= sizeof(tbuf)) { fseek(rf, (long)l, SEEK_CUR); continue; }
                if (fread(tbuf, 1, l, rf) != l) break;
                uint8_t top = (uint8_t)tbuf[0];
                if ((top == WAL_OP_DELETE || top == WAL_OP_UPDATE) && l >= 9) {
                    int64_t ref = 0;
                    for (int b = 0; b < 8; b++) ref = (ref << 8) | (uint8_t)tbuf[1 + b];
                    if (ndead == dead_cap) {
                        dead_cap *= 2;
                        int64_t *nb = arena_alloc(a, (size_t)dead_cap * sizeof(int64_t));
                        memcpy(nb, dead_offs, (size_t)ndead * sizeof(int64_t));
                        dead_offs = nb;
                    }
                    dead_offs[ndead++] = ref;
                }
                (void)this_off;
            }
            rewind(rf);
        }

        int affected = 0;
        int64_t file_off = 0;
        char line[262144];

        while (file_off < wal_end) {
            uint32_t l = 0;
            if (fread(&l, 1, 4, rf) != 4) break;
            int64_t rec_off = file_off;
            file_off += 4 + (int64_t)l;
            if (l == 0 || l >= sizeof(line)) { fseek(rf, (long)l, SEEK_CUR); continue; }
            if (fread(line, 1, l, rf) != l) break;
            line[l] = '\0';

            /* Skip tombstone records */
            uint8_t op = (uint8_t)(unsigned char)line[0];
            if (op == WAL_OP_DELETE || op == WAL_OP_UPDATE) continue;

            /* Skip records that an earlier DML already tombstoned */
            {
                bool already_dead = false;
                for (int di = 0; di < ndead; di++) {
                    if (dead_offs[di] == rec_off) { already_dead = true; break; }
                }
                if (already_dead) continue;
            }

            /* Compressed batch (version byte 0x01) — release-build format.
             *
             * The DML walker can't address individual rows by file offset
             * inside a compressed batch (a single record packs many rows).
             * Pragmatic fix: decompress, evaluate WHERE per-row, then if
             * any matched: tombstone the whole batch + re-append the
             * survivors (and UPDATE-modified rows) as uncompressed CSV
             * records. Future SELECTs read the new uncompressed records
             * normally; the old batch is masked by its tombstone.
             *
             * Limitations: not txn-safe (txn_buffer can't yet track batch
             * tombstones + appended replacements atomically), so we only
             * apply this in auto-commit. A txn-active path falls through
             * to the legacy CSV walker below — which won't match anything
             * inside compressed batches but won't crash either. */
            if (op == 0x01 && l > 1 && g_txn_current == 0) {
                CompressedBatch *cb = compressed_batch_deserialize(line + 1, (size_t)(l - 1), a);
                if (!cb) continue;
                ColBatch *batch = decompress_batch(cb, a);
                if (!batch || batch->nrows == 0) continue;

                int rcount = batch->nrows;
                bool *matched = arena_calloc(a, (size_t)rcount * sizeof(bool));
                char ***row_cells = arena_alloc(a, (size_t)rcount * sizeof(char **));
                int batch_affected = 0;

                for (int r = 0; r < rcount; r++) {
                    char **cells = arena_alloc(a, (size_t)ncols * sizeof(char *));
                    for (int ci = 0; ci < ncols; ci++) {
                        if (ci >= batch->ncols) { cells[ci] = (char *)DFO_NULL_SENTINEL; continue; }
                        if (batch->null_bitmap[ci] &&
                            (batch->null_bitmap[ci][r/8] & (1u << (r%8)))) {
                            cells[ci] = (char *)DFO_NULL_SENTINEL;
                            continue;
                        }
                        ColType t = (batch->schema && ci < batch->schema->ncols)
                                       ? batch->schema->cols[ci].type : COL_TEXT;
                        switch (t) {
                            case COL_INT64:
                                cells[ci] = arena_sprintf(a, "%lld",
                                    (long long)((int64_t *)batch->values[ci])[r]); break;
                            case COL_DOUBLE:
                                cells[ci] = dbl_fmt(a,                                     ((double *)batch->values[ci])[r]); break;
                            case COL_BOOL:
                                cells[ci] = (char *)(((int64_t *)batch->values[ci])[r]
                                                      ? "true" : "false"); break;
                            default:
                                cells[ci] = ((char **)batch->values[ci])[r]
                                              ? ((char **)batch->values[ci])[r] : (char *)DFO_NULL_SENTINEL;
                                break;
                        }
                    }
                    row_cells[r] = cells;

                    bool pass = true;
                    if (s->dml.where) {
                        JoinCtx ctx = {0};
                        ctx.n = 1;
                        ctx.schemas[0] = sc;
                        ctx.rows[0] = cells;
                        ctx.tnames[0] = tname;
                        ctx.aliases[0] = tname;
                        Val cond = eval_val(s->dml.where, &ctx, a);
                        pass = cond.is_null ? false
                              : cond.is_bool ? cond.b
                              : cond.is_num ? (cond.num != 0.0)
                              : (cond.str && *cond.str);
                    }
                    if (pass) { matched[r] = true; batch_affected++; }
                }

                if (batch_affected > 0) {
                    /* Defer writes: tombstone the batch + queue replacement
                     * CSV records. Actual WAL writes happen after we close
                     * the read scan. */
                    PENDING_TOMBSTONE(rec_off);
                    for (int r = 0; r < rcount; r++) {
                        char **out_cells = row_cells[r];
                        if (matched[r]) {
                            if (s->type == STMT_DELETE) continue;
                            /* UPDATE: apply SET */
                            for (int si = 0; si < s->dml.nset; si++) {
                                for (int ci = 0; ci < ncols; ci++) {
                                    if (strcasecmp(sc->cols[ci].name, s->dml.set_cols[si]) == 0) {
                                        out_cells[ci] = (char *)s->dml.set_vals[si];
                                        break;
                                    }
                                }
                            }
                        }
                        char csv_buf[262144]; int co = 0;
                        for (int ci = 0; ci < ncols; ci++) {
                            if (ci) csv_buf[co++] = ',';
                            const char *v = out_cells[ci] ? out_cells[ci] : "";
                            int n = snprintf(csv_buf + co, sizeof(csv_buf) - (size_t)co - 2,
                                              "%s", v);
                            if (n > 0) co += n;
                        }
                        csv_buf[co++] = '\n';
                        PENDING_APPEND(csv_buf, co);
                    }
                    affected += batch_affected;
                }
                continue;
            }

            /* Strip trailing newline */
            size_t rl = strlen(line);
            while (rl > 0 && (line[rl-1]=='\n'||line[rl-1]=='\r')) line[--rl] = '\0';
            int printable = 0;
            for (size_t ci = 0; ci < rl; ci++) if ((unsigned char)line[ci] >= 0x20) printable++;
            if (printable < 2) continue;

            /* Parse CSV row */
            char *vals[MAX_COLS] = {0}; int nv = 0;
            char row_copy[262144];
            strncpy(row_copy, line, sizeof(row_copy)-1);
            row_copy[sizeof(row_copy)-1] = '\0';
            split_line_simple(row_copy, ',', vals, MAX_COLS, &nv);
            char **cells = arena_alloc(a, (size_t)ncols * sizeof(char*));
            for (int i = 0; i < ncols; i++) cells[i] = (i < nv && vals[i]) ? vals[i] : "";

            /* Evaluate WHERE */
            if (s->dml.where) {
                JoinCtx ctx = {0};
                ctx.n = 1;
                ctx.schemas[0] = sc;
                ctx.rows[0] = cells;
                ctx.tnames[0] = tname;
                ctx.aliases[0] = tname;
                Val cond = eval_val(s->dml.where, &ctx, a);
                bool pass = cond.is_null ? false : cond.is_bool ? cond.b : cond.is_num ? (cond.num != 0.0) : (cond.str && *cond.str);
                if (!pass) continue;
            }

            /* Row matches */
            if (s->type == STMT_DELETE) {
                if (g_txn_current != 0) {
                    txn_buffer_delete(g_app.txn_mgr, g_txn_current, tname, rec_off);
                } else {
                    pthread_mutex_lock(&g_app.tables_mu);
                    table_delete(tbl, rec_off);
                    pthread_mutex_unlock(&g_app.tables_mu);
                }
                affected++;
            } else {
                /* UPDATE: apply SET assignments to build new CSV row */
                char **new_cells = arena_alloc(a, (size_t)ncols * sizeof(char*));
                for (int i = 0; i < ncols; i++) new_cells[i] = cells[i];
                for (int si = 0; si < s->dml.nset; si++) {
                    for (int ci = 0; ci < ncols; ci++) {
                        if (strcasecmp(sc->cols[ci].name, s->dml.set_cols[si]) == 0) {
                            new_cells[ci] = (char *)s->dml.set_vals[si];
                            break;
                        }
                    }
                }
                /* Serialize new row to CSV */
                char new_csv[262144]; int off = 0;
                for (int ci = 0; ci < ncols; ci++) {
                    if (ci) new_csv[off++] = ',';
                    const char *v = new_cells[ci] ? new_cells[ci] : "";
                    int n = snprintf(new_csv+off, sizeof(new_csv)-off-2, "%s", v);
                    if (n > 0) off += n;
                }
                new_csv[off++] = '\n';
                if (g_txn_current != 0) {
                    txn_buffer_update(g_app.txn_mgr, g_txn_current, tname,
                                      rec_off, new_csv, (size_t)off);
                } else {
                    pthread_mutex_lock(&g_app.tables_mu);
                    table_update(tbl, rec_off, new_csv, (size_t)off);
                    pthread_mutex_unlock(&g_app.tables_mu);
                }
                affected++;
            }
        }
        fclose(rf);

        /* Flush all pending DML writes now that we've finished reading */
        if (pending_head) {
            pthread_mutex_lock(&g_app.tables_mu);
            for (DmlOp *op = pending_head; op; op = op->next) {
                if (op->tombstone_off >= 0) {
                    table_delete(tbl, op->tombstone_off);
                } else if (op->append_csv && op->append_len > 0) {
                    table_wal_append(tbl, op->append_csv, op->append_len);
                }
            }
            pthread_mutex_unlock(&g_app.tables_mu);
        }
        #undef PENDING_TOMBSTONE
        #undef PENDING_APPEND

        /* Return result */
        const char *col_key = (s->type == STMT_DELETE) ? "deleted" : "updated";
        char **names = arena_alloc(a, sizeof(char*));
        names[0] = arena_strdup(a, col_key);
        RS *rs = rs_new(a, 1, names, 0);
        char **cells = arena_alloc(a, sizeof(char*));
        cells[0] = arena_sprintf(a, "%d", affected);
        rs_add(rs, a, cells, NULL);
        return rs;
    }

    return NULL;
}

/* ── GET /health ── */
static void h_health(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a = arena_create(4096);
    char *metrics_json = metrics_to_json(g_app.metrics, a);
    JBuf jb; jb_init(&jb, a, 512);
    jb_obj_begin(&jb);
    jb_key(&jb,"status"); jb_str(&jb,"ok");
    jb_key(&jb,"version"); jb_str(&jb,"1.0.0");
    jb_key(&jb,"metrics"); jb_raw(&jb, metrics_json);
    jb_obj_end(&jb);
    const char *body = jb_done(&jb);
    resp->status = 200; resp->content_type = "application/json";
    resp->body = body; resp->body_len = strlen(body);
}

/* ── GET /api/tables ── */
static void h_tables_list(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a = arena_create(16384);
    char *json = NULL;
    catalog_list_tables_full(g_app.catalog, &json, a);
    http_resp_json(resp, 200, json ? json : "[]");
}

/* ── POST /api/tables/query ── */
/* Главный REST-эндпоинт SQL: тело {"sql":...,"limit"?}. Парсит запрос, проверяет
 * RBAC/RLS (для SELECT — check_select_access, для DML — check_table_access +
 * роль ≥ analyst), обрабатывает BEGIN/COMMIT/ROLLBACK через req->txn_id, режет
 * SELECT до лимита (≤10000) и исполняет через exec_stmt. Ответ — JSON
 * {columns, rows, elapsed_ms, row_count} с типизированным рендерингом ячеек
 * (числа/bool без кавычек, NULL → json null). Несуществующая таблица → 400. */
static void h_query(HttpReq *req, HttpResp *resp) {
    if (!req->body || req->body_len == 0) { http_resp_error(resp,400,"empty body"); return; }
    Arena *a = arena_create(1048576); /* 1 MiB arena */
    JVal *root = json_parse(a, req->body, req->body_len);
    const char *sql = json_str(json_get(root,"sql"),"");
    if (!*sql) { http_resp_error(resp,400,"missing sql"); arena_destroy(a); return; }

    /* optional limit override from request */
    int64_t req_limit = json_int(json_get(root,"limit"), -1);

    int64_t t0 = (int64_t)clock();
    Stmt *stmt = sql_parse(a, sql, strlen(sql));
    if (stmt->error) { http_resp_error(resp,400,stmt->error); arena_destroy(a); return; }

    /* RBAC: fine-grained table access control */
    if (g_app.rbac_enabled && g_app.rbac) {
        bool rbac_ok = true;
        switch (stmt->type) {
        case STMT_SELECT:
            rbac_ok = check_select_access(req, resp, &stmt->select);
            break;
        case STMT_INSERT:
            rbac_ok = check_table_access(req, resp, stmt->insert.table, ACTION_TABLE_WRITE);
            break;
        case STMT_DELETE:
        case STMT_UPDATE:
            rbac_ok = check_table_access(req, resp, stmt->dml.table, ACTION_TABLE_WRITE);
            break;
        case STMT_SET_OP:
        case STMT_BEGIN: case STMT_COMMIT: case STMT_ROLLBACK:
        case STMT_UNKNOWN:
            rbac_ok = true;
            break;
        }
        if (!rbac_ok) { arena_destroy(a); return; }
    }

    /* DML requires at least ROLE_ANALYST */
    if (stmt->type == STMT_DELETE || stmt->type == STMT_UPDATE) {
        if (g_app.auth_enabled && req->auth.role == ROLE_VIEWER) {
            http_resp_error(resp, 403, "forbidden: DML requires analyst or admin role");
            arena_destroy(a); return;
        }
    }

    /* ── Transaction control statements ── */
    if (stmt->type == STMT_BEGIN) {
        if (req->txn_id != 0) {
            http_resp_error(resp, 400, "transaction already active");
            arena_destroy(a); return;
        }
        TxnId id = txn_begin(g_app.txn_mgr);
        if (id == TXN_ID_NONE) {
            http_resp_error(resp, 503, "no free transaction slots");
            arena_destroy(a); return;
        }
        __atomic_fetch_add(&g_app.metrics->txn_begin_total, 1, __ATOMIC_RELAXED);
        __atomic_fetch_add(&g_app.metrics->txn_active, 1, __ATOMIC_RELAXED);
        http_resp_json(resp, 200,
            arena_sprintf(a, "{\"txn_id\":%llu}", (unsigned long long)id));
        arena_destroy(a); return;
    }

    if (stmt->type == STMT_COMMIT) {
        if (req->txn_id == 0) {
            http_resp_error(resp, 400, "no active transaction");
            arena_destroy(a); return;
        }
        int rc = txn_commit(g_app.txn_mgr, req->txn_id, apply_txn_entry, NULL);
        if (rc != 0) {
            http_resp_error(resp, 500, "commit failed");
            arena_destroy(a); return;
        }
        __atomic_fetch_add(&g_app.metrics->txn_commit_total, 1, __ATOMIC_RELAXED);
        __atomic_fetch_sub(&g_app.metrics->txn_active, 1, __ATOMIC_RELAXED);
        http_resp_json(resp, 200, "{\"ok\":true,\"committed\":true}");
        arena_destroy(a); return;
    }

    if (stmt->type == STMT_ROLLBACK) {
        if (req->txn_id != 0) {
            txn_rollback(g_app.txn_mgr, req->txn_id);
            __atomic_fetch_add(&g_app.metrics->txn_rollback_total, 1, __ATOMIC_RELAXED);
            __atomic_fetch_sub(&g_app.metrics->txn_active, 1, __ATOMIC_RELAXED);
        }
        http_resp_json(resp, 200, "{\"ok\":true,\"rolled_back\":true}");
        arena_destroy(a); return;
    }

    /* Apply request-level limit cap. Раньше лимит навязывался МОЛЧА — ответ на
     * SELECT по большой таблице был неотличим от таблицы ровно в 10000 строк.
     * Теперь просим cap+1 строку как маркер «есть ещё»: лишняя строка
     * отбрасывается ниже, а ответ получает "truncated":true. */
    int64_t rest_cap = -1;
    bool rest_clipped = false;
    if (stmt->type == STMT_SELECT) {
        rest_cap = (req_limit > 0 && req_limit < 10000) ? req_limit : 10000;
        if (stmt->select.limit < 0 || stmt->select.limit > rest_cap) {
            stmt->select.limit = rest_cap + 1;
            rest_clipped = true;
        }
    }

    g_txn_current = req->txn_id;
    tl_exec_error[0] = '\0';
    RS *rs = exec_stmt(a, stmt, NULL);
    g_txn_current = 0;

    /* An unresolvable source table is an error, not an empty result: return
     * HTTP 400 "no such table: X" instead of a phantom header-only success. */
    if (tl_exec_error[0]) {
        http_resp_error(resp, 400, tl_exec_error);
        tl_exec_error[0] = '\0';
        arena_destroy(a);
        return;
    }
    if (rest_clipped && rs && rs->nrows > (int)rest_cap) {
        rs->nrows = (int)rest_cap;   /* маркерная строка не отдаётся клиенту */
        rs->truncated = true;
    }

    JBuf jb; jb_init(&jb, a, 65536);
    jb_obj_begin(&jb);

    /* columns array */
    jb_key(&jb,"columns"); jb_arr_begin(&jb);
    if (rs) for(int i=0;i<rs->ncols;i++) jb_str(&jb, rs->col_names[i]?rs->col_names[i]:"");
    jb_arr_end(&jb);

    /* rows array */
    jb_key(&jb,"rows"); jb_arr_begin(&jb);
    if (rs) {
        for(int r=0;r<rs->nrows;r++){
            jb_arr_begin(&jb);
            for(int c=0;c<rs->ncols;c++){
                const char *v=rs->rows[r].cells?rs->rows[r].cells[c]:NULL;
                if(!v||(uintptr_t)v<0x1000||!strcmp(v,DFO_NULL_SENTINEL)){ jb_null(&jb); }
                else { ColType _ct=rs->col_types?rs->col_types[c]:COL_TEXT;
                       if((_ct==COL_INT64||_ct==COL_DOUBLE)&&is_json_number(v)) jb_raw(&jb,v);
                       else if(_ct==COL_BOOL&&(!strcmp(v,"true")||!strcmp(v,"false"))) jb_raw(&jb,v);
                       else jb_str(&jb,v); }
            }
            jb_arr_end(&jb);
        }
    }
    jb_arr_end(&jb);

    int64_t t1=(int64_t)clock();
    double ms=(double)(t1-t0)*1000.0/CLOCKS_PER_SEC;
    jb_key(&jb,"elapsed_ms"); jb_double(&jb,ms);
    jb_key(&jb,"row_count");  jb_int(&jb, rs?rs->nrows:0);
    /* Явный признак неполноты: результат обрезан потолком MAX_RS_ROWS. */
    if (rs && rs->truncated) { jb_key(&jb,"truncated"); jb_bool(&jb,true); }
    jb_obj_end(&jb);

    metrics_push(&g_app.metrics->query_latency_ms, ms);
    g_app.metrics->total_queries++;

    const char *body=jb_done(&jb);
    http_resp_json(resp,200,body);
    /* arena 'a' kept alive: body lives in it until response is sent */
}

/* Replace :name tokens in `sql` with escaped literal values from the `params`
 * JSON object. `::` (Postgres cast) is passed through untouched. String values
 * become single-quoted literals with internal quotes doubled; JSON numbers are
 * substituted bare; JSON null → the SQL NULL literal. Returns NULL on an
 * unknown :param (so the caller can 400). */
static char *named_params_expand(Arena *a, const char *sql, JVal *params) {
    size_t cap = strlen(sql) * 2 + 256;
    char *out = arena_alloc(a, cap);
    size_t off = 0;
    #define NP_RESERVE(n) do { \
        while (off + (n) + 1 >= cap) { \
            cap *= 2; char *nb = arena_alloc(a, cap); memcpy(nb, out, off); out = nb; } \
    } while (0)

    for (const char *p = sql; *p; ) {
        /* Not a placeholder, or a "::" cast → copy verbatim. */
        if (*p != ':' || *(p+1) == ':') {
            NP_RESERVE(2);
            out[off++] = *p++;
            if (*(p-1) == ':' && *p == ':') out[off++] = *p++;  /* copy the 2nd ':' too */
            continue;
        }
        /* ':' followed by an identifier → a named parameter. */
        p++;  /* skip ':' */
        char name[128]; size_t nlen = 0;
        while (*p && nlen < sizeof(name)-1 &&
               ((*p>='a'&&*p<='z')||(*p>='A'&&*p<='Z')||(*p>='0'&&*p<='9')||*p=='_'))
            name[nlen++] = *p++;
        name[nlen] = '\0';
        if (nlen == 0) { NP_RESERVE(1); out[off++] = ':'; continue; }  /* lone ':' */

        JVal *val = params ? json_get(params, name) : NULL;
        if (!val) { LOG_WARN("named_params: unknown param ':%s'", name); return NULL; }

        if (val->type == JV_NULL) {
            NP_RESERVE(4); memcpy(out+off, "NULL", 4); off += 4;
        } else if (val->type == JV_NUMBER) {
            char num[64]; double d = json_dbl(val, 0);
            if (d == (double)(long long)d) snprintf(num, sizeof(num), "%lld", (long long)d);
            else                           snprintf(num, sizeof(num), "%.15g", d);
            size_t nl = strlen(num); NP_RESERVE(nl); memcpy(out+off, num, nl); off += nl;
        } else if (val->type == JV_BOOL) {
            const char *b = val->b ? "true" : "false"; size_t bl = strlen(b);
            NP_RESERVE(bl); memcpy(out+off, b, bl); off += bl;
        } else {
            /* String literal: wrap in quotes, double internal single-quotes. */
            const char *sv = json_str(val, "");
            NP_RESERVE(2); out[off++] = '\'';
            for (const char *s = sv; *s; s++) {
                NP_RESERVE(2);
                if (*s == '\'') out[off++] = '\'';
                out[off++] = *s;
            }
            NP_RESERVE(1); out[off++] = '\'';
        }
    }
    out[off] = '\0';
    #undef NP_RESERVE
    return out;
}

/* ── POST /api/query/named ──
 * Body: {"sql": "SELECT ... WHERE col = :name", "params": {"name": "value", ...}}
 * Substitutes :name placeholders with escaped literal values, then executes
 * through the same sql_parse + exec_stmt path as /api/tables/query. SELECT-only.
 * Escaping is defensive (quotes doubled) but NOT a bind-parameter protocol —
 * RBAC (check_select_access) still applies, same as h_query. */
static void h_query_named(HttpReq *req, HttpResp *resp) {
    if (!req->body || req->body_len == 0) { http_resp_error(resp,400,"empty body"); return; }
    Arena *a = req->arena;
    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root) { http_resp_error(resp,400,"invalid JSON"); return; }

    const char *sql_tmpl = json_str(json_get(root,"sql"),"");
    if (!sql_tmpl[0]) { http_resp_error(resp,400,"sql is required"); return; }

    JVal *params = json_get(root,"params");
    char *sql = named_params_expand(a, sql_tmpl, params);
    if (!sql) { http_resp_error(resp,400,"unknown named parameter in sql"); return; }

    Stmt *stmt = sql_parse(a, sql, strlen(sql));
    if (!stmt || stmt->error) {
        http_resp_error(resp, 400, arena_sprintf(a, "sql error: %s", stmt && stmt->error ? stmt->error : "null"));
        return;
    }
    if (stmt->type != STMT_SELECT) { http_resp_error(resp, 400, "only SELECT is allowed"); return; }

    /* RBAC — identical to h_query's SELECT path. */
    if (g_app.rbac_enabled && g_app.rbac) {
        if (!check_select_access(req, resp, &stmt->select)) return;
    }

    /* Apply the same request-level limit cap as h_query. */
    int64_t cap = 10000;
    if (stmt->select.limit < 0 || stmt->select.limit > cap) stmt->select.limit = cap;

    g_txn_current = req->txn_id;
    tl_exec_error[0] = '\0';
    RS *rs = exec_stmt(a, stmt, NULL);
    g_txn_current = 0;

    if (tl_exec_error[0]) {
        http_resp_error(resp, 400, tl_exec_error);
        tl_exec_error[0] = '\0';
        arena_destroy(a);
        return;
    }

    JBuf jb; jb_init(&jb, a, 65536);
    jb_obj_begin(&jb);
    jb_key(&jb,"columns"); jb_arr_begin(&jb);
    if (rs) for(int i=0;i<rs->ncols;i++) jb_str(&jb, rs->col_names[i]?rs->col_names[i]:"");
    jb_arr_end(&jb);
    jb_key(&jb,"rows"); jb_arr_begin(&jb);
    if (rs) {
        for(int r=0;r<rs->nrows;r++){
            jb_arr_begin(&jb);
            for(int c=0;c<rs->ncols;c++){
                const char *v=rs->rows[r].cells?rs->rows[r].cells[c]:NULL;
                if(!v||(uintptr_t)v<0x1000||!strcmp(v,DFO_NULL_SENTINEL)){ jb_null(&jb); }
                else { ColType _ct=rs->col_types?rs->col_types[c]:COL_TEXT;
                       if((_ct==COL_INT64||_ct==COL_DOUBLE)&&is_json_number(v)) jb_raw(&jb,v);
                       else if(_ct==COL_BOOL&&(!strcmp(v,"true")||!strcmp(v,"false"))) jb_raw(&jb,v);
                       else jb_str(&jb,v); }
            }
            jb_arr_end(&jb);
        }
    }
    jb_arr_end(&jb);
    jb_key(&jb,"row_count"); jb_int(&jb, rs?rs->nrows:0);
    /* Явный признак неполноты: результат обрезан потолком MAX_RS_ROWS. */
    if (rs && rs->truncated) { jb_key(&jb,"truncated"); jb_bool(&jb,true); }
    jb_obj_end(&jb);
    http_resp_json(resp, 200, jb_done(&jb));
}

/* ── Step 3 Week 2/3: bridge SQL execution to the PostgreSQL wire-protocol ──
 *
 * Reuses sql_parse + exec_stmt — the same path the JSON /api/tables/query
 * endpoint takes. Differences from h_query:
 *   • emits results as wire-protocol frames via pgwire helpers (text format)
 *   • RBAC is bypassed for pgwire connections — auth happens once at
 *     connect (Week 4 will tighten this)
 *   • BEGIN/COMMIT/ROLLBACK accepted as no-ops — per-connection txn
 *     state via the txn manager is a future iteration
 *   • Week 3: column OIDs inferred per-column from the first non-null
 *     cell; pg_catalog + information_schema probes are intercepted
 *     before sql_parse so DBeaver / DataGrip / psql can list tables    */

/* Heuristic: peek at the first non-null cell in this column and pick a
 * Postgres OID matching its lexical form. Defaults to text. */
static int32_t pg_infer_col_oid(const RS *rs, int col_idx) {
    if (!rs || col_idx < 0 || col_idx >= rs->ncols) return PG_OID_TEXT;
    for (int r = 0; r < rs->nrows; r++) {
        if (!rs->rows[r].cells) continue;
        const char *s = rs->rows[r].cells[col_idx];
        if (!s || !s[0]) continue;
        if (!strcasecmp(s, "true") || !strcasecmp(s, "false")) return PG_OID_BOOL;
        const char *p = s;
        if (*p == '-' || *p == '+') p++;
        bool has_digit = false;
        while (*p >= '0' && *p <= '9') { p++; has_digit = true; }
        if (has_digit && *p == '\0') return PG_OID_INT8;
        if (has_digit && *p == '.') {
            p++;
            while (*p >= '0' && *p <= '9') p++;
            if (*p == 'e' || *p == 'E') {
                p++; if (*p == '-' || *p == '+') p++;
                while (*p >= '0' && *p <= '9') p++;
            }
            if (*p == '\0') return PG_OID_FLOAT8;
        }
        return PG_OID_TEXT;
    }
    return PG_OID_TEXT;
}

/* ── pg_catalog / information_schema emulation ───────────────────────
 * Many BI tools probe these on connect. We intercept the most common
 * shapes BEFORE sql_parse and synthesize result sets from g_app.tables.
 * Returns 1 if handled (response already sent), 0 to fall through to
 * the real engine. Matching is permissive — we just look for the
 * catalog table name in a case-folded copy of the SQL.                 */

/* Нормализует SQL для сопоставления с каталогом: пропускает ведущие пробелы,
 * приводит к нижнему регистру и срезает хвостовые ';' и пробелы. */
static void pg_normalize(const char *src, char *out, size_t cap) {
    while (*src == ' ' || *src == '\t' || *src == '\n') src++;
    size_t n = 0;
    while (*src && n + 1 < cap) {
        char c = *src++;
        out[n++] = (c >= 'A' && c <= 'Z') ? (char)(c - 'A' + 'a') : c;
    }
    out[n] = '\0';
    while (n > 0 && (out[n-1] == ';' || out[n-1] == ' ' ||
                     out[n-1] == '\t' || out[n-1] == '\n')) out[--n] = '\0';
}

/* Эмуляция information_schema.tables: по строке на каждую таблицу из каталога. */
static void pg_handle_info_tables(PgConn *conn) {
    PgColumn cols[] = {
        {"table_catalog", PG_OID_TEXT, -1},
        {"table_schema",  PG_OID_TEXT, -1},
        {"table_name",    PG_OID_TEXT, -1},
        {"table_type",    PG_OID_TEXT, -1},
    };
    pgwire_send_row_description(conn, 4, cols);
    pthread_mutex_lock(&g_app.tables_mu);
    int idx = 0; const char *k; void *v; int n = 0;
    while ((idx = hm_next(&g_app.tables, idx, &k, &v)) >= 0) {
        const char *vals[4] = { pgwire_database(conn), "public", k, "BASE TABLE" };
        pgwire_send_data_row(conn, 4, vals);
        n++;
    }
    pthread_mutex_unlock(&g_app.tables_mu);
    char tag[32]; snprintf(tag, sizeof(tag), "SELECT %d", n);
    pgwire_send_command_complete(conn, tag);
}

/* Эмуляция information_schema.columns: разворачивает схему каждой таблицы в
 * строки со столбцами, маппя внутренний COL_* в имена pg-типов. */
static void pg_handle_info_columns(PgConn *conn) {
    PgColumn cols[] = {
        {"table_catalog",      PG_OID_TEXT, -1},
        {"table_schema",       PG_OID_TEXT, -1},
        {"table_name",         PG_OID_TEXT, -1},
        {"column_name",        PG_OID_TEXT, -1},
        {"ordinal_position",   PG_OID_INT8,  8},
        {"data_type",          PG_OID_TEXT, -1},
        {"is_nullable",        PG_OID_TEXT, -1},
    };
    pgwire_send_row_description(conn, 7, cols);
    pthread_mutex_lock(&g_app.tables_mu);
    int idx = 0; const char *k; void *v; int n = 0;
    while ((idx = hm_next(&g_app.tables, idx, &k, &v)) >= 0) {
        Table *t = (Table *)v;
        Schema *s = t ? table_schema(t) : NULL;
        if (!s) continue;
        for (int c = 0; c < s->ncols; c++) {
            const char *t_name = "text";
            switch (s->cols[c].type) {
                case COL_INT64:  t_name = "bigint";           break;
                case COL_DOUBLE: t_name = "double precision"; break;
                case COL_BOOL:   t_name = "boolean";          break;
                default:         t_name = "text";             break;
            }
            char ord[16]; snprintf(ord, sizeof(ord), "%d", c + 1);
            const char *vals[7] = {
                pgwire_database(conn), "public", k,
                s->cols[c].name, ord, t_name,
                s->cols[c].nullable ? "YES" : "NO",
            };
            pgwire_send_data_row(conn, 7, vals);
            n++;
        }
    }
    pthread_mutex_unlock(&g_app.tables_mu);
    char tag[32]; snprintf(tag, sizeof(tag), "SELECT %d", n);
    pgwire_send_command_complete(conn, tag);
}

/* Эмуляция pg_namespace: всегда отдаёт фиксированные схемы pg_catalog и public. */
static void pg_handle_pg_namespace(PgConn *conn) {
    PgColumn cols[] = {
        {"oid",      PG_OID_INT8, 8},
        {"nspname",  PG_OID_TEXT, -1},
        {"nspowner", PG_OID_INT8, 8},
    };
    pgwire_send_row_description(conn, 3, cols);
    const char *r1[] = { "11",   "pg_catalog", "10" };
    const char *r2[] = { "2200", "public",     "10" };
    pgwire_send_data_row(conn, 3, r1);
    pgwire_send_data_row(conn, 3, r2);
    pgwire_send_command_complete(conn, "SELECT 2");
}

/* Эмуляция pg_class: по строке (relkind 'r') на таблицу с синтетическими OID,
 * начинающимися с 16384 (диапазон пользовательских объектов в Postgres). */
static void pg_handle_pg_class(PgConn *conn) {
    PgColumn cols[] = {
        {"oid",          PG_OID_INT8, 8},
        {"relname",      PG_OID_TEXT, -1},
        {"relnamespace", PG_OID_INT8, 8},
        {"relkind",      PG_OID_TEXT, -1},
    };
    pgwire_send_row_description(conn, 4, cols);
    pthread_mutex_lock(&g_app.tables_mu);
    int idx = 0; const char *k; void *v; int n = 0;
    int64_t fake_oid = 16384;
    while ((idx = hm_next(&g_app.tables, idx, &k, &v)) >= 0) {
        char oid_buf[32]; snprintf(oid_buf, sizeof(oid_buf), "%lld", (long long)fake_oid++);
        const char *vals[4] = { oid_buf, k, "2200", "r" };
        pgwire_send_data_row(conn, 4, vals);
        n++;
    }
    pthread_mutex_unlock(&g_app.tables_mu);
    char tag[32]; snprintf(tag, sizeof(tag), "SELECT %d", n);
    pgwire_send_command_complete(conn, tag);
}

static int pg_emulate_catalog(PgConn *conn, const char *sql) {
    char norm[512]; pg_normalize(sql, norm, sizeof(norm));
    if (strstr(norm, "from information_schema.tables"))  { pg_handle_info_tables(conn);  return 1; }
    if (strstr(norm, "from information_schema.columns")) { pg_handle_info_columns(conn); return 1; }
    if (strstr(norm, "from pg_catalog.pg_namespace") ||
        strstr(norm, "from pg_namespace"))               { pg_handle_pg_namespace(conn); return 1; }
    if (strstr(norm, "from pg_catalog.pg_class") ||
        strstr(norm, "from pg_class"))                   { pg_handle_pg_class(conn);     return 1; }
    return 0;
}

/* Мост pgwire → SQL-движок (см. объявление в app.h). Сначала перехватывает
 * пробы pg_catalog/information_schema (pg_emulate_catalog), затем парсит и
 * исполняет через exec_stmt, транслируя RS в протокол: RowDescription (OID
 * столбцов выводится из первой не-NULL ячейки), поток DataRow (SQL-NULL по
 * контракту → wire-NULL) и CommandComplete-тег (SELECT/INSERT/UPDATE/DELETE n).
 * BEGIN/COMMIT/ROLLBACK — no-op с корректным тегом; ошибки → ErrorResponse. */
void api_pg_execute(PgConn *conn, const char *sql) {
    /* Catalog probes go first so we don't hand them to a parser that
     * has no knowledge of pg_class etc. */
    if (pg_emulate_catalog(conn, sql)) return;

    Arena *a = arena_create(1024 * 1024);
    Stmt *stmt = sql_parse(a, sql, strlen(sql));
    if (!stmt) {
        pgwire_send_error(conn, "42601", "internal: sql_parse returned NULL");
        arena_destroy(a); return;
    }
    if (stmt->error) {
        pgwire_send_error(conn, "42601", stmt->error);
        arena_destroy(a); return;
    }

    /* Transaction control: emit the proper tag, no-op the underlying engine.
     * Real txn integration over pgwire requires per-connection TxnId state
     * and is tracked for a future iteration. */
    if (stmt->type == STMT_BEGIN) {
        pgwire_send_command_complete(conn, "BEGIN");    arena_destroy(a); return;
    }
    if (stmt->type == STMT_COMMIT) {
        pgwire_send_command_complete(conn, "COMMIT");   arena_destroy(a); return;
    }
    if (stmt->type == STMT_ROLLBACK) {
        pgwire_send_command_complete(conn, "ROLLBACK"); arena_destroy(a); return;
    }

    /* Apply default LIMIT to keep results bounded — same cap as JSON path */
    if (stmt->type == STMT_SELECT &&
        (stmt->select.limit < 0 || stmt->select.limit > 10000)) {
        stmt->select.limit = 10000;
    }

    g_txn_current = 0;
    int64_t t0 = (int64_t)clock();
    tl_exec_error[0] = '\0';
    RS *rs = exec_stmt(a, stmt, NULL);
    g_txn_current = 0;
    if (tl_exec_error[0]) {
        /* No such source table → wire-protocol error (SQLSTATE 42P01 undefined
         * table), not an empty success. */
        pgwire_send_error(conn, "42P01", tl_exec_error);
        tl_exec_error[0] = '\0';
        arena_destroy(a); return;
    }
    if (!rs) {
        pgwire_send_error(conn, "XX000", "query execution failed");
        arena_destroy(a); return;
    }
    if (rs->truncated)
        LOG_WARN("pgwire: результат SELECT усечён потолком %d строк — клиент "
                 "получит неполные данные (используйте LIMIT/OFFSET-страницы)",
                 MAX_RS_ROWS);

    /* RowDescription — at least one column for SELECT; for DML, exec_stmt
     * returns a 1-col RS containing the affected count. Week 3: column
     * type is inferred per-column from the first non-null cell. */
    if (rs->ncols > 0) {
        PgColumn *cols = arena_alloc(a, (size_t)rs->ncols * sizeof(PgColumn));
        for (int i = 0; i < rs->ncols; i++) {
            cols[i].name      = rs->col_names[i] ? rs->col_names[i] : "";
            cols[i].type_oid  = pg_infer_col_oid(rs, i);
            cols[i].type_size = (cols[i].type_oid == PG_OID_INT8)   ? 8
                              : (cols[i].type_oid == PG_OID_FLOAT8) ? 8
                              : (cols[i].type_oid == PG_OID_BOOL)   ? 1
                              :                                       -1;
        }
        pgwire_send_row_description(conn, rs->ncols, cols);
    }

    /* DataRows */
    for (int r = 0; r < rs->nrows; r++) {
        const char **vals = arena_alloc(a, (size_t)rs->ncols * sizeof(char *));
        for (int c = 0; c < rs->ncols; c++) {
            const char *v = rs->rows[r].cells ? rs->rows[r].cells[c] : NULL;
            /* NULL-contract parity with the JSON path (h_query): a SQL NULL is
             * carried as the DFO_NULL_SENTINEL string (or a corrupt sub-page
             * pointer), never a NULL pointer. Collapse both to a real NULL so
             * pgwire_send_data_row emits a wire NULL (-1) instead of the raw
             * sentinel bytes; an empty string "" stays a real empty string.
             * The sub-page check is first so strcmp never derefs a bad pointer. */
            if (v && ((uintptr_t)v < 0x1000 || !strcmp(v, DFO_NULL_SENTINEL))) v = NULL;
            vals[c] = v;
        }
        pgwire_send_data_row(conn, rs->ncols, vals);
    }

    /* CommandComplete tag.
     * exec_stmt returns DML results as a single-row RS where col_names[0] is
     * "inserted" / "updated" / "deleted" and cells[0][0] is the count. */
    char tag[64];
    int affected = rs->nrows;
    if (rs->ncols == 1 && rs->nrows == 1 && rs->col_names[0] &&
        rs->rows[0].cells && rs->rows[0].cells[0]) {
        const char *cn = rs->col_names[0];
        if (!strcmp(cn, "inserted") || !strcmp(cn, "updated") || !strcmp(cn, "deleted"))
            affected = atoi(rs->rows[0].cells[0]);
    }
    switch (stmt->type) {
        case STMT_INSERT: snprintf(tag, sizeof(tag), "INSERT 0 %d", affected); break;
        case STMT_UPDATE: snprintf(tag, sizeof(tag), "UPDATE %d",   affected); break;
        case STMT_DELETE: snprintf(tag, sizeof(tag), "DELETE %d",   affected); break;
        default:          snprintf(tag, sizeof(tag), "SELECT %d",   rs->nrows); break;
    }
    pgwire_send_command_complete(conn, tag);

    /* Metrics: count this exactly like a JSON query */
    double ms = (double)((int64_t)clock() - t0) * 1000.0 / CLOCKS_PER_SEC;
    metrics_push(&g_app.metrics->query_latency_ms, ms);
    g_app.metrics->total_queries++;

    arena_destroy(a);
}

/* ── GET /api/tables/:name/schema ── */
static void h_table_schema(HttpReq *req, HttpResp *resp) {
    const char *name=hm_get(&req->params,"name");
    if(!name){http_resp_error(resp,400,"missing name");return;}
    Arena *a=arena_create(4096);
    Schema *schema=NULL;
    if(catalog_get_schema(g_app.catalog,name,&schema,a)<0){
        http_resp_error(resp,404,"table not found");arena_destroy(a);return;
    }
    JBuf jb; jb_init(&jb,a,1024);
    jb_obj_begin(&jb);
    jb_key(&jb,"table"); jb_str(&jb,name);
    jb_key(&jb,"columns"); jb_arr_begin(&jb);
    for(int i=0;i<schema->ncols;i++){
        jb_obj_begin(&jb);
        jb_key(&jb,"name"); jb_str(&jb,schema->cols[i].name);
        const char *tp="text";
        if(schema->cols[i].type==COL_INT64)  tp="int64";
        if(schema->cols[i].type==COL_DOUBLE) tp="double";
        if(schema->cols[i].type==COL_BOOL)   tp="bool";
        jb_key(&jb,"type"); jb_str(&jb,tp);
        jb_key(&jb,"nullable"); jb_bool(&jb,schema->cols[i].nullable);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);
    jb_obj_end(&jb);
    http_resp_json(resp,200,jb_done(&jb));
    arena_destroy(a);   /* http_resp_json копирует тело, арена больше не нужна */
}

/* ── GET /api/tables/:name/compression ── */
static void h_table_compression(HttpReq *req, HttpResp *resp) {
    const char *name = hm_get(&req->params, "name");
    if (!name) { http_resp_error(resp, 400, "missing name"); return; }

    Arena *a = arena_create(32768);
    Schema *schema = NULL;
    if (catalog_get_schema(g_app.catalog, name, &schema, a) < 0) {
        http_resp_error(resp, 404, "table not found"); arena_destroy(a); return;
    }

    char wal_path[1024];
    snprintf(wal_path, sizeof(wal_path), "%s/%s/wal.bin", g_app.data_dir, name);
    FILE *wf = fopen(wal_path, "rb");

    /* Scan WAL to find compression stats */
    size_t total_raw = 0, total_compressed = 0;
    int64_t batch_count = 0;
    /* Per-column encoding stats (last batch seen) */
    Encoding col_encs[MAX_COLS] = {0};
    float    col_ratios[MAX_COLS] = {0};
    int      ncols = schema ? schema->ncols : 0;
    for (int i = 0; i < ncols; i++) { col_encs[i]=ENC_PLAIN; col_ratios[i]=1.0f; }

    if (wf) {
        char *buf = malloc(16777216); /* 16 MiB */
        while (buf) {
            uint32_t l = 0;
            if (fread(&l, 4, 1, wf) != 1) break;
            if (l == 0 || l > 16777216) { fseek(wf, (long)l, SEEK_CUR); continue; }
            if (fread(buf, 1, l, wf) != l) break;
            uint8_t op = (uint8_t)buf[0];
            if (op == 0x01 && l > 1) {
                CompressedBatch *cb = compressed_batch_deserialize(buf+1, (size_t)(l-1), a);
                if (cb) {
                    batch_count++;
                    total_compressed += (size_t)l;
                    /* Estimate original size as nrows * avg_bytes_per_row */
                    for (int c = 0; c < cb->ncols && c < ncols; c++) {
                        col_encs[c] = cb->cols[c].enc;
                        /* Rough original size per column */
                        size_t col_orig = (size_t)cb->nrows * 8; /* assume 8B per value */
                        size_t col_comp = 0;
                        switch (cb->cols[c].enc) {
                            case ENC_RLE:   col_comp=(size_t)cb->cols[c].nruns*10; break;
                            case ENC_DICT:  col_comp=(size_t)cb->cols[c].dict_size*8+(size_t)cb->nrows*(cb->cols[c].dict_is_u8?1:2); break;
                            case ENC_DELTA: col_comp=8+(size_t)cb->nrows*4; break;
                            default: col_comp=col_orig; break;
                        }
                        col_ratios[c] = col_orig > 0 ? (float)col_orig / (float)(col_comp+1) : 1.0f;
                        total_raw += col_orig;
                    }
                }
            } else if (op >= 0x20) {
                /* Legacy CSV row */
                total_raw += l;
                total_compressed += l;
            }
        }
        free(buf);
        fclose(wf);
    }

    JBuf jb; jb_init(&jb, a, 2048);
    jb_obj_begin(&jb);
    jb_key(&jb, "table"); jb_str(&jb, name);

    static const char *enc_names[] = {"plain","rle","dict","delta"};

    jb_key(&jb, "encoding_by_column"); jb_arr_begin(&jb);
    for (int c = 0; c < ncols; c++) {
        jb_obj_begin(&jb);
        jb_key(&jb, "column"); jb_str(&jb, schema->cols[c].name ? schema->cols[c].name : "");
        jb_key(&jb, "encoding"); jb_str(&jb, enc_names[col_encs[c] < 4 ? col_encs[c] : 0]);
        jb_key(&jb, "ratio"); jb_double(&jb, col_ratios[c]);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);

    float overall = total_compressed > 0 ? (float)total_raw / (float)total_compressed : 1.0f;
    jb_key(&jb, "overall_ratio");      jb_double(&jb, overall);
    jb_key(&jb, "original_bytes");     jb_int(&jb, (int64_t)total_raw);
    jb_key(&jb, "compressed_bytes");   jb_int(&jb, (int64_t)total_compressed);
    jb_key(&jb, "batch_count");        jb_int(&jb, batch_count);
    jb_obj_end(&jb);

    http_resp_json(resp, 200, jb_done(&jb));
    /* arena 'a' is intentionally not destroyed here — resp->body points into it */
}

/* ── DELETE /api/tables/:name ── */
static void h_table_delete(HttpReq *req, HttpResp *resp) {
    const char *name = hm_get(&req->params, "name");
    if (!name || !*name) { http_resp_error(resp, 400, "missing name"); return; }
    if (!valid_table_name(name)) { http_resp_error(resp, 400, "invalid table name"); return; }

    pthread_mutex_lock(&g_app.tables_mu);
    Table *t = hm_get(&g_app.tables, name);
    if (t) { table_close(t); hm_del(&g_app.tables, name); }
    pthread_mutex_unlock(&g_app.tables_mu);

    catalog_drop_table(g_app.catalog, name);
    char wal_path[1024], dir_path[1024];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", g_app.data_dir, name);
    snprintf(wal_path, sizeof(wal_path), "%s/wal.bin", dir_path);
    unlink(wal_path); rmdir(dir_path);

    if (t) __atomic_fetch_sub(&g_app.metrics->tables_count, 1, __ATOMIC_RELAXED);

    Arena *ba = arena_create(256);
    char *msg = arena_sprintf(ba, "{\"event\":\"table_deleted\",\"table\":\"%s\"}", name);
    app_ws_broadcast(&g_app, msg);
    arena_destroy(ba);
    http_resp_json(resp, 200, "{\"status\":\"deleted\"}");
}

/* forward declaration — определение ниже */
static Table *recreate_table(App *app, Arena *a, const char *tname, Schema *schema);

/* ── POST /api/ingest/csv ── */
static void h_ingest_csv(HttpReq *req, HttpResp *resp) {
    const char *table_name=hm_get(&req->params,"table");
    if(!table_name) table_name=hm_get(&req->params,"name");
    const char *qs=req->query;
    char tname[128]="uploaded";
    if(qs){const char*p=strstr(qs,"table=");if(p)sscanf(p+6,"%127[^&]",tname);table_name=tname;}

    if (!valid_table_name(tname)) { http_resp_error(resp,400,"invalid table name"); return; }

    /* RBAC: CREATE if table is new, else WRITE */
    if (g_app.rbac_enabled && g_app.rbac) {
        pthread_mutex_lock(&g_app.tables_mu);
        bool exists = hm_get(&g_app.tables, tname) != NULL;
        pthread_mutex_unlock(&g_app.tables_mu);
        RbacAction act = exists ? ACTION_TABLE_WRITE : ACTION_TABLE_CREATE;
        if (!check_table_access(req, resp, tname, act)) return;
    }

    if(!req->body||req->body_len==0){http_resp_error(resp,400,"empty body");return;}

    char tmp_path[256]; snprintf(tmp_path,sizeof(tmp_path),"/tmp/dfo_upload_%lld.csv",(long long)time(NULL));
    FILE *f=fopen(tmp_path,"w"); if(!f){http_resp_error(resp,500,"can't write tmp");return;}
    fwrite(req->body,1,req->body_len,f); fclose(f);

    Arena *a=arena_create(131072);
    const char *body=req->body;
    const char *nl=memchr(body,'\n',req->body_len);
    if(!nl){http_resp_error(resp,400,"no header");arena_destroy(a);return;}

    char *header=arena_strndup(a,body,(size_t)(nl-body));
    char delim = detect_delim(body, (size_t)(nl-body));
    /* Parse the header with the strict RFC-4180 splitter so the column count is
     * authoritative even when header fields are quoted (e.g. "a,b"). */
    int ncols=0; int herr=0;
    char **hcols = split_line_rfc4180(a, header, delim, &ncols, &herr);
    if (herr || !hcols || ncols < 1) { arena_destroy(a); http_resp_error(resp, 400, "malformed CSV header"); return; }
    if (ncols > MAX_COLS) { arena_destroy(a); http_resp_error(resp, 400, "too many columns"); return; }

    Schema *schema=arena_calloc(a,sizeof(Schema));
    schema->ncols=ncols; schema->cols=arena_alloc(a,ncols*sizeof(ColDef));
    for(int i=0;i<ncols;i++){
        char *tok=hcols[i];
        size_t tl=strlen(tok); while(tl>0&&(tok[tl-1]=='\r'||tok[tl-1]==' ')) tok[--tl]='\0';
        schema->cols[i].name=arena_strdup(a,tok);
        schema->cols[i].type=COL_TEXT; schema->cols[i].nullable=true;
    }

    /* ── Atomicity pre-pass (banking sink): validate EVERY data row (quoting +
     * field count) BEFORE any side effect — no table creation, no table_append.
     * A single bad row ⇒ HTTP 400 and ZERO rows ingested. Without this, the old
     * code committed full BATCH_SIZE batches as it went and then 400'd on a late
     * bad row, leaving a durable partial prefix. Validation runs in a throwaway
     * arena so the main arena isn't bloated by the double pass. */
    {
        Arena *va = arena_create(65536);
        char *vrow = arena_alloc(va, 65536);
        const char *vp = nl + 1;
        while (vp < body + req->body_len) {
            const char *vne = memchr(vp, '\n', (size_t)(body + req->body_len - vp));
            if (!vne) vne = body + req->body_len;
            size_t vrlen = (size_t)(vne - vp);
            if (vrlen == 0 || (vrlen == 1 && vp[0] == '\r')) { vp = vne + 1; continue; }
            /* Reject an over-long row LOUDLY instead of silently truncating it to
             * 65535 bytes (T19): truncation reported success while irreversibly
             * dropping data. Rejecting here, in the pre-pass, keeps the ingest
             * atomic (0 rows committed). */
            if (vrlen >= 65536) {
                arena_destroy(va); arena_destroy(a);
                http_resp_error(resp, 400, "CSV row exceeds the 65535-byte maximum row size");
                return;
            }
            memcpy(vrow, vp, vrlen);
            if (vrlen > 0 && vrow[vrlen-1] == '\r') vrlen--;
            vrow[vrlen] = '\0';
            int vnf = 0, vperr = 0;
            char **vf = split_line_rfc4180(va, vrow, delim, &vnf, &vperr);
            if (vperr || !vf) {
                arena_destroy(va); arena_destroy(a);
                http_resp_error(resp, 400, "malformed CSV row (bad quoting)");
                return;
            }
            if (vnf > ncols) {
                arena_destroy(va); arena_destroy(a);
                http_resp_error(resp, 400, "CSV row has more fields than header");
                return;
            }
            vp = vne + 1;
        }
        arena_destroy(va);
    }
    /* Input is fully valid past this point — appends below cannot 400 on row
     * shape, so the ingest is atomic (all rows or none). */

    pthread_mutex_lock(&g_app.tables_mu);
    Table *t=hm_get(&g_app.tables,tname);
    if(!t){
        t=table_create(tname,schema,g_app.data_dir);
        if (t && g_app.cluster_mode && g_app.replicator)
            table_set_wal_callback(t, replicator_wal_cb, g_app.replicator);
        hm_set(&g_app.tables,tname,t);
        catalog_register_table(g_app.catalog,tname,schema);
        __atomic_fetch_add(&g_app.metrics->tables_count, 1, __ATOMIC_RELAXED);
    }
    pthread_mutex_unlock(&g_app.tables_mu);

    int row_count = 0;
    const char *p = nl + 1;
    char ***col_vals = arena_alloc(a, (size_t)ncols * sizeof(char **));
    for (int c = 0; c < ncols; c++) col_vals[c] = arena_alloc(a, BATCH_SIZE * sizeof(char *));
    ColBatch batch = {0}; batch.schema = schema; batch.ncols = ncols;
    for (int c = 0; c < ncols; c++) batch.values[c] = col_vals[c];
    char *row_copy = arena_alloc(a, 65536);

    while (p < body + req->body_len) {
        const char *ne = memchr(p, '\n', (size_t)(body + req->body_len - p));
        if (!ne) ne = body + req->body_len;
        size_t rlen = (size_t)(ne - p);
        if (rlen == 0 || (rlen == 1 && p[0] == '\r')) { p = ne + 1; continue; }
        if (rlen >= 65536) rlen = 65535;
        memcpy(row_copy, p, rlen);
        if (rlen > 0 && row_copy[rlen-1] == '\r') rlen--;
        row_copy[rlen] = '\0';
        /* Re-parse (validated already in the atomicity pre-pass, so these error
         * branches are defensive and cannot fire for a body that reached here).
         * A short row pads the missing tail with the NULL sentinel so it
         * round-trips as a true NULL, not "". */
        int nfields = 0; int perr = 0;
        char **fields = split_line_rfc4180(a, row_copy, delim, &nfields, &perr);
        if (perr || !fields) {
            arena_destroy(a);
            http_resp_error(resp, 400, "malformed CSV row (bad quoting)");
            return;
        }
        if (nfields > ncols) {
            arena_destroy(a);
            http_resp_error(resp, 400, "CSV row has more fields than header");
            return;
        }
        for (int col = 0; col < ncols; col++) {
            int rr = batch.nrows;
            /* Map a present-but-empty field (",,") to NULL too, not just a
             * missing trailing one — otherwise "" silently coerces to 0 in
             * numeric aggregates (inflating COUNT, deflating AVG/MIN). */
            const char *cv = (col < nfields && fields[col] && fields[col][0])
                             ? fields[col] : DFO_NULL_SENTINEL;
            if (strcmp(cv, DFO_NULL_SENTINEL) == 0) {
                /* True NULL (missing tail or explicit sentinel): flag the
                 * null_bitmap so the stored cell is a real NULL, matching how
                 * SQL-materialized NULLs are persisted. */
                if (!batch.null_bitmap[col])
                    batch.null_bitmap[col] = arena_calloc(a, (BATCH_SIZE + 7) / 8);
                batch.null_bitmap[col][rr / 8] |= (uint8_t)(1u << (rr % 8));
                col_vals[col][rr] = (char *)"";
            } else {
                if (batch.null_bitmap[col])
                    batch.null_bitmap[col][rr / 8] &= (uint8_t)~(1u << (rr % 8));
                col_vals[col][rr] = arena_strdup(a, cv);
            }
        }
        batch.nrows++; row_count++;
        if (batch.nrows == BATCH_SIZE) {
            if (req->txn_id != 0) {
                if (txn_buffer_insert(g_app.txn_mgr, req->txn_id, tname, &batch) != 0) {
                    arena_destroy(a); http_resp_error(resp, 500, "txn buffer failed"); return;
                }
            } else {
                if (table_append(t, &batch) != 0) {
                    arena_destroy(a); http_resp_error(resp, 500, "ingest write failed"); return;
                }
            }
            batch.nrows = 0;
        }
        p = ne + 1;
    }
    if (batch.nrows > 0) {
        if (req->txn_id != 0) {
            if (txn_buffer_insert(g_app.txn_mgr, req->txn_id, tname, &batch) != 0) {
                arena_destroy(a); http_resp_error(resp, 500, "txn buffer failed"); return;
            }
        } else {
            if (table_append(t, &batch) != 0) {
                arena_destroy(a); http_resp_error(resp, 500, "ingest write failed"); return;
            }
        }
    }

    metrics_push(&g_app.metrics->rows_ingested,(double)row_count);
    g_app.metrics->total_rows+=row_count;
    catalog_update_table_meta(g_app.catalog, tname, "ingest", (int64_t)row_count);
    unlink(tmp_path);

    Arena *ba=arena_create(256);
    char *msg=arena_sprintf(ba,"{\"event\":\"table_updated\",\"table\":\"%s\",\"rows\":%d}",tname,row_count);
    app_ws_broadcast(&g_app,msg); arena_destroy(ba);

    Arena *ra=arena_create(256);
    JBuf jb; jb_init(&jb,ra,256);
    jb_obj_begin(&jb);
    jb_key(&jb,"table");  jb_str(&jb,tname);
    jb_key(&jb,"rows");   jb_int(&jb,row_count);
    jb_key(&jb,"columns");jb_int(&jb,ncols);
    jb_key(&jb,"status"); jb_str(&jb,"ok");
    jb_obj_end(&jb);
    http_resp_json(resp,200,jb_done(&jb));
}

/* ── POST /api/ingest/parquet?table=X ── */
static void h_ingest_parquet(HttpReq *req, HttpResp *resp) {
    const char *qs=req->query;
    char tname[128]="uploaded";
    if (qs) { const char *p=strstr(qs,"table="); if(p) sscanf(p+6,"%127[^&]",tname); }

    if (!valid_table_name(tname)) { http_resp_error(resp,400,"invalid table name"); return; }

    /* RBAC */
    if (g_app.rbac_enabled && g_app.rbac) {
        pthread_mutex_lock(&g_app.tables_mu);
        bool exists = hm_get(&g_app.tables, tname) != NULL;
        pthread_mutex_unlock(&g_app.tables_mu);
        RbacAction act = exists ? ACTION_TABLE_WRITE : ACTION_TABLE_CREATE;
        if (!check_table_access(req, resp, tname, act)) return;
    }

    if (!req->body||req->body_len==0) { http_resp_error(resp,400,"empty body"); return; }

    /* Сохраняем тело во временный файл */
    char tmp_path[256];
    snprintf(tmp_path,sizeof(tmp_path),"/tmp/dfo_pq_%lld.parquet",(long long)time(NULL));
    FILE *f=fopen(tmp_path,"wb");
    if (!f) { http_resp_error(resp,500,"can't write tmp"); return; }
    fwrite(req->body,1,req->body_len,f); fclose(f);

    /* Конфиг коннектора: путь к только что записанному файлу */
    char cfg[600]; snprintf(cfg,sizeof(cfg),"{\"path\":\"%s\"}",tmp_path);
    char so_path[512];
    snprintf(so_path,sizeof(so_path),"%s/parquet_connector.so",g_app.plugins_dir);

    Arena *a=arena_create(4194304);
    ConnectorInst *inst=connector_load(so_path,cfg,a);
    if (!inst) {
        remove(tmp_path); arena_destroy(a);
        http_resp_error(resp,500,"parquet connector not found"); return;
    }
    const DfoConnector *api=connector_api(inst);
    void *ctx=connector_ctx(inst);

    int total_rows=0; Table *t=NULL; bool table_created=false;
    char cursor_buf[32]="0";

    for (;;) {
        DfoReadReq rreq={ .cursor=cursor_buf, .limit=BATCH_SIZE };
        ColBatch *batch=NULL;
        if (api->read_batch(ctx,a,&rreq,"",&batch)!=0||!batch||batch->nrows==0) break;

        if (!table_created) {
            t=recreate_table(&g_app,a,tname,batch->schema);
            catalog_register_table(g_app.catalog,tname,batch->schema);
            table_created=true;
        }
        if (req->txn_id != 0) {
            if (txn_buffer_insert(g_app.txn_mgr, req->txn_id, tname, batch) != 0) {
                connector_unload(inst); arena_destroy(a); remove(tmp_path);
                http_resp_error(resp, 500, "txn buffer failed"); return;
            }
        } else {
            table_append(t, batch);
        }
        total_rows+=batch->nrows;
        snprintf(cursor_buf,sizeof(cursor_buf),"%d",total_rows);
        if (batch->nrows<BATCH_SIZE) break;
    }

    connector_unload(inst);
    arena_destroy(a);
    remove(tmp_path);

    Arena *ra=arena_create(256);
    JBuf jb; jb_init(&jb,ra,256);
    jb_obj_begin(&jb);
    jb_key(&jb,"table");  jb_str(&jb,tname);
    jb_key(&jb,"rows");   jb_int(&jb,total_rows);
    jb_key(&jb,"status"); jb_str(&jb,"ok");
    jb_obj_end(&jb);
    http_resp_json(resp,200,jb_done(&jb));
}

/* ── Секреты внутри connector_config шага ────────────────────────────────────
 * Справочник подключений отдаётся наружу замаскированным (conn_write_public),
 * а конвейеры — нет: pipeline_to_json пишет connector_config шага байт-в-байт.
 * У шага с ВСТРОЕННЫМ конфигом (импорт YAML, легаси, MCP) внутри лежит
 * настоящий sasl_password / ssl_keystore_password, и он уезжал в ответ
 * GET /api/pipelines любому, кто имеет право смотреть конвейеры.
 *
 * Маскируем на выдаче и склеиваем обратно на приёме — как у подключений.
 * Без второй половины правка конвейера сохранила бы поверх пароля строку
 * "********". connector_config хранится СТРОКОЙ с JSON внутри, поэтому её
 * приходится разбирать отдельно от самого конвейера. */
static bool conn_key_is_secret(const char *k);
static bool conn_key_is_safe(const char *k);
static void conn_key_canon(const char *k, char *out, size_t outsz);
static bool conn_val_is_mask(JVal *v);
static JVal *conn_cfg_get_canon(JVal *cfg, const char *k);
static void conn_write_config_masked(JBuf *jb, JVal *cfg);
static void conn_write_config_merged(JBuf *jb, JVal *incoming, JVal *stored_cfg);
static void jval_serialize(JBuf *jb, JVal *v);

/* Есть ли в конфиге непустой секрет — чтобы не переписывать JSON зря. */
static bool cfg_has_secret(JVal *cfg) {
    if (!cfg || cfg->type != JV_OBJECT) return false;
    for (size_t i = 0; i < cfg->nkeys; i++) {
        if (!conn_key_is_secret(cfg->keys[i])) continue;
        JVal *v = cfg->vals[i];
        if (v && v->type == JV_STRING && v->s && v->s[0]) return true;
    }
    return false;
}

/* Ищет сохранённый connector_config шага. Вызывать под g_app.scheduler->mu. */
static const char *stored_step_cfg_locked(const char *pid, const char *sid) {
    if (!pid || !pid[0] || !sid || !sid[0] || !g_app.scheduler) return NULL;
    for (int i = 0; i < g_app.scheduler->npipelines; i++) {
        Pipeline *p = &g_app.scheduler->pipelines[i];
        if (strcmp(p->id, pid) != 0) continue;
        for (int j = 0; j < p->nsteps; j++)
            if (strcmp(p->steps[j].id, sid) == 0) return p->steps[j].connector_config;
        return NULL;
    }
    return NULL;
}

/* Переписывает конвейер, применяя к connector_config каждого шага операцию
 * mask (stored == NULL) или merge (stored — сохранённый конфиг того же шага).
 * Возвращает NULL, если менять нечего: вызывающий отдаёт исходную строку. */
static const char *pipeline_json_rewrite_cfg(const char *pj, Arena *a, bool merge,
                                             const char *pid_for_merge) {
    JVal *root = json_parse(a, pj, strlen(pj));
    if (!root || root->type != JV_OBJECT) return NULL;
    JVal *steps = json_get(root, "steps");
    if (!steps || steps->type != JV_ARRAY || steps->nitems == 0) return NULL;

    const char *pid = pid_for_merge && pid_for_merge[0]
                    ? pid_for_merge : json_str(json_get(root, "id"), "");

    JBuf jb; jb_init(&jb, a, strlen(pj) + 256);
    jb_obj_begin(&jb);
    for (size_t i = 0; i < root->nkeys; i++) {
        jb_key(&jb, root->keys[i]);
        if (strcmp(root->keys[i], "steps") != 0) { jval_serialize(&jb, root->vals[i]); continue; }

        jb_arr_begin(&jb);
        for (size_t s = 0; s < steps->nitems; s++) {
            JVal *st = steps->items[s];
            if (!st || st->type != JV_OBJECT) { jval_serialize(&jb, st); continue; }
            const char *sid = json_str(json_get(st, "id"), "");
            jb_obj_begin(&jb);
            for (size_t k = 0; k < st->nkeys; k++) {
                jb_key(&jb, st->keys[k]);
                JVal *v = st->vals[k];
                if (strcmp(st->keys[k], "connector_config") || !v ||
                    v->type != JV_STRING || !v->s || !v->s[0]) {
                    jval_serialize(&jb, v);
                    continue;
                }
                JVal *cfg = json_parse(a, v->s, v->len);
                if (!cfg || cfg->type != JV_OBJECT) { jval_serialize(&jb, v); continue; }

                JBuf inner; jb_init(&inner, a, v->len + 64);
                if (merge) {
                    const char *sc = NULL;
                    pthread_mutex_lock(&g_app.scheduler->mu);
                    const char *found = stored_step_cfg_locked(pid, sid);
                    if (found) sc = arena_strdup(a, found);
                    pthread_mutex_unlock(&g_app.scheduler->mu);
                    JVal *stored = (sc && sc[0]) ? json_parse(a, sc, strlen(sc)) : NULL;
                    if (stored && stored->type != JV_OBJECT) stored = NULL;
                    conn_write_config_merged(&inner, cfg, stored);
                } else {
                    conn_write_config_masked(&inner, cfg);
                }
                jb_str(&jb, jb_done(&inner));
            }
            jb_obj_end(&jb);
        }
        jb_arr_end(&jb);
    }
    jb_obj_end(&jb);
    return jb_done(&jb);
}

/* Конвейер для выдачи наружу: секреты во встроенных конфигах замаскированы. */
static const char *pipeline_json_public(const char *pj, Arena *a) {
    JVal *root = json_parse(a, pj, strlen(pj));
    if (!root || root->type != JV_OBJECT) return pj;
    JVal *steps = json_get(root, "steps");
    if (!steps || steps->type != JV_ARRAY) return pj;
    bool any = false;
    for (size_t s = 0; s < steps->nitems && !any; s++) {
        JVal *st = steps->items[s];
        if (!st || st->type != JV_OBJECT) continue;
        JVal *v = json_get(st, "connector_config");
        if (!v || v->type != JV_STRING || !v->s || !v->s[0]) continue;
        JVal *cfg = json_parse(a, v->s, v->len);
        if (cfg_has_secret(cfg)) any = true;
    }
    if (!any) return pj;
    const char *out = pipeline_json_rewrite_cfg(pj, a, false, NULL);
    return out ? out : pj;
}

/* Тело запроса на приёме: заглушки секретов заменяются сохранёнными значениями. */
static const char *pipeline_json_unmask(const char *body, Arena *a, const char *pid) {
    const char *out = pipeline_json_rewrite_cfg(body, a, true, pid);
    return out ? out : body;
}

/* ── GET /api/pipelines ── */
static void h_pipelines_list(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a=arena_create(8192);
    JBuf jb; jb_init(&jb,a,1024);
    jb_arr_begin(&jb);
    pthread_mutex_lock(&g_app.scheduler->mu);
    for(int i=0;i<g_app.scheduler->npipelines;i++){
        char *pj=pipeline_to_json(&g_app.scheduler->pipelines[i],a);
        jb_raw(&jb,pipeline_json_public(pj,a));
    }
    pthread_mutex_unlock(&g_app.scheduler->mu);
    jb_arr_end(&jb);
    http_resp_json(resp,200,jb_done(&jb));
}

/* ── POST /api/pipelines ── */
static void h_pipeline_create(HttpReq *req, HttpResp *resp) {
    if(!req->body||!req->body_len){http_resp_error(resp,400,"empty body");return;}
    Pipeline p; memset(&p,0,sizeof(p));
    /* Тот же POST служит и обновлением, а конвейер уехал в UI с замаскированными
     * секретами. Возвращаем на их место сохранённые значения — иначе правка
     * любого поля затёрла бы пароль строкой "********". */
    Arena *ma=arena_create(4096);
    const char *body = pipeline_json_unmask(req->body, ma, NULL);
    if(pipeline_from_json(&p,body)<0){arena_destroy(ma);http_resp_error(resp,400,"invalid pipeline json");return;}
    arena_destroy(ma);
    if(!p.id[0]) {
        static volatile int _pid_seq = 0;
        int seq = __atomic_fetch_add(&_pid_seq, 1, __ATOMIC_RELAXED);
        snprintf(p.id,sizeof(p.id),"p_%lld_%d",(long long)time(NULL), seq);
    }
    scheduler_add(g_app.scheduler,&p);
    Arena *a=arena_create(4096);
    char *pj=pipeline_to_json(&p,a);
    catalog_save_pipeline(g_app.catalog,p.id,pj);   /* в хранилище — с настоящими секретами */
    http_resp_json(resp,201,pipeline_json_public(pj,a));
    arena_destroy(a);   /* тело уже скопировано в ответ */
    Arena *ba=arena_create(256);
    app_ws_broadcast(&g_app,arena_sprintf(ba,"{\"event\":\"pipeline_created\",\"id\":\"%s\"}",p.id));
    arena_destroy(ba);
}

/* Forward declaration — implementation lives below pipeline_execute_steps.
 * `external_arena` lets a caller (preview-step) keep the arena alive after
 * the call so that Table.schema (allocated inside) remains valid for
 * follow-up queries. Pass NULL to use a self-managed internal arena. */
static void pipeline_execute_steps_internal(Pipeline *p, App *app, bool report, Arena *external_arena);

/* ── POST /api/pipelines/preview-step ──
 *
 * Body: Pipeline JSON (typically a single-step pipeline).
 * Query: ?save=0|1 (default 0). When 0, the step's target_table is overridden
 *        with a temp name and the temp table is dropped after the result is
 *        read. When 1, the step writes to its real target_table and the table
 *        persists.
 *
 * Response 200: { "columns": [...], "rows": [[...]], "rows_returned": N,
 *                 "rows_written": M, "target_table": "..." }
 * Response 400/500: { "error": "..." } — surfaces server-side error_msg.
 *
 * Unlike POST /api/pipelines → POST /api/pipelines/:id/run round-trip, this
 * does NOT touch the catalog (no pipeline row, no run-log row) and does NOT
 * broadcast any WS event. The pipeline lives only on this thread's stack. */
static void h_pipeline_preview_step(HttpReq *req, HttpResp *resp) {
    if (!req->body || !req->body_len) { http_resp_error(resp, 400, "empty body"); return; }

    /* Parse `save` flag from query string. */
    bool save = false;
    if (req->query) {
        const char *p = strstr(req->query, "save=");
        if (p) save = (p[5] == '1');
    }

    /* Parse `limit` from query string (default 100, capped at 10000). */
    int limit = 100;
    if (req->query) {
        const char *p = strstr(req->query, "limit=");
        if (p) {
            int v = atoi(p + 6);
            if (v > 0 && v <= 10000) limit = v;
        }
    }

    /* Pipeline is ~1MB (MAX_STEPS=64 × ~14KB per step). Put it on the heap
     * — putting it on the stack risks overflowing macOS's 512KB default
     * thread stack (HTTP worker threads use a small stack). */
    Pipeline *plp = calloc(1, sizeof(Pipeline));
    if (!plp) { http_resp_error(resp, 500, "out of memory"); return; }
    /* Конструктор мог загрузить конвейер с замаскированными секретами — с
     * заглушкой вместо пароля предпросмотр упал бы на аутентификации. Ставим
     * на её место сохранённое значение, как и при сохранении. */
    Arena *ma = arena_create(4096);
    const char *pbody = pipeline_json_unmask(req->body, ma, NULL);
    if (pipeline_from_json(plp, pbody) < 0) {
        arena_destroy(ma); free(plp);
        http_resp_error(resp, 400, "invalid pipeline json"); return;
    }
    arena_destroy(ma);
    if (plp->nsteps == 0) {
        free(plp); http_resp_error(resp, 400, "pipeline has no steps"); return;
    }

    /* Override target_table for preview mode so the user's real target isn't
     * touched. For save=1 we trust the caller's target_table — but require
     * that it's non-empty. */
    char tmp_table[128];
    snprintf(tmp_table, sizeof(tmp_table), "__preview_%lld_%d",
             (long long)time(NULL), rand() & 0xFFFF);
    if (!save) {
        strncpy(plp->steps[plp->nsteps - 1].target_table, tmp_table,
                sizeof(plp->steps[0].target_table) - 1);
    } else if (!plp->steps[plp->nsteps - 1].target_table[0]) {
        free(plp);
        http_resp_error(resp, 400, "save=1 requires target_table on last step");
        return;
    }
    const char *out_table = plp->steps[plp->nsteps - 1].target_table;

    /* Tag the pipeline so any log-output that does leak through is clearly
     * identifiable. We don't go through the scheduler — pipeline is heap-only. */
    snprintf(plp->id, sizeof(plp->id), "__preview_%lld_%d",
             (long long)time(NULL), rand() & 0xFFFF);

    /* Force zero retries so /run won't sleep through retry-backoff cycles. */
    for (int i = 0; i < plp->nsteps; i++) {
        plp->steps[i].max_retries = 0;
        plp->steps[i].retry_delay_sec = 1;
    }

    /* Single arena shared across step execution + the follow-up SELECT.
     * pipeline_execute_steps_internal allocates Schema/etc. into this arena
     * via recreate_table → write_rs_to_table; the schema must stay alive
     * because subsequent exec_stmt walks Table.schema for typing. */
    Arena *a = arena_create(4194304);
    LOG_INFO("preview-step: executing %d step(s) → %s (save=%d)", plp->nsteps, out_table, (int)save);
    pipeline_execute_steps_internal(plp, &g_app, /*report=*/false, /*external_arena=*/a);
    if (plp->run_status == RUN_FAILED) {
        JBuf jb; jb_init(&jb, a, 1024);
        jb_obj_begin(&jb);
        jb_key(&jb, "error"); jb_str(&jb, plp->error_msg[0] ? plp->error_msg : "step failed");
        jb_obj_end(&jb);
        /* Copy the body out of the arena BEFORE destroying it — resp->body is
         * zero-copy and the HTTP framework sends it after this handler returns
         * (matches the success path below; omitting this was a use-after-free). */
        const char *body = jb_done(&jb);
        static _Thread_local char err_buf[1024];
        snprintf(err_buf, sizeof(err_buf), "%s", body ? body : "{\"error\":\"step failed\"}");
        http_resp_json(resp, 500, err_buf);
        arena_destroy(a);
        free(plp);
        return;
    }

    /* Read the result inline — works now that CSV connector forces TEXT-only
     * types (qengine read-back bug was specific to INT64/DOUBLE columns). */
    char sql_buf[256];
    snprintf(sql_buf, sizeof(sql_buf), "SELECT * FROM %s LIMIT %d", out_table, limit);
    Stmt *stmt = sql_parse(a, sql_buf, strlen(sql_buf));
    RS *rs = (stmt && !stmt->error) ? exec_stmt(a, stmt, NULL) : NULL;

    JBuf jb; jb_init(&jb, a, 65536);
    jb_obj_begin(&jb);
    jb_key(&jb, "columns"); jb_arr_begin(&jb);
    if (rs) for (int i = 0; i < rs->ncols; i++)
        jb_str(&jb, rs->col_names[i] ? rs->col_names[i] : "");
    jb_arr_end(&jb);
    jb_key(&jb, "rows"); jb_arr_begin(&jb);
    if (rs) for (int r = 0; r < rs->nrows; r++) {
        jb_arr_begin(&jb);
        for (int c = 0; c < rs->ncols; c++) {
            const char *v = rs->rows[r].cells ? rs->rows[r].cells[c] : NULL;
            if(!v||(uintptr_t)v<0x1000||!strcmp(v,DFO_NULL_SENTINEL)){ jb_null(&jb); }
            else { ColType _ct=rs->col_types?rs->col_types[c]:COL_TEXT;
                   if((_ct==COL_INT64||_ct==COL_DOUBLE)&&is_json_number(v)) jb_raw(&jb,v);
                   else if(_ct==COL_BOOL&&(!strcmp(v,"true")||!strcmp(v,"false"))) jb_raw(&jb,v);
                   else jb_str(&jb,v); }
        }
        jb_arr_end(&jb);
    }
    jb_arr_end(&jb);
    jb_key(&jb, "rows_returned"); jb_int(&jb, rs ? rs->nrows : 0);
    jb_key(&jb, "target_table"); jb_str(&jb, save ? out_table : "");
    jb_key(&jb, "preview"); jb_bool(&jb, !save);
    jb_obj_end(&jb);

    /* Body lives in the arena — copy to thread-local buffer since arena_destroy
     * runs before the HTTP framework reads resp->body. */
    static _Thread_local char *resp_buf = NULL;
    static _Thread_local size_t resp_cap = 0;
    const char *body = jb_done(&jb);
    size_t body_len = body ? strlen(body) : 0;
    if (body_len + 1 > resp_cap) {
        free(resp_buf);
        resp_cap = body_len + 1 + 4096;
        resp_buf = malloc(resp_cap);
    }
    if (resp_buf) memcpy(resp_buf, body, body_len + 1);
    http_resp_json(resp, 200, resp_buf ? resp_buf : "{}");

    /* In preview mode (save=0) drop the temp table the step wrote to — we
     * already extracted the rows into the response body. In save mode the
     * table is the user's real target, leave it alone. */
    if (!save) {
        pthread_mutex_lock(&g_app.tables_mu);
        Table *t = hm_get(&g_app.tables, out_table);
        if (t) { table_close(t); hm_del(&g_app.tables, out_table); }
        pthread_mutex_unlock(&g_app.tables_mu);
        catalog_drop_table(g_app.catalog, out_table);
        char dir_path[1024];
        snprintf(dir_path, sizeof(dir_path), "%s/%s", g_app.data_dir, out_table);
        /* Remove all files inside (wal.bin, schema.json, any indexes) then
         * the directory itself. Don't assume a fixed set — table_create may
         * write multiple sidecar files. */
        DIR *d = opendir(dir_path);
        if (d) {
            struct dirent *ent;
            while ((ent = readdir(d))) {
                if (ent->d_name[0] == '.') continue;
                char fp[2048];
                snprintf(fp, sizeof(fp), "%s/%s", dir_path, ent->d_name);
                unlink(fp);
            }
            closedir(d);
        }
        rmdir(dir_path);
        if (t) __atomic_fetch_sub(&g_app.metrics->tables_count, 1, __ATOMIC_RELAXED);
    }

    arena_destroy(a);
    free(plp);
}

/* ── POST /api/pipelines/preview-yaml ──
 *
 * Body: text/yaml — a single pipeline document.
 * Response 200: parsed JSON pipeline (NOT saved). Used by the UI / CLI to
 *               validate YAML before applying.
 * Response 400: {"error": "...", "line": N, "col": M} on parse error.
 *
 * Validation re-uses pipeline_from_json so callers see the same fields
 * they'd get from /api/pipelines. */
static void h_pipeline_preview_yaml(HttpReq *req, HttpResp *resp) {
    if (!req->body || !req->body_len) { http_resp_error(resp, 400, "empty body"); return; }
    Arena *a = req->arena;
    YamlError yerr = {0};
    char *json = NULL;
    if (yaml_to_json(req->body, req->body_len, a, &json, &yerr) < 0) {
        char *msg = arena_sprintf(a,
            "{\"error\":\"yaml parse error\",\"detail\":\"%s\",\"line\":%d,\"col\":%d}",
            yerr.buf, yerr.line, yerr.col);
        resp->status = 400;
        resp->content_type = "application/json";
        resp->body = msg;
        resp->body_len = strlen(msg);
        return;
    }
    /* Validate by round-tripping through pipeline_from_json + pipeline_to_json */
    Pipeline p; memset(&p, 0, sizeof(p));
    if (pipeline_from_json(&p, json) < 0) {
        http_resp_error(resp, 400, "yaml parsed but pipeline schema is invalid");
        return;
    }
    char *normalized = pipeline_to_json(&p, a);
    http_resp_json(resp, 200, normalized);
}

/* ── POST /api/pipelines/from-template ──
 * Body JSON: { "template_yaml": "<yaml with {{vars}}>", "vars": { "k": "v" } }
 * Expands {{ }} placeholders (external vars{} override the template's own vars:
 * block), parses the resulting YAML → pipeline, SAVES it (catalog + scheduler),
 * and returns the full Pipeline JSON — same shape and side effects as
 * POST /api/pipelines, but driven by a template instead of ready JSON. */
static void h_pipeline_from_template(HttpReq *req, HttpResp *resp) {
    if (!req->body || !req->body_len) { http_resp_error(resp, 400, "empty body"); return; }
    Arena *a = req->arena;

    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root || root->type != JV_OBJECT) {
        http_resp_error(resp, 400, "expected JSON object"); return;
    }
    const char *tmpl = json_str(json_get(root, "template_yaml"), "");
    if (!tmpl[0]) { http_resp_error(resp, 400, "template_yaml is required"); return; }

    /* Collect vars{} → keys[]/vals[] (override the template's own vars:). */
    const char *keys[128]; const char *vals[128]; int nkvs = 0;
    JVal *vars_obj = json_get(root, "vars");
    if (vars_obj && vars_obj->type == JV_OBJECT) {
        for (int i = 0; i < (int)vars_obj->nkeys && nkvs < 128; i++) {
            keys[nkvs] = vars_obj->keys[i];
            vals[nkvs] = json_str(vars_obj->vals[i], "");
            nkvs++;
        }
    }

    char *expanded = yaml_expand_vars(a, tmpl, strlen(tmpl), keys, vals, nkvs);
    if (!expanded) { http_resp_error(resp, 500, "template expansion failed"); return; }

    YamlError yerr = {0};
    char *json = NULL;
    if (yaml_to_json(expanded, strlen(expanded), a, &json, &yerr) < 0) {
        char *msg = arena_sprintf(a,
            "{\"error\":\"yaml parse error\",\"detail\":\"%s\",\"line\":%d,\"col\":%d}",
            yerr.buf, yerr.line, yerr.col);
        resp->status = 400; resp->content_type = "application/json";
        resp->body = msg; resp->body_len = strlen(msg);
        return;
    }

    Pipeline p; memset(&p, 0, sizeof(p));
    if (pipeline_from_json(&p, json) < 0) {
        http_resp_error(resp, 400, "expanded yaml is not a valid pipeline"); return;
    }
    if (!p.id[0]) {
        static volatile int _tpl_seq = 0;
        int seq = __atomic_fetch_add(&_tpl_seq, 1, __ATOMIC_RELAXED);
        snprintf(p.id, sizeof(p.id), "p_%lld_%d", (long long)time(NULL), seq);
    }
    scheduler_add(g_app.scheduler, &p);
    char *pj = pipeline_to_json(&p, a);
    catalog_save_pipeline(g_app.catalog, p.id, pj);
    http_resp_json(resp, 201, pipeline_json_public(pj, a));

    Arena *ba = arena_create(256);
    app_ws_broadcast(&g_app, arena_sprintf(ba, "{\"event\":\"pipeline_created\",\"id\":\"%s\"}", p.id));
    arena_destroy(ba);
}

/* ── GET /api/pipelines/:id ── */
static void h_pipeline_get(HttpReq *req, HttpResp *resp) {
    const char *id=hm_get(&req->params,"id");
    if(!id){http_resp_error(resp,400,"missing id");return;}
    Pipeline *p=scheduler_find(g_app.scheduler,id);
    if(!p){
        Arena *a=arena_create(4096); char *json=NULL;
        if(catalog_load_pipeline(g_app.catalog,id,&json,a)==0) http_resp_json(resp,200,json);
        else http_resp_error(resp,404,"not found");
        arena_destroy(a); return;
    }
    Arena *a=arena_create(4096);
    http_resp_json(resp,200,pipeline_json_public(pipeline_to_json(p,a),a));
    arena_destroy(a);
}

/* ── Drop and recreate a target table, return open Table* ── */
static Table *recreate_table(App *app, Arena *a, const char *tname, Schema *schema) {
    pthread_mutex_lock(&app->tables_mu);
    Table *old = hm_get(&app->tables, tname);
    bool replaced = old != NULL;   /* replacing an existing table → net count unchanged */
    if (old) { table_close(old); hm_del(&app->tables, tname); }
    pthread_mutex_unlock(&app->tables_mu);

    catalog_drop_table(app->catalog, tname);
    char wp[1024], dp[1024];
    snprintf(dp, sizeof(dp), "%s/%s", app->data_dir, tname);
    snprintf(wp, sizeof(wp), "%s/wal.bin", dp);
    unlink(wp); rmdir(dp);

    pthread_mutex_lock(&app->tables_mu);
    Table *t = table_create(tname, schema, app->data_dir);
    if (t && app->cluster_mode && app->replicator)
        table_set_wal_callback(t, replicator_wal_cb, app->replicator);
    hm_set(&app->tables, tname, t);
    pthread_mutex_unlock(&app->tables_mu);
    catalog_register_table(app->catalog, tname, schema);
    /* Account a genuinely new table so tables_count balances the drops in
     * h_table_delete / the preview-temp cleanup / the SCD2 scratch teardown.
     * Without this, tables created through this path were never counted yet
     * still decremented on drop, driving the gauge negative. A replace
     * (drop+recreate of an existing name) is net-zero. */
    if (t && !replaced) __atomic_fetch_add(&g_app.metrics->tables_count, 1, __ATOMIC_RELAXED);
    (void)a;
    return t;
}

/* Глубокая копия схемы в арену `a` (ColDef-массив + имена колонок). Нужна,
 * когда исходная схема живёт в короткоживущей странице-арене, а получатель
 * (table_create / recreate_table) сохраняет указатель на неё. */
static Schema *schema_deep_copy(Arena *a, const Schema *src) {
    if (!src) return NULL;
    Schema *dst = arena_calloc(a, sizeof(Schema));
    dst->ncols = src->ncols;
    dst->cols  = arena_alloc(a, (size_t)src->ncols * sizeof(ColDef));
    for (int c = 0; c < src->ncols; c++) {
        dst->cols[c] = src->cols[c];
        dst->cols[c].name = arena_strdup(a, src->cols[c].name ? src->cols[c].name : "col");
    }
    return dst;
}

/* ── Write an RS (result set) into a Table ── */
static int write_rs_to_table(App *app, Arena *a, const char *tname, RS *rs) {
    if (!rs || rs->ncols == 0) return 0;

    Schema *schema = arena_calloc(a, sizeof(Schema));
    schema->ncols = rs->ncols;
    schema->cols  = arena_alloc(a, (size_t)rs->ncols * sizeof(ColDef));
    for (int c = 0; c < rs->ncols; c++) {
        schema->cols[c].name     = rs->col_names[c] ? rs->col_names[c] : "col";
        schema->cols[c].type     = COL_TEXT;
        schema->cols[c].nullable = true;
    }
    Table *t = recreate_table(app, a, tname, schema);

    char ***cv = arena_alloc(a, (size_t)rs->ncols * sizeof(char **));
    for (int c = 0; c < rs->ncols; c++)
        cv[c] = arena_alloc(a, BATCH_SIZE * sizeof(char *));
    ColBatch batch = {0};
    batch.schema = schema; batch.ncols = rs->ncols;
    for (int c = 0; c < rs->ncols; c++) batch.values[c] = cv[c];

    int total = 0;
    for (int r = 0; r < rs->nrows; r++) {
        for (int c = 0; c < rs->ncols; c++) {
            const char *v = rs->rows[r].cells ? rs->rows[r].cells[c] : "";
            int rr = batch.nrows;
            if (v && strcmp(v, DFO_NULL_SENTINEL) == 0) {
                /* True SQL NULL: flag in null_bitmap (lazily allocated, zeroed)
                 * so the stored cell is NULL rather than the sentinel text. */
                if (!batch.null_bitmap[c])
                    batch.null_bitmap[c] = arena_calloc(a, (BATCH_SIZE + 7) / 8);
                batch.null_bitmap[c][rr / 8] |= (uint8_t)(1u << (rr % 8));
                cv[c][rr] = (char *)"";
            } else {
                if (batch.null_bitmap[c])
                    batch.null_bitmap[c][rr / 8] &= (uint8_t)~(1u << (rr % 8));
                cv[c][rr] = (char *)(v ? v : "");
            }
        }
        if (++batch.nrows == BATCH_SIZE) { table_append(t, &batch); batch.nrows = 0; }
        total++;
    }
    if (batch.nrows > 0) table_append(t, &batch);
    catalog_update_table_meta(app->catalog, tname, "pipeline", (int64_t)rs->nrows);
    /* Mark datamarts depending on this table stale (+ queue on-write refresh). */
    if (app->matviews) mvs_invalidate(app->matviews, tname);
    return total;
}

/* Edge-triggered alerting: notify only when the run status CHANGES (healthy →
 * failed, or failed → recovered), so the same alert is never sent repeatedly. */
static void send_pipeline_alert(Pipeline *p, const char *message, bool success) {
    if (!p) return;
    bool now_failed = !success;
    bool changed = (now_failed != (p->last_run_failed != 0));
    p->last_run_failed = now_failed ? 1 : 0;   /* remember for the next run's edge check */

    /* Notify on ANY status change (broke OR recovered); no per-event filter —
       edge-triggering already prevents repeats, so every change is worth sending. */
    if (!p->webhook_url[0] || !changed) return;  /* no webhook, or no status change → silent */

    char body[1024];
    if (success) {
        snprintf(body, sizeof(body), "{\"text\":\"✅ Конвейер *%s* восстановлен\"}",
                 p->name[0] ? p->name : p->id);
    } else {
        snprintf(body, sizeof(body), "{\"text\":\"❌ Конвейер *%s* сломался: %s\"}",
                 p->name[0] ? p->name : p->id, message ? message : "неизвестная ошибка");
    }

    if (http_post_json(p->webhook_url, body, 5000) != 0)
        LOG_WARN("alert webhook failed for pipeline %s", p->id);
}

/* Map UI/YAML connector_type names to the built .so basename. Most names map
 * 1:1 (csv → csv_connector.so); a few plugins ship under a short basename:
 *   postgresql / postgres → pg ; greenplum → gp. */
static const char *connector_so_name(const char *conn) {
    if (!conn) return "";
    if (!strcmp(conn, "postgresql") || !strcmp(conn, "postgres")) return "pg";
    if (!strcmp(conn, "greenplum")) return "gp";
    return conn;
}

/* Заглушка, которую UI получает вместо непустого секрета. Она же, присланная
 * обратно, означает «значение не меняли»: наложить её поверх настоящего пароля
 * нельзя ни при сохранении, ни при пробе связи. */
#define CONN_SECRET_MASK "********"

static void jval_serialize(JBuf *jb, JVal *v);   /* реализация ниже, рядом с пробами */

/* Накладывает over поверх base и возвращает слитый JSON-объект в арене.
 * base — параметры подключения из справочника, over — пошаговые ключи
 * (query / table / cursor_column / topic / group_id …), которые всегда
 * перебивают базу. Любой из аргументов может быть NULL или не-объектом. */
/* Значение-заглушка вместо секрета: клиент видит только её и, отправляя конфиг
 * обратно, шлёт её же. Наложить такую «маску» поверх настоящего пароля нельзя —
 * иначе коннектор получил бы строку "********" и не аутентифицировался. */
static bool conn_val_is_mask(JVal *v) {
    return v && v->type == JV_STRING && !strcmp(v->s, CONN_SECRET_MASK);
}

/* Слияние «подключение из справочника + переопределения». Переопределения
 * побеждают, маска сверху означает «значение не присылали».
 *
 * from_directory=true — когда база пришла из справочника подключений. В этом
 * случае СЕКРЕТЫ из переопределений игнорируются: справочник объявлен
 * источником истины для учётных данных. Без этого забытый в шаге sasl_password
 * молча побеждал правильный из справочника, а в форме его не видно вовсе —
 * оператор менял пароль в подключении, прогон продолжал падать, и понять
 * причину было нечем. Ровно тот же обезличенный «invalid credentials» от
 * брокера. Отброшенные ключи пишем в лог: молчаливое игнорирование не лучше
 * молчаливого перекрытия. */
/* Есть ли в конфиге СВОЙ секрет (не маска). Если вызывающий прислал пароль, он
 * его знает — подставлять чужой из справочника не нужно, а значит и запрещать
 * ему выбирать адрес незачем: утечь нечему. */
static bool cfg_has_own_secret(JVal *cfg) {
    if (!cfg || cfg->type != JV_OBJECT) return false;
    for (size_t i = 0; i < cfg->nkeys; i++) {
        if (!conn_key_is_secret(cfg->keys[i])) continue;
        JVal *v = cfg->vals[i];
        if (v && v->type == JV_STRING && v->s[0] && !conn_val_is_mask(v)) return true;
    }
    return false;
}

/* Ключи, задающие КУДА мы идём и КАК представляемся. Когда сервер подставляет
 * секрет из справочника, эти ключи обязаны браться оттуда же — иначе тот, кто
 * пароля не знает, заставит шлюз отправить его на чужой адрес и открытым
 * текстом (достаточно переопределить brokers и понизить security_protocol до
 * PLAIN). Ограничение действует ТОЛЬКО при подстановке секрета: конфиг со
 * своими учётными данными переопределяет что угодно, как и раньше. */
static bool conn_key_is_destination(const char *k) {
    static const char *DEST[] = {
        "brokers", "bootstrap_servers", "security_protocol", "sasl_mechanism",
        "sasl_username", "ssl_ca_location", "ssl_keystore_location",
        "ssl_truststore_location", "ssl_certificate_location", "ssl_key_location",
        "host", "port", "dsn", "url", "siebel_url", "schema_registry_url",
        "endpoint", "server", NULL
    };
    if (!k) return false;
    char canon[128];
    conn_key_canon(k, canon, sizeof canon);
    for (int i = 0; DEST[i]; i++)
        if (!strcmp(k, DEST[i]) || !strcmp(canon, DEST[i])) return true;
    return false;
}

static const char *conn_merge_config_ex(Arena *a, JVal *base, JVal *over,
                                        bool from_directory, const char *whose) {
    if (base && base->type != JV_OBJECT) base = NULL;
    if (over && over->type != JV_OBJECT) over = NULL;

    JBuf jb; jb_init(&jb, a, 1024);
    jb_obj_begin(&jb);
    if (base) {
        for (size_t i = 0; i < base->nkeys; i++) {
            JVal *ov = over ? json_get(over, base->keys[i]) : NULL;
            if (ov && !conn_val_is_mask(ov)
                   && !(from_directory && (conn_key_is_secret(base->keys[i])
                                        || conn_key_is_destination(base->keys[i])))) continue;
            jb_key(&jb, base->keys[i]);
            jval_serialize(&jb, base->vals[i]);
        }
    }
    if (over) {
        for (size_t i = 0; i < over->nkeys; i++) {
            /* jb_key имена ключей НЕ экранирует (см. lib/core/json.c) — ключ с
             * кавычкой сломал бы итоговый JSON, коннектор не разобрал бы его и
             * молча ушёл на запасной сканер. conn_save такие ключи отсеивает,
             * но конфиг шага сюда приходит и другими путями. */
            if (!conn_key_is_safe(over->keys[i])) {
                LOG_WARN("%s: ключ с недопустимым именем пропущен",
                         whose ? whose : "конфиг");
                continue;
            }
            if (conn_val_is_mask(over->vals[i])) continue;   /* маска — не значение */
            if (from_directory && conn_key_is_destination(over->keys[i])) {
                LOG_WARN("%s: ключ '%s' задаёт адрес или способ аутентификации и "
                         "ИГНОРИРУЕТСЯ — при использовании подключения из "
                         "справочника они берутся только оттуда. Иначе пароль "
                         "подключения ушёл бы на подменённый адрес",
                         whose ? whose : "конфиг", over->keys[i]);
                continue;
            }
            if (from_directory && conn_key_is_secret(over->keys[i])) {
                /* Отбрасываем ТОЛЬКО если в справочнике есть чем заменить.
                 * Иначе конфигурация, где пароль намеренно живёт в шаге, а не в
                 * подключении, после обновления осталась бы вовсе без пароля —
                 * «sasl.username and sasl.password must be set». Чинить одну
                 * ловушку, создавая другую, нельзя. */
                JVal *have = conn_cfg_get_canon(base, over->keys[i]);
                const char *hv = (have && have->type == JV_STRING) ? have->s : NULL;
                if (hv && hv[0]) {
                    LOG_WARN("%s: ключ '%s' задан прямо в шаге и ИГНОРИРУЕТСЯ — "
                             "берётся из подключения '%s'. Уберите его из шага: "
                             "в форме он не отображается, а прежде молча "
                             "перекрывал пароль из справочника",
                             whose ? whose : "конфиг", over->keys[i],
                             whose ? whose : "справочника");
                    continue;
                }
                LOG_WARN("%s: секрет '%s' задан в шаге, а в подключении '%s' его "
                         "нет — использую значение из шага. Перенесите его в "
                         "подключение: в форме шага это поле не отображается",
                         whose ? whose : "конфиг", over->keys[i],
                         whose ? whose : "справочника");
            }
            jb_key(&jb, over->keys[i]);
            jval_serialize(&jb, over->vals[i]);
        }
    }
    jb_obj_end(&jb);
    return jb_done(&jb);
}

/* Совместимость: слияние без справочника (секреты в переопределениях законны). */
static const char *conn_merge_config(Arena *a, JVal *base, JVal *over) {
    return conn_merge_config_ex(a, base, over, false, NULL);
}

/* Коннектор на время шага. Для подключения из справочника это АРЕНДА постоянной
 * сессии из пула (сессия переживает прогон и живёт дальше), для шага с
 * инлайновым конфигом — как раньше, разовый экземпляр на прогон. */
typedef struct {
    ConnLease     *lease;      /* != NULL → сессия взята из пула */
    ConnectorInst *inst;
} StepConn;

/* Держать сессию между прогонами имеет смысл только там, где она вообще есть:
 * сетевой сеанс к СУБД или брокеру. Файловые и HTTP-коннекторы сессии не имеют,
 * зато ЗАПОМИНАЮТ состояние в момент create() — например csv снимает схему по
 * заголовку файла. Вечный контекст такого плагина читал бы завтрашний файл по
 * вчерашним колонкам и молча раскладывал значения не в те поля. Поэтому для них
 * экземпляр создаётся на прогон, как и раньше. */
static bool connector_keeps_session(const char *type) {
    return !strcmp(type, "postgresql") || !strcmp(type, "postgres")
        || !strcmp(type, "greenplum")  || !strcmp(type, "oracle")
        || !strcmp(type, "kafka");
}

static bool step_conn_open(StepConn *sc, const PipelineStep *st, const char *type,
                           const char *so_path, const char *cfg, Arena *a,
                           char *err, size_t errsz) {
    sc->lease = NULL;
    sc->inst  = NULL;
    if (st->connection_id[0] && g_app.conn_pool && connector_keeps_session(type)) {
        /* Для приёмника связь не проверяем: писать можно в ещё не созданный
         * файл или таблицу, и ping там закономерно не проходит. */
        sc->lease = conn_pool_acquire(g_app.conn_pool, st->connection_id,
                                      so_path, cfg, !st->is_sink, err, errsz);
        if (!sc->lease) return false;
        sc->inst = conn_lease_inst(sc->lease);
        if (!sc->inst) { conn_pool_release(sc->lease, false); sc->lease = NULL; return false; }
        return true;
    }
    sc->inst = connector_load(so_path, cfg, a);
    if (!sc->inst) { snprintf(err, errsz, "connector_load(%s) failed", so_path); return false; }
    return true;
}

/* healthy=false после ошибки чтения/записи: постоянная сессия закрывается,
 * чтобы следующий прогон не унаследовал неизвестное состояние. */
static void step_conn_close(StepConn *sc, bool healthy) {
    if (sc->lease) { conn_pool_release(sc->lease, healthy); sc->lease = NULL; sc->inst = NULL; return; }
    if (sc->inst)  { connector_unload(sc->inst); sc->inst = NULL; }
}

/* ── Разрешение ссылки шага на подключение из справочника ────────────────────
 * Когда у шага задан connection_id, тип и параметры доступа берутся из
 * справочника, а инлайновый connector_config шага накладывается СВЕРХУ. Так
 * пошаговые ключи (query / table / cursor_column / group_id …) остаются на шаге,
 * а host/port/user/password живут в одном месте: смена пароля в справочнике
 * подхватывается всеми конвейерами со следующего запуска.
 *
 * Пустой connection_id → возвращаются поля шага как есть (старое поведение,
 * на нём работают YAML-конвейеры, MCP-сервер и интеграционные тесты).
 *
 * Результат живёт в арене прогона, поэтому слитый конфиг НЕ упирается в
 * connector_config[1024] — а именно там длинные конфиги Kafka с TLS/SASL
 * молча обрезались до битого JSON. */
static int step_effective_connector(App *app, Arena *a, const PipelineStep *st,
                                    const char **out_type, const char **out_cfg,
                                    char *errbuf, size_t errsz) {
    *out_type = st->connector_type;
    *out_cfg  = st->connector_config;
    if (!st->connection_id[0]) return 0;

    char *raw = NULL;
    if (catalog_load_connection(app->catalog, st->connection_id, &raw, a) != 0) {
        snprintf(errbuf, errsz, "шаг %s: подключение '%s' не найдено в справочнике",
                 st->id, st->connection_id);
        return -1;
    }
    JVal *rec = json_parse(a, raw, strlen(raw));
    if (!rec || rec->type != JV_OBJECT) {
        snprintf(errbuf, errsz, "шаг %s: запись подключения '%s' повреждена",
                 st->id, st->connection_id);
        return -1;
    }
    const char *ctype = json_str(json_get(rec, "type"), "");
    if (!ctype[0]) {
        snprintf(errbuf, errsz, "шаг %s: у подключения '%s' не задан тип коннектора",
                 st->id, st->connection_id);
        return -1;
    }
    /* Тип берём из справочника: параметры и тип обязаны быть согласованы, а
     * источник истины для пары «тип + параметры» — само подключение. */
    *out_type = ctype;

    JVal *over = st->connector_config[0]
               ? json_parse(a, st->connector_config, strlen(st->connector_config))
               : NULL;
    /* from_directory=true: шаг ссылается на подключение из справочника, значит
     * учётные данные берём только оттуда. */
    *out_cfg = conn_merge_config_ex(a, json_get(rec, "config"), over,
                                    true, st->connection_id);
    return 0;
}

/* ── SQL templates (see docs/SQL_TEMPLATES.md) ── */

/* The effective SQL of a step: the resolved template/substituted SQL if present,
 * else the inline transform_sql. Never NULL. */
static const char *step_sql(const PipelineStep *st) {
    return (st->resolved_sql && st->resolved_sql[0]) ? st->resolved_sql : st->transform_sql;
}

/* The table this step writes to: run-scoped output_table (auto "__dfo_*" when the
 * user left target_table empty) if set, else the explicit target_table. */
static const char *step_output(const PipelineStep *st) {
    return (st->output_table && st->output_table[0]) ? st->output_table : st->target_table;
}

/* A valid table name (keep [A-Za-z0-9_], drop the rest) from a pipeline name —
 * used to name the final result table when no target_table is given. */
/* Кириллица → латиница (UTF-8, две байты на букву: 0xD0/0xD1 + хвост).
 * Без этого русское название конвейера теряло ВСЕ символы, имя схлопывалось в
 * пустое, и данные уходили в общую таблицу "result" — где следующий такой же
 * конвейер их затирал. Возвращает длину записанного или 0, если это не буква. */
static size_t translit_cyr(unsigned char b1, unsigned char b2, char *out) {
    static const char *L[64] = {  /* А..Я  для 0xD0 0x90..0xBF */
        "A","B","V","G","D","E","Zh","Z","I","J","K","L","M","N","O","P",
        "R","S","T","U","F","H","Cz","Ch","Sh","Sch","","Y","","E","Yu","Ya",
        "a","b","v","g","d","e","zh","z","i","j","k","l","m","n","o","p" };
    static const char *L2[16] = { /* р..я для 0xD1 0x80..0x8F */
        "r","s","t","u","f","h","cz","ch","sh","sch","","y","","e","yu","ya" };
    const char *s = NULL;
    if (b1 == 0xD0 && b2 >= 0x90 && b2 <= 0xBF)      s = L[b2 - 0x90];
    else if (b1 == 0xD1 && b2 >= 0x80 && b2 <= 0x8F) s = L2[b2 - 0x80];
    else if (b1 == 0xD0 && b2 == 0x81)               s = "E";   /* Ё */
    else if (b1 == 0xD1 && b2 == 0x91)               s = "e";   /* ё */
    if (!s) return 0;
    size_t n = strlen(s);
    memcpy(out, s, n);
    return n;
}

static const char *sanitize_table_name(Arena *a, const char *name) {
    const char *src = name ? name : "";
    /* Запас ×3: одна кириллическая буква даёт до трёх латинских (Sch). */
    char *out = arena_alloc(a, strlen(src) * 3 + 1);
    size_t o = 0;
    for (const unsigned char *p = (const unsigned char *)src; *p; p++) {
        if ((*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z') ||
            (*p >= '0' && *p <= '9') || *p == '_') {
            out[o++] = (char)*p;
            continue;
        }
        if (*p >= 0xD0 && *p <= 0xD1 && p[1]) {      /* кириллица в UTF-8 */
            size_t n = translit_cyr(p[0], p[1], out + o);
            o += n;
            p++;                                      /* второй байт пары */
        }
    }
    out[o] = '\0';
    return o ? out : "result";
}

/* Cache a step's RS result for later {@step:<id>:<col>} references. */
static void pipeline_set_step_result(Pipeline *p, const char *step_id, RS *rs) {
    if (!p || p->nstep_results >= MAX_STEPS) return;
    StepResultCache *c = &p->step_results[p->nstep_results++];
    snprintf(c->step_id, sizeof(c->step_id), "%s", step_id);
    c->result_rs = rs;
}

/* Retrieve a prior step's RS by id (NULL if it has not run / was not cached). */
static RS *pipeline_get_step_result(Pipeline *p, const char *step_id) {
    if (!p) return NULL;
    for (int i = 0; i < p->nstep_results; i++)
        if (!strcmp(p->step_results[i].step_id, step_id))
            return (RS *)p->step_results[i].result_rs;
    return NULL;
}

/* Load a .sql template from sql_templates_dir (max 64 KB). Arena-allocated text,
 * or NULL on error (errbuf set). Path traversal ('..') is rejected. */
static char *load_sql_template(App *app, Arena *a, const char *rel_path,
                               char *errbuf, size_t errsz) {
    if (strstr(rel_path, "..")) {
        snprintf(errbuf, errsz, "sql_template path must not contain '..'");
        return NULL;
    }
    const char *base = (app && app->sql_templates_dir[0]) ? app->sql_templates_dir : "./sql";
    char full_path[1024];
    snprintf(full_path, sizeof(full_path), "%s/%s", base, rel_path);

    FILE *f = fopen(full_path, "rb");
    if (!f) {
        snprintf(errbuf, errsz, "cannot open sql_template '%s': %s", full_path, strerror(errno));
        return NULL;
    }
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    rewind(f);
    if (sz < 0 || sz > 65536) {
        fclose(f);
        snprintf(errbuf, errsz, "sql_template too large (%ld bytes, max 65536)", sz);
        return NULL;
    }
    char *buf = arena_alloc(a, (size_t)sz + 1);
    size_t n = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[n] = '\0';
    return buf;
}

/* Runtime context for the @computed-param resolver. */
typedef struct { App *app; Pipeline *p; const char *prev_table; } ComputedCtx;

/* SqlComputedFn: resolve @now / @last_run / @step:<id>:<col> / @prev to a value. */
static char *api_computed_param(void *vctx, Arena *a, const char *expr,
                                char *errbuf, size_t errsz) {
    ComputedCtx *cc = (ComputedCtx *)vctx;
    Pipeline *p = cc ? cc->p : NULL;

    /* {@prev} / {@input} → previous step's output table name (raw identifier),
     * the implicit pipe. NULL on the first step (nothing precedes it). */
    if (!strcmp(expr, "prev") || !strcmp(expr, "input")) {
        if (!cc || !cc->prev_table || !cc->prev_table[0]) {
            snprintf(errbuf, errsz, "{@%s}: первый шаг — нет предыдущего вывода", expr);
            return NULL;
        }
        return arena_strdup(a, cc->prev_table);   /* raw table name, unquoted */
    }

    if (!strcmp(expr, "now") || !strcmp(expr, "last_run")) {
        time_t t;
        if (!strcmp(expr, "now")) t = time(NULL);
        else t = (p && p->last_run > 0) ? (time_t)p->last_run : 0;
        struct tm tm; localtime_r(&t, &tm);
        char buf[40];
        strftime(buf, sizeof(buf), "'%Y-%m-%d %H:%M:%S'", &tm);
        return arena_strdup(a, buf);
    }
    if (!strncmp(expr, "step:", 5)) {
        char step_id[64] = {0}, col[64] = {0};
        if (sscanf(expr + 5, "%63[^:]:%63s", step_id, col) != 2) {
            snprintf(errbuf, errsz, "bad @step param: %s", expr);
            return NULL;
        }
        RS *prior = pipeline_get_step_result(p, step_id);
        if (!prior) {
            snprintf(errbuf, errsz, "@step:%s: no cached result (must run earlier)", step_id);
            return NULL;
        }
        int ci = -1;
        for (int c = 0; c < prior->ncols; c++)
            if (prior->col_names[c] && !strcasecmp(prior->col_names[c], col)) { ci = c; break; }
        if (ci < 0) {
            snprintf(errbuf, errsz, "@step:%s:%s: column not found", step_id, col);
            return NULL;
        }
        if (prior->nrows == 0) return arena_strdup(a, "NULL");  /* empty IN list */

        size_t cap = 256, off = 0;
        char  *out = arena_alloc(a, cap);
        for (int r = 0; r < prior->nrows; r++) {
            const char *v = prior->rows[r].cells ? prior->rows[r].cells[ci] : NULL;
            char *qv = v ? sql_quote_literal(a, v) : arena_strdup(a, "NULL");
            size_t ql = strlen(qv);
            while (off + ql + 2 >= cap) {
                cap *= 2; char *nb = arena_alloc(a, cap); memcpy(nb, out, off); out = nb;
            }
            if (r) out[off++] = ',';
            memcpy(out + off, qv, ql); off += ql;
        }
        out[off] = '\0';
        return out;
    }
    snprintf(errbuf, errsz, "unknown computed param @%s", expr);
    return NULL;
}

/* Resolve the effective SQL for a step: load sql_template (or use transform_sql),
 * then substitute {params}. Returns arena SQL, or NULL. NULL with errbuf[0]==0
 * means "the step has no SQL" (not an error); NULL with errbuf set is an error. */
static char *resolve_step_sql(App *app, Arena *a, Pipeline *p, PipelineStep *st,
                              const char *prev_table, char *errbuf, size_t errsz) {
    errbuf[0] = '\0';
    const char *raw_sql = NULL;
    if (st->sql_template[0]) {
        raw_sql = load_sql_template(app, a, st->sql_template, errbuf, errsz);
        if (!raw_sql) return NULL;
    } else if (st->transform_sql[0]) {
        raw_sql = st->transform_sql;
    } else {
        return NULL;  /* no SQL — not all step types need it */
    }

    if (!strchr(raw_sql, '{'))
        return arena_strdup(a, raw_sql);  /* no placeholders → as-is */

    JVal *params = NULL;
    if (st->sql_params[0])
        params = json_parse(a, st->sql_params, strlen(st->sql_params));
    ComputedCtx cc = { app, p, prev_table };
    return sql_substitute_params(a, raw_sql, params, api_computed_param, &cc, errbuf, errsz);
}

/* ── CDC cursor persistence (high-watermark) ──
 * Incremental connector reads (cursor_column set) must remember the last
 * watermark BETWEEN runs, otherwise every run re-reads the whole source. The
 * scheduler.h PipelineStep struct can't be extended here, so the watermark is
 * persisted as a tiny file under data_dir, keyed by the step's output table.
 * Path: <data_dir>/.cursors/<output>.cur  (output is a validated table name). */
static void cursor_state_path(App *app, const char *pipeline_id, const char *step_id,
                              const char *output, char *out, size_t outsz) {
    out[0] = '\0';
    if (!output || !output[0]) return;
    /* Key by pipeline + step + output, not output alone: two pipelines that write
     * the same target table from different sources must NOT share one watermark
     * (sharing would make each skip the other's unread rows). ids and table names
     * are validated elsewhere; sanitize defensively to stay filesystem-safe. */
    char key[768];
    snprintf(key, sizeof(key), "%s__%s__%s",
             pipeline_id && pipeline_id[0] ? pipeline_id : "_",
             step_id && step_id[0] ? step_id : "_", output);
    for (char *c = key; *c; c++)
        if (!((*c>='A'&&*c<='Z')||(*c>='a'&&*c<='z')||(*c>='0'&&*c<='9')||*c=='_'||*c=='-')) *c='_';
    snprintf(out, outsz, "%s/.cursors/%s.cur", app->data_dir, key);
}

/* Load a previously saved watermark into buf (empty string if none). */
static void cursor_state_load(App *app, const char *pipeline_id, const char *step_id,
                              const char *output, char *buf, size_t bufsz) {
    buf[0] = '\0';
    char path[1024];
    cursor_state_path(app, pipeline_id, step_id, output, path, sizeof(path));
    if (!path[0]) return;
    FILE *f = fopen(path, "r");
    if (!f) return;
    size_t n = fread(buf, 1, bufsz - 1, f);
    fclose(f);
    buf[n] = '\0';
    /* strip a trailing newline if present */
    while (n > 0 && (buf[n-1] == '\n' || buf[n-1] == '\r')) buf[--n] = '\0';
}

/* Persist the watermark for the next run. Best-effort: a write failure only
 * costs a re-read next time, never correctness within this run. */
static void cursor_state_save(App *app, const char *pipeline_id, const char *step_id,
                              const char *output, const char *val) {
    char path[1024];
    cursor_state_path(app, pipeline_id, step_id, output, path, sizeof(path));
    if (!path[0] || !val) return;
    /* ensure <data_dir>/.cursors exists */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s/.cursors", app->data_dir);
    mkdir(dir, 0755);
    FILE *f = fopen(path, "w");
    if (!f) return;
    fwrite(val, 1, strlen(val), f);
    fclose(f);
}

/* ── Run one connector step: pull all batches into target_table ── */
/* Шаг-источник: загружает .so-коннектор по connector_type, определяет источник
 * (query/table из config либо transform_sql) и постранично тянет батчи
 * (read_batch) в целевую таблицу, пересоздавая её из схемы первого батча.
 * Значения хранятся как TEXT (батчи всегда char*). Поддерживает инкрементальный
 * режим (cursor_column): high-watermark курсор берётся/сохраняется между
 * прогонами; иначе OFFSET-курсор по числу прочитанных строк. Возвращает число
 * загруженных строк или -1 (ошибка read_batch — с причиной коннектора). */
static int run_connector_step(App *app, Arena *a, PipelineStep *st, const char *pipeline_id,
                              char *errbuf, size_t errsz) {
    /* Тип и базовый конфиг — из справочника подключений, если шаг на него ссылается. */
    const char *eff_type = NULL, *eff_cfg = NULL;
    if (step_effective_connector(app, a, st, &eff_type, &eff_cfg, errbuf, errsz) != 0)
        return -1;

    char so_path[1024];
    const char *conn = connector_so_name(eff_type);
    snprintf(so_path, sizeof(so_path), "%s/%s_connector.so",
             app->plugins_dir, conn);

    /* Kafka: when no explicit group_id is set, inject one unique to this
     * pipeline-step so two pipelines reading the SAME topic each consume it
     * independently. A single shared default consumer group let the first reader
     * commit the topic's offsets and the next read 0 already-consumed messages
     * with a silent green success (S18). An explicit group_id still wins. */
    const char *load_cfg = eff_cfg;
    /* Написание проверяем во всех формах, которые принимает cfg_get коннектора:
     * "group.id" ловит и "kafka.group.id". Проверка одного snake_case означала,
     * что явный "group.id" оператора здесь не виден, мы подставляли свой
     * group_id — а он при конфликте побеждает — и значение оператора молча
     * перебивалось вместе с семантикой офсетов. */
    if (!strcmp(conn, "kafka") && eff_cfg[0]
        && !strstr(eff_cfg, "group_id") && !strstr(eff_cfg, "group.id")) {
        const char *cc = eff_cfg;
        const char *close = strrchr(cc, '}');
        if (close) {
            size_t n = strlen(cc) + 160;
            char *cfg_mod = arena_alloc(a, n);
            snprintf(cfg_mod, n, "%.*s,\"group_id\":\"dfo-%s-%s\"}",
                     (int)(close - cc), cc, pipeline_id ? pipeline_id : "p", st->id);
            load_cfg = cfg_mod;
        }
    }

    StepConn sc;
    if (!step_conn_open(&sc, st, eff_type, so_path, load_cfg, a, errbuf, errsz)) return -1;
    ConnectorInst *inst = sc.inst;
    const DfoConnector *api = connector_api(inst);
    void *ctx = connector_ctx(inst);

    /* Source: prefer the connector config's "query"/"table" (the «Запрос к
     * источнику»); fall back to the step's transform_sql for older pipelines. */
    const char *src = step_sql(st);
    if (eff_cfg[0]) {
        JVal *cc = json_parse(a, eff_cfg, strlen(eff_cfg));
        if (cc && cc->type == JV_OBJECT) {
            const char *q = json_str(json_get(cc, "query"), "");
            const char *t = json_str(json_get(cc, "table"), "");
            if (q[0])      src = q;
            else if (t[0]) src = t;
        }
    }
    /* If the source has spaces it's a query, not a bare table name → pass as filter */
    const char *entity = src[0] ? src : "";
    const char *filter = NULL;
    if (strchr(entity, ' ')) { filter = entity; entity = ""; }

    /* Incremental (CDC high-watermark) mode is active when the connector config
     * carries a non-empty cursor_column. In that mode the connector returns the
     * next watermark in req.cursor (last cursor_column value of the batch); we
     * must reuse that value for the next page AND persist it across runs so the
     * next run starts where this one stopped, rather than re-reading everything. */
    bool incremental = false;
    if (eff_cfg[0]) {
        JVal *cc2 = json_parse(a, eff_cfg, strlen(eff_cfg));
        if (cc2 && cc2->type == JV_OBJECT) {
            const char *ccol = json_str(json_get(cc2, "cursor_column"), "");
            if (ccol[0]) incremental = true;
        }
    }

    /* Describe schema to create the target table correctly */
    Schema *schema = NULL;
    if (!filter && entity[0] && api->describe) {
        api->describe(ctx, a, entity, &schema);
    }

    /* Create target table (schema may be NULL — will be set from first batch) */
    Table *t = NULL;
    bool table_created = false;

    int total_rows = 0;
    /* Watermark string. For OFFSET mode it's an integer offset ("0" → start of
     * table). For incremental mode it's the persisted high-watermark from the
     * previous run (empty → read from the beginning the first time). Sized to
     * hold timestamps / bigints / arbitrary key values. */
    char cursor_buf[256] = "0";
    if (incremental)
        cursor_state_load(app, pipeline_id, st->id, step_output(st), cursor_buf, sizeof(cursor_buf));

    /* Страница-арена: каждый батч (JSON-парсинг, строки, текстовая конвертация)
     * живёт ровно до записи страницы в таблицу, затем арена уничтожается.
     * Раньше все страницы копились в арене запуска — на топике в сотни МБ
     * это раздувало RSS в десятки раз и приводило к OOM-kill гейтвея
     * (наблюдалось: 2.3 ГБ RSS при загрузке 72 МБ из Kafka). */
    Arena *pa = NULL;
    for (;;) {
        if (pa) arena_destroy(pa);
        pa = arena_create(1 << 20);
        DfoReadReq req = { .cursor = cursor_buf, .limit = BATCH_SIZE, .filter = filter };
        ColBatch *batch = NULL;
        /* rc: <0 = error; 0 = ok (more pages); 1 = ok, last/only page. */
        int rc = api->read_batch(ctx, pa, &req, entity, &batch);
        if (rc < 0) {
            /* A read error is a real failure — surface the connector's reason
             * instead of silently treating it as a successful 0-row read. */
            const char *why = api->last_error ? api->last_error(ctx) : NULL;
            snprintf(errbuf, errsz, "connector step %s: read_batch failed%s%s", st->id,
                     (why && why[0]) ? ": " : "", (why && why[0]) ? why : "");
            arena_destroy(pa);
            step_conn_close(&sc, false);   /* ошибка чтения → сессию не переиспользуем */
            return -1;
        }
        if (!batch || batch->nrows == 0) {
            /* A 0-row read must still make the target reflect THIS run: recreate
             * it EMPTY so a stale prior-run stage isn't re-consumed (and doubled)
             * by a downstream append sink (S11). Also makes an empty read a
             * visible empty table rather than silently leaving none. */
            if (!table_created) {
                /* Схема может жить в странице-арене — копируем в арену шага,
                 * т.к. table_create сохраняет указатель на неё. */
                Schema *sc = schema_deep_copy(a, (batch && batch->schema) ? batch->schema : schema);
                if (sc) { recreate_table(app, a, step_output(st), sc); table_created = true; }
            }
            break;
        }

        /* Create the table on first non-empty batch */
        if (!table_created) {
            Schema *sc = schema_deep_copy(a, batch->schema ? batch->schema : schema);
            if (!sc) {
                /* Build a minimal text schema from batch metadata */
                sc = arena_calloc(a, sizeof(Schema));
                sc->ncols = batch->ncols;
                sc->cols  = arena_alloc(a, (size_t)batch->ncols * sizeof(ColDef));
                for (int c = 0; c < batch->ncols; c++) {
                    sc->cols[c].name     = arena_sprintf(a, "col%d", c);
                    sc->cols[c].type     = COL_TEXT;
                    sc->cols[c].nullable = true;
                }
            }
            t = recreate_table(app, a, step_output(st), sc);
            table_created = true;
        }

        /* Store as TEXT: the table stores every value as text, while the catalog
         * (via recreate_table above) keeps the LOGICAL type. Connectors fill
         * values[c] per the DECLARED column type (native int64 or double arrays
         * for INT64/BOOL/DOUBLE, char** for TEXT — the CSV/json_http/kafka
         * contract), so native columns are FORMATTED into text here.
         * The previous code merely relabeled every column COL_TEXT without
         * converting, so compress strcmp'd native int64s as char* pointers —
         * SIGSEGV on the first kafka/csv/json_http batch with a numeric column. */
        {
            ColType _saved[MAX_COLS];
            int _nc = batch->ncols < MAX_COLS ? batch->ncols : MAX_COLS;
            for (int c = 0; c < _nc; c++) {
                _saved[c] = batch->schema->cols[c].type;
                const uint8_t *bm = batch->null_bitmap[c];
                if (_saved[c] == COL_INT64 || _saved[c] == COL_BOOL) {
                    int64_t *iv = (int64_t *)batch->values[c];
                    char   **sv = arena_alloc(pa, (size_t)batch->nrows * sizeof(char *));
                    for (int r = 0; r < batch->nrows; r++)
                        sv[r] = (bm && ((bm[r/8] >> (r%8)) & 1u)) ? NULL
                              : arena_sprintf(pa, "%lld", (long long)iv[r]);
                    batch->values[c] = sv;
                } else if (_saved[c] == COL_DOUBLE) {
                    double *dv = (double *)batch->values[c];
                    char  **sv = arena_alloc(pa, (size_t)batch->nrows * sizeof(char *));
                    for (int r = 0; r < batch->nrows; r++)
                        sv[r] = (bm && ((bm[r/8] >> (r%8)) & 1u)) ? NULL
                              : arena_sprintf(pa, "%.15g", dv[r]);
                    batch->values[c] = sv;
                }
                batch->schema->cols[c].type = COL_TEXT;
            }
            table_append(t, batch);
            for (int c = 0; c < _nc; c++) batch->schema->cols[c].type = _saved[c];
        }
        total_rows += batch->nrows;

        /* Advance the cursor for the NEXT page.
         *  - Incremental mode: the connector wrote the next high-watermark into
         *    req.cursor (the last cursor_column value of this batch). Reuse THAT
         *    value verbatim — never overwrite it with a row count, which was the
         *    duplication bug (paging restarted from a bogus watermark and
         *    re-read rows, e.g. 8200 → 15573). If the connector left it pointing
         *    at our own buffer (no advance), fall back to the row count.
         *  - OFFSET mode: the connector reads req.cursor as an integer offset and
         *    does NOT write back, so we advance by the rows pulled so far. */
        if (incremental && req.cursor && req.cursor != cursor_buf) {
            snprintf(cursor_buf, sizeof(cursor_buf), "%s", req.cursor);
        } else {
            snprintf(cursor_buf, sizeof(cursor_buf), "%d", total_rows);
        }

        /* Конец источника — ТОЛЬКО rc==1 (последняя страница) или пустая
         * страница (проверено выше). Короткая страница концом НЕ считается:
         * у потоковых источников (Kafka) poll может недобрать страницу из-за
         * I/O-паузы брокера/диска, и прежняя эвристика «short page = EOF»
         * молча обрывала загрузку (наблюдалось: 48.1 млн строк из 56.2 млн
         * при столле диска — прогон завершился «успехом»). Цена: у
         * коннекторов, не возвращающих rc==1, конец стоит один лишний
         * read_batch с пустым результатом. */
        if (rc == 1) break;
    }
    if (pa) arena_destroy(pa);

    /* Persist the final watermark so the next run resumes from here instead of
     * re-reading the whole source. Only meaningful (and only saved) in
     * incremental mode; we save even on a 0-row run so an empty re-poll keeps
     * the existing watermark file in place (cursor_buf still holds it). */
    if (incremental)
        cursor_state_save(app, pipeline_id, st->id, step_output(st), cursor_buf);

    if (table_created)
        catalog_update_table_meta(app->catalog, step_output(st), eff_type, (int64_t)total_rows);

    step_conn_close(&sc, true);    /* успех → постоянная сессия остаётся жить в пуле */
    /* Count rows ingested from a source into "Строк загружено" + the per-minute ring. */
    if (app->metrics && total_rows > 0) {
        app->metrics->total_rows += total_rows;
        metrics_push(&app->metrics->rows_ingested, (double)total_rows);
    }
    LOG_INFO("connector step '%s' → %s: %d rows", eff_type, step_output(st), total_rows);
    return total_rows;
}

/* ── Run one sink step (приёмник): SELECT rows → connector write_batch ──
 * Reads the step's input rows from transform_sql (a SELECT over existing
 * tables) and streams them OUT to an external system via the connector's
 * write_batch. The first batch applies sink_mode (append/overwrite); later
 * batches always append. Returns rows written or <0 on error. */
static int run_sink_step(App *app, Arena *a, PipelineStep *st, char *errbuf, size_t errsz) {
    const char *src_sql = step_sql(st);
    if (!src_sql[0]) {
        snprintf(errbuf, errsz, "sink step %s: transform_sql (SELECT источника) обязателен", st->id);
        return -1;
    }
    /* 1. Load the sink connector (once per step; moved before the read loop). */
    const char *eff_type = NULL, *eff_cfg = NULL;
    if (step_effective_connector(app, a, st, &eff_type, &eff_cfg, errbuf, errsz) != 0)
        return -1;
    const char *conn = connector_so_name(eff_type);
    char so_path[1024];
    snprintf(so_path, sizeof(so_path), "%s/%s_connector.so", app->plugins_dir, conn);
    StepConn sc;
    if (!step_conn_open(&sc, st, eff_type, so_path, eff_cfg, a, errbuf, errsz)) {
        if (!errbuf[0])
            snprintf(errbuf, errsz, "sink step %s: connector_load(%s) failed — проверьте connector_config",
                     st->id, so_path);
        return -1;
    }
    ConnectorInst *inst = sc.inst;
    const DfoConnector *api = connector_api(inst);
    void *ctx = connector_ctx(inst);
    if (api->abi_version < 2 || !api->write_batch) {
        step_conn_close(&sc, false);
        snprintf(errbuf, errsz, "sink step %s: коннектор '%s' не поддерживает запись (write_batch)",
                 st->id, eff_type);
        return -1;
    }

    /* Назначение. У Kafka и REST оно живёт в собственном конфиге коннектора
     * (топик / URL), поэтому форма и не показывает для них поле «Назначение» —
     * см. makeSinkFieldsHTML в ui/app.js. Но target_table у шага мог остаться
     * от прежнего типа (переключение с историзации, YAML-импорт, легаси), и
     * тогда он МОЛЧА перебивал топик из конфига: конвейер писал в топик с
     * именем таблицы, а предпросмотр показывал правильный. Для таких
     * коннекторов target_table назначением не считаем — только явный
     * sink_entity, если он задан. */
    bool dest_in_cfg = (strcmp(eff_type, "kafka") == 0 || strcmp(eff_type, "json_http") == 0);
    const char *entity = st->sink_entity[0] ? st->sink_entity
                       : (dest_in_cfg ? "" : st->target_table);
    if (dest_in_cfg && !st->sink_entity[0] && st->target_table[0])
        LOG_INFO("sink step %s: target_table='%s' игнорируется — назначение для '%s' "
                 "задаётся в конфигурации коннектора", st->id, st->target_table, eff_type);
    int mode = (strcasecmp(st->sink_mode, "overwrite") == 0) ? DFO_SINK_OVERWRITE : DFO_SINK_APPEND;

    /* 2. Пагинация источника. exec_stmt МОЛЧА обрезает результат на
     * MAX_RS_ROWS (100k) — одиночный SELECT по большой таблице тихо терял всё
     * сверх этого (наблюдалось: sink выгрузил 100000 строк из 400000). Читаем
     * страницами LIMIT/OFFSET меньше потолка, каждая страница в собственной
     * арене — и полнота, и память O(страницы). Скан движка детерминирован
     * (порядок WAL), поэтому OFFSET-страницы не пересекаются. Явный LIMIT
     * пользователя уважаем: одна страница без обёртки. */
    char *sql_clean = arena_strdup(a, src_sql);
    size_t sql_len = strlen(sql_clean);
    while (sql_len && (sql_clean[sql_len-1]==' ' || sql_clean[sql_len-1]=='\n' ||
                       sql_clean[sql_len-1]=='\t' || sql_clean[sql_len-1]==';'))
        sql_clean[--sql_len] = '\0';
    bool user_limited = false;
    for (const char *p = sql_clean; *p; p++) {
        if ((p == sql_clean || (!isalnum((unsigned char)p[-1]) && p[-1] != '_'))
            && strncasecmp(p, "limit", 5) == 0
            && !isalnum((unsigned char)p[5]) && p[5] != '_') { user_limited = true; break; }
    }
    /* Хвостовой «LIMIT N» (без OFFSET): при N > MAX_RS_ROWS одна страница
     * усекается и truncated-страж валит шаг. Снимаем суффикс и превращаем N
     * в постраничный предел — выгрузка страницами до N строк. */
    long long user_cap = -1;
    if (user_limited) {
        size_t e = strlen(sql_clean), d = e;
        while (d > 0 && isdigit((unsigned char)sql_clean[d-1])) d--;
        if (d < e && d >= 6) {
            size_t k = d;
            while (k > 0 && (sql_clean[k-1]==' '||sql_clean[k-1]=='\n'||sql_clean[k-1]=='\t')) k--;
            if (k >= 5 && strncasecmp(sql_clean + k - 5, "limit", 5) == 0 &&
                (k == 5 || (!isalnum((unsigned char)sql_clean[k-6]) && sql_clean[k-6] != '_'))) {
                user_cap = atoll(sql_clean + d);
                sql_clean[k - 5] = '\0';
                size_t sl2 = strlen(sql_clean);
                while (sl2 && (sql_clean[sl2-1]==' '||sql_clean[sl2-1]=='\n'||sql_clean[sl2-1]=='\t'))
                    sql_clean[--sl2] = '\0';
                user_limited = false;    /* дальше обычная пагинация до user_cap */
            }
        }
    }
    const int SINK_PAGE = 50000;

    int total = 0, first = 1;
    for (long long page_off = 0; ; page_off += SINK_PAGE) {
    if (user_cap >= 0 && page_off >= user_cap) break;      /* пользовательский предел исчерпан */
    long long page_lim = SINK_PAGE;
    if (user_cap >= 0 && user_cap - page_off < page_lim) page_lim = user_cap - page_off;
    Arena *pa = arena_create(1 << 20);
    const char *psql = user_limited ? sql_clean
        : arena_sprintf(pa, "%s LIMIT %lld OFFSET %lld", sql_clean, page_lim, page_off);
    Stmt *stmt = sql_parse(pa, psql, strlen(psql));
    if (stmt->error) {
        snprintf(errbuf, errsz, "sink step %s: parse: %s", st->id, stmt->error);
        arena_destroy(pa); step_conn_close(&sc, false);
        return -1;
    }
    tl_exec_error[0] = '\0';
    RS *rs = exec_stmt(pa, stmt, NULL);
    if (tl_exec_error[0]) {
        /* Source table doesn't exist → fail the sink step (status=1) instead of
         * silently writing 0 rows out. Distinct from an existing-but-empty
         * source, which yields a valid 0-row rs and takes the success path. */
        snprintf(errbuf, errsz, "sink step %s: %s", st->id, tl_exec_error);
        tl_exec_error[0] = '\0';
        arena_destroy(pa); step_conn_close(&sc, false);
        return -1;
    }
    if (!rs) {
        snprintf(errbuf, errsz, "sink step %s: исходный SELECT вернул NULL", st->id);
        arena_destroy(pa); step_conn_close(&sc, false);
        return -1;
    }
    if (rs->truncated) {
        /* Страница усечена потолком ДО среза LIMIT/OFFSET — так бывает, когда
         * ранняя пагинация неприменима (ORDER BY/агрегация/DISTINCT/окна на
         * >MAX_RS_ROWS строк): выгрузка была бы неполной. Отказ вместо тихой
         * потери. */
        snprintf(errbuf, errsz, "sink step %s: запрос с сортировкой/агрегацией на > %d строк "
                 "не может быть выгружен полностью — уберите ORDER BY/DISTINCT или сузьте выборку.",
                 st->id, MAX_RS_ROWS);
        arena_destroy(pa); step_conn_close(&sc, false);
        return -1;
    }

    /* 3. Схема страницы из колонок результата. Значения всегда текст
     * (text-always), но логический тип сохраняем, чтобы типизированный sink
     * (например PG) создавал BIGINT/NUMERIC/BOOLEAN, а не сплошной TEXT.
     * NULL идут через null_bitmap (настоящий NULL, не текст-сентинел). Схема
     * живёт в арене страницы: write_batch не удерживает указателей. */
    Schema *schema = arena_calloc(pa, sizeof(Schema));
    schema->ncols = rs->ncols;
    schema->cols  = arena_alloc(pa, (size_t)rs->ncols * sizeof(ColDef));
    for (int c = 0; c < rs->ncols; c++) {
        ColType lt = rs->col_types ? rs->col_types[c] : COL_TEXT;
        schema->cols[c].name     = rs->col_names[c] ? rs->col_names[c] : "col";
        schema->cols[c].type     = (lt==COL_INT64||lt==COL_DOUBLE||lt==COL_BOOL) ? lt : COL_TEXT;
        schema->cols[c].nullable = true;
    }

    /* 4. Stream rows out in BATCH_SIZE chunks. */
    char ***cv = arena_alloc(pa, (size_t)rs->ncols * sizeof(char **));
    for (int c = 0; c < rs->ncols; c++)
        cv[c] = arena_alloc(pa, BATCH_SIZE * sizeof(char *));
    ColBatch batch = {0};
    batch.schema = schema; batch.ncols = rs->ncols;
    for (int c = 0; c < rs->ncols; c++) batch.values[c] = cv[c];

    /* ВНИМАНИЕ: здесь НЕЛЬЗЯ заводить свои total/first — они объявлены снаружи
     * страничного цикла. Локальные копии перекрывали внешние, из-за чего
     * шаг-приёмник всегда возвращал 0 строк, а флаг first сбрасывался на каждой
     * странице: в режиме overwrite каждая следующая страница затирала
     * предыдущую, и от выгрузки больше SINK_PAGE строк оставался только хвост. */
    for (int r = 0; r < rs->nrows; r++) {
        for (int c = 0; c < rs->ncols; c++) {
            const char *v = rs->rows[r].cells ? rs->rows[r].cells[c] : "";
            int rr = batch.nrows;
            if (v && strcmp(v, DFO_NULL_SENTINEL) == 0) {
                /* True SQL NULL → flag null_bitmap so write_batch emits a real
                 * NULL to the sink instead of the sentinel text. */
                if (!batch.null_bitmap[c])
                    batch.null_bitmap[c] = arena_calloc(pa, (BATCH_SIZE + 7) / 8);
                batch.null_bitmap[c][rr / 8] |= (uint8_t)(1u << (rr % 8));
                cv[c][rr] = (char *)"";
            } else {
                if (batch.null_bitmap[c])
                    batch.null_bitmap[c][rr / 8] &= (uint8_t)~(1u << (rr % 8));
                cv[c][rr] = (char *)(v ? v : "");
            }
        }
        if (++batch.nrows == BATCH_SIZE) {
            int n = api->write_batch(ctx, pa, entity, schema, &batch, first ? mode : DFO_SINK_APPEND);
            if (n < 0) {
                const char *why = api->last_error ? api->last_error(ctx) : NULL;
                snprintf(errbuf, errsz, "sink step %s: write_batch failed%s%s", st->id,
                         (why && why[0]) ? ": " : "", (why && why[0]) ? why : "");
                arena_destroy(pa); step_conn_close(&sc, false); return -1; }
            total += batch.nrows; batch.nrows = 0; first = 0;
        }
    }
    /* Хвост страницы; на первой странице пишем и при 0 строк, чтобы
     * overwrite-режим опустошил приёмник даже при пустом источнике. */
    if (batch.nrows > 0 || (first && page_off == 0)) {
        int n = api->write_batch(ctx, pa, entity, schema, &batch, first ? mode : DFO_SINK_APPEND);
        if (n < 0) {
            const char *why = api->last_error ? api->last_error(ctx) : NULL;
            snprintf(errbuf, errsz, "sink step %s: write_batch failed%s%s", st->id,
                     (why && why[0]) ? ": " : "", (why && why[0]) ? why : "");
            arena_destroy(pa); step_conn_close(&sc, false); return -1; }
        total += batch.nrows; first = 0;
    }

    int page_rows = rs->nrows;
    arena_destroy(pa);
    if (user_limited || (long long)page_rows < page_lim) break;
    }  /* конец страничного цикла */

    step_conn_close(&sc, true);   /* успех → постоянная сессия остаётся жить */
    LOG_INFO("sink step '%s' → %s:%s: %d rows (%s)", st->id, eff_type,
             entity, total, st->sink_mode[0] ? st->sink_mode : "append");
    return total;
}

/* ── Script steps (Python / Scala) ──────────────────────────────
 * A "script step" hands the step's input rows to a subprocess as CSV
 * on stdin, runs user code that mutates a tabular `df`, and reads the
 * result back as CSV on stdout. Python (pandas) and Scala (Spark) are
 * two front-ends over the exact same plumbing; the three helpers below
 * are shared so adding a language is just "build a wrapper + argv".
 *
 * Why subprocess and not an embedded runtime:
 * - keeps the gateway's pure-C core free of libpython / a JVM dependency
 * - matches the trust model of the existing bash/connector step
 *   (no sandbox; same uid as gateway)
 * - lets users install pandas / scala-cli without coordinating with the
 *   gateway lifecycle
 *
 * Error handling: on non-zero exit, the child's stderr tail is captured
 * into `errbuf`; the step is marked failed and retry semantics apply
 * normally. `label` is a human prefix like "python step s1" reused in
 * every diagnostic so messages are language-aware. */

/* 1. Materialize the step's transform_sql result as a CSV blob (header +
 * rows). Empty transform_sql → empty string. 0 on success (sets
 * out_csv/out_len), -1 on SQL parse/exec error (errbuf set). */
static int script_step_input_csv(Arena *a, PipelineStep *st, const char *label,
                                 char **out_csv, size_t *out_len,
                                 char *errbuf, size_t errsz) {
    char *input_csv = NULL;
    size_t input_len = 0;
    if (step_sql(st)[0]) {
        const char *isql = step_sql(st);
        Stmt *stmt = sql_parse(a, isql, strlen(isql));
        if (!stmt || stmt->error) {
            snprintf(errbuf, errsz, "%s: input SQL parse error: %s",
                     label, stmt && stmt->error ? stmt->error : "null");
            return -1;
        }
        tl_exec_error[0] = '\0';
        RS *rs = exec_stmt(a, stmt, NULL);
        if (tl_exec_error[0]) {
            snprintf(errbuf, errsz, "%s: %s", label, tl_exec_error);
            tl_exec_error[0] = '\0';
            return -1;
        }
        if (!rs) {
            snprintf(errbuf, errsz, "%s: input SQL exec failed", label);
            return -1;
        }
        /* Build CSV: header + rows */
        size_t cap = 4096; size_t off = 0;
        input_csv = arena_alloc(a, cap);
        #define CSV_RESERVE(n) do { \
            if (off + (n) + 1 > cap) { \
                while (off + (n) + 1 > cap) cap *= 2; \
                char *nb = arena_alloc(a, cap); memcpy(nb, input_csv, off); input_csv = nb; \
            } } while (0)
        for (int c = 0; c < rs->ncols; c++) {
            const char *cn = rs->col_names[c] ? rs->col_names[c] : "";
            size_t cl = strlen(cn);
            CSV_RESERVE(cl + 2);
            if (c) input_csv[off++] = ',';
            memcpy(input_csv + off, cn, cl); off += cl;
        }
        CSV_RESERVE(1); input_csv[off++] = '\n';
        for (int r = 0; r < rs->nrows; r++) {
            for (int c = 0; c < rs->ncols; c++) {
                const char *v = rs->rows[r].cells ? rs->rows[r].cells[c] : "";
                if (!v) v = "";
                size_t vl = strlen(v);
                /* Quote any value with comma/quote/newline */
                int needs_q = 0;
                for (size_t i = 0; i < vl; i++)
                    if (v[i] == ',' || v[i] == '"' || v[i] == '\n') { needs_q = 1; break; }
                CSV_RESERVE(vl * 2 + 4);
                if (c) input_csv[off++] = ',';
                if (needs_q) {
                    input_csv[off++] = '"';
                    for (size_t i = 0; i < vl; i++) {
                        if (v[i] == '"') input_csv[off++] = '"';
                        input_csv[off++] = v[i];
                    }
                    input_csv[off++] = '"';
                } else {
                    memcpy(input_csv + off, v, vl); off += vl;
                }
            }
            CSV_RESERVE(1); input_csv[off++] = '\n';
        }
        input_csv[off] = '\0';
        input_len = off;
        #undef CSV_RESERVE
    } else {
        input_csv = (char *)"";
        input_len = 0;
    }
    *out_csv = input_csv;
    *out_len = input_len;
    return 0;
}

/* 2. Spawn argv[] (argv[0] resolved via $PATH), write input_csv to its
 * stdin, collect stdout (CSV) into *out_buf_p and the tail of stderr for
 * diagnostics. Enforces a wall-clock timeout (SIGKILL on expiry).
 * 0 on a clean exit(0); -1 otherwise (errbuf set, incl. stderr tail). */
static int run_csv_subprocess(Arena *a, char *const argv[],
                              const char *input_csv, size_t input_len,
                              int timeout_sec, const char *label,
                              char **out_buf_p, size_t *out_len_p,
                              char *errbuf, size_t errsz,
                              char *const envextra[]) {
    int in_pipe[2], out_pipe[2], err_pipe[2];
    if (pipe(in_pipe) || pipe(out_pipe) || pipe(err_pipe)) {
        snprintf(errbuf, errsz, "%s: pipe() failed", label);
        return -1;
    }
    /* Build the child's full environment AND resolve the binary to an absolute
     * path HERE, in the parent (before fork). After fork() in a multithreaded
     * process (the HTTP server now runs per-connection threads) the child may
     * call only async-signal-safe functions before exec — putenv() and execvp()
     * can take the allocator lock and would DEADLOCK the child if another thread
     * held it at the fork instant. execve() with a pre-built envp + absolute path
     * is async-signal-safe, so the child does no allocation. */
    extern char **environ;
    int n_env = 0; while (environ && environ[n_env]) n_env++;
    int n_extra = 0; while (envextra && envextra[n_extra]) n_extra++;
    char **child_envp = arena_alloc(a, (size_t)(n_env + n_extra + 1) * sizeof(char *));
    { int ei = 0;
      for (int i = 0; i < n_env;   i++) child_envp[ei++] = environ[i];
      for (int i = 0; i < n_extra; i++) child_envp[ei++] = envextra[i];
      child_envp[ei] = NULL; }
    char child_path[1024];
    if (strchr(argv[0], '/')) {
        snprintf(child_path, sizeof(child_path), "%s", argv[0]);
    } else {                                 /* PATH search in the parent (malloc-safe) */
        child_path[0] = '\0';
        const char *path = getenv("PATH"); if (!path || !*path) path = "/usr/bin:/bin:/usr/local/bin";
        char pbuf[4096]; snprintf(pbuf, sizeof(pbuf), "%s", path);
        char *save = NULL;
        for (char *dir = strtok_r(pbuf, ":", &save); dir; dir = strtok_r(NULL, ":", &save)) {
            char cand[1024]; snprintf(cand, sizeof(cand), "%s/%s", dir, argv[0]);
            if (access(cand, X_OK) == 0) { snprintf(child_path, sizeof(child_path), "%s", cand); break; }
        }
        if (!child_path[0]) snprintf(child_path, sizeof(child_path), "%s", argv[0]);  /* execve fails cleanly */
    }

    pid_t pid = fork();
    if (pid < 0) {
        snprintf(errbuf, errsz, "%s: fork() failed", label);
        return -1;
    }
    if (pid == 0) {
        /* CHILD — async-signal-safe only: dup2/close/execve, no malloc. */
        dup2(in_pipe[0],  STDIN_FILENO);
        dup2(out_pipe[1], STDOUT_FILENO);
        dup2(err_pipe[1], STDERR_FILENO);
        close(in_pipe[0]); close(in_pipe[1]);
        close(out_pipe[0]); close(out_pipe[1]);
        close(err_pipe[0]); close(err_pipe[1]);
        execve(child_path, argv, child_envp);
        _exit(127);
    }
    /* PARENT */
    close(in_pipe[0]); close(out_pipe[1]); close(err_pipe[1]);

    /* Write input CSV. Simple: write all at once since CSV is bounded by
     * query result size. */
    if (input_len > 0) {
        ssize_t left = (ssize_t)input_len;
        const char *p = input_csv;
        while (left > 0) {
            ssize_t w = write(in_pipe[1], p, (size_t)left);
            if (w < 0) { if (errno == EINTR) continue; break; }
            p += w; left -= w;
        }
    }
    close(in_pipe[1]);

    /* Read stdout + stderr with a per-fd buffer. poll() with the timeout. */
    int timeout_ms = (timeout_sec > 0 ? timeout_sec : 300) * 1000;
    int64_t deadline = (int64_t)time(NULL) * 1000 + timeout_ms;
    size_t out_cap = 65536, out_len = 0;
    char *out_buf = arena_alloc(a, out_cap);
    char err_tail[1024]; size_t err_len = 0;

    fcntl(out_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(err_pipe[0], F_SETFL, O_NONBLOCK);
    int fds_open = 2;
    while (fds_open > 0) {
        int64_t now_ms = (int64_t)time(NULL) * 1000;
        if (now_ms >= deadline) {
            kill(pid, SIGKILL);
            snprintf(errbuf, errsz, "%s: timeout (%ds)",
                     label, timeout_sec > 0 ? timeout_sec : 300);
            close(out_pipe[0]); close(err_pipe[0]);
            int s; waitpid(pid, &s, 0);
            return -1;
        }
        struct pollfd pfd[2] = {
            { .fd = out_pipe[0], .events = POLLIN },
            { .fd = err_pipe[0], .events = POLLIN },
        };
        int rc = poll(pfd, 2, 200);
        if (rc < 0) { if (errno == EINTR) continue; break; }
        char tmp[4096];
        for (int i = 0; i < 2; i++) {
            if (!(pfd[i].revents & (POLLIN | POLLHUP))) continue;
            ssize_t r = read(pfd[i].fd, tmp, sizeof(tmp));
            if (r > 0) {
                if (i == 0) {
                    if (out_len + (size_t)r + 1 > out_cap) {
                        while (out_len + (size_t)r + 1 > out_cap) out_cap *= 2;
                        char *nb = arena_alloc(a, out_cap);
                        memcpy(nb, out_buf, out_len);
                        out_buf = nb;
                    }
                    memcpy(out_buf + out_len, tmp, (size_t)r);
                    out_len += (size_t)r;
                } else {
                    /* err: keep just the tail (last 1KB) for diagnostics */
                    size_t take = (size_t)r > sizeof(err_tail) - 1
                                    ? sizeof(err_tail) - 1 : (size_t)r;
                    if (err_len + take > sizeof(err_tail) - 1) {
                        size_t shift = err_len + take - (sizeof(err_tail) - 1);
                        memmove(err_tail, err_tail + shift, err_len - shift);
                        err_len -= shift;
                    }
                    memcpy(err_tail + err_len, tmp + ((size_t)r - take), take);
                    err_len += take;
                }
            } else if (r == 0 || (r < 0 && errno != EAGAIN)) {
                close(pfd[i].fd); pfd[i].fd = -1; fds_open--;
            }
        }
    }
    out_buf[out_len] = '\0';
    err_tail[err_len] = '\0';

    int wstatus = 0;
    waitpid(pid, &wstatus, 0);
    if (!WIFEXITED(wstatus) || WEXITSTATUS(wstatus) != 0) {
        snprintf(errbuf, errsz, "%s: exit=%d, stderr: %.300s",
                 label, WEXITSTATUS(wstatus), err_tail);
        return -1;
    }
    *out_buf_p = out_buf;
    *out_len_p = out_len;
    return 0;
}

/* 3. Parse the subprocess's CSV stdout and ingest into st->target_table.
 * Returns rows written (0 if no target / empty output), -1 on malformed
 * CSV (errbuf set). out_buf is mutated in place during tokenization. */
static int script_step_ingest_output(App *app, Arena *a, PipelineStep *st,
                                      const char *label,
                                      char *out_buf, size_t out_len,
                                      char *errbuf, size_t errsz) {
    /* Reuse the same code path as POST /api/ingest/csv: parse the CSV
     * ourselves into a Schema+rows and call write_rs_to_table. */
    if (!step_output(st)[0] || out_len == 0) {
        return 0;
    }
    /* Header line. Parse with the RFC-4180 splitter (same as /api/ingest/csv):
     * a naive strtok(",") split inside quoted fields ("Иванов, Иван", a JSON
     * payload) and collapsed empty fields (,,), shifting every later column and
     * silently scrambling identity/classification data at status=0 (J2). */
    char *nl = strchr(out_buf, '\n');
    if (!nl) {
        snprintf(errbuf, errsz, "%s: output CSV has no header", label);
        return -1;
    }
    *nl = '\0';
    int header_ncols = 0, herr = 0;
    char **col_names = split_line_rfc4180(a, out_buf, ',', &header_ncols, &herr);
    if (!col_names || header_ncols <= 0) {
        snprintf(errbuf, errsz, "%s: malformed output CSV header", label);
        return -1;
    }

    /* Build RS with the rest as text rows, aligned to the header by position. */
    RS *rs = rs_new(a, header_ncols, col_names, 0);
    char *cursor = nl + 1;
    while (cursor && *cursor) {
        char *line_end = strchr(cursor, '\n');
        if (line_end) *line_end = '\0';
        if (*cursor == '\0') { cursor = line_end ? line_end + 1 : NULL; continue; }
        int nf = 0, perr = 0;
        char **fields = split_line_rfc4180(a, cursor, ',', &nf, &perr);
        char **cells = arena_alloc(a, (size_t)header_ncols * sizeof(char *));
        for (int i = 0; i < header_ncols; i++)
            /* Empty/absent field → SQL NULL, matching /api/ingest/csv so a pandas
             * NaN/empty cell stays NULL instead of an empty string. */
            cells[i] = (i < nf && fields && fields[i] && fields[i][0])
                       ? fields[i] : (char *)DFO_NULL_SENTINEL;
        rs_add(rs, a, cells, NULL);
        cursor = line_end ? line_end + 1 : NULL;
    }
    if (rs->truncated) {
        snprintf(errbuf, errsz, "%s: вывод скрипта превышает %d строк — усечён, "
                 "шаг остановлен, чтобы не записать неполные данные.", label, MAX_RS_ROWS);
        return -1;
    }

    int rows_written = write_rs_to_table(app, a, step_output(st), rs);
    LOG_INFO("%s → %s: %d rows", label, step_output(st), rows_written);
    return rows_written;
}

/* Python step: `python3 -c <wrapper>`. The wrapper reads stdin CSV into a
 * pandas DataFrame `df`, runs the user code, writes `df` back as CSV. */
static int run_python_step(App *app, Arena *a, PipelineStep *st,
                           char *errbuf, size_t errsz) {
    char label[128];
    snprintf(label, sizeof(label), "python step %s", st->id);

    char *input_csv; size_t input_len;
    if (script_step_input_csv(a, st, label, &input_csv, &input_len, errbuf, errsz) < 0)
        return -1;

    /* Source of the user code: a .py file on disk (no size limit beyond 512 KB)
     * or the inline python_code field (≤ 8 KB). python_file wins when both set. */
    const char *user_code = NULL;
    if (st->python_file[0]) {
        FILE *f = fopen(st->python_file, "r");
        if (!f) {
            snprintf(errbuf, errsz, "python step %s: cannot open python_file '%s': %s",
                     st->id, st->python_file, strerror(errno));
            return -1;
        }
        fseek(f, 0, SEEK_END);
        long fsz = ftell(f);
        rewind(f);
        if (fsz < 0 || fsz > 524288) {   /* 512 KB hard limit */
            fclose(f);
            snprintf(errbuf, errsz, "python step %s: python_file too large (%ld bytes, max 524288)",
                     st->id, fsz);
            return -1;
        }
        char *file_buf = arena_alloc(a, (size_t)fsz + 1);
        size_t nread = fread(file_buf, 1, (size_t)fsz, f);
        fclose(f);
        file_buf[nread] = '\0';
        user_code = file_buf;
        LOG_INFO("python step '%s': loaded %zu bytes from %s", st->id, nread, st->python_file);
    } else {
        user_code = st->python_code;
        if (!user_code[0]) {
            snprintf(errbuf, errsz, "python step %s: neither python_code nor python_file set", st->id);
            return -1;
        }
    }

    size_t wrap_cap = strlen(user_code) + 1024;
    char *wrapper = arena_alloc(a, wrap_cap);
    snprintf(wrapper, wrap_cap,
        "import sys, io\n"
        "try:\n"
        "    import pandas as pd\n"
        "except ImportError:\n"
        "    sys.stderr.write('error: `pandas` is not installed for python3. '\n"
        "                     'Install with: python3 -m pip install pandas\\n')\n"
        "    sys.exit(2)\n"
        "_csv_in = sys.stdin.read()\n"
        "df = pd.read_csv(io.StringIO(_csv_in)) if _csv_in.strip() else pd.DataFrame()\n"
        "# Route user print()/stdout to stderr so diagnostics never mix into the\n"
        "# result CSV (a single print() used to corrupt the target table silently).\n"
        "_dfo_out = sys.stdout; sys.stdout = sys.stderr\n"
        "# ── user code ──\n"
        "%s\n"
        "# ── /user code ──\n"
        "sys.stdout = _dfo_out\n"
        "df.to_csv(sys.stdout, index=False)\n",
        user_code);

    /* Optional shared context directory for stateful multi-step pipelines:
     * mkdirp it and expose DFO_CONTEXT_DIR + DFO_STEP_ID to the subprocess. */
    char *envextra[3]; int nenv = 0;
    if (st->python_context_dir[0]) {
        char tmp[512];
        strncpy(tmp, st->python_context_dir, sizeof(tmp) - 1);
        tmp[sizeof(tmp) - 1] = '\0';
        for (char *p = tmp + 1; *p; p++) {
            if (*p == '/') { *p = '\0'; mkdir(tmp, 0755); *p = '/'; }
        }
        mkdir(st->python_context_dir, 0755);   /* ignore EEXIST */
        envextra[nenv++] = arena_sprintf(a, "DFO_CONTEXT_DIR=%s", st->python_context_dir);
    }
    envextra[nenv++] = arena_sprintf(a, "DFO_STEP_ID=%s", st->id);
    envextra[nenv] = NULL;

    char *argv[] = { (char *)"python3", (char *)"-c", wrapper, NULL };
    int timeout = st->python_timeout_sec > 0 ? st->python_timeout_sec : 300;
    char *out_buf; size_t out_len;
    if (run_csv_subprocess(a, argv, input_csv, input_len, timeout, label,
                           &out_buf, &out_len, errbuf, errsz, envextra) < 0)
        return -1;

    return script_step_ingest_output(app, a, st, label, out_buf, out_len, errbuf, errsz);
}

/* ── Scala step ─────────────────────────────────────────────────
 * Runs `scala-cli run <script.scala>` as a subprocess. The script spins up
 * an embedded local[*] SparkSession, reads stdin CSV into a Spark DataFrame
 * `df`, runs the user's Scala code, then writes `df` back to stdout as CSV.
 * On par with the Python step — same input/output plumbing, different
 * front-end. Spark dependency is pulled by scala-cli via a `//> using`
 * directive, so no separate cluster is required.
 *
 * Unlike `python3 -c`, scala-cli takes a script *file* (stdin carries the
 * data), so the wrapper is written to a temp `.scala` file in a private temp
 * dir (scala-cli drops a `.scala-build/` next to it), executed, then the whole
 * dir is removed afterwards. */

/* Recursively delete a file or directory tree. Best-effort: failures are
 * ignored (the path lives under /tmp). */
static void rmrf_path(const char *path) {
    struct stat stbuf;
    if (lstat(path, &stbuf) != 0) return;
    if (S_ISDIR(stbuf.st_mode)) {
        DIR *d = opendir(path);
        if (d) {
            struct dirent *e;
            while ((e = readdir(d))) {
                if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
                char child[2048];
                snprintf(child, sizeof(child), "%s/%s", path, e->d_name);
                rmrf_path(child);
            }
            closedir(d);
        }
        rmdir(path);
    } else {
        unlink(path);
    }
}

/* Шаг Scala: вход предыдущего шага выгружается в CSV, оборачивается в скрипт с
 * пользовательским кодом (df: DataFrame), исполняется, результат ингестится назад.
 * Возвращает >=0 (число строк) либо -1 c сообщением в errbuf. */
static int run_scala_step(App *app, Arena *a, PipelineStep *st,
                          char *errbuf, size_t errsz) {
    char label[128];
    snprintf(label, sizeof(label), "scala step %s", st->id);

    char *input_csv; size_t input_len;
    if (script_step_input_csv(a, st, label, &input_csv, &input_len, errbuf, errsz) < 0)
        return -1;

    /* Build the wrapper script. The user code is interpolated as-is into
     * main(), where `df` is an in-scope `var df: DataFrame`. All columns are
     * read as String (mirrors the CSV round-trip of the Python step). */
    const char *user_code = st->scala_code;
    size_t wrap_cap = strlen(user_code) + 4096;
    char *wrapper = arena_alloc(a, wrap_cap);
    snprintf(wrapper, wrap_cap,
        "//> using scala 2.13.14\n"
        "//> using dep org.apache.spark::spark-sql:3.5.1\n"
        /* Spark 3.5 on JDK 17 needs these module opens or SparkContext init
         * dies with IllegalAccessError on sun.nio.ch.DirectBuffer. Harmless
         * on JDK 11/8. scala-cli passes them at JVM launch. */
        "//> using javaOpt \"--add-opens=java.base/java.lang=ALL-UNNAMED\", "
        "\"--add-opens=java.base/java.lang.invoke=ALL-UNNAMED\", "
        "\"--add-opens=java.base/java.io=ALL-UNNAMED\", "
        "\"--add-opens=java.base/java.net=ALL-UNNAMED\", "
        "\"--add-opens=java.base/java.nio=ALL-UNNAMED\", "
        "\"--add-opens=java.base/java.util=ALL-UNNAMED\", "
        "\"--add-opens=java.base/java.util.concurrent=ALL-UNNAMED\", "
        "\"--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED\", "
        "\"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\", "
        "\"--add-opens=java.base/sun.nio.cs=ALL-UNNAMED\", "
        "\"--add-opens=java.base/sun.security.action=ALL-UNNAMED\", "
        "\"--add-opens=java.base/sun.util.calendar=ALL-UNNAMED\"\n"
        "import org.apache.spark.sql.{SparkSession, DataFrame}\n"
        "object DfoScalaStep {\n"
        "  def esc(s: String): String =\n"
        "    if (s.contains(\",\") || s.contains(\"\\\"\") || s.contains(\"\\n\"))\n"
        "      \"\\\"\" + s.replace(\"\\\"\", \"\\\"\\\"\") + \"\\\"\" else s\n"
        "  def main(args: Array[String]): Unit = {\n"
        "    val spark = SparkSession.builder()\n"
        "      .appName(\"dfo-scala-step\").master(\"local[*]\")\n"
        "      .config(\"spark.ui.enabled\", \"false\")\n"
        "      .config(\"spark.sql.shuffle.partitions\", \"4\")\n"
        "      .getOrCreate()\n"
        "    spark.sparkContext.setLogLevel(\"ERROR\")\n"
        "    import spark.implicits._\n"
        "    val lines = scala.io.Source.stdin.getLines().toList\n"
        "    var df: DataFrame =\n"
        "      if (lines.isEmpty || lines.forall(_.trim.isEmpty)) spark.emptyDataFrame\n"
        "      else spark.read.option(\"header\", \"true\").option(\"inferSchema\", \"false\").csv(lines.toDS())\n"
        "    // Route user println/Console output to stderr so diagnostics never\n"
        "    // mix into the result CSV the engine reads from stdout (same class of\n"
        "    // silent target-table corruption fixed for python steps). `df` is an\n"
        "    // outer var, so user reassignments inside the block still take effect.\n"
        "    // ── user code ──\n"
        "    Console.withOut(System.err) {\n"
        "    %s\n"
        "    }\n"
        "    // ── /user code ──\n"
        "    val cols = df.columns\n"
        "    val sb = new StringBuilder\n"
        "    if (cols.nonEmpty) {\n"
        "      sb.append(cols.map(esc).mkString(\",\")).append(\"\\n\")\n"
        "      df.collect().foreach { r =>\n"
        "        sb.append((0 until r.length).map(i => esc(Option(r.get(i)).map(_.toString).getOrElse(\"\"))).mkString(\",\")).append(\"\\n\")\n"
        "      }\n"
        "    }\n"
        "    print(sb.toString)\n"
        "    spark.stop()\n"
        "  }\n"
        "}\n",
        user_code);

    /* Write the wrapper to a temp .scala file inside a private temp dir.
     * scala-cli needs a file path (stdin is reserved for the input data) and
     * a `.scala` extension to recognise the source. */
    char tmpdir[] = "/tmp/dfo_scala_XXXXXX";
    if (!mkdtemp(tmpdir)) {
        snprintf(errbuf, errsz, "%s: mkdtemp() failed: %s", label, strerror(errno));
        return -1;
    }
    char script_path[256];
    snprintf(script_path, sizeof(script_path), "%s/step.scala", tmpdir);
    int sfd = open(script_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (sfd < 0) {
        snprintf(errbuf, errsz, "%s: open(%s) failed: %s", label, script_path, strerror(errno));
        rmrf_path(tmpdir);
        return -1;
    }
    {
        ssize_t left = (ssize_t)strlen(wrapper);
        const char *p = wrapper;
        while (left > 0) {
            ssize_t w = write(sfd, p, (size_t)left);
            if (w < 0) { if (errno == EINTR) continue; break; }
            p += w; left -= w;
        }
        close(sfd);
    }

    char *argv[] = { (char *)"scala-cli", (char *)"run", (char *)"--quiet",
                     script_path, NULL };
    int timeout = st->scala_timeout_sec > 0 ? st->scala_timeout_sec : 600;
    char *out_buf; size_t out_len;
    int rc = run_csv_subprocess(a, argv, input_csv, input_len, timeout, label,
                                &out_buf, &out_len, errbuf, errsz, NULL);
    rmrf_path(tmpdir);
    if (rc < 0) return -1;

    return script_step_ingest_output(app, a, st, label, out_buf, out_len, errbuf, errsz);
}

/* ── SCD2 helpers ───────────────────────────────────────────────── */
#define SCD2_MAX_COLS 64

/* Index of a column by (case-insensitive) name, or -1. */
static int scd2_rs_col_index(const RS *rs, const char *name) {
    if (!rs || !name || !name[0]) return -1;
    for (int c = 0; c < rs->ncols; c++)
        if (rs->col_names[c] && !strcasecmp(rs->col_names[c], name)) return c;
    return -1;
}

/* Split a comma-separated list into trimmed tokens (arena-copied). Returns count. */
static int scd2_split_list(Arena *a, const char *s, char **out, int max) {
    int n = 0;
    if (!s || !s[0]) return 0;
    const char *p = s;
    while (*p && n < max) {
        while (*p == ' ' || *p == ',') p++;
        if (!*p) break;
        const char *start = p;
        while (*p && *p != ',') p++;
        const char *end = p;
        while (end > start && (end[-1] == ' ')) end--;
        if (end > start) out[n++] = arena_strndup(a, start, (size_t)(end - start));
    }
    return n;
}

/* Compose a single key string from the business-key column cells (US-separated
 * so multi-column keys can't collide). */
static char *scd2_build_key(Arena *a, char **cells, const int *idx, int nidx) {
    char buf[1024]; size_t off = 0;
    for (int i = 0; i < nidx; i++) {
        const char *v = (idx[i] >= 0 && cells[idx[i]]) ? cells[idx[i]] : "";
        if (i) { if (off < sizeof(buf) - 1) buf[off++] = '\x1f'; }
        size_t vl = strlen(v);
        if (off + vl >= sizeof(buf)) vl = sizeof(buf) - 1 - off;
        memcpy(buf + off, v, vl); off += vl;
    }
    buf[off] = '\0';
    return arena_strdup(a, buf);
}

/* Loose truthiness for the source soft-delete flag. */
static bool scd2_truthy(const char *v) {
    if (!v || !v[0]) return false;
    if (!strcasecmp(v, "0") || !strcasecmp(v, "false") ||
        !strcasecmp(v, "f") || !strcasecmp(v, "no") || !strcasecmp(v, "n")) return false;
    return true;
}

/* True iff a stored effective_to cell denotes an OPEN (current) version.
 * An open end-bound may be materialised either as a real SQL NULL (the
 * DFO_NULL_SENTINEL when read back through the engine) — the industrial form
 * that lets downstream "<effective_to> IS NULL" find current versions — or as
 * a legacy empty string "" (older history written before the NULL contract).
 * Both must classify as open so the close-pass keeps recognising current
 * versions across the upgrade. */
static bool scd2_is_open(const char *v) {
    if (!v) return true;
    if (!v[0]) return true;
    if (!strcmp(v, DFO_NULL_SENTINEL)) return true;
    return false;
}

/* True iff any business-key component of this row is NULL (sentinel) or empty.
 * Such a row must NOT be historised: merging under an empty/NULL key would
 * collapse unrelated entities into one dimension lineage (a banking-grade data
 * integrity hazard), so the SCD2 run is failed instead. */
static bool scd2_key_is_null_or_empty(char **cells, const int *idx, int nidx) {
    for (int i = 0; i < nidx; i++) {
        const char *v = (idx[i] >= 0 && cells[idx[i]]) ? cells[idx[i]] : "";
        if (!v[0] || !strcmp(v, DFO_NULL_SENTINEL)) return true;
    }
    return false;
}

/* Compute MD5 hex over cmp_cols values (unit-separator delimited) for a single
 * source row. Used by scd2_hash_col change detection: one hash compare replaces
 * N column strcmps, and a stored hash distinguishes "" from a genuinely
 * different value across runs. `out` must be char[33] (32 hex + NUL). */
static void scd2_compute_hash(const RS *src, int row,
                               char **cmp_cols, int ncmp,
                               char out[33]) {
    char raw[4096]; size_t off = 0;
    for (int i = 0; i < ncmp; i++) {
        int ci = scd2_rs_col_index(src, cmp_cols[i]);
        const char *v = (ci >= 0 && src->rows[row].cells[ci])
                        ? src->rows[row].cells[ci] : "";
        if (i && off < sizeof(raw) - 1) raw[off++] = '\x1f';
        size_t vl = strlen(v);
        if (off + vl >= sizeof(raw)) vl = sizeof(raw) - 1 - off;
        memcpy(raw + off, v, vl); off += vl;
    }
    raw[off] = '\0';
    unsigned char digest[16];
    MD5((const unsigned char *)raw, off, digest);
    for (int b = 0; b < 16; b++) snprintf(out + b*2, 3, "%02x", digest[b]);
    out[32] = '\0';
}

/* ── SCD2 (slowly-changing-dimension, type 2) step ──────────────────
 * Historises target_table from the step's transform_sql snapshot. This
 * step does NOT invent any storage mechanism: it generates SQL strings
 * and runs them through the SAME sql_parse + exec_stmt the other steps
 * use, and performs the single write through write_rs_to_table — there
 * is no second write path.
 *
 * Why a full rebuild instead of UPDATE + INSERT (chosen design):
 *   - the engine's parser has no INSERT statement, so new versions cannot
 *     be appended via SQL DML; and
 *   - write_rs_to_table is a full table replace (recreate_table).
 * So the only way to add new versions while preserving history through
 * write_rs_to_table is to assemble the complete next state of the table
 * in memory and write it once:
 *   1. transform_sql → exec_stmt → materialise the incoming source slice
 *      into a per-step temp table __scd2_src_<id> (via write_rs_to_table).
 *   2. Pick the CURRENT version per business key from target with the
 *      window function ROW_NUMBER() OVER (PARTITION BY <bk>
 *      ORDER BY <transaction_time> DESC) = 1 (falls back to deriving the
 *      open versions from history in memory if the engine rejects the
 *      window query — an open version is a NULL (or, for legacy history,
 *      empty) <valid_to> cell; see scd2_is_open).
 *   3. Classify each source row: NEW (no current version), CHANGED (a
 *      compare column differs), DELETED (soft-delete flag set), or
 *      unchanged. Closed keys = CHANGED ∪ DELETED.
 *   4. Build the full next table: every existing row, with valid_to set
 *      to now() on the current version of a closed key; plus one new
 *      version (valid_from=now(), valid_to=SQL NULL) for every NEW/CHANGED
 *      key. DELETED keys are only closed. Industrial metadata columns are
 *      materialised when configured: scd2_current_flag_col (1 on the open
 *      version, 0 on closed) and scd2_version_col (per-key version number,
 *      max+1 on each new version). Write it via write_rs_to_table.
 *   5. Drop the scratch __scd2_src table, then return the number of new
 *      versions inserted, or <0 with errbuf set.
 *
 * TODO(scd2): comparison is plain strcmp with NULL≡"" — a NULL-safe
 *   compare scalar isn't available in the engine yet, so genuine
 *   NULL-vs-'' distinctions are not detected.
 * TODO(scd2): current-version lookup is O(source × current); fine for
 *   modest dimensions, hash it if this grows hot. */
static int run_scd2_step(App *app, Arena *a, Pipeline *p, PipelineStep *st, char *errbuf, size_t errsz) {
    const char *bk_spec = st->scd2_business_key;
    const char *vf      = st->scd2_effective_from_col;
    const char *vt      = st->scd2_effective_to_col;
    /* transaction_time defaults to the effective-from column when unset */
    const char *tt      = st->scd2_transaction_time[0] ? st->scd2_transaction_time : vf;

    if (!bk_spec[0]) { snprintf(errbuf, errsz, "scd2 step %s: scd2_business_key is required", st->id); return -1; }
    if (!vf[0] || !vt[0]) { snprintf(errbuf, errsz, "scd2 step %s: effective_from/effective_to cols are required", st->id); return -1; }
    if (!step_sql(st)[0]) { snprintf(errbuf, errsz, "scd2 step %s: transform_sql (source slice) is required", st->id); return -1; }
    if (!st->target_table[0]) { snprintf(errbuf, errsz, "scd2 step %s: target_table is required", st->id); return -1; }

    char *bk_cols[SCD2_MAX_COLS];
    int   nbk = scd2_split_list(a, bk_spec, bk_cols, SCD2_MAX_COLS);
    if (nbk == 0) { snprintf(errbuf, errsz, "scd2 step %s: empty business key", st->id); return -1; }

    /* 1. Materialise the source slice into a per-(pipeline,step) temp table.
     * Scoping the name by pipeline id as well as step id keeps two pipelines
     * that happen to reuse a step id from clobbering each other's scratch table
     * and avoids orphaning it under the bare step id (the temp is dropped at the
     * end of the run, see cleanup below). */
    char src_tbl[200];
    snprintf(src_tbl, sizeof(src_tbl), "__scd2_src_%s_%s",
             (p && p->id[0]) ? p->id : "p", st->id);
    const char *scd2_sql = step_sql(st);
    Stmt *sstmt = sql_parse(a, scd2_sql, strlen(scd2_sql));
    if (!sstmt || sstmt->error) {
        snprintf(errbuf, errsz, "scd2 step %s: source SQL parse error: %s", st->id,
                 sstmt && sstmt->error ? sstmt->error : "null");
        return -1;
    }
    LOG_INFO("scd2 step '%s': source SQL → %s: %s", st->id, src_tbl, scd2_sql);
    tl_exec_error[0] = '\0';
    RS *src = exec_stmt(a, sstmt, NULL);
    if (tl_exec_error[0]) {
        snprintf(errbuf, errsz, "scd2 step %s: %s", st->id, tl_exec_error);
        tl_exec_error[0] = '\0';
        return -1;
    }
    if (!src) { snprintf(errbuf, errsz, "scd2 step %s: source SQL exec failed", st->id); return -1; }
    if (src->truncated) {
        /* Неполный источник -> сверка закрыла/продублировала бы версии по
         * отсутствующим ключам. Отказ вместо тихой порчи истории. */
        snprintf(errbuf, errsz, "scd2 step %s: источник превышает %d строк — результат усечён, "
                 "сверка прервана. Сузьте source_sql (фильтр/инкремент).", st->id, MAX_RS_ROWS);
        return -1;
    }
    write_rs_to_table(app, a, src_tbl, src);

    /* Resolve business-key column indices in the source. */
    int bk_idx_src[SCD2_MAX_COLS];
    for (int i = 0; i < nbk; i++) {
        bk_idx_src[i] = scd2_rs_col_index(src, bk_cols[i]);
        if (bk_idx_src[i] < 0) {
            snprintf(errbuf, errsz, "scd2 step %s: business-key col '%s' not in source", st->id, bk_cols[i]);
            return -1;
        }
    }
    int del_idx_src = st->scd2_deleted_flag[0] ? scd2_rs_col_index(src, st->scd2_deleted_flag) : -1;

    /* Determine compare columns: explicit list, else every source column
     * except ignored, the SCD2 metadata cols and the business key. */
    char *cmp_cols[SCD2_MAX_COLS];
    int   ncmp = scd2_split_list(a, st->scd2_compare_columns, cmp_cols, SCD2_MAX_COLS);
    if (ncmp == 0) {
        char *ign[SCD2_MAX_COLS];
        int nign = scd2_split_list(a, st->scd2_ignored_columns, ign, SCD2_MAX_COLS);
        for (int c = 0; c < src->ncols && ncmp < SCD2_MAX_COLS; c++) {
            const char *cn = src->col_names[c]; if (!cn) continue;
            if (!strcasecmp(cn, vf) || !strcasecmp(cn, vt) || !strcasecmp(cn, tt)) continue;
            if (st->scd2_current_flag_col[0] && !strcasecmp(cn, st->scd2_current_flag_col)) continue;
            if (st->scd2_version_col[0]      && !strcasecmp(cn, st->scd2_version_col))      continue;
            if (st->scd2_deleted_flag[0] && !strcasecmp(cn, st->scd2_deleted_flag)) continue;
            bool is_bk = false; for (int i = 0; i < nbk; i++) if (!strcasecmp(cn, bk_cols[i])) { is_bk = true; break; }
            if (is_bk) continue;
            bool is_ign = false; for (int i = 0; i < nign; i++) if (!strcasecmp(cn, ign[i])) { is_ign = true; break; }
            if (is_ign) continue;
            cmp_cols[ncmp++] = arena_strdup(a, cn);
        }
    }

    char now_str[32];
    snprintf(now_str, sizeof(now_str), "%lld", (long long)time(NULL));

    /* Industrial SCD2 metadata columns (optional). When configured they are
     * materialised on the target so downstream readers get a Joker-grade
     * dimension:
     *   - current_flag_col: "1"/"0" — 1 on the open (current) version of each
     *     key, 0 on every closed version.
     *   - version_col: a monotonic version number per business key, starting at
     *     1 and incremented on each new version. */
    const char *cf  = st->scd2_current_flag_col;   /* "" when unused */
    const char *vc  = st->scd2_version_col;         /* "" when unused */

    /* 2. Read full history, then pick the current version per business key.
     *
     * Two engine quirks force the exact shape here (both verified):
     *   - "SELECT *" only expands the star when it is the sole select-item;
     *     "SELECT *, <expr>" yields a literal "*" column. So the window
     *     query must list the target columns explicitly, which we learn
     *     from the plain "SELECT * FROM target" history read.
     *   - open versions are written with a real SQL NULL <valid_to> (so
     *     downstream "<valid_to> IS NULL" finds current versions). Read back
     *     through the engine a NULL cell surfaces as DFO_NULL_SENTINEL, and
     *     legacy history may still carry "". The in-memory fallback and the
     *     close-pass therefore classify both via scd2_is_open(). */
    bool target_exists;
    pthread_mutex_lock(&g_app.tables_mu);
    target_exists = hm_get(&g_app.tables, st->target_table) != NULL;
    pthread_mutex_unlock(&g_app.tables_mu);

    RS *cur = NULL, *hist = NULL;
    if (target_exists) {
        char *hist_sql = arena_sprintf(a, "SELECT * FROM %s", st->target_table);
        Stmt *hstmt = sql_parse(a, hist_sql, strlen(hist_sql));
        hist = (hstmt && !hstmt->error) ? exec_stmt(a, hstmt, NULL) : NULL;
        if (hist && hist->truncated) {
            /* Неполная история -> close-pass закрыл бы не те версии. */
            snprintf(errbuf, errsz, "scd2 step %s: история %s превышает %d строк — "
                     "усечена, сверка прервана.", st->id, st->target_table, MAX_RS_ROWS);
            return -1;
        }
    }
    if (hist && hist->ncols) {
        /* Explicit column list (star won't expand alongside the window col). */
        char cols[4096]; size_t co = 0;
        for (int c = 0; c < hist->ncols; c++)
            co += (size_t)snprintf(cols + co, sizeof(cols) - co, "%s%s",
                                   c ? "," : "", hist->col_names[c]);
        char part[1024]; size_t po = 0;
        for (int i = 0; i < nbk; i++)
            po += (size_t)snprintf(part + po, sizeof(part) - po, "%s%s", i ? "," : "", bk_cols[i]);
        char *cur_sql = arena_sprintf(a,
            "SELECT * FROM (SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) "
            "AS __scd2_rn FROM %s) WHERE __scd2_rn = 1",
            cols, part, tt, st->target_table);
        LOG_INFO("scd2 step '%s': current-version SQL: %s", st->id, cur_sql);
        Stmt *cstmt = sql_parse(a, cur_sql, strlen(cur_sql));
        cur = (cstmt && !cstmt->error) ? exec_stmt(a, cstmt, NULL) : NULL;
        if (cur && cur->truncated) {
            snprintf(errbuf, errsz, "scd2 step %s: текущие версии %s превышают %d строк — "
                     "усечены, сверка прервана.", st->id, st->target_table, MAX_RS_ROWS);
            return -1;
        }

        if (!cur || !cur->ncols) {
            /* Fallback: derive current = open versions from history in memory
             * (open == empty valid_to, since IS NULL can't match "" here). */
            LOG_WARN("scd2 step '%s': window query unavailable, deriving current versions in memory", st->id);
            int hvt = scd2_rs_col_index(hist, vt);
            cur = rs_new(a, hist->ncols, hist->col_names, 0);
            for (int r = 0; r < hist->nrows; r++) {
                const char *vtc = (hvt >= 0) ? hist->rows[r].cells[hvt] : NULL;
                if (scd2_is_open(vtc)) rs_add(cur, a, hist->rows[r].cells, NULL);
            }
        }
    }

    int *bk_idx_cur = NULL;
    if (cur && cur->ncols) {
        bk_idx_cur = arena_alloc(a, (size_t)nbk * sizeof(int));
        for (int i = 0; i < nbk; i++) bk_idx_cur[i] = scd2_rs_col_index(cur, bk_cols[i]);
    }

    /* 3. Classify each source row. */
    char *closed_keys[MAX_RS_ROWS]; int n_closed = 0;       /* CHANGED ∪ DELETED */
    int   new_rows[MAX_RS_ROWS];    int n_new = 0;           /* source-row indices to insert */
    /* Track business keys already seen in this batch: a duplicate key would
     * otherwise classify as two NEW/CHANGED rows and open two concurrent
     * versions for the same entity. */
    char *seen_keys[MAX_RS_ROWS]; int n_seen = 0;
    for (int r = 0; r < src->nrows; r++) {
        char **cells = src->rows[r].cells;
        /* (a) Reject NULL/empty business keys — never merge under an empty key. */
        if (scd2_key_is_null_or_empty(cells, bk_idx_src, nbk)) {
            snprintf(errbuf, errsz,
                     "scd2 step %s: source row %d has a NULL/empty business key", st->id, r);
            return -1;
        }
        char  *key = scd2_build_key(a, cells, bk_idx_src, nbk);
        /* (b) Reject duplicate business keys within the same batch. */
        for (int s = 0; s < n_seen; s++) {
            if (!strcmp(seen_keys[s], key)) {
                snprintf(errbuf, errsz,
                         "scd2 step %s: duplicate business key in source batch (row %d)", st->id, r);
                return -1;
            }
        }
        if (n_seen < MAX_RS_ROWS) seen_keys[n_seen++] = key;
        bool deleted = del_idx_src >= 0 && scd2_truthy(cells[del_idx_src]);

        /* Locate the current version of this key. */
        char **cur_cells = NULL;
        if (cur && bk_idx_cur) {
            for (int cr = 0; cr < cur->nrows; cr++) {
                char *ck = scd2_build_key(a, cur->rows[cr].cells, bk_idx_cur, nbk);
                if (!strcmp(ck, key)) { cur_cells = cur->rows[cr].cells; break; }
            }
        }

        if (deleted) {
            if (cur_cells && n_closed < MAX_RS_ROWS) closed_keys[n_closed++] = key;  /* close only */
        } else if (!cur_cells) {
            if (n_new < MAX_RS_ROWS) new_rows[n_new++] = r;                            /* NEW */
        } else {
            bool changed = false;
            bool hash_compared = false;
            /* O(1) hash compare when the current version carries a stored hash.
             * This also detects "" vs NULL changes the column strcmp can't (a
             * stored hash differs from a freshly computed one). */
            if (st->scd2_hash_col[0]) {
                int hash_cur_idx = scd2_rs_col_index(cur, st->scd2_hash_col);
                if (hash_cur_idx >= 0) {
                    char src_hex[33];
                    scd2_compute_hash(src, r, cmp_cols, ncmp, src_hex);
                    const char *cur_hex = cur_cells[hash_cur_idx]
                                          ? cur_cells[hash_cur_idx] : "";
                    changed = strcmp(src_hex, cur_hex) != 0;
                    hash_compared = true;
                }
            }
            if (!hash_compared) {
                /* Fallback: column-by-column strcmp (existing behaviour). */
                for (int k = 0; k < ncmp && !changed; k++) {
                    int si = scd2_rs_col_index(src, cmp_cols[k]);
                    int ci = scd2_rs_col_index(cur, cmp_cols[k]);
                    const char *vs = (si >= 0 && cells[si]) ? cells[si] : "";
                    const char *vc = (ci >= 0 && cur_cells[ci]) ? cur_cells[ci] : "";
                    if (strcmp(vs, vc) != 0) changed = true;   /* TODO(scd2): NULL-safe compare */
                }
            }
            if (changed) {
                if (n_closed < MAX_RS_ROWS) closed_keys[n_closed++] = key;
                if (n_new < MAX_RS_ROWS) new_rows[n_new++] = r;
            }
        }
    }

    /* Trace the equivalent DML (realised below via the in-memory rebuild). */
    LOG_INFO("scd2 step '%s': close %d current version(s): "
             "UPDATE %s SET %s='%s' WHERE <business_key> IN (%d keys) AND %s IS NULL",
             st->id, n_closed, st->target_table, vt, now_str, n_closed, vt);
    LOG_INFO("scd2 step '%s': insert %d new version(s) with %s='%s', %s=NULL",
             st->id, n_new, vf, now_str, vt);

    /* 4. Build the full next state of the table. */
    /* Target column order: existing history's columns, or (first run) the
     * source columns plus the valid_from/valid_to metadata columns. */
    char **tcols; int tncols;
    if (hist && hist->ncols) {
        tcols = hist->col_names; tncols = hist->ncols;
    } else {
        tncols = 0;
        tcols = arena_alloc(a, (size_t)(src->ncols + 5) * sizeof(char *));
        for (int c = 0; c < src->ncols; c++) tcols[tncols++] = src->col_names[c];
        if (scd2_rs_col_index(src, vf) < 0) tcols[tncols++] = arena_strdup(a, vf);
        if (scd2_rs_col_index(src, vt) < 0) tcols[tncols++] = arena_strdup(a, vt);
        /* Industrial metadata columns (optional; appended once on first run). */
        if (cf[0] && scd2_rs_col_index(src, cf) < 0 && tncols < MAX_COLS)
            tcols[tncols++] = arena_strdup(a, cf);
        if (vc[0] && scd2_rs_col_index(src, vc) < 0 && tncols < MAX_COLS)
            tcols[tncols++] = arena_strdup(a, vc);
        /* Row-hash column for change detection (optional; off when unset). */
        if (st->scd2_hash_col[0] && scd2_rs_col_index(src, st->scd2_hash_col) < 0
                                 && tncols < MAX_COLS)
            tcols[tncols++] = arena_strdup(a, st->scd2_hash_col);
    }
    int t_vt_idx = -1;
    for (int j = 0; j < tncols; j++) if (tcols[j] && !strcasecmp(tcols[j], vt)) { t_vt_idx = j; break; }
    /* Resolve metadata column positions in the target layout (-1 = absent). */
    int t_cf_idx = -1, t_vc_idx = -1;
    if (cf[0]) for (int j = 0; j < tncols; j++) if (tcols[j] && !strcasecmp(tcols[j], cf)) { t_cf_idx = j; break; }
    if (vc[0]) for (int j = 0; j < tncols; j++) if (tcols[j] && !strcasecmp(tcols[j], vc)) { t_vc_idx = j; break; }

    /* Per-business-key max version number seen in history, so a new version is
     * stamped with max+1 (first version of a key = 1). Built only when a
     * version column is configured and present in the target. */
    char **ver_keys = NULL; long *ver_max = NULL; int n_ver = 0;
    if (vc[0] && t_vc_idx >= 0 && hist && hist->ncols) {
        int hvc = scd2_rs_col_index(hist, vc);
        ver_keys = arena_alloc(a, (size_t)hist->nrows * sizeof(char *));
        ver_max  = arena_alloc(a, (size_t)hist->nrows * sizeof(long));
        int *bk_idx_h = arena_alloc(a, (size_t)nbk * sizeof(int));
        for (int i = 0; i < nbk; i++) bk_idx_h[i] = scd2_rs_col_index(hist, bk_cols[i]);
        for (int r = 0; r < hist->nrows && hvc >= 0; r++) {
            char *key = scd2_build_key(a, hist->rows[r].cells, bk_idx_h, nbk);
            const char *vv = hist->rows[r].cells[hvc];
            long v = (vv && vv[0] && strcmp(vv, DFO_NULL_SENTINEL)) ? strtol(vv, NULL, 10) : 0;
            int slot = -1;
            for (int s = 0; s < n_ver; s++) if (!strcmp(ver_keys[s], key)) { slot = s; break; }
            if (slot < 0 && n_ver < hist->nrows) { slot = n_ver++; ver_keys[slot] = key; ver_max[slot] = 0; }
            if (slot >= 0 && v > ver_max[slot]) ver_max[slot] = v;
        }
    }

    RS *out = rs_new(a, tncols, tcols, 0);

    /* 4a. Carry every existing row; close the current version of closed keys. */
    if (hist && hist->ncols) {
        int *bk_idx_hist = arena_alloc(a, (size_t)nbk * sizeof(int));
        for (int i = 0; i < nbk; i++) bk_idx_hist[i] = scd2_rs_col_index(hist, bk_cols[i]);
        for (int r = 0; r < hist->nrows; r++) {
            char **cells = arena_alloc(a, (size_t)tncols * sizeof(char *));
            for (int j = 0; j < tncols; j++) cells[j] = hist->rows[r].cells[j] ? hist->rows[r].cells[j] : "";
            bool was_open = t_vt_idx < 0 || scd2_is_open(cells[t_vt_idx]);
            bool now_closed = false;
            if (was_open && t_vt_idx >= 0) {
                char *key = scd2_build_key(a, hist->rows[r].cells, bk_idx_hist, nbk);
                for (int k = 0; k < n_closed; k++)
                    if (!strcmp(key, closed_keys[k])) { cells[t_vt_idx] = now_str; now_closed = true; break; }
            }
            /* current_flag: 1 only on versions that remain open after this run. */
            if (t_cf_idx >= 0)
                cells[t_cf_idx] = (char *)((was_open && !now_closed) ? "1" : "0");
            rs_add(out, a, cells, NULL);
        }
    }

    /* 4b. Append a new version for every NEW/CHANGED source row. */
    /* When scd2_transaction_time names a real source column, valid_from is
     * sourced from that business time; otherwise it stays wall-clock. tt
     * defaults to vf (a target-only metadata col usually absent from the
     * source), so the default path resolves to -1 and keeps the now_str
     * behaviour, preserving back-compat for pipelines without transaction_time. */
    int tt_src_idx = scd2_rs_col_index(src, tt);
    for (int i = 0; i < n_new; i++) {
        char **scells = src->rows[new_rows[i]].cells;
        char **cells = arena_alloc(a, (size_t)tncols * sizeof(char *));
        /* Next version number for this key: max(history) + 1, defaulting to 1. */
        char *vbuf = NULL;
        if (t_vc_idx >= 0) {
            char *key = scd2_build_key(a, scells, bk_idx_src, nbk);
            long base = 0;
            for (int s = 0; s < n_ver; s++) if (!strcmp(ver_keys[s], key)) { base = ver_max[s]; break; }
            vbuf = arena_sprintf(a, "%ld", base + 1);
        }
        for (int j = 0; j < tncols; j++) {
            const char *name = tcols[j];
            if (j == t_vt_idx)              cells[j] = (char *)DFO_NULL_SENTINEL; /* open: SQL NULL */
            else if (j == t_cf_idx)         cells[j] = (char *)"1";              /* current */
            else if (j == t_vc_idx)         cells[j] = vbuf ? vbuf : (char *)"1";
            else if (!strcasecmp(name, vf)) {
                /* Prefer the configured transaction-time source value (business
                 * time); fall back to wall-clock when absent/empty/NULL so the
                 * non-empty valid_from guarantee (relied on by cdi/mdc tests) holds. */
                const char *ttv = (tt_src_idx >= 0) ? scells[tt_src_idx] : NULL;
                cells[j] = (ttv && ttv[0] && strcmp(ttv, DFO_NULL_SENTINEL) != 0)
                           ? (char *)ttv : now_str;
            }
            else if (!strcasecmp(name, vt)) cells[j] = (char *)DFO_NULL_SENTINEL;
            else {
                int si = scd2_rs_col_index(src, name);
                cells[j] = (si >= 0 && scells[si]) ? scells[si] : (char *)"";
            }
        }
        /* Stamp the row hash for this new version (if requested). */
        if (st->scd2_hash_col[0]) {
            int ti = -1;
            for (int j = 0; j < tncols; j++)
                if (tcols[j] && !strcasecmp(tcols[j], st->scd2_hash_col)) { ti = j; break; }
            if (ti >= 0) {
                char hex[33];
                scd2_compute_hash(src, new_rows[i], cmp_cols, ncmp, hex);
                cells[ti] = arena_strdup(a, hex);
            }
        }
        rs_add(out, a, cells, NULL);
    }

    /* 5. Single write through the sanctioned path. */
    write_rs_to_table(app, a, st->target_table, out);
    LOG_INFO("scd2 step '%s' → %s: %d new version(s), %d closed, %d total rows",
             st->id, st->target_table, n_new, n_closed, out->nrows);

    /* 6. Drop the scratch source table — it is run-private and must not be left
     * materialised after the step (it would otherwise leak into the table list,
     * the catalog and the on-disk data dir). Mirrors recreate_table's teardown:
     * close+evict the engine table, drop from the catalog, remove the WAL/dir,
     * and balance tables_count (the table was counted when recreate_table built
     * it). */
    {
        pthread_mutex_lock(&app->tables_mu);
        Table *tmp = hm_get(&app->tables, src_tbl);
        if (tmp) { table_close(tmp); hm_del(&app->tables, src_tbl); }
        pthread_mutex_unlock(&app->tables_mu);
        catalog_drop_table(app->catalog, src_tbl);
        char dp[1024], wp[1024];
        snprintf(dp, sizeof(dp), "%s/%s", app->data_dir, src_tbl);
        snprintf(wp, sizeof(wp), "%s/wal.bin", dp);
        unlink(wp); rmdir(dp);
        if (tmp) __atomic_fetch_sub(&g_app.metrics->tables_count, 1, __ATOMIC_RELAXED);
    }
    return n_new;
}

/* ── Match step — declarative rule-chain entity resolution ──────────────
 *
 * Runs a sequence of match rules against the candidate set from transform_sql.
 * Each rule: group candidates by an exact-match scan key, then within each group
 * score every unordered pair with metric_function(test_key_a, test_key_b) and
 * keep the pairs passing `metric operator threshold`. Output rows:
 *   [id_a, id_b, rule_name, metric_value]   → written to target_table.
 * Rules don't deduplicate across each other (a pair matching two rules appears
 * twice with different rule_name). See docs/MATCH_RULES.md for the JSON format. */
static int run_match_step(App *app, Arena *a, PipelineStep *st,
                          char *errbuf, size_t errsz) {
    if (!step_sql(st)[0]) {
        snprintf(errbuf, errsz, "match step %s: transform_sql (candidate set) is required", st->id);
        return -1;
    }
    if (!st->match_rules[0]) {
        snprintf(errbuf, errsz, "match step %s: match_rules is required", st->id);
        return -1;
    }
    if (!step_output(st)[0]) {
        snprintf(errbuf, errsz, "match step %s: target_table is required", st->id);
        return -1;
    }

    /* 1. Materialise the candidate set. */
    const char *cand_sql = step_sql(st);
    Stmt *sstmt = sql_parse(a, cand_sql, strlen(cand_sql));
    if (!sstmt || sstmt->error) {
        snprintf(errbuf, errsz, "match step %s: SQL parse error: %s",
                 st->id, sstmt && sstmt->error ? sstmt->error : "null");
        return -1;
    }
    tl_exec_error[0] = '\0';
    RS *cand = exec_stmt(a, sstmt, NULL);
    if (tl_exec_error[0]) {
        snprintf(errbuf, errsz, "match step %s: %s", st->id, tl_exec_error);
        tl_exec_error[0] = '\0';
        return -1;
    }
    if (!cand) { snprintf(errbuf, errsz, "match step %s: SQL exec failed", st->id); return -1; }
    if (cand->truncated) {
        snprintf(errbuf, errsz, "match step %s: кандидаты превышают %d строк — усечены, "
                 "сопоставление прервано (пропущенные пары = ложные не-дубли).", st->id, MAX_RS_ROWS);
        return -1;
    }

    /* Output RS — always created (empty candidate set → empty result table). */
    const char *out_cols[] = {"id_a", "id_b", "rule_name", "metric_value"};
    RS *out = rs_new(a, 4, (char **)out_cols, 0);

    if (cand->nrows == 0) {
        LOG_INFO("match step '%s': empty candidate set", st->id);
        write_rs_to_table(app, a, step_output(st), out);
        return 0;
    }

    /* 2. Parse the rules array. */
    JVal *rules = json_parse(a, st->match_rules, strlen(st->match_rules));
    if (!rules || rules->type != JV_ARRAY) {
        snprintf(errbuf, errsz, "match step %s: match_rules must be a JSON array", st->id);
        return -1;
    }

    /* 3. Execute each rule. */
    for (int ri = 0; ri < (int)rules->nitems; ri++) {
        JVal *rule = rules->items[ri];
        const char *rule_name = json_str(json_get(rule, "rule_name"), "");
        const char *id_col    = json_str(json_get(rule, "id_column"), "");
        const char *metric_fn = json_str(json_get(rule, "metric_function"), "word_similarity");
        const char *op_str    = json_str(json_get(rule, "operator"), ">=");
        double      threshold = json_dbl(json_get(rule, "threshold"), 0.8);
        const char *normalize = json_str(json_get(rule, "normalize"), "none");
        JVal *scan_arr = json_get(rule, "scan_columns");
        JVal *test_arr = json_get(rule, "test_columns");

        if (!scan_arr || scan_arr->type != JV_ARRAY ||
            !test_arr || test_arr->type != JV_ARRAY || !id_col[0]) {
            LOG_WARN("match step '%s': rule %d missing required fields, skipping", st->id, ri);
            continue;
        }
        int id_idx = scd2_rs_col_index(cand, id_col);
        if (id_idx < 0) {
            snprintf(errbuf, errsz, "match step %s rule %d: id_column '%s' not found",
                     st->id, ri, id_col);
            return -1;
        }

        /* Pre-resolve scan/test column indices. */
        int nscan = (int)scan_arr->nitems, ntest = (int)test_arr->nitems;
        int scan_idx[32], test_idx[32];
        if (nscan > 32) nscan = 32;
        if (ntest > 32) ntest = 32;
        for (int k = 0; k < nscan; k++)
            scan_idx[k] = scd2_rs_col_index(cand, json_str(scan_arr->items[k], ""));
        for (int k = 0; k < ntest; k++)
            test_idx[k] = scd2_rs_col_index(cand, json_str(test_arr->items[k], ""));

        int use_name = !strcasecmp(normalize, "name");
        int use_inn  = !strcasecmp(normalize, "inn");

        /* Build the scan key + normalised test string for every row once. */
        char **scan_key = arena_alloc(a, (size_t)cand->nrows * sizeof(char *));
        char **test_str = arena_alloc(a, (size_t)cand->nrows * sizeof(char *));
        for (int r = 0; r < cand->nrows; r++) {
            char sbuf[1024] = {0}, tbuf[1024] = {0};
            bool key_null = false;
            for (int k = 0; k < nscan; k++) {
                const char *v = (scan_idx[k] >= 0 && cand->rows[r].cells[scan_idx[k]])
                                ? cand->rows[r].cells[scan_idx[k]] : "";
                /* NULL-aware grouping (NULL != NULL, as in industrial MDM/SQL): a
                 * missing/NULL/sentinel scan-key component makes the whole key
                 * un-matchable, so rows with an unknown identifier (e.g. no INN)
                 * are never clustered together. */
                if (!v[0] || !strcmp(v, DFO_NULL_SENTINEL)) { key_null = true; break; }
                if (k) strncat(sbuf, "\x1f", sizeof(sbuf) - strlen(sbuf) - 1);
                strncat(sbuf, v, sizeof(sbuf) - strlen(sbuf) - 1);
            }
            if (key_null) sbuf[0] = '\0';   /* empty key → skipped by the guard below */
            for (int k = 0; k < ntest; k++) {
                const char *v = (test_idx[k] >= 0 && cand->rows[r].cells[test_idx[k]])
                                ? cand->rows[r].cells[test_idx[k]] : "";
                if (use_name)      v = qe_normalize_name(v, a);
                else if (use_inn)  v = qe_normalize_inn(v, a);
                if (k) strncat(tbuf, " ", sizeof(tbuf) - strlen(tbuf) - 1);
                strncat(tbuf, v, sizeof(tbuf) - strlen(tbuf) - 1);
            }
            scan_key[r] = arena_strdup(a, sbuf);
            test_str[r] = arena_strdup(a, tbuf);
        }

        int n_matched = 0;
        for (int i = 0; i < cand->nrows; i++) {
            if (!scan_key[i][0]) continue;   /* skip empty scan keys */
            for (int j = i + 1; j < cand->nrows; j++) {
                if (strcmp(scan_key[i], scan_key[j]) != 0) continue;   /* exact group match */

                double metric;
                if (!strcasecmp(metric_fn, "jaro_winkler"))
                    metric = qe_jaro_winkler(test_str[i], test_str[j], a);
                else if (!strcasecmp(metric_fn, "levenshtein"))
                    metric = (double)qe_levenshtein(test_str[i], test_str[j], a);
                else
                    metric = qe_word_similarity(test_str[i], test_str[j], a);

                bool pass = false;
                if      (!strcmp(op_str, ">=")) pass = metric >= threshold;
                else if (!strcmp(op_str, "<=")) pass = metric <= threshold;
                else if (!strcmp(op_str, ">"))  pass = metric >  threshold;
                else if (!strcmp(op_str, "<"))  pass = metric <  threshold;

                if (pass) {
                    const char *id_i = cand->rows[i].cells[id_idx];
                    const char *id_j = cand->rows[j].cells[id_idx];
                    char mbuf[32];
                    snprintf(mbuf, sizeof(mbuf), "%.6f", metric);
                    char **cells = arena_alloc(a, 4 * sizeof(char *));
                    cells[0] = arena_strdup(a, id_i ? id_i : "");
                    cells[1] = arena_strdup(a, id_j ? id_j : "");
                    cells[2] = arena_strdup(a, rule_name);
                    cells[3] = arena_strdup(a, mbuf);
                    rs_add(out, a, cells, NULL);
                    n_matched++;
                }
            }
        }
        LOG_INFO("match step '%s' rule '%s': %d pair(s) matched", st->id, rule_name, n_matched);
    }

    write_rs_to_table(app, a, step_output(st), out);
    LOG_INFO("match step '%s' → %s: %d total matched pairs", st->id, step_output(st), out->nrows);
    return out->nrows;
}

/* Topologically sort pipeline steps by their deps[] so a step always runs
 * after every step it depends on (merge/branch steps after their inputs).
 * Writes the execution order (original step indices) into `order` and returns
 * the count; returns -1 on a dependency cycle (errbuf gets the offending ids).
 * Out-of-range dep indices are ignored (treated as "no such dependency").
 * Steps with equal dependency depth keep their original array order, so simple
 * linear pipelines (no deps) run exactly as before. Kahn's algorithm. */
static int pipeline_topo_order(const Pipeline *p, int *order, char *errbuf, size_t errsz) {
    int n = p->nsteps;
    int indeg[MAX_STEPS] = {0};

    /* Count in-degrees from valid dep edges (dep -> step). */
    for (int i = 0; i < n; i++) {
        for (int d = 0; d < p->steps[i].ndeps; d++) {
            int dep = p->steps[i].deps[d];
            if (dep >= 0 && dep < n && dep != i) indeg[i]++;
        }
    }

    int out = 0;
    bool done_[MAX_STEPS] = {false};
    /* Repeatedly emit the lowest-index ready step (in-degree 0, not yet done) to
     * keep output stable/deterministic and original-order-preserving. */
    for (int emitted = 0; emitted < n; emitted++) {
        int pick = -1;
        for (int i = 0; i < n; i++) {
            if (!done_[i] && indeg[i] == 0) { pick = i; break; }
        }
        if (pick < 0) {
            /* No ready step but steps remain → cycle. List the stuck ids. */
            char ids[256]; size_t off = 0; ids[0] = '\0';
            for (int i = 0; i < n; i++) {
                if (!done_[i]) {
                    int w = snprintf(ids + off, sizeof(ids) - off, "%s%s",
                                     off ? "," : "", p->steps[i].id[0] ? p->steps[i].id : "?");
                    if (w > 0 && (size_t)w < sizeof(ids) - off) off += (size_t)w;
                }
            }
            snprintf(errbuf, errsz, "dependency cycle among steps: %s", ids);
            return -1;
        }
        done_[pick] = true;
        order[out++] = pick;
        /* Relax: decrement in-degree of steps that depend on `pick`. */
        for (int i = 0; i < n; i++) {
            if (done_[i]) continue;
            for (int d = 0; d < p->steps[i].ndeps; d++) {
                int dep = p->steps[i].deps[d];
                if (dep == pick) { if (indeg[i] > 0) indeg[i]--; }
            }
        }
    }
    return out;
}

/* ── Execute all steps of a pipeline ── */
/* Internal: execute pipeline steps with optional run-logging/broadcast.
 * `report=false` is used by the preview-step endpoint so transient one-shot
 * runs don't clutter catalog's runs history or fire pipeline_done WS events.
 * `external_arena=non-NULL` skips internal arena create/destroy so the
 * caller can keep Schema/table allocations alive for follow-up queries. */
/* Retry-DoS guard: cap on total wall-clock seconds spent sleeping across all
 * retry backoffs of a single run, so a step that fails with an ever-growing
 * backoff cannot freeze the (synchronous) gateway worker indefinitely. */
#define RETRY_TOTAL_DELAY_CAP_SEC 30

/* True for deterministic, non-transient failures where retrying just burns
 * the worker: SQL parse errors, auth/permission/credential problems, unknown
 * roles, missing objects and unreachable/unresolvable hosts. Matched on the
 * step error text (case-insensitive substring) — best-effort but safe: a
 * false negative only means we still retry, never that we skip a real retry
 * of a transient error. */
static bool error_is_nonretryable(const char *msg) {
    if (!msg || !msg[0]) return false;
    static const char *needles[] = {
        "parse", "syntax",
        "auth", "unauthorized", "forbidden", "permission", "denied",
        "credential", "password", "login",
        "role", "rbac",
        "no such", "not found", "does not exist", "unknown table", "unknown column",
        "no route to host", "name or service not known", "could not resolve",
        "connection refused", "unknown host",
        /* DNS-resolution failures — librdkafka/macOS getaddrinfo phrasings. These
         * never recover within a run's retry budget; retrying just pins a worker
         * thread for the whole 30 s backoff (saw the gateway wedged by an
         * unresolvable prod broker). */
        "failed to resolve", "nodename nor servname", "nodename", "servname",
        "all brokers down", "all broker connections", "broker transport failure",
        /* transport refusals + connect timeouts (libcurl/libpq phrasings) */
        "couldn't connect", "could not connect", "no connection",
        "timeout expired", "timed out", "timeout", "connection timed",
        /* deterministic sink/config errors — these fail identically every time, so
         * burning the whole retry-backoff budget on them only delays the failure */
        "target table", "empty target",
        "constraint", "violates", "duplicate key", "already exists", "no unique",
        "invalid", "malformed",
        /* Russian phrasings emitted by the pg/json_http connectors */
        "не задан", "нет подключени", "целевая таблица", "недоступ",
        "уже существ", "соединени",
        NULL
    };
    for (const char **n = needles; *n; n++)
        if (strcasestr(msg, *n)) return true;
    return false;
}

static void pipeline_execute_steps_internal(Pipeline *p, App *app, bool report, Arena *external_arena) {
    Arena *a = external_arena ? external_arena : arena_create(4194304); /* 4 MiB */
    int64_t started = (int64_t)time(NULL);
    struct timespec _mt0; clock_gettime(CLOCK_MONOTONIC, &_mt0);
    int total_rows = 0;
    const char *run_error = NULL;
    int total_retries = 0;
    int total_delay_sec = 0;   /* cumulative retry sleep — capped to avoid DoS */

    p->nstep_results = 0;   /* fresh {@step:...} cache for this run */
    const char *prev_output = NULL;   /* previous step's output table → {@prev} */

    /* Order steps by their deps[] (topological): a merge/branch step must run
     * after every step it reads from, regardless of array order. Cycles fail the
     * run up-front with a clear message. No-deps pipelines keep array order. */
    int topo[MAX_STEPS];
    char topo_err[320];
    int topo_n = pipeline_topo_order(p, topo, topo_err, sizeof(topo_err));
    if (topo_n < 0) {
        snprintf(p->error_msg, sizeof(p->error_msg), "%s", topo_err);
        LOG_ERROR("pipeline %s: %s", p->id, p->error_msg);
        p->run_status = RUN_FAILED;
        run_error = p->error_msg;
        goto done;
    }

    for (int oi = 0; oi < topo_n; oi++) {
        int si = topo[oi];
        PipelineStep *st = &p->steps[si];
        st->retry_count = 0;
        st->retry_after = 0;
        st->resolved_sql = NULL;

        /* Implicit pipe: this step writes to target_table if the user named one;
         * else the LAST step's output is the pipeline's visible result table
         * (named after the pipeline), and intermediate steps go to hidden
         * "__dfo_*" tables. {@prev} = previous step's output. */
        st->output_table = st->target_table[0]
            ? st->target_table
            : (si == p->nsteps - 1
                ? sanitize_table_name(a, p->name[0] ? p->name : p->id)
                : arena_sprintf(a, "__dfo_%s_s%d", p->id, si));

        /* Resolve sql_template / {param} / {@prev} substitution once, before
         * dispatch. A deterministic resolve error fails the run immediately. */
        char sql_err[256];
        char *eff_sql = resolve_step_sql(app, a, p, st, prev_output, sql_err, sizeof(sql_err));
        if (!eff_sql && sql_err[0]) {
            snprintf(p->error_msg, sizeof(p->error_msg), "step %s: %s", st->id, sql_err);
            LOG_ERROR("pipeline %s: %s", p->id, p->error_msg);
            st->status = STEP_FAILED;
            p->run_status = RUN_FAILED;
            run_error = p->error_msg;
            goto done;
        }
        st->resolved_sql = eff_sql;  /* NULL when the step has no SQL */

        while (true) {
            st->status = STEP_RUNNING;

            if (st->is_sink) {
                /* Sink step (приёмник): write transform_sql rows OUT via the
                 * connector. Checked before the connector-source branch since a
                 * sink also has connector_type set. */
                int n = run_sink_step(app, a, st, p->error_msg, sizeof(p->error_msg));
                if (n < 0) {
                    st->status = STEP_FAILED;
                } else {
                    total_rows += n;
                    st->status = STEP_SUCCESS;
                }
            } else if (st->python_code[0] || st->python_file[0]) {
                /* Python step takes precedence over connector / transform_sql.
                 * Code source is python_file (disk) or python_code (inline).
                 * If transform_sql is set, it provides input data; otherwise
                 * the user's script starts with an empty DataFrame. */
                int n = run_python_step(app, a, st, p->error_msg, sizeof(p->error_msg));
                if (n < 0) {
                    st->status = STEP_FAILED;
                } else {
                    total_rows += n;
                    st->status = STEP_SUCCESS;
                }
            } else if (st->scala_code[0]) {
                /* Scala step (Spark DataFrame) — same precedence rules as the
                 * Python step: takes input from transform_sql if set, else the
                 * user's script starts from an empty DataFrame. */
                int n = run_scala_step(app, a, st, p->error_msg, sizeof(p->error_msg));
                if (n < 0) {
                    st->status = STEP_FAILED;
                } else {
                    total_rows += n;
                    st->status = STEP_SUCCESS;
                }
            } else if (st->connector_type[0] || st->connection_id[0]) {
                /* connection_id без connector_type — шаг, целиком опирающийся на
                 * справочник подключений: тип возьмётся оттуда при запуске. */
                int n = run_connector_step(app, a, st, p->id, p->error_msg, sizeof(p->error_msg));
                if (n < 0) {
                    st->status = STEP_FAILED;
                } else {
                    total_rows += n;
                    st->status = STEP_SUCCESS;
                }
            } else if (st->match_rules[0]) {
                /* Match step (declarative entity resolution) — uses transform_sql
                 * as the candidate set, so it must be caught before the generic
                 * transform_sql branch. */
                int n = run_match_step(app, a, st, p->error_msg, sizeof(p->error_msg));
                if (n < 0) {
                    st->status = STEP_FAILED;
                } else {
                    total_rows += n;
                    st->status = STEP_SUCCESS;
                }
            } else if (st->scd2_business_key[0]) {
                /* SCD2 historisation step — must be caught before the generic
                 * transform_sql branch because it uses transform_sql as its
                 * source slice (writes via write_rs_to_table). */
                int n = run_scd2_step(app, a, p, st, p->error_msg, sizeof(p->error_msg));
                if (n < 0) {
                    st->status = STEP_FAILED;
                } else {
                    total_rows += n;
                    st->status = STEP_SUCCESS;
                }
            } else if (!step_sql(st)[0]) {
                st->status = STEP_SUCCESS;
            } else {
                const char *gsql = step_sql(st);
                Stmt *stmt = sql_parse(a, gsql, strlen(gsql));
                if (stmt->error) {
                    st->status = STEP_FAILED;
                    snprintf(p->error_msg, sizeof(p->error_msg), "step[%d] parse: %s", si, stmt->error);
                } else {
                    tl_exec_error[0] = '\0';
                    RS *rs = exec_stmt(a, stmt, NULL);
                    if (tl_exec_error[0]) {
                        /* Unresolvable source table → fail the step, not a silent
                         * 0-row success (existing-but-empty source still succeeds). */
                        st->status = STEP_FAILED;
                        snprintf(p->error_msg, sizeof(p->error_msg), "step[%d]: %s", si, tl_exec_error);
                        tl_exec_error[0] = '\0';
                    } else if (!rs) {
                        st->status = STEP_FAILED;
                        snprintf(p->error_msg, sizeof(p->error_msg), "step[%d]: execution returned null", si);
                    } else if (rs->truncated) {
                        /* Неполный результат в таблицу не пишем — это тихая
                         * потеря данных (потолок MAX_RS_ROWS). */
                        st->status = STEP_FAILED;
                        snprintf(p->error_msg, sizeof(p->error_msg),
                                 "step[%d]: результат превышает %d строк — усечён; шаг остановлен, "
                                 "чтобы не записать неполные данные. Сузьте запрос фильтром.",
                                 si, MAX_RS_ROWS);
                    } else {
                        /* Cache for {@step:<id>:<col>} references by later steps. */
                        pipeline_set_step_result(p, st->id, rs);
                        total_rows += write_rs_to_table(app, a, step_output(st), rs);
                        st->status = STEP_SUCCESS;
                    }
                }
            }

            if (st->status == STEP_SUCCESS) {
                st->retry_count = 0;
                st->retry_after = 0;
                prev_output = step_output(st);   /* feed this output to {@prev} of the next step */
                break;
            }

            /* Don't retry deterministic failures (auth/parse/missing/unreachable):
             * they will fail identically on every attempt and only freeze the
             * synchronous gateway worker through the backoff sleeps. */
            if (error_is_nonretryable(p->error_msg)) {
                LOG_WARN("step %s: non-retryable failure, not retrying: %s",
                         st->id, p->error_msg);
                p->run_status = RUN_FAILED;
                run_error = p->error_msg[0] ? p->error_msg : "step failed";
                goto done;
            }

            if (st->retry_count >= st->max_retries) {
                p->run_status = RUN_FAILED;
                run_error = p->error_msg[0] ? p->error_msg : "step failed";
                goto done;
            }

            st->retry_count++;
            total_retries += 1;
            int delay = st->retry_delay_sec * (1 << (st->retry_count - 1));
            /* Cap the cumulative sleep across the whole run. Once the budget is
             * spent, fail fast instead of sleeping the worker any longer. */
            if (total_delay_sec >= RETRY_TOTAL_DELAY_CAP_SEC) {
                LOG_WARN("step %s: retry delay budget (%ds) exhausted, failing run",
                         st->id, RETRY_TOTAL_DELAY_CAP_SEC);
                p->run_status = RUN_FAILED;
                run_error = p->error_msg[0] ? p->error_msg : "step failed";
                goto done;
            }
            if (delay > RETRY_TOTAL_DELAY_CAP_SEC - total_delay_sec)
                delay = RETRY_TOTAL_DELAY_CAP_SEC - total_delay_sec;
            total_delay_sec += delay;
            st->retry_after = (int64_t)time(NULL) + delay;
            st->status = STEP_PENDING;
            LOG_WARN("step %s: retry %d/%d in %ds", st->id, st->retry_count, st->max_retries, delay);
            sleep(delay);
        }
    }
    p->run_status = RUN_SUCCESS;

done:
    if (report) {
        struct timespec _mt1; clock_gettime(CLOCK_MONOTONIC, &_mt1);
        double _run_ms = (_mt1.tv_sec - _mt0.tv_sec) * 1000.0 + (_mt1.tv_nsec - _mt0.tv_nsec) / 1e6;
        if (app->metrics) metrics_push(&app->metrics->pipeline_latency_ms, _run_ms);
        catalog_log_run(app->catalog, p->id, started, (int64_t)time(NULL),
                        p->run_status == RUN_SUCCESS ? 0 : 1, run_error, total_retries);
        send_pipeline_alert(p, run_error, p->run_status == RUN_SUCCESS);
        Arena *ba = arena_create(512);
        app_ws_broadcast(app, arena_sprintf(ba,
            "{\"event\":\"pipeline_done\",\"id\":\"%s\",\"status\":\"%s\",\"rows_written\":%d}",
            p->id, p->run_status == RUN_SUCCESS ? "success" : "failed", total_rows));
        arena_destroy(ba);
    } else {
        (void)started; (void)run_error; (void)total_retries; (void)total_rows; (void)_mt0;
    }
    /* Only destroy the arena if WE created it. */
    if (!external_arena) arena_destroy(a);
}

/* Публичная точка входа: выполнить пайплайн с записью прогона и своей ареной. */
void pipeline_execute_steps(Pipeline *p, App *app) {
    pipeline_execute_steps_internal(p, app, /*report=*/true, /*external_arena=*/NULL);
}

/* ── Async pipeline execution ───────────────────────────────────────────────
 * A /run request copies the (fully-inline) Pipeline into a heap task and hands
 * it to the worker pool, returning immediately. This frees the single HTTP
 * event-loop thread: a long or blocking run can no longer wedge the whole
 * gateway. The copy is self-contained — Pipeline owns no heap members except the
 * run-scoped step_results cache (cleared below; the executor also resets it), so
 * it is immune to concurrent scheduler add/remove (which swap-compacts the live
 * array). The live pipeline stays RUN_RUNNING (set by the handler) until the
 * worker writes the final status back atomically via scheduler_finish_run. */
typedef struct { Pipeline pipe; App *app; } RunTask;

static void run_task_fn(void *arg) {
    RunTask *t = (RunTask *)arg;
    memset(t->pipe.step_results, 0, sizeof(t->pipe.step_results));
    t->pipe.nstep_results = 0;
    pipeline_execute_steps_internal(&t->pipe, t->app, /*report=*/true, /*external_arena=*/NULL);
    scheduler_finish_run(t->app->scheduler, t->pipe.id, t->pipe.run_status,
                         t->pipe.run_status == RUN_SUCCESS ? 0 : 1, t->pipe.error_msg);
    free(t);
}

/* ── POST /api/pipelines/:id/run ── */
static void h_pipeline_run(HttpReq *req, HttpResp *resp) {
    const char *id=hm_get(&req->params,"id");
    if(!id){http_resp_error(resp,400,"missing id");return;}
    Pipeline *p=scheduler_find(g_app.scheduler,id);
    if(!p){http_resp_error(resp,404,"not found");return;}
    if(p->run_status==RUN_RUNNING){http_resp_error(resp,409,"already running");return;}
    p->run_status=RUN_RUNNING; p->last_run=(int64_t)time(NULL);
    g_app.metrics->total_pipelines_run++;
    Arena *ba=arena_create(256);
    app_ws_broadcast(&g_app,arena_sprintf(ba,"{\"event\":\"pipeline_run_started\",\"id\":\"%s\"}",id));
    arena_destroy(ba);
    /* Optional synchronous run: ?wait=true|1 or ?sync=1 runs the pipeline inline
     * on the HTTP thread and returns only after it finishes, with the terminal
     * status. Lets callers reliably run-then-read (orchestration / tests). The
     * default (no flag) keeps the async worker-pool path below byte-for-byte. */
    if (req->query && (strstr(req->query,"wait=true") || strstr(req->query,"wait=1") ||
                       strstr(req->query,"sync=1")    || strstr(req->query,"sync=true"))) {
        pipeline_execute_steps(p, &g_app);   /* inline on live pipeline: sets run_status + logs the run */
        scheduler_finish_run(g_app.scheduler, p->id, p->run_status,
                             p->run_status == RUN_SUCCESS ? 0 : 1, p->error_msg);
        http_resp_json(resp, 200, p->run_status == RUN_SUCCESS
                       ? "{\"status\":\"success\"}" : "{\"status\":\"failed\"}");
        return;
    }
    /* Hand the run to the worker pool so the HTTP thread returns at once. */
    RunTask *t = malloc(sizeof(RunTask));
    if (!t) {                                  /* OOM: degrade to a synchronous run */
        pipeline_execute_steps(p, &g_app);
        http_resp_json(resp,200,"{\"status\":\"triggered\"}");
        return;
    }
    t->pipe = *p;                              /* full inline copy — stable for the worker */
    t->app  = &g_app;
    tp_submit(g_app.workers, run_task_fn, t);  /* executes off the HTTP event loop */
    http_resp_json(resp,200,"{\"status\":\"queued\"}");
}

/* True if `tbl` occurs as a whole identifier token inside `hay` (so "orders"
 * does not match "orders_detail"). */
static bool sql_refs_table(const char *hay, const char *tbl) {
    if (!hay || !tbl || !tbl[0]) return false;
    size_t tl = strlen(tbl);
    for (const char *p = strstr(hay, tbl); p; p = strstr(p + tl, tbl)) {
        char b = (p == hay) ? ' ' : p[-1];
        char a = p[tl];
        #define DFO_IDENT_CH(c) (((c)>='A'&&(c)<='Z')||((c)>='a'&&(c)<='z')||((c)>='0'&&(c)<='9')||(c)=='_')
        if (!DFO_IDENT_CH(b) && !DFO_IDENT_CH(a)) return true;
        #undef DFO_IDENT_CH
    }
    return false;
}

/* True if any pipeline other than `exclude_id` reads (in transform_sql) or writes
 * (target_table) the engine table `table` — such tables must survive deletion.
 * Note: connector_config "table" names refer to EXTERNAL source tables (a different
 * namespace), so they are deliberately not consulted here. */
static bool table_used_by_other_pipeline(const char *table, const char *exclude_id) {
    Scheduler *s = g_app.scheduler;
    if (!s || !table || !table[0]) return false;
    for (int i = 0; i < s->npipelines; i++) {
        Pipeline *q = &s->pipelines[i];
        if (!strcmp(q->id, exclude_id)) continue;
        for (int si = 0; si < q->nsteps; si++) {
            PipelineStep *st = &q->steps[si];
            if (st->target_table[0] && !strcasecmp(st->target_table, table)) return true;
            if (sql_refs_table(st->transform_sql, table)) return true;
        }
    }
    return false;
}

/* ── DELETE /api/pipelines/:id ── */
static void h_pipeline_delete(HttpReq *req, HttpResp *resp) {
    const char *id=hm_get(&req->params,"id");
    if(!id){http_resp_error(resp,400,"missing id");return;}

    /* Refuse to delete while a run is in flight on the worker pool: the worker
     * still writes this pipeline's tables and logs its run. */
    Pipeline *rp = scheduler_find(g_app.scheduler, id);
    if(rp && rp->run_status==RUN_RUNNING){http_resp_error(resp,409,"pipeline is running");return;}

    /* Drop the engine tables this pipeline produced, using the same names the
     * runner assigns: an explicit target_table, else the pipeline name for the
     * last step, else __dfo_<id>_s<i> for intermediate steps. Sink steps write
     * to external systems and own no engine table, so they are skipped. */
    Pipeline *p = scheduler_find(g_app.scheduler, id);
    if (p) {
        Arena *a = arena_create(8192);
        int dropped = 0;
        for (int si = 0; si < p->nsteps; si++) {
            PipelineStep *st = &p->steps[si];
            if (st->is_sink) continue;
            const char *out = st->target_table[0]
                ? st->target_table
                : (si == p->nsteps - 1
                    ? sanitize_table_name(a, p->name[0] ? p->name : p->id)
                    : arena_sprintf(a, "__dfo_%s_s%d", p->id, si));
            if (!out || !out[0]) continue;
            if (table_used_by_other_pipeline(out, id) || mvs_table_used(g_app.matviews, out, "")) {
                LOG_INFO("pipeline %s: keep table '%s' (used by another pipeline/datamart)", id, out);
                continue;
            }
            if (catalog_drop_table(g_app.catalog, out) == 0) dropped++;
        }
        arena_destroy(a);
        LOG_INFO("pipeline %s: dropped %d associated table(s) on delete", id, dropped);
    }

    scheduler_remove(g_app.scheduler,id); catalog_delete_pipeline(g_app.catalog,id);
    http_resp_json(resp,200,"{\"status\":\"deleted\"}");
}

/* ── GET /api/pipelines/:id/runs ── */
static void h_pipeline_runs(HttpReq *req, HttpResp *resp) {
    const char *id=hm_get(&req->params,"id");
    if(!id){http_resp_error(resp,400,"missing id");return;}
    Arena *a=arena_create(8192); char *json=NULL;
    catalog_list_runs(g_app.catalog,id,&json,a);
    http_resp_json(resp,200,json?json:"[]");
}

/* ── POST /api/triggers/:token ── (webhook trigger; public — token is the auth)
 *
 * Looks up a pipeline by its TRIGGER_WEBHOOK token, fires it, returns 202
 * with the pipeline name. Body is accepted (any JSON / form-encoded text)
 * but currently not forwarded into the pipeline's environment — that's a
 * future enhancement. */
static void h_webhook_trigger(HttpReq *req, HttpResp *resp) {
    const char *token = hm_get(&req->params, "token");
    if (!token || !*token) { http_resp_error(resp, 400, "missing token"); return; }
    Pipeline *p = scheduler_find_by_webhook_token(g_app.scheduler, token);
    if (!p) { http_resp_error(resp, 404, "unknown trigger"); return; }

    /* Enforce the trigger's configured HTTP method: locate the matching webhook
     * trigger (by token) and compare its webhook_method to the request verb.
     * Empty/unset method defaults to POST (mirrors the scheduler load default). */
    const char *want_method = "POST";
    for (int i = 0; i < p->ntriggers; i++) {
        if (p->triggers[i].type == TRIGGER_WEBHOOK &&
            strcmp(p->triggers[i].webhook_token, token) == 0) {
            if (p->triggers[i].webhook_method[0]) want_method = p->triggers[i].webhook_method;
            break;
        }
    }
    if (req->method && strcasecmp(req->method, want_method) != 0) {
        http_resp_error(resp, 405, "method not allowed"); return;
    }

    if (p->run_status == RUN_RUNNING) {
        http_resp_error(resp, 409, "pipeline already running"); return;
    }

    /* Method validated above against the trigger's webhook_method; token is the auth. */
    g_app.metrics->total_pipelines_run++;
    Arena *ba = arena_create(256);
    app_ws_broadcast(&g_app, arena_sprintf(ba,
        "{\"event\":\"pipeline_run_started\",\"id\":\"%s\",\"trigger\":\"webhook\"}", p->id));
    arena_destroy(ba);
    scheduler_run_pipeline_now(g_app.scheduler, p);
    pipeline_execute_steps(p, &g_app);

    http_resp_json(resp, 202, arena_sprintf(req->arena,
        "{\"status\":\"triggered\",\"pipeline_id\":\"%s\",\"pipeline_name\":\"%s\"}",
        p->id, p->name));
}

/* ── GET /api/metrics ── */
static void h_metrics(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a=arena_create(2048);
    http_resp_json(resp,200,metrics_to_json(g_app.metrics,a));  /* копирует тело */
    arena_destroy(a);
}

/* ── GET /metrics  (Prometheus text format) ── */
static void h_metrics_prometheus(HttpReq *req, HttpResp *resp) {
    (void)req;
    Metrics *m = g_app.metrics;
    int64_t now = (int64_t)time(NULL);
    int64_t uptime = now - m->uptime_start;

    Arena *a = arena_create(8192);

    /* Helper: append text to a growable buffer in arena */
#define PM(buf, buflen, used, ...) \
    do { \
        int _n = snprintf((buf)+(used), (buflen)-(used), __VA_ARGS__); \
        if (_n > 0) (used) += (size_t)_n; \
    } while(0)

    size_t cap = 8192;
    char *buf = arena_alloc(a, cap);
    size_t used = 0;

    PM(buf,cap,used,
        "# HELP dfo_uptime_seconds Time since process start\n"
        "# TYPE dfo_uptime_seconds gauge\n"
        "dfo_uptime_seconds %lld\n", (long long)uptime);

    PM(buf,cap,used,
        "# HELP dfo_queries_total Total SQL queries executed\n"
        "# TYPE dfo_queries_total counter\n"
        "dfo_queries_total %lld\n", (long long)m->total_queries);

    PM(buf,cap,used,
        "# HELP dfo_rows_total Total rows ingested\n"
        "# TYPE dfo_rows_total counter\n"
        "dfo_rows_total %lld\n", (long long)m->total_rows);

    PM(buf,cap,used,
        "# HELP dfo_pipelines_run_total Total pipeline runs\n"
        "# TYPE dfo_pipelines_run_total counter\n"
        "dfo_pipelines_run_total %lld\n", (long long)m->total_pipelines_run);

    PM(buf,cap,used,
        "# HELP dfo_query_latency_ms_avg Average query latency (last 60s)\n"
        "# TYPE dfo_query_latency_ms_avg gauge\n"
        "dfo_query_latency_ms_avg %.3f\n", metrics_avg(&m->query_latency_ms, 60));

    PM(buf,cap,used,
        "# HELP dfo_pipeline_latency_ms_avg Average pipeline latency (last 60s)\n"
        "# TYPE dfo_pipeline_latency_ms_avg gauge\n"
        "dfo_pipeline_latency_ms_avg %.3f\n", metrics_avg(&m->pipeline_latency_ms, 60));

    PM(buf,cap,used,
        "# HELP dfo_ingest_rows_per_sec Average ingest rate (last 60s)\n"
        "# TYPE dfo_ingest_rows_per_sec gauge\n"
        "dfo_ingest_rows_per_sec %.3f\n", metrics_avg(&m->rows_ingested, 60));

    PM(buf,cap,used,
        "# HELP dfo_http_requests_total Total HTTP requests\n"
        "# TYPE dfo_http_requests_total counter\n"
        "dfo_http_requests_total %lld\n", (long long)m->http_requests_total);

    PM(buf,cap,used,
        "# HELP dfo_http_errors_4xx_total HTTP 4xx responses\n"
        "# TYPE dfo_http_errors_4xx_total counter\n"
        "dfo_http_errors_4xx_total %lld\n", (long long)m->http_errors_4xx);

    PM(buf,cap,used,
        "# HELP dfo_http_errors_5xx_total HTTP 5xx responses\n"
        "# TYPE dfo_http_errors_5xx_total counter\n"
        "dfo_http_errors_5xx_total %lld\n", (long long)m->http_errors_5xx);

    PM(buf,cap,used,
        "# HELP dfo_http_request_duration_ms_avg Average HTTP request duration (last 60s)\n"
        "# TYPE dfo_http_request_duration_ms_avg gauge\n"
        "dfo_http_request_duration_ms_avg %.3f\n",
        metrics_avg(&m->http_request_duration_ms, 60));

    PM(buf,cap,used,
        "# HELP dfo_wal_bytes_written_total Bytes written to WAL\n"
        "# TYPE dfo_wal_bytes_written_total counter\n"
        "dfo_wal_bytes_written_total %lld\n", (long long)m->wal_bytes_written);

    PM(buf,cap,used,
        "# HELP dfo_wal_bytes_compressed_total Bytes after WAL compression\n"
        "# TYPE dfo_wal_bytes_compressed_total counter\n"
        "dfo_wal_bytes_compressed_total %lld\n", (long long)m->wal_bytes_compressed);

    PM(buf,cap,used,
        "# HELP dfo_tables_count Current number of tables\n"
        "# TYPE dfo_tables_count gauge\n"
        "dfo_tables_count %lld\n", (long long)m->tables_count);

    PM(buf,cap,used,
        "# HELP dfo_txn_begin_total Transactions started\n"
        "# TYPE dfo_txn_begin_total counter\n"
        "dfo_txn_begin_total %lld\n", (long long)m->txn_begin_total);

    PM(buf,cap,used,
        "# HELP dfo_txn_commit_total Transactions committed\n"
        "# TYPE dfo_txn_commit_total counter\n"
        "dfo_txn_commit_total %lld\n", (long long)m->txn_commit_total);

    PM(buf,cap,used,
        "# HELP dfo_txn_rollback_total Transactions rolled back\n"
        "# TYPE dfo_txn_rollback_total counter\n"
        "dfo_txn_rollback_total %lld\n", (long long)m->txn_rollback_total);

    PM(buf,cap,used,
        "# HELP dfo_txn_timeout_total Transactions timed out\n"
        "# TYPE dfo_txn_timeout_total counter\n"
        "dfo_txn_timeout_total %lld\n", (long long)m->txn_timeout_total);

    PM(buf,cap,used,
        "# HELP dfo_txn_active Currently open transactions\n"
        "# TYPE dfo_txn_active gauge\n"
        "dfo_txn_active %lld\n", (long long)m->txn_active);

#undef PM

    http_resp_text(resp, 200, buf);   /* копирует тело во владение ответа */
    resp->content_type = "text/plain; version=0.0.4; charset=utf-8";
    arena_destroy(a);
}

/* ── JVal serializer helper ── */
static void jval_serialize(JBuf *jb, JVal *v) {
    if (!v) { jb_null(jb); return; }
    switch (v->type) {
        case JV_NULL:   jb_null(jb); break;
        case JV_BOOL:   jb_bool(jb, v->b); break;
        case JV_NUMBER: jb_double(jb, v->n); break;
        case JV_STRING: jb_str(jb, v->s ? v->s : ""); break;
        case JV_ARRAY:
            jb_arr_begin(jb);
            for (size_t i = 0; i < v->nitems; i++) jval_serialize(jb, v->items[i]);
            jb_arr_end(jb);
            break;
        case JV_OBJECT:
            jb_obj_begin(jb);
            for (size_t i = 0; i < v->nkeys; i++) {
                jb_key(jb, v->keys[i] ? v->keys[i] : "");
                jval_serialize(jb, v->vals[i]);
            }
            jb_obj_end(jb);
            break;
        default: jb_null(jb); break;
    }
}

/* ── POST /api/analytics/results ── */
static void h_result_save(HttpReq *req, HttpResp *resp) {
    if (!req->body || !req->body_len) { http_resp_error(resp,400,"empty body"); return; }
    Arena *a = arena_create(524288); /* 512 KiB */
    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root || root->type != JV_OBJECT) {
        http_resp_error(resp,400,"invalid json"); arena_destroy(a); return;
    }
    const char *name = json_str(json_get(root,"name"), "");
    if (!*name) { http_resp_error(resp,400,"missing name"); arena_destroy(a); return; }
    const char *sql  = json_str(json_get(root,"sql"), "");
    JVal *cols_v = json_get(root,"columns");
    JVal *rows_v = json_get(root,"rows");

    /* Serialize columns and rows arrays back to JSON strings */
    JBuf cjb; jb_init(&cjb, a, 512);
    jval_serialize(&cjb, cols_v);
    const char *columns_json = jb_done(&cjb);

    JBuf rjb; jb_init(&rjb, a, 65536);
    jval_serialize(&rjb, rows_v);
    const char *rows_json = jb_done(&rjb);

    int row_count = (int)json_int(json_get(root,"row_count"),
                    (rows_v && rows_v->type==JV_ARRAY) ? (long long)rows_v->nitems : 0);

    int64_t new_id = 0;
    if (catalog_save_result(g_app.catalog, name, sql, columns_json, rows_json,
                            row_count, &new_id) != 0) {
        http_resp_error(resp,500,"save failed"); arena_destroy(a); return;
    }
    JBuf jb; jb_init(&jb,a,64);
    jb_obj_begin(&jb);
    jb_key(&jb,"id"); jb_int(&jb,new_id);
    jb_obj_end(&jb);
    http_resp_json(resp,201,(char*)jb_done(&jb));
}

/* ── GET /api/analytics/results ── */
static void h_result_list(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a = arena_create(16384);
    char *json = NULL;
    catalog_list_results(g_app.catalog, &json, a);
    http_resp_json(resp, 200, json ? json : "[]");
}

/* ── GET /api/analytics/results/:id ── */
static void h_result_get(HttpReq *req, HttpResp *resp) {
    const char *ids = hm_get(&req->params,"id");
    if (!ids) { http_resp_error(resp,400,"missing id"); return; }
    int64_t id = (int64_t)strtoll(ids, NULL, 10);
    Arena *a = arena_create(65536);
    char *json = NULL;
    if (catalog_get_result(g_app.catalog, id, &json, a) != 0) {
        http_resp_error(resp,404,"not found"); arena_destroy(a); return;
    }
    http_resp_json(resp,200,json);
}

/* ── DELETE /api/analytics/results/:id ── */
static void h_result_delete(HttpReq *req, HttpResp *resp) {
    const char *ids = hm_get(&req->params,"id");
    if (!ids) { http_resp_error(resp,400,"missing id"); return; }
    int64_t id = (int64_t)strtoll(ids, NULL, 10);
    if (catalog_delete_result(g_app.catalog, id) != 0) {
        http_resp_error(resp,500,"delete failed"); return;
    }
    http_resp_json(resp,200,"{\"ok\":true}");
}

/* ── POST /api/tables/:name/indexes  {"column":"col_name"} ── */
static void h_index_create(HttpReq *req, HttpResp *resp) {
    const char *tname = hm_get(&req->params, "name");
    if (!tname) { http_resp_error(resp,400,"missing name"); return; }

    Arena *a = arena_create(8192);
    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root) { http_resp_error(resp,400,"invalid json"); arena_destroy(a); return; }
    const char *col_name = json_str(json_get(root,"column"), "");
    if (!col_name[0]) { http_resp_error(resp,400,"missing column"); arena_destroy(a); return; }

    /* find open Table* in hashmap */
    pthread_mutex_lock(&g_app.tables_mu);
    Table *tbl = hm_get(&g_app.tables, tname);
    pthread_mutex_unlock(&g_app.tables_mu);
    if (!tbl) { http_resp_error(resp,404,"table not found"); arena_destroy(a); return; }

    /* resolve col_idx from catalog schema */
    Schema *sc = NULL;
    catalog_get_schema(g_app.catalog, tname, &sc, a);
    if (!sc) { http_resp_error(resp,500,"no schema"); arena_destroy(a); return; }
    int col_idx = -1;
    for (int i = 0; i < sc->ncols; i++) {
        if (strcasecmp(sc->cols[i].name, col_name) == 0) { col_idx = i; break; }
    }
    if (col_idx < 0) { http_resp_error(resp,400,"column not found"); arena_destroy(a); return; }
    if (sc->cols[col_idx].type != COL_INT64) {
        http_resp_error(resp,400,"only INT64 columns can be indexed"); arena_destroy(a); return;
    }

    if (table_create_index(tbl, col_idx, g_app.catalog) != 0) {
        http_resp_error(resp,500,"index creation failed"); arena_destroy(a); return;
    }
    http_resp_json(resp, 200, "{\"ok\":true}");
    arena_destroy(a);
}

/* ── GET /api/tables/:name/indexes ── */
static void h_index_list(HttpReq *req, HttpResp *resp) {
    const char *tname = hm_get(&req->params, "name");
    if (!tname) { http_resp_error(resp,400,"missing name"); return; }
    Arena *a = arena_create(16384);
    char *json = NULL;
    if (catalog_list_indexes_json(g_app.catalog, tname, &json, a) != 0 || !json) {
        http_resp_json(resp, 200, "[]"); arena_destroy(a); return;
    }
    http_resp_json(resp, 200, json);
    arena_destroy(a);
}

/* ─────────────────────────────────────────────────────────────────────
   Connector probe endpoints — test a connector config from the UI
   POST /api/connector/probe/entities
     Body: {"type":"postgresql","config":{...}}
   POST /api/connector/probe/schema
     Body: {"type":"postgresql","config":{...},"entity":"users"}
   ───────────────────────────────────────────────────────────────────── */

/* Load connector by type name → ConnectorInst* (caller must unload) */
static ConnectorInst *load_connector_by_type(Arena *a, const char *type,
                                               const char *cfg_json) {
    char so_path[1024];
    snprintf(so_path, sizeof(so_path), "%s/%s_connector.so",
             g_app.plugins_dir, connector_so_name(type));
    return connector_load(so_path, cfg_json, a);
}

/* POST /api/connector/probe/entities — грузит коннектор по type+config и
 * возвращает список доступных сущностей (таблиц/потоков). */
/* Извлекает из тела пробы эффективные тип и конфиг. Тело может нести:
 *   {"type":…, "config":{…}}           — всё пришло от UI (старая форма);
 *   {"connection_id":…, "config":{…}}  — параметры доступа берутся из
 *                                        справочника, config кладётся сверху.
 * Вторая форма обязательна: UI видит секреты только как "********" и физически
 * не может прислать настоящий пароль обратно, поэтому проба подключения,
 * выбранного из справочника, работает лишь при слиянии на сервере.
 * Побочно чинит старую беду проб: раньше вложенные объекты/массивы в config
 * превращались в null, теперь сериализация рекурсивная. */
static int probe_effective(Arena *a, JVal *root, const char **out_type,
                           const char **out_cfg, const char **err) {
    JVal *cfg_v = json_get(root, "config");
    if (cfg_v && cfg_v->type == JV_STRING)
        cfg_v = json_parse(a, cfg_v->s, strlen(cfg_v->s));

    const char *cid = json_str(json_get(root, "connection_id"), "");
    if (cid[0]) {
        char *raw = NULL;
        if (catalog_load_connection(g_app.catalog, cid, &raw, a) != 0) {
            *err = "подключение не найдено в справочнике"; return -1;
        }
        JVal *rec = json_parse(a, raw, strlen(raw));
        if (!rec || rec->type != JV_OBJECT) {
            *err = "запись подключения повреждена"; return -1;
        }
        const char *t = json_str(json_get(rec, "type"), "");
        if (!t[0]) { *err = "у подключения не задан тип коннектора"; return -1; }
        *out_type = t;
        /* Подставляем из справочника ТОЛЬКО когда вызывающий не прислал своего
         * секрета. Редактор подключения — это тот же виртуальный «шаг», и он
         * всегда шлёт connection_id редактируемой записи: считая его признаком
         * «база из справочника» безусловно, мы проверяли бы СОХРАНЁННЫЙ пароль
         * вместо только что введённого — оператор не смог бы проверить новый.
         * А если свой секрет прислан, подставлять нечего, и запрещать выбор
         * адреса незачем: утечь нечему. */
        bool own = cfg_has_own_secret(cfg_v);
        *out_cfg  = conn_merge_config_ex(a, json_get(rec, "config"), cfg_v, !own, cid);
        return 0;
    }

    const char *t = json_str(json_get(root, "type"), "");
    if (!t[0]) { *err = "missing type"; return -1; }
    *out_type = t;
    *out_cfg  = conn_merge_config(a, NULL, cfg_v);
    return 0;
}

static void h_connector_probe_entities(HttpReq *req, HttpResp *resp) {
    Arena *a = arena_create(65536);
    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root) { http_resp_error(resp,400,"invalid json"); arena_destroy(a); return; }

    const char *type = NULL, *cfg_json = NULL, *perr = NULL;
    if (probe_effective(a, root, &type, &cfg_json, &perr) != 0) {
        http_resp_error(resp,400,perr); arena_destroy(a); return;
    }

    ConnectorInst *inst = load_connector_by_type(a, type, cfg_json);
    if (!inst) { http_resp_error(resp,500,"connector load failed"); arena_destroy(a); return; }

    DfoEntityList el = {0};
    if (connector_api(inst)->list_entities(connector_ctx(inst), a, &el) != 0) {
        connector_unload(inst);
        http_resp_error(resp,500,"list_entities failed"); arena_destroy(a); return;
    }

    JBuf jb; jb_init(&jb, a, 1024);
    jb_arr_begin(&jb);
    for (int i = 0; i < el.count; i++) {
        jb_obj_begin(&jb);
        jb_key(&jb,"name"); jb_str(&jb, el.items[i].entity);
        jb_key(&jb,"type"); jb_str(&jb, el.items[i].type);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);

    connector_unload(inst);
    http_resp_json(resp, 200, (char *)jb_done(&jb));
    arena_destroy(a);
}

/* POST /api/connector/probe/ping  {"type":"postgresql","config":{...}}
 * Loads the connector with the config and calls ping(). 200 {"ok":true} on
 * success, else {"ok":false,"error":...}. Used by the "Подключиться" button. */
static void h_connector_probe_ping(HttpReq *req, HttpResp *resp) {
    Arena *a = arena_create(16384);
    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root) { http_resp_error(resp,400,"invalid json"); arena_destroy(a); return; }
    const char *type = NULL, *cfg_json = NULL, *perr = NULL;
    if (probe_effective(a, root, &type, &cfg_json, &perr) != 0) {
        JBuf eb; jb_init(&eb, a, 256);
        jb_obj_begin(&eb);
        jb_key(&eb,"ok");    jb_bool(&eb, false);
        jb_key(&eb,"error"); jb_str(&eb, perr);
        jb_obj_end(&eb);
        http_resp_json(resp, 200, (char *)jb_done(&eb));
        arena_destroy(a); return;
    }

    ConnectorInst *inst = load_connector_by_type(a, type, cfg_json);
    if (!inst) {
        http_resp_json(resp, 200, "{\"ok\":false,\"error\":\"не удалось загрузить коннектор\"}");
        arena_destroy(a); return;
    }
    const DfoConnector *api = connector_api(inst);
    void *ctx = connector_ctx(inst);
    int rc = api->ping ? api->ping(ctx) : -1;

    if (rc == 0) {
        connector_unload(inst);
        http_resp_json(resp, 200, "{\"ok\":true}");
        arena_destroy(a); return;
    }
    /* Failure → include a human-readable reason if the connector reports one. */
    const char *reason = api->last_error ? api->last_error(ctx) : NULL;
    JBuf jb; jb_init(&jb, a, 256);
    jb_obj_begin(&jb);
    jb_key(&jb,"ok"); jb_bool(&jb, false);
    jb_key(&jb,"error"); jb_str(&jb, (reason && reason[0]) ? reason : "подключение не удалось");
    jb_obj_end(&jb);
    char *out = (char *)jb_done(&jb);
    connector_unload(inst);
    http_resp_json(resp, 200, out);
    arena_destroy(a);
}

/* Предпросмотр обязан быть БЕЗ ПОСЛЕДСТВИЙ.
 *
 * У Kafka явный group_id включает инкрементальную семантику: читаем с
 * закоммиченного смещения и коммитим после чтения. Поэтому «Показать
 * сообщения» в интерфейсе съедало сообщения у боевого конвейера — на живом
 * брокере из 200 сообщений в конвейер доезжало 195, ровно на длину
 * предпросмотра. Подменяем group_id на значение по умолчанию: коннектор
 * заводит эфемерную группу, читает с начала и ничего не коммитит.
 * Для прочих типов ключ незнакомый и просто игнорируется. */
static const char *probe_readonly_cfg(Arena *a, const char *cfg_json) {
    if (!cfg_json) return cfg_json;
    JVal *base = json_parse(a, cfg_json, strlen(cfg_json));
    if (!base || base->type != JV_OBJECT) return cfg_json;
    const char *ov = "{\"group_id\":\"dfo-consumer\"}";
    JVal *over = json_parse(a, ov, strlen(ov));
    return conn_merge_config(a, base, over);
}

/* POST /api/connector/probe/preview  {"type","config","query","limit"}
 * Reads one batch from the source via the connector and returns it as
 * {"columns":[...], "rows":[[...]]} — used to preview the source query. */
static void h_connector_probe_preview(HttpReq *req, HttpResp *resp) {
    Arena *a = arena_create(1<<20);
    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root) { http_resp_error(resp,400,"invalid json"); arena_destroy(a); return; }
    const char *query = json_str(json_get(root,"query"), "");
    int limit = (int)json_int(json_get(root,"limit"), 100);
    if (limit <= 0 || limit > 1000) limit = 100;

    const char *type = NULL, *cfg_json = NULL, *perr = NULL;
    if (probe_effective(a, root, &type, &cfg_json, &perr) != 0) {
        http_resp_error(resp,400,perr); arena_destroy(a); return;
    }

    /* Запрос не прислали — берём его из слитого конфига, как это делает шаг при
     * запуске (query/table). Иначе предпросмотр по одному connection_id не знал
     * бы, что читать, хотя в подключении это задано. */
    if (!query[0]) {
        JVal *mc = json_parse(a, cfg_json, strlen(cfg_json));
        if (mc && mc->type == JV_OBJECT) {
            const char *q = json_str(json_get(mc, "query"), "");
            const char *t = json_str(json_get(mc, "table"), "");
            query = q[0] ? q : t;
        }
    }

    ConnectorInst *inst = load_connector_by_type(a, type, probe_readonly_cfg(a, cfg_json));
    if (!inst) { http_resp_error(resp,500,"connector load failed"); arena_destroy(a); return; }
    const DfoConnector *api = connector_api(inst);
    void *ctx = connector_ctx(inst);

    /* Source = query (SQL if it has spaces, else a bare table/entity name). */
    const char *entity = query[0] ? query : "";
    const char *filter = NULL;
    if (strchr(entity, ' ')) { filter = entity; entity = ""; }

    DfoReadReq rr = { .cursor = "0", .limit = limit, .filter = filter };
    ColBatch *b = NULL;
    int rc = api->read_batch(ctx, a, &rr, entity, &b);
    /* rc: <0 = error; 0 = ok (more pages); 1 = ok, last/only page (data present).
       Only a negative code is a real failure — 1 still carries a valid batch. */
    if (rc < 0) {
        const char *reason = api->last_error ? api->last_error(ctx) : NULL;
        JBuf jb; jb_init(&jb, a, 256); jb_obj_begin(&jb);
        jb_key(&jb,"error"); jb_str(&jb, (reason && reason[0]) ? reason : "запрос не выполнен");
        jb_obj_end(&jb);
        char *out = (char *)jb_done(&jb);
        connector_unload(inst); http_resp_json(resp, 200, out); arena_destroy(a); return;
    }

    JBuf jb; jb_init(&jb, a, 4096);
    jb_obj_begin(&jb);
    jb_key(&jb,"columns"); jb_arr_begin(&jb);
    Schema *sc = b ? b->schema : NULL;
    int ncols = sc ? sc->ncols : 0, nrows = b ? b->nrows : 0;
    /* Коннектор вправе вернуть больше запрошенного: у Siebel, например, размер
     * страницы задан в конфиге и req->limit его не уменьшает — предпросмотр на
     * 3 строки притаскивал всю страницу в 100. Обрезаем здесь, чтобы «показать
     * N строк» означало ровно N у любого коннектора. */
    if (nrows > limit) nrows = limit;
    for (int c = 0; c < ncols; c++) jb_str(&jb, sc->cols[c].name);
    jb_arr_end(&jb);
    jb_key(&jb,"rows"); jb_arr_begin(&jb);
    for (int r = 0; r < nrows; r++) {
        jb_arr_begin(&jb);
        for (int c = 0; c < ncols; c++) {
            const uint8_t *bm = b->null_bitmap[c];
            if (bm && ((bm[r/8] >> (r%8)) & 1u)) { jb_null(&jb); continue; }
            switch (sc->cols[c].type) {
                case COL_INT64:
                case COL_BOOL:   jb_int(&jb, ((int64_t*)b->values[c])[r]); break;
                case COL_DOUBLE: jb_double(&jb, ((double*)b->values[c])[r]); break;
                default: {
                    char *s = ((char**)b->values[c])[r];
                    jb_str(&jb, s ? s : "");
                }
            }
        }
        jb_arr_end(&jb);
    }
    jb_arr_end(&jb);
    jb_obj_end(&jb);
    char *out = (char *)jb_done(&jb);
    connector_unload(inst);
    http_resp_json(resp, 200, out);
    arena_destroy(a);
}

/* POST /api/connector/probe/schema — возвращает схему (колонки/типы) одной
 * сущности коннектора, заданной в поле "entity". */
static void h_connector_probe_schema(HttpReq *req, HttpResp *resp) {
    Arena *a = arena_create(65536);
    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root) { http_resp_error(resp,400,"invalid json"); arena_destroy(a); return; }

    const char *entity = json_str(json_get(root,"entity"), "");
    if (!entity[0]) {
        http_resp_error(resp,400,"missing entity"); arena_destroy(a); return;
    }

    const char *type = NULL, *cfg_json = NULL, *perr = NULL;
    if (probe_effective(a, root, &type, &cfg_json, &perr) != 0) {
        http_resp_error(resp,400,perr); arena_destroy(a); return;
    }

    ConnectorInst *inst = load_connector_by_type(a, type, cfg_json);
    if (!inst) { http_resp_error(resp,500,"connector load failed"); arena_destroy(a); return; }

    Schema *sc = NULL;
    if (connector_api(inst)->describe(connector_ctx(inst), a, entity, &sc) != 0 || !sc) {
        connector_unload(inst);
        http_resp_error(resp,500,"describe failed"); arena_destroy(a); return;
    }

    JBuf jb; jb_init(&jb, a, 1024);
    jb_obj_begin(&jb);
    jb_key(&jb,"entity"); jb_str(&jb, entity);
    jb_key(&jb,"columns"); jb_arr_begin(&jb);
    static const char *type_names[] = {"int64","double","text","bool","null"};
    for (int c = 0; c < sc->ncols; c++) {
        jb_obj_begin(&jb);
        jb_key(&jb,"name");     jb_str(&jb, sc->cols[c].name);
        jb_key(&jb,"type");     jb_str(&jb, type_names[sc->cols[c].type]);
        jb_key(&jb,"nullable"); jb_bool(&jb, sc->cols[c].nullable);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);
    jb_obj_end(&jb);

    connector_unload(inst);
    http_resp_json(resp, 200, (char *)jb_done(&jb));
    arena_destroy(a);
}

/* ── Auth handlers ── */
static void h_auth_token(HttpReq *req, HttpResp *resp) {
    if (!req->body || req->body_len == 0) {
        http_resp_error(resp, 400, "missing body");
        return;
    }
    Arena *a = req->arena;  // Use request arena, don't create new one
    JVal *body = json_parse(a, req->body, req->body_len);
    if (!body || body->type != JV_OBJECT) {
        http_resp_error(resp, 400, "invalid json");
        return;
    }
    const char *username = json_str(json_get(body, "username"), NULL);
    const char *password = json_str(json_get(body, "password"), NULL);
    if (!username || !password) {
        http_resp_error(resp, 400, "missing username or password");
        return;
    }
    /* Вход: встроенный admin (пароль из конфига) ИЛИ учётка из таблицы users. */
    AuthClaims claims;
    memset(&claims, 0, sizeof(claims));
    int authed = 0;
    if (strcmp(username, "admin") == 0 && strcmp(password, g_app.admin_password) == 0) {
        strncpy(claims.user_id, username, sizeof(claims.user_id) - 1);
        claims.role = ROLE_ADMIN;
        authed = 1;
    } else if (g_app.auth_store &&
               auth_user_verify(g_app.auth_store, username, password, &claims) == 0) {
        authed = 1;   /* claims заполнены (user_id, role) */
    }
    if (!authed) {
        if (g_app.audit) {
            AuditEvent aev = {
                .type           = AUDIT_AUTH_FAIL,
                .user_id        = username,
                .role           = ROLE_VIEWER,
                .resource       = "",
                .action_detail  = "invalid credentials",
                .correlation_id = req->correlation_id,
                .client_ip      = "",
                .result_code    = 401,
                .duration_ms    = 0,
            };
            audit_log_event(g_app.audit, &aev);
        }
        http_resp_error(resp, 401, "invalid credentials");
        return;
    }
    claims.exp = (int64_t)time(NULL) + 86400;  // 1 day
    char token[1024];
    if (auth_jwt_sign(g_app.jwt_secret, &claims, token, sizeof(token)) != 0) {
        http_resp_error(resp, 500, "token generation failed");
        return;
    }
    JBuf jb; jb_init(&jb, a, 512);
    jb_obj_begin(&jb);
    jb_key(&jb, "token"); jb_str(&jb, token);
    jb_key(&jb, "expires_in"); jb_int(&jb, 86400);
    jb_obj_end(&jb);
    http_resp_json(resp, 200, jb_done(&jb));
}

/* POST /api/auth/apikeys — выпускает новый API-ключ для пользователя. */
static void h_auth_apikey_create(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) {
        http_resp_error(resp, 403, "admin required");
        return;
    }
    if (!req->body || req->body_len == 0) {
        http_resp_error(resp, 400, "missing body");
        return;
    }
    Arena *a = req->arena;
    JVal *body = json_parse(a, req->body, req->body_len);
    if (!body || body->type != JV_OBJECT) {
        http_resp_error(resp, 400, "invalid json");
        return;
    }
    const char *user_id = json_str(json_get(body, "user_id"), NULL);
    const char *role_str = json_str(json_get(body, "role"), NULL);
    if (!user_id || !role_str) {
        http_resp_error(resp, 400, "missing user_id or role");
        return;
    }
    AuthRole role;
    if (strcmp(role_str, "admin") == 0) role = ROLE_ADMIN;
    else if (strcmp(role_str, "analyst") == 0) role = ROLE_ANALYST;
    else if (strcmp(role_str, "viewer") == 0) role = ROLE_VIEWER;
    else {
        http_resp_error(resp, 400, "invalid role");
        return;
    }
    char key[128];
    if (auth_apikey_create(g_app.auth_store, user_id, role, key, sizeof(key)) != 0) {
        http_resp_error(resp, 500, "key creation failed");
        return;
    }
    char *json_resp = arena_sprintf(a, "{\"key\":\"%s\",\"user_id\":\"%s\"}", key, user_id);
    http_resp_json(resp, 200, json_resp);
}

/* GET /api/auth/apikeys — список API-ключей текущего пользователя. */
static void h_auth_apikey_list(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) {
        http_resp_error(resp, 403, "admin required");
        return;
    }
    // Simple implementation: query SQLite
    sqlite3_stmt *stmt;
    const char *sql = "SELECT key, user_id, role, created_at FROM auth_keys WHERE revoked = 0;";
    if (sqlite3_prepare_v2(g_app.auth_store->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        http_resp_error(resp, 500, "db error");
        return;
    }
    Arena *a = req->arena;
    JBuf jb; jb_init(&jb, a, 4096);
    jb_arr_begin(&jb);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *key = (const char *)sqlite3_column_text(stmt, 0);
        const char *user_id = (const char *)sqlite3_column_text(stmt, 1);
        int role = sqlite3_column_int(stmt, 2);
        int64_t created_at = sqlite3_column_int64(stmt, 3);
        jb_obj_begin(&jb);
        jb_key(&jb, "key"); jb_str(&jb, key);
        jb_key(&jb, "user_id"); jb_str(&jb, user_id);
        jb_key(&jb, "role"); jb_str(&jb, role == ROLE_ADMIN ? "admin" : role == ROLE_ANALYST ? "analyst" : "viewer");
        jb_key(&jb, "created_at"); jb_int(&jb, created_at);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);
    sqlite3_finalize(stmt);
    http_resp_json(resp, 200, jb_done(&jb));
}

/* DELETE /api/auth/apikeys/:key — отзывает указанный API-ключ. */
static void h_auth_apikey_delete(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) {
        http_resp_error(resp, 403, "admin required");
        return;
    }
    const char *key = hm_get(&req->params, "key");
    if (!key) {
        http_resp_error(resp, 400, "missing key");
        return;
    }
    if (auth_apikey_revoke(g_app.auth_store, key) != 0) {
        http_resp_error(resp, 404, "key not found");
        return;
    }
    http_resp_json(resp, 200, "{\"ok\":true}");
}

/* GET /api/auth/me — возвращает идентичность и роль текущего вызывающего. */
static void h_auth_me(HttpReq *req, HttpResp *resp) {
    const char *role_str = req->auth.role == ROLE_ADMIN ? "admin" :
                           req->auth.role == ROLE_ANALYST ? "analyst" : "viewer";
    char *json_resp = arena_sprintf(req->arena, "{\"user_id\":\"%s\",\"role\":\"%s\"}",
                                    req->auth.user_id, role_str);
    http_resp_json(resp, 200, json_resp);
}

/* ── User account handlers (учётки для входа) ── */

/* POST /api/auth/users — заводит нового пользователя (логин+пароль+роль). */
static void h_auth_user_create(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) { http_resp_error(resp, 403, "admin required"); return; }
    if (!req->body || req->body_len == 0) { http_resp_error(resp, 400, "missing body"); return; }
    Arena *a = req->arena;
    JVal *body = json_parse(a, req->body, req->body_len);
    if (!body || body->type != JV_OBJECT) { http_resp_error(resp, 400, "invalid json"); return; }
    const char *username = json_str(json_get(body, "username"), NULL);
    const char *password = json_str(json_get(body, "password"), NULL);
    const char *role_str = json_str(json_get(body, "role"), NULL);
    if (!username || !password || !role_str) {
        http_resp_error(resp, 400, "missing username, password or role"); return;
    }
    if (strcmp(username, "admin") == 0) { http_resp_error(resp, 400, "reserved username"); return; }
    /* Второго администратора завести нельзя: единственный админ — встроенный 'admin'. */
    AuthRole role;
    if (strcmp(role_str, "analyst") == 0) role = ROLE_ANALYST;
    else if (strcmp(role_str, "viewer") == 0) role = ROLE_VIEWER;
    else if (strcmp(role_str, "admin") == 0) {
        http_resp_error(resp, 403, "cannot create admin users (only built-in 'admin' allowed)"); return;
    }
    else { http_resp_error(resp, 400, "invalid role"); return; }
    int rc = auth_user_create(g_app.auth_store, username, password, role);
    if (rc == -2) { http_resp_error(resp, 409, "username already exists"); return; }
    if (rc != 0)  { http_resp_error(resp, 500, "user creation failed"); return; }
    if (g_app.audit) {
        AuditEvent aev = {
            .type = AUDIT_POLICY_CHANGE, .user_id = req->auth.user_id, .role = req->auth.role,
            .resource = username, .action_detail = "user created",
            .correlation_id = req->correlation_id, .client_ip = "", .result_code = 200, .duration_ms = 0,
        };
        audit_log_event(g_app.audit, &aev);
    }
    char *json_resp = arena_sprintf(a, "{\"username\":\"%s\",\"role\":\"%s\"}", username, role_str);
    http_resp_json(resp, 200, json_resp);
}

/* GET /api/auth/users — список учёток (без паролей). */
static void h_auth_user_list(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) { http_resp_error(resp, 403, "admin required"); return; }
    sqlite3_stmt *stmt;
    const char *sql = "SELECT username, role, created_at, enabled FROM users ORDER BY created_at DESC;";
    if (sqlite3_prepare_v2(g_app.auth_store->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        http_resp_error(resp, 500, "db error"); return;
    }
    Arena *a = req->arena;
    JBuf jb; jb_init(&jb, a, 4096);
    jb_arr_begin(&jb);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *username = (const char *)sqlite3_column_text(stmt, 0);
        int role = sqlite3_column_int(stmt, 1);
        int64_t created_at = sqlite3_column_int64(stmt, 2);
        int enabled = sqlite3_column_int(stmt, 3);
        jb_obj_begin(&jb);
        jb_key(&jb, "username"); jb_str(&jb, username);
        jb_key(&jb, "role"); jb_str(&jb, role == ROLE_ADMIN ? "admin" : role == ROLE_ANALYST ? "analyst" : "viewer");
        jb_key(&jb, "created_at"); jb_int(&jb, created_at);
        jb_key(&jb, "enabled"); jb_int(&jb, enabled);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);
    sqlite3_finalize(stmt);
    http_resp_json(resp, 200, jb_done(&jb));
}

/* DELETE /api/auth/users/:username — удаляет учётку. */
static void h_auth_user_delete(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) { http_resp_error(resp, 403, "admin required"); return; }
    const char *username = hm_get(&req->params, "username");
    if (!username) { http_resp_error(resp, 400, "missing username"); return; }
    if (strcmp(username, "admin") == 0) { http_resp_error(resp, 400, "cannot delete admin"); return; }
    if (auth_user_delete(g_app.auth_store, username) != 0) {
        http_resp_error(resp, 404, "user not found"); return;
    }
    if (g_app.audit) {
        AuditEvent aev = {
            .type = AUDIT_POLICY_CHANGE, .user_id = req->auth.user_id, .role = req->auth.role,
            .resource = username, .action_detail = "user deleted",
            .correlation_id = req->correlation_id, .client_ip = "", .result_code = 200, .duration_ms = 0,
        };
        audit_log_event(g_app.audit, &aev);
    }
    http_resp_json(resp, 200, "{\"ok\":true}");
}

/* ── RBAC handlers ── */
static void h_rbac_policies_list(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) { http_resp_error(resp, 403, "admin required"); return; }
    Arena *a = req->arena;
    char *json = NULL;
    if (rbac_policy_list(g_app.rbac, (AuthRole)-1, &json, a) < 0) {
        http_resp_error(resp, 500, "rbac error"); return;
    }
    http_resp_json(resp, 200, json);
}

/* POST /api/rbac/policies — создаёт/обновляет RBAC-политику (только admin). */
static void h_rbac_policy_set(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) { http_resp_error(resp, 403, "admin required"); return; }
    if (!req->body || req->body_len == 0) { http_resp_error(resp, 400, "missing body"); return; }
    Arena *a = req->arena;
    JVal *body = json_parse(a, req->body, req->body_len);
    if (!body || body->type != JV_OBJECT) { http_resp_error(resp, 400, "invalid json"); return; }
    int role_i     = (int)json_int(json_get(body, "role"), -1);
    const char *pat= json_str(json_get(body, "table_pattern"), NULL);
    int actions    = (int)json_int(json_get(body, "allowed_actions"), 0);
    const char *rf = json_str(json_get(body, "row_filter"), "");
    if (role_i < 0 || !pat) { http_resp_error(resp, 400, "missing role or table_pattern"); return; }
    if (rbac_policy_set(g_app.rbac, (AuthRole)role_i, pat, (uint32_t)actions, rf) < 0) {
        http_resp_error(resp, 500, "rbac set failed"); return;
    }
    http_resp_json(resp, 200, "{\"ok\":true}");
}

/* DELETE /api/rbac/policies/:id — удаляет RBAC-политику по id (только admin). */
static void h_rbac_policy_del(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) { http_resp_error(resp, 403, "admin required"); return; }
    const char *id_s = hm_get(&req->params, "id");
    int id = id_s ? atoi(id_s) : 0;
    if (id <= 0) { http_resp_error(resp, 400, "invalid id"); return; }
    if (rbac_policy_del(g_app.rbac, id) < 0) {
        http_resp_error(resp, 500, "rbac delete failed"); return;
    }
    http_resp_json(resp, 200, "{\"ok\":true}");
}

/* Достаёт значение параметра key из query-string qs (arena-копия), NULL если нет. */
static char *qs_get(const char *qs, const char *key, Arena *a) {
    if (!qs || !key) return NULL;
    size_t klen = strlen(key);
    const char *p = qs;
    while (*p) {
        if (strncmp(p, key, klen) == 0 && p[klen] == '=') {
            const char *val = p + klen + 1;
            const char *end = strchr(val, '&');
            size_t vlen = end ? (size_t)(end - val) : strlen(val);
            char *out = arena_alloc(a, vlen + 1);
            memcpy(out, val, vlen); out[vlen] = '\0';
            return out;
        }
        p = strchr(p, '&');
        if (!p) break;
        p++;
    }
    return NULL;
}

/* ── Audit handlers ── */
static void h_audit_query(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) { http_resp_error(resp, 403, "admin required"); return; }
    Arena *a = req->arena;
    const char *uid   = qs_get(req->query, "user_id", a);
    const char *from_s= qs_get(req->query, "from",    a);
    const char *to_s  = qs_get(req->query, "to",      a);
    const char *lim_s = qs_get(req->query, "limit",   a);
    int64_t from_ts = from_s ? (int64_t)atoll(from_s) : 0;
    int64_t to_ts   = to_s   ? (int64_t)atoll(to_s)   : 0;
    int     limit   = lim_s  ? atoi(lim_s)             : 100;
    char *json = NULL;
    if (audit_log_query(g_app.audit, uid, from_ts, to_ts, limit, &json, a) < 0) {
        http_resp_error(resp, 500, "audit error"); return;
    }
    http_resp_json(resp, 200, json);
}

/* ── Matview handlers ── */

/* Materialise a datamart: run its definition SQL and write the result into the
 * table `target_table` (= the view's own name, so it's queryable by name).
 * Returns the row count, or -1 on parse/exec failure. Wired via mvs_set_exec_fn. */
static int matview_exec_fn(const char *def_sql, const char *target_table, void *app_v) {
    App *app = (App *)app_v;
    Arena *a = arena_create(4 * 1024 * 1024);
    if (!a) return -1;
    int n = -1;
    Stmt *stmt = sql_parse(a, def_sql, strlen(def_sql));
    if (stmt && !stmt->error) {
        tl_exec_error[0] = '\0';
        RS *rs = exec_stmt(a, stmt, NULL);
        /* A missing source table fails the refresh (n stays -1) rather than
         * silently materialising an empty datamart. A truncated result also
         * fails: a datamart with silently missing rows is worse than a stale one. */
        if (rs && rs->truncated) {
            LOG_ERROR("matview %s: определение возвращает > %d строк — результат "
                      "усечён, обновление витрины отклонено.", target_table, MAX_RS_ROWS);
        } else if (rs && !tl_exec_error[0]) {
            n = write_rs_to_table(app, a, target_table, rs);
        }
        tl_exec_error[0] = '\0';
    }
    arena_destroy(a);
    return n;
}

/* POST /api/matviews — создаёт datamart (def SQL + cron) и материализует сразу. */
static void h_matview_create(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN && req->auth.role != ROLE_ANALYST) {
        http_resp_error(resp, 403, "insufficient role"); return;
    }
    if (!req->body || req->body_len == 0) { http_resp_error(resp, 400, "missing body"); return; }
    Arena *a = req->arena;
    JVal *body = json_parse(a, req->body, req->body_len);
    if (!body || body->type != JV_OBJECT) { http_resp_error(resp, 400, "invalid json"); return; }
    const char *name = json_str(json_get(body, "name"), NULL);
    const char *sql  = json_str(json_get(body, "definition_sql"), NULL);
    if (!name || !sql) { http_resp_error(resp, 400, "name and definition_sql required"); return; }
    MatView mv; memset(&mv, 0, sizeof(mv));
    strncpy(mv.name,           name, sizeof(mv.name)-1);
    strncpy(mv.definition_sql, sql,  sizeof(mv.definition_sql)-1);
    mv.refresh_mode = (MvRefreshMode)(int)json_int(json_get(body,"refresh_mode"),0);
    const char *cron = json_str(json_get(body,"refresh_cron"), "");
    strncpy(mv.refresh_cron, cron, sizeof(mv.refresh_cron)-1);
    JVal *srcs = json_get(body, "source_tables");
    if (srcs && srcs->type == JV_ARRAY) {
        for (size_t si = 0; si < srcs->nitems && mv.nsource_tables < 16; si++) {
            const char *t = json_str(srcs->items[si], NULL);
            if (t) strncpy(mv.source_tables[mv.nsource_tables++], t, 127);
        }
    }
    if (mvs_create_view(g_app.matviews, &mv) < 0) {
        http_resp_error(resp, 500, "matview create failed"); return;
    }
    mvs_refresh(g_app.matviews, name, &g_app);   /* materialise immediately on create */
    http_resp_json(resp, 200, arena_sprintf(a, "{\"name\":\"%s\",\"ok\":true}", name));
}

/* GET /api/matviews — список зарегистрированных datamart-ов. */
static void h_matviews_list(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a = req->arena;
    char *json = NULL;
    if (mvs_list(g_app.matviews, &json, a) < 0) {
        http_resp_error(resp, 500, "matview list failed"); return;
    }
    http_resp_json(resp, 200, json);
}

/* POST /api/matviews/:name/refresh — принудительно пересобирает datamart. */
static void h_matview_refresh(HttpReq *req, HttpResp *resp) {
    const char *name = hm_get(&req->params, "name");
    if (!name) { http_resp_error(resp, 400, "missing name"); return; }
    if (mvs_refresh(g_app.matviews, name, &g_app) < 0) {
        http_resp_error(resp, 500, "refresh failed"); return;
    }
    http_resp_json(resp, 200, "{\"ok\":true}");
}

/* DELETE /api/matviews/:name — удаляет datamart (только admin). */
static void h_matview_drop(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) { http_resp_error(resp, 403, "admin required"); return; }
    const char *name = hm_get(&req->params, "name");
    if (!name) { http_resp_error(resp, 400, "missing name"); return; }
    /* Drop the datamart's materialised table — unless a pipeline or another
     * datamart still reads it (the datamart's table = its own name). */
    if (name[0]
        && !table_used_by_other_pipeline(name, "")
        && !mvs_table_used(g_app.matviews, name, name)) {
        catalog_drop_table(g_app.catalog, name);
    } else {
        LOG_INFO("matview drop: keep table '%s' (still used by a pipeline/datamart)", name);
    }
    if (mvs_drop_view(g_app.matviews, name) < 0) {
        http_resp_error(resp, 500, "drop failed"); return;
    }
    http_resp_json(resp, 200, "{\"ok\":true}");
}

/* ── GET /api/matviews/:name/runs ── refresh history of one datamart ── */
static void h_matview_runs(HttpReq *req, HttpResp *resp) {
    const char *name = hm_get(&req->params, "name");
    if (!name) { http_resp_error(resp, 400, "missing name"); return; }
    Arena *a = arena_create(65536); char *json = NULL;
    if (g_app.matviews) mvs_list_refreshes(g_app.matviews, name, &json, a);
    http_resp_json(resp, 200, json ? json : "[]");
}

/* ── Cluster status ── */
static void h_cluster_status(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a = req->arena;
    if (!g_app.replicator) {
        http_resp_json(resp, 200,
            "{\"cluster_mode\":false,\"is_leader\":false,\"replicas\":[]}");
        return;
    }
    char *buf = arena_alloc(a, 4096);
    replicator_get_status(g_app.replicator, buf, 4096);
    http_resp_json(resp, 200, buf);
}

/* ══ Справочник подключений к внешним системам ═══════════════════════════════
 * Подключение — именованная пара «тип коннектора + параметры доступа», на
 * которую шаг конвейера ссылается через connection_id. Запись хранится целиком
 * как JSON в таблице connections каталога:
 *   {id,name,type,config,created_at,updated_at}
 *
 * Роли у подключения НЕТ: это просто доступ к системе. Будет она источником или
 * приёмником — решает шаг конвейера, он же задаёт запрос к данным (что читать
 * или куда писать). Раньше здесь хранилось поле direction; оно убрано, а из
 * старых записей вычищается миграцией при старте.
 *
 * config тут без потолка (TEXT в SQLite), в отличие от
 * PipelineStep.connector_config[1024], который на длинных конфигах Kafka с
 * TLS/SASL молча обрезается.
 * ─────────────────────────────────────────────────────────────────────────── */

/* Ключи конфига, значения которых нельзя отдавать в UI открытым текстом
 * (см. схемы плагинов в каталоге lib/connector/plugins). */
/* Kafka-коннектор принимает одно свойство в трёх написаниях: snake_case,
 * точечное имя librdkafka и его же с префиксом "kafka." (см. cfg_get в
 * kafka_connector.c). Приводим к канону: срезаем префикс, точки →
 * подчёркивания. Пустой результат означает, что канонизировать нечего. */
static void conn_key_canon(const char *k, char *out, size_t outsz) {
    out[0] = '\0';
    if (!k || !outsz) return;
    const char *src = (strncmp(k, "kafka.", 6) == 0) ? k + 6 : k;
    size_t n = strlen(src);
    if (n >= outsz) n = outsz - 1;
    for (size_t i = 0; i < n; i++) out[i] = (src[i] == '.') ? '_' : src[i];
    out[n] = '\0';
}

/* Ищет значение в конфиге, сравнивая ключи по канону, а не побайтово. Нужно
 * там, где присланный ключ и сохранённый могут быть в разных написаниях одного
 * свойства: точное сравнение промахивалось, и вызывающий считал, что поля в
 * прежней записи нет. */
static JVal *conn_cfg_get_canon(JVal *cfg, const char *k) {
    if (!cfg || cfg->type != JV_OBJECT || !k) return NULL;
    JVal *exact = json_get(cfg, k);
    if (exact) return exact;

    char want[128], have[128];
    conn_key_canon(k, want, sizeof want);
    if (!want[0]) return NULL;
    for (size_t i = 0; i < cfg->nkeys; i++) {
        conn_key_canon(cfg->keys[i], have, sizeof have);
        if (!strcmp(want, have)) return cfg->vals[i];
    }
    return NULL;
}

static bool conn_key_is_secret(const char *k) {
    static const char *SECRETS[] = {
        "password", "siebel_password", "auth_token", "sasl_password",
        "ssl_key_password", "ssl_keystore_password", "ssl_truststore_password",
        "schema_registry_auth", NULL
    };
    if (!k) return false;

    /* Сравнение строго по snake_case означало, что пароль, записанный как
     * "kafka.sasl.password", коннектор подхватывает и всё работает, а
     * маскирование считает ключ несекретным и отдаёт значение открытым текстом. */
    char canon[128];
    conn_key_canon(k, canon, sizeof canon);

    for (int i = 0; SECRETS[i]; i++)
        if (!strcmp(k, SECRETS[i]) || !strcmp(canon, SECRETS[i])) return true;
    return false;
}

/* jb_key не экранирует имя ключа (lib/core/json.c), поэтому ключи конфига
 * ограничиваем безопасным алфавитом — иначе имя с кавычкой ломает JSON. */
static bool conn_key_is_safe(const char *k) {
    if (!k || !k[0] || strlen(k) > 64) return false;
    for (const char *p = k; *p; p++)
        if (!isalnum((unsigned char)*p) && *p != '_' && *p != '.' && *p != '-')
            return false;
    return true;
}

static bool conn_type_is_known(const char *t) {
    static const char *TYPES[] = {
        "csv", "parquet", "json_http", "postgresql", "postgres", "greenplum",
        "oracle", "kafka", "siebel", "soap", "xml", NULL
    };
    for (int i = 0; TYPES[i]; i++) if (!strcmp(t, TYPES[i])) return true;
    return false;
}


/* Пишет config, подменяя НЕПУСТЫЕ секреты на CONN_SECRET_MASK. Пустой секрет
 * остаётся пустым, чтобы UI отличал «пароль задан» от «пароля нет». */
static void conn_write_config_masked(JBuf *jb, JVal *cfg) {
    if (!cfg || cfg->type != JV_OBJECT) { jb_raw(jb, "{}"); return; }
    jb_obj_begin(jb);
    for (size_t i = 0; i < cfg->nkeys; i++) {
        jb_key(jb, cfg->keys[i]);
        JVal *v = cfg->vals[i];
        if (conn_key_is_secret(cfg->keys[i])) {
            const char *s = (v && v->type == JV_STRING) ? v->s : "";
            jb_str(jb, (s && s[0]) ? CONN_SECRET_MASK : "");
        } else {
            jval_serialize(jb, v);
        }
    }
    jb_obj_end(jb);
}

/* Отдаёт запись наружу: всё как в хранилище, но config замаскирован. */
static void conn_write_public(JBuf *jb, JVal *rec) {
    jb_obj_begin(jb);
    for (size_t i = 0; i < rec->nkeys; i++) {
        if (!strcmp(rec->keys[i], "direction")) continue;   /* поле упразднено */
        jb_key(jb, rec->keys[i]);
        if (!strcmp(rec->keys[i], "config")) conn_write_config_masked(jb, rec->vals[i]);
        else                                 jval_serialize(jb, rec->vals[i]);
    }
    /* Живое состояние постоянной сессии из пула — чтобы в разделе «Подключения»
     * было видно, подключено сейчас или нет, а не только в момент прогона. */
    const char *state = NULL, *lerr = NULL; int64_t lok = 0;
    int live = conn_pool_status(g_app.conn_pool, json_str(json_get(rec, "id"), ""),
                                &state, &lok, &lerr);
    jb_key(jb, "status");   jb_str(jb, state ? state : "idle");
    jb_key(jb, "sessions"); jb_int(jb, live);
    jb_key(jb, "last_ok");  jb_int(jb, lok);
    if (lerr && lerr[0]) { jb_key(jb, "last_error"); jb_str(jb, lerr); }
    jb_obj_end(jb);
}

/* Склеивает присланный config с сохранённым: секрет, пришедший пустым или
 * равным маске, берётся из прежней записи («поле не трогали»). */
static void conn_write_config_merged(JBuf *jb, JVal *incoming, JVal *stored_cfg) {
    jb_obj_begin(jb);
    if (incoming && incoming->type == JV_OBJECT) {
        for (size_t i = 0; i < incoming->nkeys; i++) {
            const char *k = incoming->keys[i];
            JVal *v = incoming->vals[i];
            jb_key(jb, k);
            if (conn_key_is_secret(k)) {
                const char *s = (v && v->type == JV_STRING) ? v->s : "";
                if (!s[0] || !strcmp(s, CONN_SECRET_MASK)) {
                    /* Поиск по канону, а не по точному ключу. Конфиг, заведённый
                     * в точечном написании ("sasl.password"), UI канонизирует и
                     * присылает как "sasl_password" с маской: точное сравнение
                     * промахивалось, и вместо сохранённого пароля записывалась
                     * пустая строка — то есть любое сохранение формы молча
                     * стирало пароль. */
                    JVal *old = conn_cfg_get_canon(stored_cfg, k);
                    const char *os = (old && old->type == JV_STRING) ? old->s : "";
                    jb_str(jb, os);
                    continue;
                }
            }
            jval_serialize(jb, v);
        }
    }
    jb_obj_end(jb);
}

/* Общая часть POST и PUT: валидация тела, слияние с прежней записью, запись в
 * каталог. Возвращает 0 и кладёт в *out_public замаскированный JSON ответа;
 * при ошибке возвращает HTTP-код и текст в *err. */
static int conn_save(Arena *a, const char *id, JVal *body, JVal *stored,
                     char **out_public, const char **err) {
    const char *name = json_str(json_get(body, "name"), "");
    const char *type = json_str(json_get(body, "type"), "");
    if (!name[0]) { *err = "missing name"; return 400; }
    if (!type[0]) { *err = "missing type"; return 400; }
    if (!conn_type_is_known(type)) { *err = "unknown connector type"; return 400; }

    JVal *cfg = json_get(body, "config");
    if (cfg && cfg->type == JV_OBJECT) {
        for (size_t i = 0; i < cfg->nkeys; i++)
            if (!conn_key_is_safe(cfg->keys[i])) { *err = "invalid config key"; return 400; }
    }

    JVal *stored_cfg = stored ? json_get(stored, "config") : NULL;
    int64_t now = (int64_t)time(NULL);
    int64_t created = stored ? (int64_t)json_int(json_get(stored, "created_at"), now) : now;

    /* Полная запись — уходит в каталог как есть, с настоящими секретами. */
    JBuf full; jb_init(&full, a, 1024);
    jb_obj_begin(&full);
    jb_key(&full, "id");         jb_str(&full, id);
    jb_key(&full, "name");       jb_str(&full, name);
    jb_key(&full, "type");       jb_str(&full, type);
    jb_key(&full, "config");     conn_write_config_merged(&full, cfg, stored_cfg);
    jb_key(&full, "created_at"); jb_int(&full, created);
    jb_key(&full, "updated_at"); jb_int(&full, now);
    jb_obj_end(&full);
    const char *full_json = jb_done(&full);

    if (catalog_save_connection(g_app.catalog, id, full_json) != 0) {
        *err = "save failed"; return 500;
    }
    /* Параметры доступа могли измениться — рвём постоянные сессии, иначе
     * прежний пароль/адрес «залипнет» в живом соединении. */
    conn_pool_invalidate(g_app.conn_pool, id);

    /* Ответ — та же запись, но с замаскированными секретами. */
    JVal *rec = json_parse(a, full_json, strlen(full_json));
    JBuf pub; jb_init(&pub, a, 1024);
    if (rec && rec->type == JV_OBJECT) conn_write_public(&pub, rec);
    else                               jb_raw(&pub, "{}");
    *out_public = (char *)jb_done(&pub);
    return 0;
}

/* Разовая миграция справочника: вычистить упразднённое поле direction из
 * сохранённых записей. Идемпотентна — записи без поля не трогает. Вызывается
 * один раз при старте гейтвея. */
void api_connections_migrate(void) {
    if (!g_app.catalog) return;
    Arena *a = arena_create(65536);
    char *raw = NULL;
    if (catalog_list_connections(g_app.catalog, &raw, a) != 0 || !raw) {
        arena_destroy(a); return;
    }
    JVal *arr = json_parse(a, raw, strlen(raw));
    if (!arr || arr->type != JV_ARRAY) { arena_destroy(a); return; }

    int fixed = 0;
    for (size_t i = 0; i < arr->nitems; i++) {
        JVal *rec = arr->items[i];
        if (!rec || rec->type != JV_OBJECT) continue;
        if (!json_get(rec, "direction")) continue;          /* уже чистая */
        const char *id = json_str(json_get(rec, "id"), "");
        if (!id[0]) continue;

        JBuf jb; jb_init(&jb, a, 1024);
        jb_obj_begin(&jb);
        for (size_t k = 0; k < rec->nkeys; k++) {
            if (!strcmp(rec->keys[k], "direction")) continue;
            jb_key(&jb, rec->keys[k]);
            jval_serialize(&jb, rec->vals[k]);
        }
        jb_obj_end(&jb);
        if (catalog_save_connection(g_app.catalog, id, jb_done(&jb)) == 0) fixed++;
    }
    if (fixed)
        LOG_INFO("справочник подключений: убрано упразднённое поле direction в %d записях", fixed);
    arena_destroy(a);
}

/* Обслуживание пула постоянных сессий: пинг простаивающих + ПРОГРЕВ — открытие
 * сессии для подключений, у которых её ещё нет. Благодаря прогреву соединение
 * существует постоянно, а не только на время запуска конвейера. Вызывается
 * фоновым потоком гейтвея (см. main.c), поэтому недоступная система задерживает
 * только этот поток, а не старт и не конвейеры. */
void api_conn_pool_maintain(void) {
    if (!g_app.conn_pool || !g_app.catalog) return;

    conn_pool_tick(g_app.conn_pool);      /* keepalive уже открытых */

    Arena *a = arena_create(65536);
    char *raw = NULL;
    if (catalog_list_connections(g_app.catalog, &raw, a) != 0 || !raw) {
        arena_destroy(a); return;
    }
    JVal *arr = json_parse(a, raw, strlen(raw));
    if (!arr || arr->type != JV_ARRAY) { arena_destroy(a); return; }

    /* Обход ограничен по числу попыток за тик: подключений может быть много, а
     * коннект к недоступной системе упирается в таймаут. Без предела один тик
     * растягивался бы на минуты, и статусы остальных подключений не обновлялись
     * бы вовсе. Остальные проверятся на следующем тике. */
    const int MAX_WARMUP_PER_TICK = 8;
    int attempts = 0;

    for (size_t i = 0; i < arr->nitems && attempts < MAX_WARMUP_PER_TICK; i++) {
        JVal *rec = arr->items[i];
        if (!rec || rec->type != JV_OBJECT) continue;
        const char *id   = json_str(json_get(rec, "id"), "");
        const char *type = json_str(json_get(rec, "type"), "");
        if (!id[0] || !type[0]) continue;

        const char *state = NULL;
        conn_pool_status(g_app.conn_pool, id, &state, NULL, NULL);
        if (state && !strcmp(state, "ok")) continue;   /* сессия уже живая */
        attempts++;

        char so_path[1024];
        snprintf(so_path, sizeof(so_path), "%s/%s_connector.so",
                 g_app.plugins_dir, connector_so_name(type));
        const char *cfg = conn_merge_config(a, json_get(rec, "config"), NULL);

        char err[256] = {0};
        ConnLease *l = conn_pool_acquire(g_app.conn_pool, id, so_path, cfg, true, err, sizeof(err));
        if (l) conn_pool_release(l, true);       /* сразу вернули — сессия осталась жить */
    }
    arena_destroy(a);
}

/* GET /api/connections — весь справочник одним массивом, секреты замаскированы. */
static void h_connections_list(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a = arena_create(65536);
    char *raw = NULL;
    if (catalog_list_connections(g_app.catalog, &raw, a) != 0 || !raw) {
        http_resp_error(resp, 500, "list failed"); arena_destroy(a); return;
    }
    JVal *arr = json_parse(a, raw, strlen(raw));
    JBuf jb; jb_init(&jb, a, 4096);
    jb_arr_begin(&jb);
    if (arr && arr->type == JV_ARRAY)
        for (size_t i = 0; i < arr->nitems; i++)
            if (arr->items[i] && arr->items[i]->type == JV_OBJECT)
                conn_write_public(&jb, arr->items[i]);
    jb_arr_end(&jb);
    http_resp_json(resp, 200, (char *)jb_done(&jb));
    arena_destroy(a);
}

/* GET /api/connections/:id */
static void h_connection_get(HttpReq *req, HttpResp *resp) {
    const char *id = hm_get(&req->params, "id");
    if (!id || !id[0]) { http_resp_error(resp, 400, "missing id"); return; }
    Arena *a = arena_create(16384);
    char *raw = NULL;
    if (catalog_load_connection(g_app.catalog, id, &raw, a) != 0) {
        http_resp_error(resp, 404, "connection not found"); arena_destroy(a); return;
    }
    JVal *rec = json_parse(a, raw, strlen(raw));
    if (!rec || rec->type != JV_OBJECT) {
        http_resp_error(resp, 500, "corrupt record"); arena_destroy(a); return;
    }
    JBuf jb; jb_init(&jb, a, 1024);
    conn_write_public(&jb, rec);
    http_resp_json(resp, 200, (char *)jb_done(&jb));
    arena_destroy(a);
}

/* POST /api/connections — создание. id можно передать свой, иначе генерится. */
static void h_connection_create(HttpReq *req, HttpResp *resp) {
    if (!req->body || !req->body_len) { http_resp_error(resp, 400, "empty body"); return; }
    Arena *a = arena_create(32768);
    JVal *body = json_parse(a, req->body, req->body_len);
    if (!body || body->type != JV_OBJECT) {
        http_resp_error(resp, 400, "invalid json"); arena_destroy(a); return;
    }
    char id[64];
    const char *want = json_str(json_get(body, "id"), "");
    if (want[0]) {
        if (!conn_key_is_safe(want)) {
            http_resp_error(resp, 400, "invalid id"); arena_destroy(a); return;
        }
        snprintf(id, sizeof(id), "%s", want);
    } else {
        static volatile int _cid_seq = 0;
        int seq = __atomic_fetch_add(&_cid_seq, 1, __ATOMIC_RELAXED);
        snprintf(id, sizeof(id), "c_%lld_%d", (long long)time(NULL), seq);
    }

    char *pub = NULL; const char *err = NULL;
    int rc = conn_save(a, id, body, NULL, &pub, &err);
    if (rc != 0) { http_resp_error(resp, rc, err); arena_destroy(a); return; }
    http_resp_json(resp, 201, pub);
    arena_destroy(a);
}

/* PUT /api/connections/:id — идемпотентное обновление (без DELETE+POST, чтобы
 * ссылающиеся конвейеры ни на мгновение не оставались с битой ссылкой). */
static void h_connection_update(HttpReq *req, HttpResp *resp) {
    const char *id = hm_get(&req->params, "id");
    if (!id || !id[0]) { http_resp_error(resp, 400, "missing id"); return; }
    if (!req->body || !req->body_len) { http_resp_error(resp, 400, "empty body"); return; }
    Arena *a = arena_create(32768);
    JVal *body = json_parse(a, req->body, req->body_len);
    if (!body || body->type != JV_OBJECT) {
        http_resp_error(resp, 400, "invalid json"); arena_destroy(a); return;
    }
    char *raw = NULL;
    if (catalog_load_connection(g_app.catalog, id, &raw, a) != 0) {
        http_resp_error(resp, 404, "connection not found"); arena_destroy(a); return;
    }
    JVal *stored = json_parse(a, raw, strlen(raw));

    char *pub = NULL; const char *err = NULL;
    int rc = conn_save(a, id, body, stored, &pub, &err);
    if (rc != 0) { http_resp_error(resp, rc, err); arena_destroy(a); return; }
    http_resp_json(resp, 200, pub);
    arena_destroy(a);
}

/* DELETE /api/connections/:id. Шаги, ссылавшиеся на удалённое подключение, при
 * запуске упадут с внятной ошибкой (см. step_resolve_connection) — молча
 * подставлять пустой конфиг и лезть в localhost:5432 хуже. */
static void h_connection_delete(HttpReq *req, HttpResp *resp) {
    const char *id = hm_get(&req->params, "id");
    if (!id || !id[0]) { http_resp_error(resp, 400, "missing id"); return; }
    if (catalog_delete_connection(g_app.catalog, id) != 0) {
        http_resp_error(resp, 500, "delete failed"); return;
    }
    conn_pool_invalidate(g_app.conn_pool, id);   /* закрыть постоянные сессии */
    http_resp_json(resp, 200, "{\"ok\":true}");
}

/* Регистрирует все HTTP-маршруты гейтвея в роутере (UI, таблицы, запросы,
 * пайплайны, коннекторы, auth/RBAC/audit, matview, кластер). */
void api_register_routes(Router *r) {
    router_add(r,"GET",  "/",           h_ui_html);
    router_add(r,"GET",  "/style.css",  h_ui_css);
    router_add(r,"GET",  "/app.js",     h_ui_js);
    router_add(r,"GET",  "/health",                  h_health);
    router_add(r,"GET",  "/api/tables",              h_tables_list);
    router_add(r,"POST", "/api/tables/query",        h_query);
    router_add(r,"POST", "/api/query/named",         h_query_named);
    router_add(r,"GET",  "/api/tables/:name/schema",      h_table_schema);
    router_add(r,"GET",  "/api/tables/:name/compression", h_table_compression);
    router_add(r,"DELETE","/api/tables/:name",       h_table_delete);
    router_add(r,"POST", "/api/ingest/csv",            h_ingest_csv);
    router_add(r,"POST", "/api/ingest/parquet",        h_ingest_parquet);
    router_add(r,"GET",  "/api/pipelines",           h_pipelines_list);
    router_add(r,"POST", "/api/pipelines",           h_pipeline_create);
    router_add(r,"POST", "/api/pipelines/preview-yaml", h_pipeline_preview_yaml);
    router_add(r,"POST", "/api/pipelines/preview-step", h_pipeline_preview_step);
    router_add(r,"POST", "/api/pipelines/from-template", h_pipeline_from_template);
    router_add(r,"GET",  "/api/pipelines/:id",       h_pipeline_get);
    router_add(r,"POST", "/api/pipelines/:id/run",   h_pipeline_run);
    router_add(r,"DELETE","/api/pipelines/:id",      h_pipeline_delete);
    router_add(r,"GET",  "/api/pipelines/:id/runs",  h_pipeline_runs);
    /* Step 4: event-driven triggers — webhook (public route, token=auth) */
    router_add(r,"POST", "/api/triggers/:token",     h_webhook_trigger);
    router_add(r,"GET",  "/api/triggers/:token",     h_webhook_trigger);
    router_add(r,"GET",  "/api/metrics",             h_metrics);
    router_add(r,"GET",  "/metrics",                 h_metrics_prometheus);
    router_add(r,"POST",   "/api/analytics/results",     h_result_save);
    router_add(r,"GET",    "/api/analytics/results",     h_result_list);
    router_add(r,"GET",    "/api/analytics/results/:id", h_result_get);
    router_add(r,"DELETE", "/api/analytics/results/:id", h_result_delete);
    router_add(r,"POST",  "/api/tables/:name/indexes",  h_index_create);
    router_add(r,"GET",   "/api/tables/:name/indexes",  h_index_list);
    router_add(r,"POST",  "/api/connector/probe/entities", h_connector_probe_entities);
    router_add(r,"POST",  "/api/connector/probe/ping",     h_connector_probe_ping);
    router_add(r,"POST",  "/api/connector/probe/preview",  h_connector_probe_preview);
    router_add(r,"POST",  "/api/connector/probe/schema",   h_connector_probe_schema);
    /* Справочник подключений. Статические пути — до :id (route_match берёт
     * первый подошедший маршрут в порядке регистрации). */
    router_add(r,"GET",   "/api/connections",      h_connections_list);
    router_add(r,"POST",  "/api/connections",      h_connection_create);
    router_add(r,"GET",   "/api/connections/:id",  h_connection_get);
    router_add(r,"PUT",   "/api/connections/:id",  h_connection_update);
    router_add(r,"DELETE","/api/connections/:id",  h_connection_delete);
    // Auth endpoints
    router_add(r,"POST", "/api/auth/token",    h_auth_token);
    router_add(r,"POST", "/api/auth/apikeys",  h_auth_apikey_create);
    router_add(r,"GET",  "/api/auth/apikeys",  h_auth_apikey_list);
    router_add(r,"DELETE","/api/auth/apikeys/:key", h_auth_apikey_delete);
    router_add(r,"GET",  "/api/auth/me",       h_auth_me);
    // User accounts (учётки для входа)
    router_add(r,"POST",  "/api/auth/users",           h_auth_user_create);
    router_add(r,"GET",   "/api/auth/users",           h_auth_user_list);
    router_add(r,"DELETE","/api/auth/users/:username", h_auth_user_delete);
    // RBAC endpoints
    router_add(r,"GET",   "/api/rbac/policies",      h_rbac_policies_list);
    router_add(r,"POST",  "/api/rbac/policies",      h_rbac_policy_set);
    router_add(r,"DELETE","/api/rbac/policies/:id",  h_rbac_policy_del);
    // Audit endpoints
    router_add(r,"GET",   "/api/audit",              h_audit_query);
    // Matview endpoints
    router_add(r,"POST",  "/api/matviews",           h_matview_create);
    router_add(r,"GET",   "/api/matviews",           h_matviews_list);
    router_add(r,"POST",  "/api/matviews/:name/refresh", h_matview_refresh);
    router_add(r,"GET",   "/api/matviews/:name/runs", h_matview_runs);
    router_add(r,"DELETE","/api/matviews/:name",     h_matview_drop);
    if (g_app.matviews) mvs_set_exec_fn(g_app.matviews, matview_exec_fn, &g_app);  /* wire materialisation */
    // Cluster endpoint
    router_add(r,"GET",   "/api/cluster/status",     h_cluster_status);
}
