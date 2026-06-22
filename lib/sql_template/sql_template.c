#include "sql_template.h"
#include <string.h>
#include <stdio.h>

char *sql_quote_literal(Arena *a, const char *s)
{
    if (!s) s = "";
    size_t len = strlen(s);
    char  *out = arena_alloc(a, len * 2 + 3);
    size_t o = 0;
    out[o++] = '\'';
    for (const char *p = s; *p; p++) {
        if (*p == '\'') out[o++] = '\'';   /* double internal quotes */
        out[o++] = *p;
    }
    out[o++] = '\'';
    out[o]   = '\0';
    return out;
}

/* Grow `*buf` (cap `*cap`) so it can hold `*off + need + 1` bytes. */
static void ensure_cap(Arena *a, char **buf, size_t *cap, size_t off, size_t need)
{
    if (off + need + 1 <= *cap) return;
    size_t ncap = *cap;
    while (off + need + 1 > ncap) ncap *= 2;
    char *nb = arena_alloc(a, ncap);
    memcpy(nb, *buf, off);
    *buf = nb;
    *cap = ncap;
}

/* Format a JSON number param without a trailing ".0" when it is integral. */
static char *format_number(Arena *a, JVal *pv)
{
    double d = json_dbl(pv, 0);
    char buf[64];
    if (d == (double)(long long)d)
        snprintf(buf, sizeof(buf), "%lld", (long long)d);
    else
        snprintf(buf, sizeof(buf), "%.17g", d);
    return arena_strdup(a, buf);
}

char *sql_substitute_params(Arena *a, const char *sql, JVal *params,
                            SqlComputedFn resolver, void *resolver_ctx,
                            char *err, size_t errsz)
{
    if (!sql) sql = "";
    size_t cap = strlen(sql) * 2 + 256;
    char  *out = arena_alloc(a, cap);
    size_t off = 0;

    for (const char *q = sql; *q; ) {
        if (q[0] == '{' && q[1] == '{') {        /* {{ → literal { */
            ensure_cap(a, &out, &cap, off, 1);
            out[off++] = '{'; q += 2; continue;
        }
        if (*q != '{') {
            ensure_cap(a, &out, &cap, off, 1);
            out[off++] = *q++; continue;
        }

        /* Read placeholder name up to '}' */
        q++;  /* skip '{' */
        char   name[256];
        size_t nl = 0;
        while (*q && *q != '}' && nl < sizeof(name) - 1) name[nl++] = *q++;
        name[nl] = '\0';
        if (*q == '}') q++;

        char *value = NULL;  /* arena-allocated, already quoted/formatted */

        if (name[0] == '@') {
            if (!resolver) {
                snprintf(err, errsz, "computed param '{%s}' has no runtime context", name);
                return NULL;
            }
            value = resolver(resolver_ctx, a, name + 1, err, errsz);
            if (!value) return NULL;
        } else {
            JVal *pv = params ? json_get(params, name) : NULL;
            if (!pv) {
                snprintf(err, errsz, "unresolved SQL param '{%s}'", name);
                return NULL;
            }
            size_t namelen = strlen(name);
            bool   is_raw  = (namelen > 4 && !strcmp(name + namelen - 4, "_raw"));
            if (pv->type == JV_NUMBER) {
                value = format_number(a, pv);
            } else if (pv->type == JV_BOOL) {
                value = arena_strdup(a, json_bool(pv, false) ? "true" : "false");
            } else if (is_raw) {
                value = arena_strdup(a, json_str(pv, ""));      /* identifier, verbatim */
            } else {
                value = sql_quote_literal(a, json_str(pv, "")); /* quoted literal */
            }
        }

        size_t vl = strlen(value);
        ensure_cap(a, &out, &cap, off, vl);
        memcpy(out + off, value, vl);
        off += vl;
    }

    out[off] = '\0';
    return out;
}
