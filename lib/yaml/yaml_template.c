/* yaml_expand_vars — {{ var }} substitution + vars: section handling.
 *
 * Pipelines for many similar tables (e.g. one per CDH table) share a single
 * YAML template; the per-table values come from a vars: block in the file
 * and/or an external dict passed by the caller (POST /api/pipelines/from-template
 * forwards its vars{} here). External values win over the file's vars:.
 *
 * The vars: block is cut out of the result so the downstream YAML parser — which
 * knows nothing about vars: — never sees it. Unknown {{ tokens }} are left as-is
 * with a LOG_WARN so a typo degrades to a visible literal instead of silently
 * vanishing. */
#include "yaml_template.h"
#include "../core/log.h"
#include <string.h>
#include <stdlib.h>

#define TMPL_MAX_VARS 128
#define TMPL_MAX_VAL  1024   /* value length incl NUL → ≤ 1023 chars */

/* Trim leading/trailing ASCII blanks (space, tab, CR, NL) of [s,e); also strips
 * one layer of matching surrounding quotes. Returns an arena copy. */
static char *trim_dup(Arena *a, const char *s, const char *e) {
    while (s < e && (*s == ' ' || *s == '\t' || *s == '\r' || *s == '\n')) s++;
    while (e > s && (e[-1] == ' ' || e[-1] == '\t' || e[-1] == '\r' || e[-1] == '\n')) e--;
    if (e - s >= 2 && ((s[0] == '"' && e[-1] == '"') || (s[0] == '\'' && e[-1] == '\''))) {
        s++; e--;
    }
    size_t n = (size_t)(e - s);
    if (n >= TMPL_MAX_VAL) n = TMPL_MAX_VAL - 1;
    return arena_strndup(a, s, n);
}

/* Append n bytes to a growable arena buffer (buf, cap, len by ref). Arena has no
 * realloc, so growth allocates a fresh larger block and copies — fine for the
 * short-lived per-request/per-file arenas this runs in. Returns 0 / -1 (OOM). */
static int buf_append(Arena *a, char **buf, size_t *cap, size_t *len,
                      const char *src, size_t n) {
    if (*len + n + 1 > *cap) {
        size_t ncap = *cap ? *cap : 4096;
        while (*len + n + 1 > ncap) ncap *= 2;
        char *nb = arena_alloc(a, ncap);
        if (!nb) return -1;
        if (*len) memcpy(nb, *buf, *len);
        *buf = nb; *cap = ncap;
    }
    memcpy(*buf + *len, src, n);
    *len += n;
    (*buf)[*len] = '\0';
    return 0;
}

/* Раскрывает {{ var }} в YAML-шаблоне и вырезает блок vars:.
 * a — арена для всех аллокаций; src/srclen — исходный текст; keys/vals/nkvs —
 * внешний словарь от вызывающего (перекрывает file-level vars:).
 * Возвращает новую строку в арене, "" для NULL-входа, или NULL при OOM. */
char *yaml_expand_vars(Arena *a,
                       const char *src, size_t srclen,
                       const char **keys, const char **vals, int nkvs) {
    if (!src) return arena_strdup(a, "");

    /* ── 1. Locate and parse the vars: section ── */
    const char *vars_start = NULL, *vars_end = NULL;
    char *vk[TMPL_MAX_VARS]; char *vv[TMPL_MAX_VARS]; int nv = 0;

    size_t i = 0;
    while (i < srclen) {
        size_t ls = i, le = i;
        while (le < srclen && src[le] != '\n') le++;
        size_t next = (le < srclen) ? le + 1 : le;

        /* Header must be at column 0 (not indented) and equal "vars:". */
        if (src[ls] != ' ' && src[ls] != '\t') {
            char *line = trim_dup(a, src + ls, src + le);
            if (!strcmp(line, "vars:")) {
                vars_start = src + ls;
                /* Consume indented "  key: value" lines. */
                size_t j = next;
                while (j < srclen && (src[j] == ' ' || src[j] == '\t')) {
                    size_t es = j, ee = j;
                    while (ee < srclen && src[ee] != '\n') ee++;
                    const char *colon = memchr(src + es, ':', (size_t)(ee - es));
                    if (colon && nv < TMPL_MAX_VARS) {
                        vk[nv] = trim_dup(a, src + es, colon);
                        vv[nv] = trim_dup(a, colon + 1, src + ee);
                        if (vk[nv][0]) nv++;
                    }
                    j = (ee < srclen) ? ee + 1 : ee;
                }
                vars_end = src + j;   /* first non-indented line, or EOF */
                break;
            }
        }
        i = next;
    }

    /* ── 2. Build the merged dictionary (vars: first, caller overrides) ── */
    const char *dk[TMPL_MAX_VARS]; const char *dv[TMPL_MAX_VARS]; int nd = 0;
    for (int k = 0; k < nv && nd < TMPL_MAX_VARS; k++) { dk[nd] = vk[k]; dv[nd] = vv[k]; nd++; }
    for (int k = 0; k < nkvs; k++) {
        if (!keys[k]) continue;
        int found = -1;
        for (int d = 0; d < nd; d++) if (!strcmp(dk[d], keys[k])) { found = d; break; }
        if (found >= 0) dv[found] = vals[k] ? vals[k] : "";
        else if (nd < TMPL_MAX_VARS) { dk[nd] = keys[k]; dv[nd] = vals[k] ? vals[k] : ""; nd++; }
    }

    /* ── 3. Emit, skipping the vars: block and expanding {{ var }} ── */
    size_t cap = srclen + 4096, olen = 0;
    char *out = arena_alloc(a, cap);
    if (!out) { LOG_ERROR("yaml template: arena exhausted"); return NULL; }
    out[0] = '\0';

    i = 0;
    while (i < srclen) {
        if (vars_start && src + i == vars_start) {   /* drop the whole vars: block */
            i = (size_t)(vars_end - src);
            continue;
        }
        if (src[i] == '{' && i + 1 < srclen && src[i + 1] == '{') {
            /* find closing }} */
            size_t j = i + 2;
            while (j + 1 < srclen && !(src[j] == '}' && src[j + 1] == '}')) j++;
            if (j + 1 < srclen && src[j] == '}' && src[j + 1] == '}') {
                char *name = trim_dup(a, src + i + 2, src + j);
                const char *val = NULL;
                for (int d = 0; d < nd; d++) if (!strcmp(dk[d], name)) { val = dv[d]; break; }
                if (val) {
                    if (buf_append(a, &out, &cap, &olen, val, strlen(val)) < 0) goto oom;
                } else {
                    /* leave the token verbatim so a typo is visible, not silent */
                    LOG_WARN("yaml template: unresolved variable '%s'", name);
                    if (buf_append(a, &out, &cap, &olen, src + i, (size_t)(j + 2 - i)) < 0) goto oom;
                }
                i = j + 2;
                continue;
            }
            /* no closing }} — emit '{' literally and move on */
        }
        if (buf_append(a, &out, &cap, &olen, src + i, 1) < 0) goto oom;
        i++;
    }
    return out;

oom:
    LOG_ERROR("yaml template: arena exhausted during expansion");
    return NULL;
}
