/*
 * qengine.c — векторизованный (батчевый) исполнитель запросов DataFlow OS.
 *
 * Содержит: вычислитель выражений (eval_expr/eval_bool) со строковыми и
 * fuzzy-match builtin'ами для MDM (levenshtein/jaro_winkler/normalize_*),
 * и набор физических операторов с единым pull-интерфейсом OpVtable
 * (open/next/close): scan, filter, limit, window, hash-agg, sort, join.
 * qengine_build() собирает дерево операторов из логического плана,
 * qengine_exec_json() прогоняет его и отдаёт строки результата в JSON.
 */
#include "qengine.h"
#include "../core/log.h"
#include "../core/json.h"
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <math.h>

/* ── Forward declarations ── */
static bool is_agg_func(Expr *e, const char **fn_out, Expr **arg_out);

/* ── Expression evaluator ── */
static bool bit_is_null(uint8_t *bm, int i) {
    return bm && !!(bm[i/8] & (1u << (i%8)));
}

/* Coerce any scalar to text for the string / fuzzy-match builtins below.
 * Returns NULL for SQL NULL so callers can propagate NULL. */
static const char *qe_text(Scalar s, Arena *a) {
    switch (s.type) {
    case SV_TEXT:   return s.val.sval;
    case SV_INT:    return arena_sprintf(a, "%lld", (long long)s.val.ival);
    case SV_DOUBLE: return arena_sprintf(a, "%.10g", s.val.fval);
    case SV_BOOL:   return s.val.bval ? "true" : "false";
    default:        return NULL;  /* SV_NULL */
    }
}

/* Levenshtein edit distance (two-row DP, arena-backed). */
int qe_levenshtein(const char *x, const char *y, Arena *a) {
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

/* Jaro-Winkler similarity in [0,1] (prefix weight p=0.1, max prefix 4). */
double qe_jaro_winkler(const char *s1, const char *s2, Arena *a) {
    size_t l1 = strlen(s1), l2 = strlen(s2);
    if (l1 == 0 || l2 == 0) return (l1 == l2) ? 1.0 : 0.0;
    size_t maxl = l1 > l2 ? l1 : l2;
    long win = (long)(maxl / 2) - 1; if (win < 0) win = 0;
    char *m1 = arena_calloc(a, l1);  /* match flags */
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

/* Trigram word similarity: fraction of query trigrams found in document.
 * Mirrors pg_trgm word_similarity(query, document). Returns [0.0, 1.0].
 * Byte-level (works for UTF-8 as long as both sides share the encoding). */
double qe_word_similarity(const char *query, const char *doc, Arena *a) {
    (void)a;
    if (!query || !doc) return 0.0;
    size_t qlen = strlen(query), dlen = strlen(doc);
    if (qlen < 3 && dlen < 3) return (strcasecmp(query, doc) == 0) ? 1.0 : 0.0;
    if (qlen < 3) return 0.0;

    int q_ntri = (int)qlen - 2;
    int d_ntri = (int)dlen > 2 ? (int)dlen - 2 : 0;
    if (q_ntri <= 0) return 0.0;

    int matches = 0;
    for (int i = 0; i < q_ntri; i++) {
        const char *qt = query + i;   /* 3-char trigram at position i */
        for (int j = 0; j < d_ntri; j++) {
            if (doc[j] == qt[0] && doc[j+1] == qt[1] && doc[j+2] == qt[2]) {
                matches++;
                break;   /* count each query trigram at most once */
            }
        }
    }
    return (double)matches / (double)q_ntri;
}

/* normalize_inn(s): keep digits only (drops spaces, dashes, letters). */
const char *qe_normalize_inn(const char *s, Arena *a) {
    if (!s) return "";
    size_t len = strlen(s);
    char *out = arena_alloc(a, len + 1);
    size_t n = 0;
    for (size_t i = 0; i < len; i++)
        if (s[i] >= '0' && s[i] <= '9') out[n++] = s[i];
    out[n] = '\0';
    return out;
}

/* normalize_name(s): lowercase, trim, collapse internal whitespace runs to a
 * single space. Case-folds BOTH ASCII and Cyrillic (UTF-8), mirroring pg_trgm's
 * case-insensitive behaviour — essential for Russian MDM name matching where the
 * same name arrives as «Иванов» from one system and «ИВАНОВ» from another. */
const char *qe_normalize_name(const char *s, Arena *a) {
    if (!s) return "";
    size_t len = strlen(s);
    char *out = arena_alloc(a, len + 1);   /* lowercase keeps byte length */
    size_t n = 0;
    bool prev_space = true;   /* trim leading */
    for (size_t i = 0; i < len; ) {
        unsigned char c = (unsigned char)s[i];
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
            if (!prev_space && n > 0) { out[n++] = ' '; prev_space = true; }
            i++;
            continue;
        }
        /* Cyrillic uppercase (2-byte UTF-8) → lowercase. А..Я = U+0410..U+042F,
         * Ё = U+0401. Lowercase counterparts add 0x20 to the code point. */
        if (c == 0xD0 && i + 1 < len) {
            unsigned char d = (unsigned char)s[i+1];
            if (d >= 0x90 && d <= 0x9F) {            /* А..П → а..п (stay 0xD0) */
                out[n++] = (char)0xD0; out[n++] = (char)(d + 0x20);
            } else if (d >= 0xA0 && d <= 0xAF) {     /* Р..Я → р..я (0xD0→0xD1) */
                out[n++] = (char)0xD1; out[n++] = (char)(d - 0x20);
            } else if (d == 0x81) {                  /* Ё → ё */
                out[n++] = (char)0xD1; out[n++] = (char)0x91;
            } else {                                  /* already lowercase / other */
                out[n++] = (char)c; out[n++] = (char)d;
            }
            i += 2; prev_space = false;
            continue;
        }
        out[n++] = (c >= 'A' && c <= 'Z') ? (char)(c + 32) : (char)c;   /* ASCII lower */
        prev_space = false;
        i++;
    }
    while (n > 0 && out[n-1] == ' ') n--;   /* trim trailing */
    out[n] = '\0';
    return out;
}

/* Рекурсивно вычисляет выражение для текущей строки (ctx->row) и возвращает
 * скаляр. NULL распространяется по правилам SQL; неизвестные операции дают NULL. */
Scalar eval_expr(Expr *e, EvalCtx *ctx, Arena *a) {
    Scalar s = {SV_NULL};
    if (!e) return s;
    switch (e->type) {
    case EXPR_LITERAL_INT:   s.type=SV_INT;    s.val.ival=e->ival;  return s;
    case EXPR_LITERAL_FLOAT: s.type=SV_DOUBLE; s.val.fval=e->fval;  return s;
    case EXPR_LITERAL_STR:   s.type=SV_TEXT;   s.val.sval=e->sval;  return s;
    case EXPR_LITERAL_BOOL:  s.type=SV_BOOL;   s.val.bval=e->bval;  return s;
    case EXPR_LITERAL_NULL:  return s;
    case EXPR_ALIAS:         return eval_expr(e->expr, ctx, a);
    case EXPR_COL: {
        if (!ctx || !ctx->schema || !ctx->batch) return s;
        for (int c = 0; c < ctx->schema->ncols; c++) {
            if (strcmp(ctx->schema->cols[c].name, e->name) != 0) continue;
            if (bit_is_null(ctx->batch->null_bitmap[c], ctx->row)) return s;
            switch (ctx->schema->cols[c].type) {
            case COL_INT64:  s.type=SV_INT;    s.val.ival=((int64_t*)ctx->batch->values[c])[ctx->row]; break;
            case COL_DOUBLE: s.type=SV_DOUBLE; s.val.fval=((double*)ctx->batch->values[c])[ctx->row]; break;
            case COL_TEXT:   s.type=SV_TEXT;   s.val.sval=((char**)ctx->batch->values[c])[ctx->row]; break;
            case COL_BOOL:   s.type=SV_BOOL;   s.val.bval=((int*)ctx->batch->values[c])[ctx->row]!=0; break;
            default: break;
            }
            return s;
        }
        return s;
    }
    case EXPR_FUNC: {
        const char *fn = e->func_name;
        /* In post-agg context (HAVING): look up pre-computed value as a column */
        if (ctx && ctx->schema && ctx->batch && is_agg_func(e, NULL, NULL)) {
            for (int c = 0; c < ctx->schema->ncols; c++) {
                if (!strcasecmp(ctx->schema->cols[c].name, fn)) {
                    if (ctx->batch->null_bitmap[c] &&
                        bit_is_null(ctx->batch->null_bitmap[c], ctx->row))
                        return s;
                    switch (ctx->schema->cols[c].type) {
                    case COL_INT64:  s.type=SV_INT;    s.val.ival=((int64_t*)ctx->batch->values[c])[ctx->row]; break;
                    case COL_DOUBLE: s.type=SV_DOUBLE; s.val.fval=((double*)ctx->batch->values[c])[ctx->row];  break;
                    case COL_TEXT:   s.type=SV_TEXT;   s.val.sval=((char**)ctx->batch->values[c])[ctx->row];   break;
                    default: break;
                    }
                    return s;
                }
            }
        }
        /* aggregate functions return 0/null in scalar context */
        if (!strcasecmp(fn,"count")) { s.type=SV_INT; s.val.ival=0; return s; }
        if (!strcasecmp(fn,"now"))   { s.type=SV_INT; s.val.ival=(int64_t)time(NULL); return s; }
        /* variadic string builtins (handle their own args) */
        if (!strcasecmp(fn,"coalesce") && e->nargs > 0) {
            for (int i = 0; i < e->nargs; i++) {
                Scalar v = eval_expr(e->args[i], ctx, a);
                if (v.type != SV_NULL) return v;
            }
            return s;  /* all NULL */
        }
        if (!strcasecmp(fn,"concat") && e->nargs > 0) {
            size_t cap = 64, off = 0;
            char *out = arena_alloc(a, cap);
            for (int i = 0; i < e->nargs; i++) {
                const char *t = qe_text(eval_expr(e->args[i], ctx, a), a);
                if (!t) continue;  /* NULL treated as empty, like SQL CONCAT */
                size_t tl = strlen(t);
                if (off + tl + 1 > cap) { while (off+tl+1>cap) cap*=2;
                    char *nb=arena_alloc(a,cap); memcpy(nb,out,off); out=nb; }
                memcpy(out+off, t, tl); off += tl;
            }
            out[off] = '\0';
            s.type=SV_TEXT; s.val.sval=out; return s;
        }
        if (e->nargs > 0) {
            Scalar arg0 = eval_expr(e->args[0], ctx, a);
            if (!strcasecmp(fn,"abs")) {
                if (arg0.type==SV_INT)    { s=arg0; if(s.val.ival<0) s.val.ival=-s.val.ival; return s; }
                if (arg0.type==SV_DOUBLE) { s=arg0; s.val.fval=fabs(s.val.fval); return s; }
            }
            if (!strcasecmp(fn,"upper") && arg0.type==SV_TEXT) {
                char *out=arena_strdup(a,arg0.val.sval);
                for(char*p=out;*p;p++) if(*p>='a'&&*p<='z') *p-=32;
                s.type=SV_TEXT; s.val.sval=out; return s;
            }
            if (!strcasecmp(fn,"lower") && arg0.type==SV_TEXT) {
                char *out=arena_strdup(a,arg0.val.sval);
                for(char*p=out;*p;p++) if(*p>='A'&&*p<='Z') *p+=32;
                s.type=SV_TEXT; s.val.sval=out; return s;
            }
            if (!strcasecmp(fn,"length") && arg0.type==SV_TEXT) {
                s.type=SV_INT; s.val.ival=(int64_t)strlen(arg0.val.sval); return s;
            }
            if (!strcasecmp(fn,"trim")) {
                const char *t = qe_text(arg0, a);
                if (!t) return s;  /* NULL → NULL */
                const char *b = t;
                while (*b==' '||*b=='\t'||*b=='\n'||*b=='\r') b++;
                const char *en = t + strlen(t);
                while (en>b && (en[-1]==' '||en[-1]=='\t'||en[-1]=='\n'||en[-1]=='\r')) en--;
                char *out = arena_alloc(a, (size_t)(en-b)+1);
                memcpy(out, b, (size_t)(en-b)); out[en-b]='\0';
                s.type=SV_TEXT; s.val.sval=out; return s;
            }
            if (!strcasecmp(fn,"substr") && e->nargs >= 2) {
                const char *t = qe_text(arg0, a);
                if (!t) return s;
                Scalar a1 = eval_expr(e->args[1], ctx, a);
                long tl = (long)strlen(t);
                long start = (a1.type==SV_INT)?(long)a1.val.ival
                           : (a1.type==SV_DOUBLE)?(long)a1.val.fval : 1;
                if (start < 1) start = 1;            /* SQL substr is 1-indexed */
                long n = tl - (start - 1); if (n < 0) n = 0;
                if (e->nargs >= 3) {
                    Scalar a2 = eval_expr(e->args[2], ctx, a);
                    long want = (a2.type==SV_INT)?(long)a2.val.ival
                              : (a2.type==SV_DOUBLE)?(long)a2.val.fval : n;
                    if (want < 0) want = 0;
                    if (want < n) n = want;
                }
                char *out = arena_alloc(a, (size_t)n + 1);
                memcpy(out, t + (start - 1), (size_t)n); out[n]='\0';
                s.type=SV_TEXT; s.val.sval=out; return s;
            }
            if (!strcasecmp(fn,"levenshtein") && e->nargs >= 2) {
                const char *x = qe_text(arg0, a);
                const char *y = qe_text(eval_expr(e->args[1], ctx, a), a);
                if (!x || !y) return s;  /* NULL → NULL */
                s.type=SV_INT; s.val.ival = qe_levenshtein(x, y, a); return s;
            }
            if (!strcasecmp(fn,"jaro_winkler") && e->nargs >= 2) {
                const char *x = qe_text(arg0, a);
                const char *y = qe_text(eval_expr(e->args[1], ctx, a), a);
                if (!x || !y) return s;
                s.type=SV_DOUBLE; s.val.fval = qe_jaro_winkler(x, y, a); return s;
            }
            if (!strcasecmp(fn,"word_similarity") && e->nargs >= 2) {
                const char *x = qe_text(arg0, a);
                const char *y = qe_text(eval_expr(e->args[1], ctx, a), a);
                if (!x || !y) return s;
                s.type=SV_DOUBLE; s.val.fval = qe_word_similarity(x, y, a); return s;
            }
            if (!strcasecmp(fn,"normalize_inn") && e->nargs >= 1) {
                const char *x = qe_text(arg0, a);
                if (!x) return s;
                s.type=SV_TEXT; s.val.sval = qe_normalize_inn(x, a); return s;
            }
            if (!strcasecmp(fn,"normalize_name") && e->nargs >= 1) {
                const char *x = qe_text(arg0, a);
                if (!x) return s;
                s.type=SV_TEXT; s.val.sval = qe_normalize_name(x, a); return s;
            }
        }
        return s;
    }
    case EXPR_BINOP: {
        Scalar L = eval_expr(e->left, ctx, a);
        /* short-circuit for AND/OR */
        if (e->op == OP_AND) {
            bool lb = (L.type==SV_BOOL)?L.val.bval:(L.type!=SV_NULL);
            if (!lb) { s.type=SV_BOOL; s.val.bval=false; return s; }
            Scalar R = eval_expr(e->right, ctx, a);
            s.type=SV_BOOL; s.val.bval=(R.type==SV_BOOL)?R.val.bval:(R.type!=SV_NULL); return s;
        }
        if (e->op == OP_OR) {
            bool lb = (L.type==SV_BOOL)?L.val.bval:(L.type!=SV_NULL);
            if (lb) { s.type=SV_BOOL; s.val.bval=true; return s; }
            Scalar R = eval_expr(e->right, ctx, a);
            s.type=SV_BOOL; s.val.bval=(R.type==SV_BOOL)?R.val.bval:(R.type!=SV_NULL); return s;
        }
        Scalar R = eval_expr(e->right, ctx, a);
        /* numeric coercion */
        double lv=0, rv=0; bool numeric=false;
        if ((L.type==SV_INT||L.type==SV_DOUBLE)&&(R.type==SV_INT||R.type==SV_DOUBLE)) {
            lv = L.type==SV_INT ? (double)L.val.ival : L.val.fval;
            rv = R.type==SV_INT ? (double)R.val.ival : R.val.fval;
            numeric = true;
        }
        switch (e->op) {
        case OP_EQ:
            if (L.type==SV_TEXT&&R.type==SV_TEXT) { s.type=SV_BOOL; s.val.bval=!strcmp(L.val.sval,R.val.sval); return s; }
            s.type=SV_BOOL; s.val.bval=numeric&&lv==rv; return s;
        case OP_NE:
            if (L.type==SV_TEXT&&R.type==SV_TEXT) { s.type=SV_BOOL; s.val.bval=!!strcmp(L.val.sval,R.val.sval); return s; }
            s.type=SV_BOOL; s.val.bval=numeric&&lv!=rv; return s;
        case OP_LT: s.type=SV_BOOL; s.val.bval=numeric&&lv< rv; return s;
        case OP_LE: s.type=SV_BOOL; s.val.bval=numeric&&lv<=rv; return s;
        case OP_GT: s.type=SV_BOOL; s.val.bval=numeric&&lv> rv; return s;
        case OP_GE: s.type=SV_BOOL; s.val.bval=numeric&&lv>=rv; return s;
        case OP_ADD:
            if (L.type==SV_INT&&R.type==SV_INT) { s.type=SV_INT; s.val.ival=L.val.ival+R.val.ival; return s; }
            s.type=SV_DOUBLE; s.val.fval=lv+rv; return s;
        case OP_SUB:
            if (L.type==SV_INT&&R.type==SV_INT) { s.type=SV_INT; s.val.ival=L.val.ival-R.val.ival; return s; }
            s.type=SV_DOUBLE; s.val.fval=lv-rv; return s;
        case OP_MUL:
            if (L.type==SV_INT&&R.type==SV_INT) { s.type=SV_INT; s.val.ival=L.val.ival*R.val.ival; return s; }
            s.type=SV_DOUBLE; s.val.fval=lv*rv; return s;
        case OP_DIV:
            s.type=SV_DOUBLE; s.val.fval=rv!=0?lv/rv:0; return s;
        case OP_BETWEEN: {
            /* right is packed as AND(low,high) */
            Scalar lo=eval_expr(e->right->left,ctx,a);
            Scalar hi=eval_expr(e->right->right,ctx,a);
            double loval=lo.type==SV_INT?(double)lo.val.ival:lo.val.fval;
            double hival=hi.type==SV_INT?(double)hi.val.ival:hi.val.fval;
            s.type=SV_BOOL; s.val.bval=numeric&&lv>=loval&&lv<=hival; return s;
        }
        case OP_LIKE: {
            if (L.type!=SV_TEXT||R.type!=SV_TEXT) { s.type=SV_BOOL; s.val.bval=false; return s; }
            /* simple % wildcard matching */
            const char *str=L.val.sval, *pat=R.val.sval;
            /* naive but correct for basic patterns */
            const char *star=strchr(pat,'%');
            bool match=false;
            if (!star) match=!strcmp(str,pat);
            else if (star==pat) match=!!strstr(str,pat+1);
            else {
                size_t pre=(size_t)(star-pat);
                match=!strncmp(str,pat,pre)&&!!strstr(str+pre,star+1);
            }
            s.type=SV_BOOL; s.val.bval=match; return s;
        }
        default: break;
        }
        return s;
    }
    case EXPR_UNOP: {
        if (e->op==OP_IS_NULL) {
            Scalar inner=eval_expr(e->left,ctx,a);
            s.type=SV_BOOL; s.val.bval=(inner.type==SV_NULL); return s;
        }
        if (e->op==OP_IS_NOT_NULL) {
            Scalar inner=eval_expr(e->left,ctx,a);
            s.type=SV_BOOL; s.val.bval=(inner.type!=SV_NULL); return s;
        }
        if (e->op==OP_NOT) {
            Scalar inner=eval_expr(e->left,ctx,a);
            s.type=SV_BOOL; s.val.bval=!(inner.type==SV_BOOL?inner.val.bval:(inner.type!=SV_NULL)); return s;
        }
        if (e->op==OP_SUB) {
            Scalar inner=eval_expr(e->left,ctx,a);
            if(inner.type==SV_INT){s=inner;s.val.ival=-s.val.ival;return s;}
            if(inner.type==SV_DOUBLE){s=inner;s.val.fval=-s.val.fval;return s;}
        }
        return s;
    }
    default: return s;
    }
}

/* Вычисляет выражение и приводит результат к булеву (для WHERE/HAVING/ON).
 * SQL NULL трактуется как false. */
bool eval_bool(Expr *e, EvalCtx *ctx, Arena *a) {
    Scalar s = eval_expr(e, ctx, a);
    if (s.type == SV_BOOL)   return s.val.bval;
    if (s.type == SV_NULL)   return false;
    if (s.type == SV_INT)    return s.val.ival != 0;
    if (s.type == SV_DOUBLE) return s.val.fval != 0.0;
    if (s.type == SV_TEXT)   return s.val.sval && *s.val.sval;
    return false;
}

/* ── Scan operator ── */
typedef struct {
    char   data_dir[512];
    char   table_name[128];
    FILE  *wal_file;
    int    done;
    char   delim;
    char **headers;
    int    ncols;
} ScanState;

/* Читает schema.json таблицы (имена/типы/nullable колонок) в Schema. NULL при ошибке. */
static Schema *scan_load_schema(Arena *a, const char *data_dir, const char *table_name) {
    char path[700];
    snprintf(path, sizeof(path), "%s/%s/schema.json", data_dir, table_name);
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END); long sz = ftell(f); rewind(f);
    if (sz <= 0 || sz > 65536) { fclose(f); return NULL; }
    char *buf = arena_alloc(a, (size_t)sz + 1);
    fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[sz] = '\0';
    JVal *root = json_parse(a, buf, (size_t)sz);
    if (!root || root->type != JV_OBJECT) return NULL;
    JVal *cols_arr = json_get(root, "cols");
    if (!cols_arr || cols_arr->type != JV_ARRAY) return NULL;
    int ncols = (int)json_int(json_get(root, "ncols"), 0);
    if (ncols <= 0 || ncols > MAX_COLS) return NULL;
    Schema *sc = arena_calloc(a, sizeof(Schema));
    sc->cols = arena_calloc(a, sizeof(ColDef) * (size_t)ncols);
    sc->ncols = ncols;
    for (int i = 0; i < ncols && i < (int)cols_arr->nitems; i++) {
        JVal *col = cols_arr->items[i];
        sc->cols[i].name     = arena_strdup(a, json_str(json_get(col, "name"), ""));
        sc->cols[i].type     = (ColType)(int)json_int(json_get(col, "type"), 0);
        sc->cols[i].nullable = json_bool(json_get(col, "nullable"), false);
    }
    return sc;
}

static int scan_open(Operator *op) {
    ScanState *st = op->state;
    if (!op->output_schema)
        op->output_schema = scan_load_schema(op->arena, st->data_dir, st->table_name);
    char path[700];
    snprintf(path, sizeof(path), "%s/%s/wal.bin", st->data_dir, st->table_name);
    st->wal_file = fopen(path, "r");
    st->done = (st->wal_file == NULL);
    return 0;
}

/* Читает из WAL до BATCH_SIZE строк, разбирает CSV-значения по типам колонок
 * и заполняет один ColBatch. Возвращает 1 (EOF) когда строк больше нет. */
static int scan_next(Operator *op, ColBatch **out) {
    ScanState *st = op->state;
    if (st->done || !st->wal_file) return 1; /* EOF */
    Schema *schema = op->output_schema;
    if (!schema) return 1;
    int ncols = schema->ncols;
    ColBatch *batch = arena_calloc(op->arena, sizeof(ColBatch));
    batch->schema = schema; batch->ncols = ncols;
    int64_t **iv = arena_calloc(op->arena, ncols * sizeof(void*));
    double  **dv = arena_calloc(op->arena, ncols * sizeof(void*));
    char   ***sv = arena_calloc(op->arena, ncols * sizeof(void*));
    for (int c = 0; c < ncols; c++) {
        switch (schema->cols[c].type) {
        case COL_INT64:  iv[c]=arena_alloc(op->arena,BATCH_SIZE*sizeof(int64_t)); batch->values[c]=iv[c]; break;
        case COL_DOUBLE: dv[c]=arena_alloc(op->arena,BATCH_SIZE*sizeof(double));  batch->values[c]=dv[c]; break;
        default:         sv[c]=arena_alloc(op->arena,BATCH_SIZE*sizeof(char*));   batch->values[c]=sv[c]; break;
        }
        batch->null_bitmap[c]=arena_calloc(op->arena,(BATCH_SIZE+7)/8);
    }
    /* Read WAL TLV records */
    int row = 0;
    uint32_t rec_len;
    char line_buf[65536];
    while (row < BATCH_SIZE) {
        if (fread(&rec_len, 4, 1, st->wal_file) != 1) { st->done=1; break; }
        if (rec_len >= sizeof(line_buf)) { fseek(st->wal_file, rec_len, SEEK_CUR); continue; }
        if (fread(line_buf, 1, rec_len, st->wal_file) != rec_len) { st->done=1; break; }
        line_buf[rec_len] = '\0';
        /* parse comma-separated line */
        char *p = line_buf; int col = 0;
        while (col < ncols) {
            char *comma = strchr(p, ',');
            size_t vlen = comma ? (size_t)(comma - p) : strlen(p);
            /* strip \n */
            while (vlen > 0 && (p[vlen-1]=='\n'||p[vlen-1]=='\r')) vlen--;
            char tmp[4096]; if(vlen>=sizeof(tmp)) vlen=sizeof(tmp)-1;
            memcpy(tmp,p,vlen); tmp[vlen]='\0';
            if (strcmp(tmp,"NULL")==0) {
                batch->null_bitmap[col][row/8] |= (1u<<(row%8));
            } else {
                switch (schema->cols[col].type) {
                case COL_INT64:  iv[col][row]=strtoll(tmp,NULL,10); break;
                case COL_DOUBLE: dv[col][row]=strtod(tmp,NULL); break;
                case COL_BOOL:   ((int*)sv[col])[row]=(strcasecmp(tmp,"true")==0||strcmp(tmp,"1")==0); break;
                default:         sv[col][row]=arena_strdup(op->arena,tmp); break;
                }
            }
            p = comma ? comma+1 : p+strlen(p);
            col++;
        }
        row++;
    }
    batch->nrows = row;
    *out = batch;
    return row > 0 ? 0 : 1;
}

static void scan_close(Operator *op) {
    ScanState *st = op->state;
    if (st->wal_file) { fclose(st->wal_file); st->wal_file=NULL; }
}

static const OpVtable SCAN_VT = {scan_open, scan_next, scan_close};

Operator *op_scan(Arena *a, const char *table, Schema *schema, const char *data_dir) {
    Operator *op = arena_calloc(a, sizeof(Operator));
    op->vt = &SCAN_VT; op->arena = a; op->output_schema = schema;
    ScanState *st = arena_calloc(a, sizeof(ScanState));
    strncpy(st->table_name, table, sizeof(st->table_name)-1);
    strncpy(st->data_dir, data_dir, sizeof(st->data_dir)-1);
    op->state = st;
    return op;
}

/* ── Filter operator ── */
typedef struct { Expr *predicate; } FilterState;

static int filter_open(Operator *op) { return op->left->vt->open(op->left); }

/* Тянет батчи из дочернего оператора и оставляет строки, прошедшие predicate.
 * Пустые после фильтрации батчи пропускаются — берём следующий из ребёнка. */
static int filter_next(Operator *op, ColBatch **out) {
    FilterState *st = op->state;
    for (;;) {
        ColBatch *src = NULL;
        int rc = op->left->vt->next(op->left, &src);
        if (rc != 0) return rc;
        /* filter rows */
        ColBatch *dst = arena_calloc(op->arena, sizeof(ColBatch));
        dst->schema = src->schema; dst->ncols = src->ncols;
        /* allocate same layout */
        for (int c = 0; c < src->ncols; c++) {
            size_t esz = (src->schema->cols[c].type==COL_INT64)?sizeof(int64_t):(src->schema->cols[c].type==COL_DOUBLE)?sizeof(double):sizeof(char*);
            dst->values[c]      = arena_alloc(op->arena, (size_t)src->nrows * esz);
            dst->null_bitmap[c] = arena_calloc(op->arena, ((size_t)src->nrows+7)/8);
        }
        int out_row = 0;
        EvalCtx ctx = {src->schema, src, 0};
        for (int r = 0; r < src->nrows; r++) {
            ctx.row = r;
            if (!eval_bool(st->predicate, &ctx, op->arena)) continue;
            /* copy row */
            for (int c = 0; c < src->ncols; c++) {
                bool is_null = src->null_bitmap[c] && !!(src->null_bitmap[c][r/8]&(1u<<(r%8)));
                if (is_null) { dst->null_bitmap[c][out_row/8] |= 1u<<(out_row%8); continue; }
                switch (src->schema->cols[c].type) {
                case COL_INT64:  ((int64_t*)dst->values[c])[out_row]=((int64_t*)src->values[c])[r]; break;
                case COL_DOUBLE: ((double*)dst->values[c])[out_row]=((double*)src->values[c])[r]; break;
                default:         ((char**)dst->values[c])[out_row]=((char**)src->values[c])[r]; break;
                }
            }
            out_row++;
        }
        dst->nrows = out_row;
        *out = dst;
        if (out_row > 0) return 0;
        /* empty batch — try next batch from child */
    }
}

static void filter_close(Operator *op) { op->left->vt->close(op->left); }
static const OpVtable FILTER_VT = {filter_open, filter_next, filter_close};

Operator *op_filter(Arena *a, Operator *src, Expr *predicate) {
    Operator *op = arena_calloc(a, sizeof(Operator));
    op->vt=&FILTER_VT; op->left=src; op->arena=a;
    op->output_schema=src->output_schema;
    FilterState *st=arena_calloc(a,sizeof(FilterState)); st->predicate=predicate;
    op->state=st;
    return op;
}

/* ── Limit operator ── */
typedef struct { int64_t limit, offset, emitted, skipped; } LimitState;

static int limit_open(Operator *op) { return op->left->vt->open(op->left); }

/* Применяет OFFSET/LIMIT: пропускает offset строк, затем отдаёт не более limit.
 * limit < 0 означает «без ограничения». */
static int limit_next(Operator *op, ColBatch **out) {
    LimitState *st = op->state;
    if (st->limit >= 0 && st->emitted >= st->limit) return 1;
    ColBatch *src=NULL;
    int rc = op->left->vt->next(op->left, &src);
    if (rc != 0) return rc;
    /* apply offset/limit */
    int start=0;
    if (st->skipped < st->offset) {
        int64_t skip = st->offset - st->skipped;
        if (skip >= src->nrows) { st->skipped+=src->nrows; *out=src; src->nrows=0; return 0; }
        start=(int)skip; st->skipped=st->offset;
    }
    int avail=src->nrows-start;
    if (st->limit>=0&&st->emitted+avail>st->limit) avail=(int)(st->limit-st->emitted);
    /* create sub-batch */
    ColBatch *dst=arena_calloc(op->arena,sizeof(ColBatch));
    dst->schema=src->schema; dst->ncols=src->ncols;
    for(int c=0;c<src->ncols;c++){
        size_t esz=(src->schema->cols[c].type==COL_INT64)?sizeof(int64_t):(src->schema->cols[c].type==COL_DOUBLE)?sizeof(double):sizeof(char*);
        dst->values[c]=arena_alloc(op->arena,(size_t)avail*esz+(size_t)1);
        dst->null_bitmap[c]=arena_calloc(op->arena,((size_t)avail+7)/8);
        for(int r=0;r<avail;r++){
            int sr=start+r;
            bool is_null=src->null_bitmap[c]&&!!(src->null_bitmap[c][sr/8]&(1u<<(sr%8)));
            if(is_null){dst->null_bitmap[c][r/8]|=1u<<(r%8);continue;}
            switch(src->schema->cols[c].type){
            case COL_INT64:  ((int64_t*)dst->values[c])[r]=((int64_t*)src->values[c])[sr]; break;
            case COL_DOUBLE: ((double*)dst->values[c])[r]=((double*)src->values[c])[sr]; break;
            default:         ((char**)dst->values[c])[r]=((char**)src->values[c])[sr]; break;
            }
        }
    }
    dst->nrows=avail; st->emitted+=avail;
    *out=dst; return 0;
}

static void limit_close(Operator *op) { op->left->vt->close(op->left); }
static const OpVtable LIMIT_VT={limit_open,limit_next,limit_close};

Operator *op_limit(Arena *a, Operator *src, int64_t limit, int64_t offset) {
    Operator *op=arena_calloc(a,sizeof(Operator));
    op->vt=&LIMIT_VT; op->left=src; op->arena=a; op->output_schema=src->output_schema;
    LimitState *st=arena_calloc(a,sizeof(LimitState));
    st->limit=limit; st->offset=offset;
    op->state=st; return op;
}

/* ── Window operator (blocking: materialises all rows, computes ROW_NUMBER/RANK/LAG) ── */
typedef struct {
    Expr     **partition_keys;  int npart;
    OrderItem *order_keys;      int norder;
    Expr     **window_exprs;    int nwexprs;
    /* Materialised result batch, emitted in chunks */
    ColBatch  *result;
    int        total_rows;
    int        cur_pos;
    bool       built;
} WindowState;

static int window_open(Operator *op) { return op->left->vt->open(op->left); }

/* Блокирующий: при первом вызове материализует все строки ребёнка в один
 * буфер, затем отдаёт их чанками по BATCH_SIZE. */
static int window_next(Operator *op, ColBatch **out) {
    WindowState *st = op->state;

    if (!st->built) {
        /* Drain all batches from child into a flat per-column list */
        int cap = 256;
        int nrows = 0;
        int ncols = op->output_schema ? op->output_schema->ncols : 0;

        /* Temporary storage: parallel arrays per column */
        void **cols = arena_calloc(op->arena, (size_t)ncols * sizeof(void*));
        for (int c = 0; c < ncols && op->output_schema; c++) {
            size_t esz = op->output_schema->cols[c].type == COL_INT64  ? sizeof(int64_t) :
                         op->output_schema->cols[c].type == COL_DOUBLE ? sizeof(double)  :
                                                                          sizeof(char*);
            cols[c] = arena_alloc(op->arena, (size_t)cap * esz);
        }
        uint8_t **nulls = arena_calloc(op->arena, (size_t)ncols * sizeof(uint8_t*));
        for (int c = 0; c < ncols; c++)
            nulls[c] = arena_calloc(op->arena, ((size_t)cap + 7) / 8);

        ColBatch *src = NULL;
        while (op->left->vt->next(op->left, &src) == 0 && src && src->nrows > 0) {
            if (nrows + src->nrows > cap) {
                int nc2 = (nrows + src->nrows) * 2;
                for (int c = 0; c < ncols && op->output_schema; c++) {
                    size_t esz = op->output_schema->cols[c].type == COL_INT64  ? sizeof(int64_t) :
                                 op->output_schema->cols[c].type == COL_DOUBLE ? sizeof(double)  :
                                                                                  sizeof(char*);
                    void *nb = arena_alloc(op->arena, (size_t)nc2 * esz);
                    memcpy(nb, cols[c], (size_t)nrows * esz);
                    cols[c] = nb;
                    uint8_t *nb2 = arena_calloc(op->arena, ((size_t)nc2 + 7) / 8);
                    memcpy(nb2, nulls[c], ((size_t)nrows + 7) / 8);
                    nulls[c] = nb2;
                }
                cap = nc2;
            }
            for (int r = 0; r < src->nrows; r++) {
                for (int c = 0; c < ncols && op->output_schema; c++) {
                    bool is_null = src->null_bitmap[c] && !!(src->null_bitmap[c][r/8] & (1u << (r%8)));
                    int dr = nrows + r;
                    if (is_null) { nulls[c][dr/8] |= 1u << (dr%8); continue; }
                    switch (op->output_schema->cols[c].type) {
                    case COL_INT64:  ((int64_t*)cols[c])[dr] = ((int64_t*)src->values[c])[r]; break;
                    case COL_DOUBLE: ((double* )cols[c])[dr] = ((double* )src->values[c])[r]; break;
                    default:         ((char**  )cols[c])[dr] = ((char**  )src->values[c])[r]; break;
                    }
                }
            }
            nrows += src->nrows;
            src = NULL;
        }

        /* Build the result batch from materialised data */
        ColBatch *rb = arena_calloc(op->arena, sizeof(ColBatch));
        rb->schema = op->output_schema;
        rb->ncols  = ncols;
        rb->nrows  = nrows;
        for (int c = 0; c < ncols; c++) {
            rb->values[c]      = cols[c];
            rb->null_bitmap[c] = nulls[c];
        }
        st->result     = rb;
        st->total_rows = nrows;
        st->built      = true;
    }

    if (st->cur_pos >= st->total_rows) return 1;

    int avail = st->total_rows - st->cur_pos;
    if (avail > BATCH_SIZE) avail = BATCH_SIZE;

    /* Emit a sub-batch from [cur_pos, cur_pos+avail) */
    ColBatch *dst = arena_calloc(op->arena, sizeof(ColBatch));
    dst->schema = op->output_schema;
    dst->ncols  = st->result->ncols;
    int ncols   = dst->ncols;

    for (int c = 0; c < ncols && op->output_schema; c++) {
        size_t esz = op->output_schema->cols[c].type == COL_INT64  ? sizeof(int64_t) :
                     op->output_schema->cols[c].type == COL_DOUBLE ? sizeof(double)  :
                                                                      sizeof(char*);
        dst->values[c]      = arena_alloc(op->arena, (size_t)avail * esz);
        dst->null_bitmap[c] = arena_calloc(op->arena, ((size_t)avail + 7) / 8);
        for (int r = 0; r < avail; r++) {
            int sr = st->cur_pos + r;
            bool is_null = st->result->null_bitmap[c] &&
                           !!(st->result->null_bitmap[c][sr/8] & (1u << (sr%8)));
            if (is_null) { dst->null_bitmap[c][r/8] |= 1u << (r%8); continue; }
            switch (op->output_schema->cols[c].type) {
            case COL_INT64:  ((int64_t*)dst->values[c])[r] = ((int64_t*)st->result->values[c])[sr]; break;
            case COL_DOUBLE: ((double* )dst->values[c])[r] = ((double* )st->result->values[c])[sr]; break;
            default:         ((char**  )dst->values[c])[r] = ((char**  )st->result->values[c])[sr]; break;
            }
        }
    }
    dst->nrows = avail;
    st->cur_pos += avail;
    *out = dst;
    return 0;
}

static void window_close(Operator *op) { op->left->vt->close(op->left); }
static const OpVtable WINDOW_VT = {window_open, window_next, window_close};

Operator *op_window(Arena *a, Operator *src, Expr **window_exprs, int nwexprs) {
    Operator *op = arena_calloc(a, sizeof(Operator));
    op->vt = &WINDOW_VT; op->left = src; op->arena = a;
    op->output_schema = src ? src->output_schema : NULL;
    WindowState *st = arena_calloc(a, sizeof(WindowState));
    st->window_exprs = window_exprs;
    st->nwexprs = nwexprs;
    op->state = st;
    return op;
}

/* ── Aggregate / sort / project helpers ── */

/* Является ли выражение агрегатом (count/sum/avg/min/max). Опционально
 * возвращает имя функции и её первый аргумент. Разворачивает EXPR_ALIAS. */
static bool is_agg_func(Expr *e, const char **fn_out, Expr **arg_out) {
    if (!e) return false;
    if (e->type == EXPR_ALIAS) return is_agg_func(e->expr, fn_out, arg_out);
    if (e->type != EXPR_FUNC) return false;
    const char *fn = e->func_name;
    if (!strcasecmp(fn,"count")||!strcasecmp(fn,"sum")||
        !strcasecmp(fn,"avg")||!strcasecmp(fn,"min")||!strcasecmp(fn,"max")) {
        if (fn_out) *fn_out = fn;
        if (arg_out) *arg_out = (e->nargs > 0) ? e->args[0] : NULL;
        return true;
    }
    return false;
}

static const char *expr_col_name(Expr *e) {
    if (!e) return "?";
    if (e->type == EXPR_ALIAS) return e->alias;
    if (e->type == EXPR_COL) return e->name;
    if (e->type == EXPR_FUNC) return e->func_name;
    return "expr";
}

static bool scalar_eq(Scalar a, Scalar b) {
    if (a.type != b.type) return false;
    switch (a.type) {
    case SV_INT:    return a.val.ival == b.val.ival;
    case SV_DOUBLE: return a.val.fval == b.val.fval;
    case SV_TEXT:   return a.val.sval && b.val.sval && strcmp(a.val.sval, b.val.sval)==0;
    case SV_BOOL:   return a.val.bval == b.val.bval;
    default:        return true;
    }
}

/* ── Hash-agg operator ── */
#define AGG_MAX_GROUPS 4096
#define AGG_MAX_EXPRS  64

typedef struct {
    Expr   **group_keys;  int ngroup;
    Expr   **out_exprs;   int nout;
    int      ngroups;
    /* [ngroups][ngroup] group key values */
    Scalar  *gkeys;       /* flat: [g*ngroup + k] */
    /* [ngroups][nout] accumulators */
    double  *sum_acc;     /* numeric sum */
    int64_t *cnt_acc;     /* row count per group per expr */
    Scalar  *min_acc;
    Scalar  *max_acc;
    int8_t  *has_val;     /* for min/max init */
    bool     built;
    int      emit_pos;
    Schema  *out_schema;
} AggState;

/* Линейный поиск группы по значениям ключей; при отсутствии создаёт новую
 * (текстовые ключи копируются в арену). Возвращает индекс группы или -1 при
 * переполнении AGG_MAX_GROUPS. */
static int agg_find_group(AggState *st, Scalar *keys, Arena *a) {
    for (int g = 0; g < st->ngroups; g++) {
        bool match = true;
        for (int k = 0; k < st->ngroup && match; k++)
            match = scalar_eq(st->gkeys[g * st->ngroup + k], keys[k]);
        if (match) return g;
    }
    /* new group */
    if (st->ngroups >= AGG_MAX_GROUPS) return -1;
    int g = st->ngroups++;
    for (int k = 0; k < st->ngroup; k++) {
        Scalar s = keys[k];
        if (s.type == SV_TEXT && s.val.sval)
            s.val.sval = arena_strdup(a, s.val.sval);
        st->gkeys[g * st->ngroup + k] = s;
    }
    return g;
}

/* Обновляет аккумуляторы группы g для выходного выражения oi одним значением:
 * счётчик строк, сумму (для sum/avg) и текущие min/max. */
static void agg_accumulate(AggState *st, int g, int oi, const char *fn, Scalar val, Arena *a) {
    int idx = g * st->nout + oi;
    st->cnt_acc[idx]++;
    if (!strcasecmp(fn,"count")) return;
    if (val.type == SV_NULL) return;
    if (!strcasecmp(fn,"sum") || !strcasecmp(fn,"avg")) {
        double v = (val.type==SV_INT) ? (double)val.val.ival : val.val.fval;
        st->sum_acc[idx] += v;
    }
    if (!strcasecmp(fn,"min")) {
        if (!st->has_val[idx]) {
            st->min_acc[idx] = val;
            if (val.type==SV_TEXT && val.val.sval)
                st->min_acc[idx].val.sval = arena_strdup(a, val.val.sval);
            st->has_val[idx] = 1;
        } else {
            Scalar cur = st->min_acc[idx];
            bool less = false;
            if (val.type==SV_INT)    less = val.val.ival < cur.val.ival;
            else if (val.type==SV_DOUBLE) less = val.val.fval < cur.val.fval;
            else if (val.type==SV_TEXT && val.val.sval && cur.val.sval)
                less = strcmp(val.val.sval, cur.val.sval) < 0;
            if (less) {
                st->min_acc[idx] = val;
                if (val.type==SV_TEXT && val.val.sval)
                    st->min_acc[idx].val.sval = arena_strdup(a, val.val.sval);
            }
        }
    }
    if (!strcasecmp(fn,"max")) {
        if (!st->has_val[idx]) {
            st->max_acc[idx] = val;
            if (val.type==SV_TEXT && val.val.sval)
                st->max_acc[idx].val.sval = arena_strdup(a, val.val.sval);
            st->has_val[idx] = 1;
        } else {
            Scalar cur = st->max_acc[idx];
            bool more = false;
            if (val.type==SV_INT)    more = val.val.ival > cur.val.ival;
            else if (val.type==SV_DOUBLE) more = val.val.fval > cur.val.fval;
            else if (val.type==SV_TEXT && val.val.sval && cur.val.sval)
                more = strcmp(val.val.sval, cur.val.sval) > 0;
            if (more) {
                st->max_acc[idx] = val;
                if (val.type==SV_TEXT && val.val.sval)
                    st->max_acc[idx].val.sval = arena_strdup(a, val.val.sval);
            }
        }
    }
}

static int agg_open(Operator *op) { return op->left->vt->open(op->left); }

/* Блокирующий: при первом вызове прогоняет всего ребёнка, группирует и считает
 * агрегаты, выводит схему результата; затем отдаёт по одной строке на группу. */
static int agg_next(Operator *op, ColBatch **out_batch) {
    AggState *st = op->state;
    Arena *a = op->arena;

    if (!st->built) {
        /* allocate accumulators */
        int ng = AGG_MAX_GROUPS, no = st->nout;
        st->gkeys   = calloc((size_t)(ng * st->ngroup + 1), sizeof(Scalar));
        st->sum_acc = calloc((size_t)(ng * no + 1), sizeof(double));
        st->cnt_acc = calloc((size_t)(ng * no + 1), sizeof(int64_t));
        st->min_acc = calloc((size_t)(ng * no + 1), sizeof(Scalar));
        st->max_acc = calloc((size_t)(ng * no + 1), sizeof(Scalar));
        st->has_val = calloc((size_t)(ng * no + 1), sizeof(int8_t));

        /* drain child and accumulate */
        ColBatch *src = NULL;
        while (op->left->vt->next(op->left, &src) == 0 && src && src->nrows > 0) {
            Schema *sc = src->schema;
            EvalCtx ctx = {sc, src, 0};
            for (int r = 0; r < src->nrows; r++) {
                ctx.row = r;
                /* evaluate group keys */
                Scalar keys[16]; /* ngroup <= 16 */
                for (int k = 0; k < st->ngroup && k < 16; k++)
                    keys[k] = eval_expr(st->group_keys[k], &ctx, a);
                int g = agg_find_group(st, keys, a);
                if (g < 0) continue;
                /* accumulate each output expression */
                for (int oi = 0; oi < st->nout; oi++) {
                    Expr *e = st->out_exprs[oi];
                    const char *fn = NULL; Expr *arg = NULL;
                    if (is_agg_func(e, &fn, &arg)) {
                        Scalar val = (arg && strcasecmp(fn,"count")!=0)
                            ? eval_expr(arg, &ctx, a)
                            : (Scalar){SV_INT, {.ival=1}};
                        agg_accumulate(st, g, oi, fn, val, a);
                    } else {
                        /* non-agg: store value from first row in group */
                        int idx = g * st->nout + oi;
                        if (!st->has_val[idx]) {
                            Scalar v = eval_expr(e, &ctx, a);
                            if (v.type==SV_TEXT && v.val.sval)
                                v.val.sval = arena_strdup(a, v.val.sval);
                            st->min_acc[idx] = v; /* reuse min_acc for passthrough */
                            st->has_val[idx] = 1;
                        }
                    }
                }
            }
            src = NULL;
        }
        st->built = true;

        /* build output schema */
        Schema *sc = arena_calloc(a, sizeof(Schema));
        sc->ncols = st->nout;
        sc->cols  = arena_calloc(a, sizeof(ColDef) * (size_t)st->nout);
        for (int oi = 0; oi < st->nout; oi++) {
            sc->cols[oi].name = arena_strdup(a, expr_col_name(st->out_exprs[oi]));
            const char *fn = NULL; Expr *arg = NULL;
            if (is_agg_func(st->out_exprs[oi], &fn, NULL)) {
                if (!strcasecmp(fn,"count")) sc->cols[oi].type = COL_INT64;
                else if (!strcasecmp(fn,"min")||!strcasecmp(fn,"max")) {
                    /* infer from min/max_acc if available */
                    Scalar s = st->min_acc[oi]; /* use first group */
                    if (!strcasecmp(fn,"max")) s = st->max_acc[oi];
                    sc->cols[oi].type = (s.type==SV_TEXT) ? COL_TEXT : COL_DOUBLE;
                    (void)arg;
                } else sc->cols[oi].type = COL_DOUBLE;
            } else {
                /* passthrough: infer from stored value */
                Scalar s = st->min_acc[oi];
                sc->cols[oi].type = (s.type==SV_INT) ? COL_INT64 :
                                    (s.type==SV_DOUBLE) ? COL_DOUBLE : COL_TEXT;
            }
        }
        op->output_schema = sc;
        st->out_schema = sc;
    }

    if (st->emit_pos >= st->ngroups) {
        free(st->gkeys); free(st->sum_acc); free(st->cnt_acc);
        free(st->min_acc); free(st->max_acc); free(st->has_val);
        st->gkeys = NULL;
        return 1;
    }

    /* emit one row per group per call */
    int g = st->emit_pos++;
    Schema *sc = st->out_schema;
    ColBatch *b = arena_calloc(a, sizeof(ColBatch));
    b->schema = sc; b->ncols = st->nout; b->nrows = 1;
    for (int oi = 0; oi < st->nout; oi++) {
        const char *fn = NULL;
        bool is_agg = is_agg_func(st->out_exprs[oi], &fn, NULL);
        int idx = g * st->nout + oi;
        Scalar val = {SV_NULL};
        if (!is_agg) {
            val = st->min_acc[idx]; /* passthrough value */
        } else if (!strcasecmp(fn,"count")) {
            val.type = SV_INT; val.val.ival = st->cnt_acc[idx];
        } else if (!strcasecmp(fn,"avg")) {
            val.type = SV_DOUBLE;
            val.val.fval = st->cnt_acc[idx] ? st->sum_acc[idx]/(double)st->cnt_acc[idx] : 0.0;
        } else if (!strcasecmp(fn,"sum")) {
            val.type = SV_DOUBLE; val.val.fval = st->sum_acc[idx];
        } else if (!strcasecmp(fn,"min")) {
            val = st->has_val[idx] ? st->min_acc[idx] : (Scalar){SV_NULL};
        } else if (!strcasecmp(fn,"max")) {
            val = st->has_val[idx] ? st->max_acc[idx] : (Scalar){SV_NULL};
        }
        switch (sc->cols[oi].type) {
        case COL_INT64: {
            int64_t *arr = arena_alloc(a, sizeof(int64_t));
            *arr = (val.type==SV_DOUBLE) ? (int64_t)val.val.fval : val.val.ival;
            b->values[oi] = arr; break;
        }
        case COL_DOUBLE: {
            double *arr = arena_alloc(a, sizeof(double));
            *arr = (val.type==SV_INT) ? (double)val.val.ival : val.val.fval;
            b->values[oi] = arr; break;
        }
        default: {
            char **arr = arena_alloc(a, sizeof(char*));
            *arr = val.type==SV_TEXT && val.val.sval
                   ? arena_strdup(a, val.val.sval) : arena_strdup(a, "");
            b->values[oi] = arr; break;
        }
        }
        b->null_bitmap[oi] = arena_calloc(a, 1);
        if (val.type == SV_NULL) b->null_bitmap[oi][0] = 1;
    }
    *out_batch = b;
    return 0;
}

static void agg_close(Operator *op) { op->left->vt->close(op->left); }
static const OpVtable AGG_VT = {agg_open, agg_next, agg_close};

Operator *op_hash_agg(Arena *a, Operator *src,
                      Expr **group_keys, int ngroup,
                      Expr **agg_exprs, int nagg) {
    Operator *op = arena_calloc(a, sizeof(Operator));
    op->vt = &AGG_VT; op->left = src; op->arena = a;
    AggState *st = arena_calloc(a, sizeof(AggState));
    st->group_keys = group_keys; st->ngroup = ngroup;
    st->out_exprs  = agg_exprs;  st->nout   = nagg;
    /* for scalar agg (no group keys) start with 1 group */
    if (ngroup == 0) { st->ngroups = 1; }
    op->state = st;
    return op;
}

/* ── Sort operator (blocking) ── */
typedef struct {
    OrderItem *order;   int norder;
    ColBatch  *result;
    int        total_rows;
    int        emit_pos;
    bool       built;
    Arena     *a;
    Schema    *sc;
} SortState;

/* qsort context (not thread-safe, but single-threaded here) */
static SortState *g_sort_ctx = NULL;

/* Компаратор qsort над индексами строк: сравнивает по ключам ORDER BY
 * (учитывая ASC/DESC) через g_sort_ctx. */
static int sort_cmp(const void *va, const void *vb) {
    int ia = *(const int*)va, ib = *(const int*)vb;
    SortState *st = g_sort_ctx;
    ColBatch *b = st->result;
    Schema *sc = st->sc;
    for (int o = 0; o < st->norder; o++) {
        Scalar la = {SV_NULL}, lb = {SV_NULL};
        EvalCtx ca = {sc, b, ia}, cb = {sc, b, ib};
        la = eval_expr(st->order[o].expr, &ca, st->a);
        lb = eval_expr(st->order[o].expr, &cb, st->a);
        int cmp = 0;
        if (la.type == SV_INT && lb.type == SV_INT)
            cmp = (la.val.ival > lb.val.ival) - (la.val.ival < lb.val.ival);
        else if ((la.type==SV_INT||la.type==SV_DOUBLE) && (lb.type==SV_INT||lb.type==SV_DOUBLE)) {
            double da = la.type==SV_INT?(double)la.val.ival:la.val.fval;
            double db = lb.type==SV_INT?(double)lb.val.ival:lb.val.fval;
            cmp = (da > db) - (da < db);
        } else if (la.type == SV_TEXT && lb.type == SV_TEXT && la.val.sval && lb.val.sval)
            cmp = strcmp(la.val.sval, lb.val.sval);
        if (cmp != 0) return st->order[o].desc ? -cmp : cmp;
    }
    return 0;
}

static int sort_open(Operator *op) { return op->left->vt->open(op->left); }

/* Блокирующий: материализует все строки, сортирует массив индексов qsort'ом,
 * переставляет колонки в новый батч и отдаёт весь результат за один вызов. */
static int sort_next(Operator *op, ColBatch **out) {
    SortState *st = op->state;
    Arena *a = op->arena;

    if (!st->built) {
        /* materialise all rows — schema may not be set yet (e.g., from agg), read from first batch */
        Schema *sc = op->left->output_schema;
        int ncols = 0;
        int cap = 256, nrows = 0;
        void **cols = NULL;

        ColBatch *src = NULL;
        while (op->left->vt->next(op->left, &src) == 0 && src && src->nrows > 0) {
            /* initialise col buffers on first batch */
            if (!sc && src->schema) sc = src->schema;
            if (!cols && sc) {
                ncols = sc->ncols;
                cols = calloc((size_t)ncols, sizeof(void*));
                for (int c = 0; c < ncols; c++) {
                    size_t esz = sc->cols[c].type==COL_INT64 ? sizeof(int64_t) :
                                 sc->cols[c].type==COL_DOUBLE ? sizeof(double) : sizeof(char*);
                    cols[c] = malloc((size_t)cap * esz);
                }
            }
            if (!cols) { src=NULL; continue; }
            if (nrows + src->nrows > cap) {
                int nc2 = (nrows + src->nrows) * 2;
                for (int c = 0; c < ncols; c++) {
                    size_t esz = sc->cols[c].type==COL_INT64 ? sizeof(int64_t) :
                                 sc->cols[c].type==COL_DOUBLE ? sizeof(double) : sizeof(char*);
                    cols[c] = realloc(cols[c], (size_t)nc2 * esz);
                }
                cap = nc2;
            }
            for (int r = 0; r < src->nrows; r++) {
                int dr = nrows + r;
                for (int c = 0; c < ncols; c++) {
                    switch (sc->cols[c].type) {
                    case COL_INT64:  ((int64_t*)cols[c])[dr]=((int64_t*)src->values[c])[r]; break;
                    case COL_DOUBLE: ((double* )cols[c])[dr]=((double* )src->values[c])[r]; break;
                    default:         ((char**  )cols[c])[dr]=((char**  )src->values[c])[r]; break;
                    }
                }
            }
            nrows += src->nrows;
        }

        if (!cols || !sc || nrows == 0) {
            if (cols) { for (int c=0;c<ncols;c++) free(cols[c]); free(cols); }
            st->built = true; return 1;
        }

        /* build a flat ColBatch with nrows rows for sort comparisons */
        ColBatch *rb = arena_calloc(a, sizeof(ColBatch));
        rb->schema = sc; rb->ncols = ncols; rb->nrows = nrows;
        for (int c = 0; c < ncols; c++) {
            size_t esz = sc->cols[c].type==COL_INT64 ? sizeof(int64_t) :
                         sc->cols[c].type==COL_DOUBLE ? sizeof(double) : sizeof(char*);
            void *arr = arena_alloc(a, (size_t)nrows * esz + 1);
            memcpy(arr, cols[c], (size_t)nrows * esz);
            rb->values[c] = arr;
            free(cols[c]);
        }
        free(cols);

        /* build index array and sort */
        int *idx = malloc((size_t)nrows * sizeof(int));
        for (int i = 0; i < nrows; i++) idx[i] = i;
        st->result = rb; st->sc = sc; st->a = a;
        g_sort_ctx = st;
        qsort(idx, (size_t)nrows, sizeof(int), sort_cmp);

        /* reorder columns by sorted index */
        ColBatch *sorted = arena_calloc(a, sizeof(ColBatch));
        sorted->schema = sc; sorted->ncols = ncols; sorted->nrows = nrows;
        for (int c = 0; c < ncols; c++) {
            size_t esz = sc->cols[c].type==COL_INT64 ? sizeof(int64_t) :
                         sc->cols[c].type==COL_DOUBLE ? sizeof(double) : sizeof(char*);
            void *arr = arena_alloc(a, (size_t)nrows * esz + 1);
            for (int r = 0; r < nrows; r++) {
                int sr = idx[r];
                switch (sc->cols[c].type) {
                case COL_INT64:  ((int64_t*)arr)[r]=((int64_t*)rb->values[c])[sr]; break;
                case COL_DOUBLE: ((double* )arr)[r]=((double* )rb->values[c])[sr]; break;
                default:         ((char**  )arr)[r]=((char**  )rb->values[c])[sr]; break;
                }
            }
            sorted->values[c] = arr;
            sorted->null_bitmap[c] = arena_calloc(a, ((size_t)nrows+7)/8);
        }
        free(idx);
        st->result = sorted; st->total_rows = nrows;
        st->built = true;
    }

    if (st->emit_pos >= st->total_rows) return 1;
    *out = st->result;
    st->emit_pos = st->total_rows; /* emit all at once */
    return 0;
}

static void sort_close(Operator *op) { op->left->vt->close(op->left); }
static const OpVtable SORT_VT = {sort_open, sort_next, sort_close};

Operator *op_sort(Arena *a, Operator *src, OrderItem *order, int norder) {
    Operator *op = arena_calloc(a, sizeof(Operator));
    op->vt = &SORT_VT; op->left = src; op->arena = a;
    op->output_schema = src ? src->output_schema : NULL;
    SortState *st = arena_calloc(a, sizeof(SortState));
    st->order = order; st->norder = norder;
    op->state = st;
    return op;
}

/* ── Project operator ── */
Operator *op_project(Arena *a, Operator *src, Expr **exprs, int nexprs, Schema *out_schema) {
    /* For now: if any expr is an agg func and no group-by was done → scalar agg */
    bool has_agg = false;
    for (int i = 0; i < nexprs; i++)
        if (is_agg_func(exprs[i], NULL, NULL)) { has_agg = true; break; }
    if (has_agg) {
        /* treat as scalar aggregate: 1 group, 0 group keys */
        return op_hash_agg(a, src, NULL, 0, exprs, nexprs);
    }
    /* no-op projection: just return src (schema from child is sufficient) */
    (void)out_schema;
    return src;
}

/* Есть ли где-то ниже по левой ветви плана узел агрегации. */
static bool plan_has_agg(PlanNode *p) {
    if (!p) return false;
    if (p->type == PLAN_AGG) return true;
    return plan_has_agg(p->left);
}

/* ── Hash join (nested-loop fallback) ── */
typedef struct {
    Expr    *on;
    JoinType jtype;
    /* materialised right side */
    ColBatch *right_batch;
    bool      built;
    /* current left batch and position */
    ColBatch *left_batch;
    int       left_pos;
    int       right_pos;
    Schema   *out_schema;
} JoinState;

/* Конкатенирует схемы левой и правой сторон джойна (колонки left, затем right). */
static Schema *join_schema(Arena *a, Schema *ls, Schema *rs) {
    if (!ls || !rs) return ls ? ls : rs;
    int nc = ls->ncols + rs->ncols;
    Schema *sc = arena_calloc(a, sizeof(Schema));
    sc->ncols = nc;
    sc->cols  = arena_calloc(a, sizeof(ColDef) * (size_t)nc);
    for (int i=0;i<ls->ncols;i++) sc->cols[i]=ls->cols[i];
    for (int i=0;i<rs->ncols;i++) sc->cols[ls->ncols+i]=rs->cols[i];
    return sc;
}

static int join_open(Operator *op) {
    op->left->vt->open(op->left);
    if (op->right) op->right->vt->open(op->right);
    return 0;
}

/* Nested-loop джойн: один раз материализует правую сторону, затем для каждой
 * левой строки перебирает правые, проверяя ON, и отдаёт по одной склеенной строке. */
static int join_next(Operator *op, ColBatch **out) {
    JoinState *st = op->state;
    Arena *a = op->arena;

    /* materialise right side once */
    if (!st->built && op->right) {
        Schema *rsc = op->right->output_schema;
        if (rsc) {
            int ncols=rsc->ncols, cap=256, nrows=0;
            void **cols = calloc((size_t)ncols, sizeof(void*));
            for(int c=0;c<ncols;c++){
                size_t esz=rsc->cols[c].type==COL_INT64?sizeof(int64_t):
                           rsc->cols[c].type==COL_DOUBLE?sizeof(double):sizeof(char*);
                cols[c]=malloc((size_t)cap*esz);
            }
            ColBatch *src=NULL;
            while(op->right->vt->next(op->right,&src)==0&&src&&src->nrows>0){
                if(nrows+src->nrows>cap){ cap=(nrows+src->nrows)*2;
                    for(int c=0;c<ncols;c++){
                        size_t esz=rsc->cols[c].type==COL_INT64?sizeof(int64_t):
                                   rsc->cols[c].type==COL_DOUBLE?sizeof(double):sizeof(char*);
                        cols[c]=realloc(cols[c],(size_t)cap*esz);
                    }
                }
                for(int r=0;r<src->nrows;r++){int dr=nrows+r;
                    for(int c=0;c<ncols;c++){
                        switch(rsc->cols[c].type){
                        case COL_INT64:((int64_t*)cols[c])[dr]=((int64_t*)src->values[c])[r];break;
                        case COL_DOUBLE:((double*)cols[c])[dr]=((double*)src->values[c])[r];break;
                        default:((char**)cols[c])[dr]=((char**)src->values[c])[r];break;
                        }
                    }
                }
                nrows+=src->nrows;
            }
            ColBatch *rb=arena_calloc(a,sizeof(ColBatch));
            rb->schema=rsc;rb->ncols=ncols;rb->nrows=nrows;
            for(int c=0;c<ncols;c++){
                size_t esz=rsc->cols[c].type==COL_INT64?sizeof(int64_t):
                           rsc->cols[c].type==COL_DOUBLE?sizeof(double):sizeof(char*);
                void *arr=arena_alloc(a,(size_t)nrows*esz+1);
                if(nrows>0)memcpy(arr,cols[c],(size_t)nrows*esz);
                rb->values[c]=arr; free(cols[c]);
                rb->null_bitmap[c]=arena_calloc(a,((size_t)nrows+7)/8);
            }
            free(cols);
            st->right_batch=rb;
        }
        st->built=true;
    }

    if (!op->right || !st->right_batch || st->right_batch->nrows==0) {
        /* no right side or empty: just pass left through */
        return op->left->vt->next(op->left, out);
    }

    Schema *lsc = op->left->output_schema;
    Schema *rsc = st->right_batch->schema;
    if (!st->out_schema)
        st->out_schema = join_schema(a, lsc, rsc);

    /* nested loop: get next left batch */
    for (;;) {
        if (!st->left_batch || st->left_pos >= st->left_batch->nrows) {
            ColBatch *lb=NULL;
            if(op->left->vt->next(op->left,&lb)!=0||!lb||lb->nrows==0) return 1;
            st->left_batch=lb; st->left_pos=0; st->right_pos=0;
        }
        /* find a matching right row */
        ColBatch *rb=st->right_batch;
        int lp=st->left_pos, rp=st->right_pos;
        while(rp<rb->nrows){
            /* check ON condition if present */
            bool match=true;
            if(st->on){
                /* build merged row for eval */
                int lnc=lsc?lsc->ncols:0, rnc=rsc?rsc->ncols:0;
                int nc=lnc+rnc;
                Schema *msc=st->out_schema;
                ColBatch mb={0}; mb.schema=msc; mb.ncols=nc; mb.nrows=1;
                for(int c=0;c<lnc&&lsc;c++){mb.values[c]=&((char**)st->left_batch->values[c])[lp];}
                for(int c=0;c<rnc&&rsc;c++){mb.values[lnc+c]=&((char**)rb->values[c])[rp];}
                EvalCtx ctx={msc,&mb,0};
                match=eval_bool(st->on,&ctx,a);
            }
            rp++;
            if(match){
                st->right_pos=rp;
                /* emit 1 merged row */
                int lnc=lsc?lsc->ncols:0, rnc=rsc?rsc->ncols:0;
                int nc=lnc+rnc;
                Schema *msc=st->out_schema;
                ColBatch *b=arena_calloc(a,sizeof(ColBatch));
                b->schema=msc; b->ncols=nc; b->nrows=1;
                for(int c=0;c<lnc&&lsc;c++){
                    size_t esz=lsc->cols[c].type==COL_INT64?sizeof(int64_t):
                               lsc->cols[c].type==COL_DOUBLE?sizeof(double):sizeof(char*);
                    void *arr=arena_alloc(a,esz);
                    switch(lsc->cols[c].type){
                    case COL_INT64:*(int64_t*)arr=((int64_t*)st->left_batch->values[c])[lp];break;
                    case COL_DOUBLE:*(double*)arr=((double*)st->left_batch->values[c])[lp];break;
                    default:*(char**)arr=((char**)st->left_batch->values[c])[lp];break;
                    }
                    b->values[c]=arr; b->null_bitmap[c]=arena_calloc(a,1);
                }
                for(int c=0;c<rnc&&rsc;c++){
                    size_t esz=rsc->cols[c].type==COL_INT64?sizeof(int64_t):
                               rsc->cols[c].type==COL_DOUBLE?sizeof(double):sizeof(char*);
                    void *arr=arena_alloc(a,esz);
                    switch(rsc->cols[c].type){
                    case COL_INT64:*(int64_t*)arr=((int64_t*)rb->values[c])[rp-1];break;
                    case COL_DOUBLE:*(double*)arr=((double*)rb->values[c])[rp-1];break;
                    default:*(char**)arr=((char**)rb->values[c])[rp-1];break;
                    }
                    b->values[lnc+c]=arr; b->null_bitmap[lnc+c]=arena_calloc(a,1);
                }
                *out=b; return 0;
            }
        }
        /* exhausted right side for this left row → advance left */
        st->left_pos++; st->right_pos=0;
    }
}

static void join_close(Operator *op) {
    op->left->vt->close(op->left);
    if (op->right) op->right->vt->close(op->right);
}
static const OpVtable JOIN_VT = {join_open, join_next, join_close};

Operator *op_hash_join(Arena *a, Operator *left, Operator *right,
                       Expr *on, JoinType jtype) {
    Operator *op = arena_calloc(a, sizeof(Operator));
    op->vt = &JOIN_VT; op->left = left; op->right = right; op->arena = a;
    JoinState *st = arena_calloc(a, sizeof(JoinState));
    st->on = on; st->jtype = jtype;
    op->state = st;
    Schema *lsc = left ? left->output_schema : NULL;
    Schema *rsc = right ? right->output_schema : NULL;
    op->output_schema = join_schema(a, lsc, rsc);
    return op;
}

/* ── Build from logical plan ── */
/* Рекурсивно собирает дерево физических операторов из логического плана. */
Operator *qengine_build(Arena *a, PlanNode *plan, const char *data_dir) {
    if (!plan) return NULL;
    switch (plan->type) {
    case PLAN_SCAN: {
        return op_scan(a, plan->table, NULL, data_dir);
    }
    case PLAN_FILTER: {
        Operator *src=qengine_build(a,plan->left,data_dir);
        return op_filter(a,src,plan->predicate);
    }
    case PLAN_LIMIT: {
        Operator *src=qengine_build(a,plan->left,data_dir);
        return op_limit(a,src,plan->limit,plan->offset);
    }
    case PLAN_WINDOW: {
        Operator *src = qengine_build(a, plan->left, data_dir);
        return op_window(a, src, plan->window_exprs, plan->nwindow_exprs);
    }
    case PLAN_AGG: {
        Operator *src = qengine_build(a, plan->left, data_dir);
        return op_hash_agg(a, src,
                           plan->group_keys, plan->ngroup_keys,
                           plan->agg_exprs, plan->nagg_exprs);
    }
    case PLAN_SORT: {
        Operator *src = qengine_build(a, plan->left, data_dir);
        return op_sort(a, src, plan->order, plan->norder);
    }
    case PLAN_PROJECT: {
        Operator *src = qengine_build(a, plan->left, data_dir);
        /* If an AGG is anywhere below, it already produced aggregated rows — just pass through */
        if (plan_has_agg(plan->left)) return src;
        return op_project(a, src, plan->exprs, plan->nexprs, NULL);
    }
    case PLAN_JOIN: {
        Operator *left  = qengine_build(a, plan->left,  data_dir);
        Operator *right = qengine_build(a, plan->right, data_dir);
        return op_hash_join(a, left, right, plan->join_on, plan->join_type);
    }
    default:
        return NULL;
    }
}

/* ── Execute to JSON rows ── */
static void scalar_to_json(Scalar *s, JBuf *jb) {
    switch (s->type) {
    case SV_NULL:   jb_null(jb); break;
    case SV_INT:    jb_int(jb, s->val.ival); break;
    case SV_DOUBLE: jb_double(jb, s->val.fval); break;
    case SV_TEXT:   jb_str(jb, s->val.sval); break;
    case SV_BOOL:   jb_bool(jb, s->val.bval); break;
    }
}

/* Прогоняет дерево операторов (open→next*→close), сериализует каждую строку в
 * JSON-объект и отдаёт через колбэк cb. В rows_out — число строк. */
int qengine_exec_json(Operator *root, Arena *a, RowJsonCb cb, void *ud, int *rows_out) {
    if (!root) { if(rows_out)*rows_out=0; return 0; }
    root->vt->open(root);
    int total=0;
    ColBatch *batch=NULL;
    while (root->vt->next(root,&batch)==0 && batch && batch->nrows>0) {
        Schema *schema=batch->schema;
        for (int r=0;r<batch->nrows;r++){
            JBuf jb; jb_init(&jb,a,256);
            jb_obj_begin(&jb);
            if (schema) {
                for (int c=0;c<batch->ncols;c++){
                    jb_key(&jb,schema->cols[c].name);
                    bool is_null=batch->null_bitmap[c]&&!!(batch->null_bitmap[c][r/8]&(1u<<(r%8)));
                    if(is_null){jb_null(&jb);continue;}
                    Scalar s={SV_NULL};
                    switch(schema->cols[c].type){
                    case COL_INT64:  s.type=SV_INT; s.val.ival=((int64_t*)batch->values[c])[r]; break;
                    case COL_DOUBLE: s.type=SV_DOUBLE; s.val.fval=((double*)batch->values[c])[r]; break;
                    case COL_TEXT:   s.type=SV_TEXT; s.val.sval=((char**)batch->values[c])[r]; break;
                    case COL_BOOL:   s.type=SV_BOOL; s.val.bval=((int*)batch->values[c])[r]!=0; break;
                    default: break;
                    }
                    scalar_to_json(&s,&jb);
                }
            }
            jb_obj_end(&jb);
            if(cb) cb(jb_done(&jb),ud);
            total++;
        }
        batch=NULL;
    }
    root->vt->close(root);
    if(rows_out)*rows_out=total;
    return 0;
}
