/* yaml_loader.c — pure-C YAML → JSON converter (minimal subset).
 *
 * Strategy: two-phase
 *   1. Pre-process source into a flat array of YamlLine{indent, kind, ...}
 *   2. Recursively emit JSON, walking lines with parent-indent comparison
 *
 * The parser is intentionally strict: features outside the documented
 * subset trigger a clear error rather than producing surprising JSON. */

#include "yaml_loader.h"
#include "../core/json.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <ctype.h>

/* ────────── Line kinds & data ────────── */
typedef enum {
    YL_BLANK,           /* empty or comment-only */
    YL_MAP_KV,          /* "key: value"            (value possibly empty) */
    YL_MAP_BLOCK_SCALAR,/* "key: |" or "key: >"    (value is multi-line) */
    YL_MAP_KEY_ONLY,    /* "key:"                  (value is nested block) */
    YL_SEQ_INLINE,      /* "- some scalar"         (inline scalar value) */
    YL_SEQ_KV,          /* "- key: value"          (inline mapping; first key) */
    YL_SEQ_BLOCK,       /* "-"                     (next lines = element) */
    YL_DOC_MARKER       /* "---" or "..."           (skipped) */
} YamlKind;

typedef struct {
    int      indent;       /* leading spaces */
    YamlKind kind;
    char    *key;          /* may be NULL (e.g. seq scalar) */
    char    *value;        /* may be NULL or empty (then nested block follows) */
    char     bs_kind;      /* '|' or '>' for YL_MAP_BLOCK_SCALAR */
    int      lineno;       /* 1-based source line for errors */
    char    *raw;          /* for block-scalar collection: trimmed source line */
} YamlLine;

typedef struct {
    YamlLine *lines;
    int       n;
    int       cap;
    Arena    *a;
    YamlError *err;
} Yaml;

/* ────────── Error helpers ────────── */
/* Записать в err позицию (line:col) и printf-сообщение; всегда возвращает -1,
 * чтобы вызов можно было сразу вернуть как код ошибки. err может быть NULL. */
__attribute__((format(printf, 4, 5)))
static int set_err(YamlError *err, int line, int col, const char *fmt, ...) {
    if (err) {
        err->line = line;
        err->col  = col;
        va_list ap; va_start(ap, fmt);
        vsnprintf(err->buf, sizeof(err->buf), fmt, ap);
        va_end(ap);
    }
    return -1;
}

/* ────────── String helpers ────────── */
/* Скопировать первые len байт s в арену как NUL-терминированную строку. */
static char *arena_substr(Arena *a, const char *s, size_t len) {
    char *out = arena_alloc(a, len + 1);
    memcpy(out, s, len);
    out[len] = '\0';
    return out;
}

/* Строка состоит только из пробельных символов (или пустая)? */
static int is_blank(const char *s) {
    while (*s) {
        if (!isspace((unsigned char)*s)) return 0;
        s++;
    }
    return 1;
}

/* Срезать хвостовые пробелы на месте; возвращает s. */
static char *rstrip(char *s) {
    size_t n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n - 1])) s[--n] = '\0';
    return s;
}

/* Strip trailing comment ` # …` from a line, preserving content inside
 * single/double quotes. Modifies in place. */
static void strip_comment(char *s) {
    int q = 0; /* 0 = none, '"', '\'' */
    for (char *p = s; *p; p++) {
        if (q) {
            if (*p == '\\' && p[1]) { p++; continue; }
            if (*p == q) q = 0;
            continue;
        }
        if (*p == '"' || *p == '\'') { q = *p; continue; }
        if (*p == '#' && (p == s || isspace((unsigned char)p[-1]))) {
            *p = '\0';
            break;
        }
    }
    rstrip(s);
}

/* Число ведущих пробелов (отступ строки). Табы как отступ не считаются. */
static int count_indent(const char *s) {
    int n = 0;
    while (s[n] == ' ') n++;
    return n;
}

/* ────────── Quoted-scalar parser ────────── */
/* Decode "double-quoted" or 'single-quoted'. Returns NULL on error.
 * `*advance` set to chars consumed (including the closing quote). */
static char *parse_quoted(const char *s, char qc, Arena *a, size_t *advance) {
    /* s points at the opening quote; advance starts there */
    const char *p = s + 1;
    size_t cap = 64, len = 0;
    char  *out = arena_alloc(a, cap);
    while (*p) {
        if (*p == '\\' && qc == '"' && p[1]) {
            char c;
            switch (p[1]) {
                case 'n': c = '\n'; break;
                case 't': c = '\t'; break;
                case 'r': c = '\r'; break;
                case '"': c = '"';  break;
                case '\\': c = '\\'; break;
                default:  c = p[1]; break;
            }
            if (len + 1 >= cap) {
                cap *= 2;
                char *nb = arena_alloc(a, cap);
                memcpy(nb, out, len); out = nb;
            }
            out[len++] = c;
            p += 2;
            continue;
        }
        if (*p == qc) {
            *advance = (size_t)(p - s) + 1;
            out[len] = '\0';
            return out;
        }
        if (len + 1 >= cap) {
            cap *= 2;
            char *nb = arena_alloc(a, cap);
            memcpy(nb, out, len); out = nb;
        }
        out[len++] = *p++;
    }
    return NULL; /* unterminated */
}

/* helper: append a YamlLine to y->lines */
/* Добавить разобранную строку в массив y->lines, удваивая ёмкость по мере роста
 * (арена без realloc — новый блок + копирование старого). */
static void push_line(Yaml *y, YamlLine yl) {
    if (y->n >= y->cap) {
        int nc = y->cap * 2;
        YamlLine *nb = arena_alloc(y->a, sizeof(YamlLine) * nc);
        memcpy(nb, y->lines, sizeof(YamlLine) * y->n);
        y->lines = nb; y->cap = nc;
    }
    y->lines[y->n++] = yl;
}

/* ────────── Preprocess: source → YamlLine[] ────────── */
/* Фаза 1: разбить исходник на строки и классифицировать каждую (отступ + вид:
 * map/seq/скаляр/маркер). Определяет ключ/значение, распознаёт заголовки блочных
 * скаляров "|"/">" и переводит парсер в режим absorbing, где вложенные строки
 * сохраняются как непрозрачный контент (YL_BLANK+raw) до возврата отступа.
 * Возврат: 0 — ok, -1 — синтаксическая ошибка (через set_err). */
static int preprocess(Yaml *y, const char *src, size_t len) {
    /* Make a working copy we can chop into lines */
    char *buf = arena_alloc(y->a, len + 1);
    memcpy(buf, src, len); buf[len] = '\0';

    y->cap = 32;
    y->lines = arena_alloc(y->a, sizeof(YamlLine) * y->cap);
    y->n = 0;

    int lineno = 0;
    char *cur = buf;
    /* Block-scalar absorption state — set after a YL_MAP_BLOCK_SCALAR header
     * so subsequent indented lines are treated as opaque content, not parsed
     * for structure. Cleared when indent drops back to header's level. */
    int  absorbing       = 0;
    int  absorb_indent   = -1;

    while (cur < buf + len) {
        lineno++;
        char *eol = strchr(cur, '\n');
        size_t line_len = eol ? (size_t)(eol - cur) : strlen(cur);
        char *line = arena_substr(y->a, cur, line_len);
        cur += line_len + (eol ? 1 : 0);

        YamlLine yl = { .lineno = lineno, .raw = arena_substr(y->a, line, strlen(line)) };
        rstrip(yl.raw); /* raw kept for block scalar collection */

        /* In absorb mode, EVERYTHING (including bare lines) becomes YL_BLANK
         * until indent drops back to or below the header. */
        if (absorbing) {
            int rind = count_indent(yl.raw);
            int blank = (yl.raw[0] == '\0');
            if (!blank && rind <= absorb_indent) {
                absorbing = 0;
                /* fall through to normal classification below */
            } else {
                yl.kind   = YL_BLANK;
                yl.indent = rind;
                push_line(y, yl);
                continue;
            }
        }

        /* Strip comment + trailing whitespace for normal classification */
        strip_comment(line);

        if (line[0] == '\0' || is_blank(line)) {
            yl.kind = YL_BLANK;
            yl.indent = count_indent(yl.raw);
            push_line(y, yl);
            continue;
        }

        yl.indent = count_indent(line);
        char *content = line + yl.indent;

        if (strcmp(content, "---") == 0 || strcmp(content, "...") == 0) {
            yl.kind = YL_DOC_MARKER;
            push_line(y, yl);
            continue;
        }

        if (content[0] == '-' && (content[1] == '\0' || content[1] == ' ')) {
            /* sequence item */
            char *after_dash = (content[1] == ' ') ? content + 2 : content + 1;
            while (*after_dash == ' ') after_dash++;
            if (*after_dash == '\0') {
                yl.kind = YL_SEQ_BLOCK;
            } else {
                /* check for "key: value" inside the dash content */
                char *colon = NULL;
                int q = 0; /* активная кавычка ('"'/'\''), чтобы пропускать ':' внутри строк */
                for (char *p = after_dash; *p; p++) {
                    if (q) { if (*p == '\\' && p[1]) p++; else if (*p == q) q = 0; continue; }
                    if (*p == '"' || *p == '\'') { q = *p; continue; }
                    if (*p == ':' && (p[1] == '\0' || p[1] == ' ')) { colon = p; break; }
                }
                if (colon) {
                    yl.kind = YL_SEQ_KV;
                    *colon = '\0';
                    yl.key = arena_substr(y->a, after_dash, strlen(after_dash));
                    rstrip(yl.key);
                    char *val = colon + 1;
                    while (*val == ' ') val++;
                    yl.value = arena_substr(y->a, val, strlen(val));
                    /* If value is "|" / ">", absorb continuation lines */
                    if ((val[0] == '|' || val[0] == '>') &&
                        (val[1] == '\0' || isspace((unsigned char)val[1]))) {
                        absorbing = 1;
                        absorb_indent = yl.indent;
                    }
                } else {
                    yl.kind = YL_SEQ_INLINE;
                    yl.value = arena_substr(y->a, after_dash, strlen(after_dash));
                }
            }
            push_line(y, yl);
            continue;
        }

        /* mapping entry: find unquoted ':' */
        {
            char *colon = NULL;
            int q = 0;
            for (char *p = content; *p; p++) {
                if (q) { if (*p == '\\' && p[1]) p++; else if (*p == q) q = 0; continue; }
                if (*p == '"' || *p == '\'') { q = *p; continue; }
                if (*p == ':' && (p[1] == '\0' || p[1] == ' ')) { colon = p; break; }
            }
            if (!colon) {
                return set_err(y->err, lineno, 1, "expected mapping or sequence");
            }
            *colon = '\0';
            yl.key = arena_substr(y->a, content, strlen(content));
            rstrip(yl.key);
            char *val = colon + 1;
            while (*val == ' ') val++;
            if (*val == '\0') {
                yl.kind = YL_MAP_KEY_ONLY;
                yl.value = NULL;
            } else if ((val[0] == '|' || val[0] == '>') &&
                       (val[1] == '\0' || isspace((unsigned char)val[1]))) {
                yl.kind = YL_MAP_BLOCK_SCALAR;
                yl.bs_kind = val[0];
                yl.value = NULL; /* collected later */
                absorbing = 1;
                absorb_indent = yl.indent;
            } else {
                yl.kind = YL_MAP_KV;
                yl.value = arena_substr(y->a, val, strlen(val));
            }
        }
        push_line(y, yl);
    }
    return 0;
}

/* ────────── Forward decls ────────── */
static int emit_value(Yaml *y, int *idx, int parent_indent, JBuf *out);
static int emit_block_map(Yaml *y, int *idx, int my_indent, JBuf *out);
static int emit_block_seq(Yaml *y, int *idx, int my_indent, JBuf *out);
static int emit_inline_seq_kv(Yaml *y, int *idx, int my_indent, JBuf *out);
static void emit_scalar(JBuf *out, const char *text, Arena *a);
static int  emit_flow_seq(JBuf *out, const char *text, Arena *a, YamlError *err, int lineno);
static int  emit_block_scalar(Yaml *y, int *idx, int parent_indent,
                              char kind, JBuf *out);

/* skip blank/marker lines */
/* Продвинуть *idx за пустые строки и маркеры документа (--- / ...). */
static void skip_blank(Yaml *y, int *idx) {
    while (*idx < y->n &&
           (y->lines[*idx].kind == YL_BLANK || y->lines[*idx].kind == YL_DOC_MARKER))
        (*idx)++;
}

/* peek next non-blank line (returns NULL if none) */
/* Как skip_blank, но возвращает первую значимую строку (и сдвигает *idx на неё);
 * NULL, если до конца остались только пустые/маркеры. */
static YamlLine *peek_nonblank(Yaml *y, int *idx) {
    int i = *idx;
    while (i < y->n &&
           (y->lines[i].kind == YL_BLANK || y->lines[i].kind == YL_DOC_MARKER)) i++;
    if (i >= y->n) return NULL;
    *idx = i;
    return &y->lines[i];
}

/* ────────── Scalar emission ────────── */
/* Auto-type plain scalars into JSON value: null/true/false/int/double/string */
static void emit_scalar(JBuf *out, const char *text, Arena *a) {
    if (!text || !text[0]) { jb_str(out, ""); return; }

    /* quoted? */
    if (text[0] == '"' || text[0] == '\'') {
        size_t adv = 0;
        char *unq = parse_quoted(text, text[0], a, &adv);
        if (unq) { jb_str(out, unq); return; }
        /* fall through to literal if unterminated */
    }

    /* keywords */
    if (strcmp(text, "null") == 0 || strcmp(text, "~") == 0 ||
        strcmp(text, "Null") == 0 || strcmp(text, "NULL") == 0) {
        jb_null(out); return;
    }
    if (strcmp(text, "true") == 0  || strcmp(text, "True") == 0  || strcmp(text, "TRUE")  == 0) {
        jb_bool(out, true);  return;
    }
    if (strcmp(text, "false") == 0 || strcmp(text, "False") == 0 || strcmp(text, "FALSE") == 0) {
        jb_bool(out, false); return;
    }

    /* numeric? */
    char *end = NULL;
    long long iv = strtoll(text, &end, 10);
    if (end && *end == '\0' && end != text) { jb_int(out, iv); return; }
    end = NULL;
    double dv = strtod(text, &end);
    if (end && *end == '\0' && end != text) { jb_double(out, dv); return; }

    /* default: string */
    jb_str(out, text);
}

/* Emit a flow sequence: text begins with '[' */
static int emit_flow_seq(JBuf *out, const char *text, Arena *a, YamlError *err, int lineno) {
    if (text[0] != '[') {
        return set_err(err, lineno, 1, "expected '[' for flow sequence");
    }
    jb_arr_begin(out);
    const char *p = text + 1;
    /* skip whitespace */
    while (*p == ' ' || *p == '\t') p++;
    if (*p == ']') { jb_arr_end(out); return 0; }

    while (*p) {
        const char *start = p;
        if (*p == '"' || *p == '\'') {
            size_t adv = 0;
            char *q = parse_quoted(p, *p, a, &adv);
            if (!q) return set_err(err, lineno, 1, "unterminated quoted scalar in flow sequence");
            jb_str(out, q);
            p += adv;
        } else {
            while (*p && *p != ',' && *p != ']') p++;
            char *tok = arena_substr(a, start, (size_t)(p - start));
            rstrip(tok);
            emit_scalar(out, tok, a);
        }
        while (*p == ' ' || *p == '\t') p++;
        if (*p == ',') { p++; while (*p == ' ' || *p == '\t') p++; continue; }
        if (*p == ']') break;
        if (*p == '\0')
            return set_err(err, lineno, 1, "missing ']' in flow sequence");
    }
    jb_arr_end(out);
    return 0;
}

/* Block scalar: collect lines absorbed by preprocess into the value.
 *
 * Absorbed continuation lines are marked YL_BLANK with their original text in
 * `raw`. We distinguish them from true blank lines by checking is_blank(raw).
 * The block scalar terminates when:
 *   - we hit a non-YL_BLANK line that signals structural parsing resumed; or
 *   - we hit YL_DOC_MARKER. */
static int emit_block_scalar(Yaml *y, int *idx, int parent_indent,
                             char kind, JBuf *out) {
    (void)parent_indent;
    int i = *idx;
    int child_indent = -1;
    size_t cap = 1024, len = 0;
    char *buf = arena_alloc(y->a, cap);
    int first = 1;

    while (i < y->n) {
        YamlLine *l = &y->lines[i];
        if (l->kind == YL_DOC_MARKER) break;
        if (l->kind != YL_BLANK) break;

        const char *raw = l->raw ? l->raw : "";
        int truly_blank = (raw[0] == '\0' || is_blank(raw));
        const char *content;
        if (truly_blank) {
            content = "";
        } else {
            int ind = count_indent(raw);
            if (child_indent < 0) child_indent = ind;
            /* Строка с отступом МЕНЬШЕ блока завершает block scalar (правило YAML).
             * Без этого дедентованный комментарий «  # …» втягивался в значение, а
             * срез child_indent байт резал многобайтный символ → битый UTF-8. */
            else if (ind < child_indent) break;
            int strip = child_indent > 0 ? child_indent : 0;
            int rl = (int)strlen(raw);
            if (strip > rl) strip = rl;
            content = raw + strip;
        }
        size_t cl = strlen(content);
        size_t sep = first ? 0 : 1;
        if (len + sep + cl + 1 > cap) {
            while (len + sep + cl + 1 > cap) cap *= 2;
            char *nb = arena_alloc(y->a, cap);
            memcpy(nb, buf, len); buf = nb;
        }
        if (!first) {
            /* For folded `>`, blank lines still produce '\n'; non-blank
             * adjacency joins with ' '. For literal `|`, always '\n'. */
            if (kind == '>') {
                if (truly_blank || cl == 0) buf[len++] = '\n';
                else                        buf[len++] = ' ';
            } else {
                buf[len++] = '\n';
            }
        }
        memcpy(buf + len, content, cl); len += cl;
        first = 0;
        i++;
    }
    buf[len] = '\0';
    *idx = i;
    jb_str(out, buf);
    return 0;
}

/* ────────── Block sequence ────────── */
/* Сериализовать блочную последовательность (строки "- …" с отступом my_indent)
 * в JSON-массив. Элемент = inline-скаляр, вложенный блок ("-") или inline-mapping
 * ("- key: value"). Завершается на первой строке с иным отступом/видом. */
static int emit_block_seq(Yaml *y, int *idx, int my_indent, JBuf *out) {
    jb_arr_begin(out);
    while (*idx < y->n) {
        skip_blank(y, idx);
        if (*idx >= y->n) break;
        YamlLine *l = &y->lines[*idx];
        if (l->indent != my_indent) break;
        if (l->kind != YL_SEQ_INLINE && l->kind != YL_SEQ_KV && l->kind != YL_SEQ_BLOCK) break;

        switch (l->kind) {
            case YL_SEQ_INLINE: {
                if (l->value && l->value[0] == '[') {
                    if (emit_flow_seq(out, l->value, y->a, y->err, l->lineno) < 0) return -1;
                } else {
                    emit_scalar(out, l->value ? l->value : "", y->a);
                }
                (*idx)++;
                break;
            }
            case YL_SEQ_BLOCK: {
                (*idx)++;
                if (emit_value(y, idx, my_indent, out) < 0) return -1;
                break;
            }
            case YL_SEQ_KV: {
                /* The inline mapping started by the dash. The "logical" indent
                 * of mapping keys is my_indent + 2 (where "- " was). */
                if (emit_inline_seq_kv(y, idx, my_indent, out) < 0) return -1;
                break;
            }
            default: break;
        }
    }
    jb_arr_end(out);
    return 0;
}

/* When we see "- key: value", the dash starts a sequence element which is
 * itself a mapping. The first key is on the same line; subsequent keys are
 * on lines indented at (my_indent + 2). */
static int emit_inline_seq_kv(Yaml *y, int *idx, int my_indent, JBuf *out) {
    YamlLine *first = &y->lines[*idx];
    int inner_indent = my_indent + 2;
    jb_obj_begin(out);

    /* emit the first key:value from the dash line */
    jb_key(out, first->key);
    if (!first->value || first->value[0] == '\0') {
        /* "- key:" with no value — nested block */
        (*idx)++;
        if (emit_value(y, idx, my_indent, out) < 0) return -1;
    } else if (first->value[0] == '[') {
        if (emit_flow_seq(out, first->value, y->a, y->err, first->lineno) < 0) return -1;
        (*idx)++;
    } else if ((first->value[0] == '|' || first->value[0] == '>') &&
               (first->value[1] == '\0' || isspace((unsigned char)first->value[1]))) {
        char k = first->value[0];
        (*idx)++;
        if (emit_block_scalar(y, idx, my_indent, k, out) < 0) return -1;
    } else {
        emit_scalar(out, first->value, y->a);
        (*idx)++;
    }

    /* continuation lines: same logical indent (inner_indent), but they're
     * regular MAP_KV/MAP_KEY_ONLY lines because no leading dash. */
    while (*idx < y->n) {
        skip_blank(y, idx);
        if (*idx >= y->n) break;
        YamlLine *l = &y->lines[*idx];
        if (l->indent != inner_indent) break;
        if (l->kind != YL_MAP_KV && l->kind != YL_MAP_KEY_ONLY && l->kind != YL_MAP_BLOCK_SCALAR) break;

        jb_key(out, l->key);
        if (l->kind == YL_MAP_KV) {
            if (l->value && l->value[0] == '[') {
                if (emit_flow_seq(out, l->value, y->a, y->err, l->lineno) < 0) return -1;
            } else {
                emit_scalar(out, l->value, y->a);
            }
            (*idx)++;
        } else if (l->kind == YL_MAP_BLOCK_SCALAR) {
            char k = l->bs_kind;
            (*idx)++;
            if (emit_block_scalar(y, idx, inner_indent, k, out) < 0) return -1;
        } else {
            (*idx)++;
            if (emit_value(y, idx, inner_indent, out) < 0) return -1;
        }
    }

    jb_obj_end(out);
    return 0;
}

/* ────────── Block mapping ────────── */
/* Сериализовать блочное отображение (строки "key: …" с отступом my_indent) в
 * JSON-объект: скаляр, flow-массив [..], блочный скаляр |/> или вложенный блок
 * (emit_value). Завершается на строке с иным отступом или не-map-видом. */
static int emit_block_map(Yaml *y, int *idx, int my_indent, JBuf *out) {
    jb_obj_begin(out);
    while (*idx < y->n) {
        skip_blank(y, idx);
        if (*idx >= y->n) break;
        YamlLine *l = &y->lines[*idx];
        if (l->indent != my_indent) break;
        if (l->kind != YL_MAP_KV && l->kind != YL_MAP_KEY_ONLY && l->kind != YL_MAP_BLOCK_SCALAR) break;

        jb_key(out, l->key);
        if (l->kind == YL_MAP_KV) {
            if (l->value && l->value[0] == '[') {
                if (emit_flow_seq(out, l->value, y->a, y->err, l->lineno) < 0) return -1;
            } else {
                emit_scalar(out, l->value, y->a);
            }
            (*idx)++;
        } else if (l->kind == YL_MAP_BLOCK_SCALAR) {
            char k = l->bs_kind;
            (*idx)++;
            if (emit_block_scalar(y, idx, my_indent, k, out) < 0) return -1;
        } else {
            (*idx)++;
            if (emit_value(y, idx, my_indent, out) < 0) return -1;
        }
    }
    jb_obj_end(out);
    return 0;
}

/* ────────── emit_value: dispatch based on next non-blank line ────────── */
/* Сериализовать вложенное значение (после "key:" / "-"). Если следующая значимая
 * строка имеет отступ <= parent_indent, вложенного блока нет → пишем JSON null;
 * иначе диспетчеризуем в map или seq по её виду. */
static int emit_value(Yaml *y, int *idx, int parent_indent, JBuf *out) {
    YamlLine *l = peek_nonblank(y, idx);
    if (!l || l->indent <= parent_indent) {
        jb_null(out); /* nothing to put here */
        return 0;
    }
    switch (l->kind) {
        case YL_MAP_KV:
        case YL_MAP_KEY_ONLY:
        case YL_MAP_BLOCK_SCALAR:
            return emit_block_map(y, idx, l->indent, out);
        case YL_SEQ_INLINE:
        case YL_SEQ_KV:
        case YL_SEQ_BLOCK:
            return emit_block_seq(y, idx, l->indent, out);
        default:
            return set_err(y->err, l->lineno, 1, "unexpected line");
    }
}

/* ────────── Public entry ────────── */
int yaml_to_json(const char *src, size_t len, Arena *a,
                 char **json_out, YamlError *err) {
    Yaml y = { .a = a, .err = err };
    if (preprocess(&y, src, len) < 0) return -1;

    JBuf jb; jb_init(&jb, a, 4096);
    int idx = 0;
    /* root-level value: scan for first non-blank to know indent baseline */
    YamlLine *l = peek_nonblank(&y, &idx);
    if (!l) {
        jb_null(&jb);
    } else {
        switch (l->kind) {
            case YL_MAP_KV:
            case YL_MAP_KEY_ONLY:
            case YL_MAP_BLOCK_SCALAR:
                if (emit_block_map(&y, &idx, l->indent, &jb) < 0) return -1;
                break;
            case YL_SEQ_INLINE:
            case YL_SEQ_KV:
            case YL_SEQ_BLOCK:
                if (emit_block_seq(&y, &idx, l->indent, &jb) < 0) return -1;
                break;
            default:
                jb_null(&jb);
        }
    }
    *json_out = (char *)jb_done(&jb);
    return 0;
}
