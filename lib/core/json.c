/* Минимальный JSON-строитель (streaming) + рекурсивно-нисходящий парсер.
 * Весь результат живёт в Arena — никаких отдельных free(). */
#include "json.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <ctype.h>

/* ── Строитель (JBuf) ── */

/* Геометрическое удвоение буфера в арене: не освобождаем старый (он в арене),
 * просто выделяем больший и копируем. */
static void jb_ensure(JBuf *j, size_t need) {
    if (j->len + need <= j->cap) return;
    size_t nc = j->cap * 2 + need + 64;
    char *nb = arena_alloc(j->a, nc);
    memcpy(nb, j->buf, j->len);
    j->buf = nb; j->cap = nc;
}

static void jb_append(JBuf *j, const char *s, size_t n) {
    jb_ensure(j, n); memcpy(j->buf + j->len, s, n); j->len += n;
}

static void jb_ch(JBuf *j, char c) { jb_ensure(j, 1); j->buf[j->len++] = c; }

/* Автоматическая запятая: смотрим на последний символ буфера.
 * После '[', '{', ':' запятая не нужна — значение идёт первым. */
static void jb_comma(JBuf *j) {
    if (j->len > 0) {
        char last = j->buf[j->len-1];
        if (last != '[' && last != '{' && last != ':') jb_ch(j, ',');
    }
}

void jb_init(JBuf *j, Arena *a, size_t init) {
    j->a = a; j->cap = init ? init : 256;
    j->buf = arena_alloc(a, j->cap); j->len = 0;
}

void jb_obj_begin(JBuf *j) { jb_comma(j); jb_ch(j, '{'); }
void jb_obj_end(JBuf *j)   { jb_ch(j, '}'); }
void jb_arr_begin(JBuf *j) { jb_comma(j); jb_ch(j, '['); }
void jb_arr_end(JBuf *j)   { jb_ch(j, ']'); }

void jb_key(JBuf *j, const char *k) {
    jb_comma(j);
    jb_ch(j, '"');
    jb_append(j, k, strlen(k));
    jb_append(j, "\":", 2);   /* ключ всегда без экранирования — имена полей ASCII */
}

void jb_str(JBuf *j, const char *s) {
    if (!s) { jb_raw(j, "null"); return; }
    jb_strn(j, s, strlen(s));
}

/* Экранирование строк согласно RFC 8259: спецсимволы и байты < 0x20. */
void jb_strn(JBuf *j, const char *s, size_t n) {
    jb_comma(j); jb_ch(j, '"');
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '"')  { jb_append(j, "\\\"", 2); }
        else if (c == '\\') { jb_append(j, "\\\\", 2); }
        else if (c == '\n') { jb_append(j, "\\n", 2); }
        else if (c == '\r') { jb_append(j, "\\r", 2); }
        else if (c == '\t') { jb_append(j, "\\t", 2); }
        else if (c < 0x20)  { char esc[8]; snprintf(esc,sizeof(esc),"\\u%04x",c); jb_append(j,esc,6); }
        else jb_ch(j, (char)c);
    }
    jb_ch(j, '"');
}

void jb_int(JBuf *j, long long v) {
    char buf[32]; int n = snprintf(buf, sizeof(buf), "%lld", v);
    jb_comma(j); jb_append(j, buf, (size_t)n);
}

/* Кратчайшая десятичная запись, читающаяся обратно В ТЕ ЖЕ БИТЫ.
 * Начинаем с 15 значащих цифр, чтобы простые числа оставались чистыми
 * (0.1 -> "0.1"), и наращиваем только если короче не хватает.
 *
 * Прежний %.10g округлял деньги свыше ~1e8 (123456789.99 -> 123456790), а
 * фиксированные 15 цифр не вытягивали контрольную сумму на 16 разрядов
 * (10000123456790.16 -> 10000123456790.2) — то есть ровно те балансы порядка
 * 1e13 с копейками, ради которых 15 и выбирали. Здесь потолок не фиксирован:
 * сколько разрядов нужно значению, столько и пишем, но не больше 17 —
 * дальше double всё равно не различает. */
int json_fmt_double(char *buf, size_t cap, double v) {
    int n = 0;
    for (int p = 15; ; p++) {
        n = snprintf(buf, cap, "%.*g", p, v);
        if (p >= 17) break;
        char *end = NULL;
        double back = strtod(buf, &end);
        if (back == v) break;
    }
    return n;
}

void jb_double(JBuf *j, double v) {
    char buf[64]; int n = json_fmt_double(buf, sizeof(buf), v);
    jb_comma(j); jb_append(j, buf, (size_t)n);
}

void jb_bool(JBuf *j, bool v) { jb_comma(j); jb_append(j, v?"true":"false", v?4:5); }
void jb_null(JBuf *j)         { jb_comma(j); jb_append(j, "null", 4); }

/* jb_raw — вставляет уже готовый JSON-фрагмент без экранирования.
 * Используется для вложенных объектов, уже хранящихся в виде строки (например, из каталога). */
void jb_raw(JBuf *j, const char *raw) { jb_comma(j); jb_append(j, raw, strlen(raw)); }

/* NUL-терминируем буфер и возвращаем указатель. Буфер живёт в арене вызывающего. */
const char *jb_done(JBuf *j) {
    jb_ensure(j, 1); j->buf[j->len] = '\0'; return j->buf;
}

/* ── Парсер (рекурсивный спуск) ── */

/* Состояние парсера: src — входная строка, pos — текущая позиция, a — арена для JVal. */
typedef struct { const char *src; size_t pos, len; Arena *a; } JP;

static void skip_ws(JP *p) {
    while (p->pos < p->len && isspace((unsigned char)p->src[p->pos])) p->pos++;
}

/* arena_calloc обнуляет union — важно, чтобы неинициализированные поля не содержали мусор. */
static JVal *jv_new(JP *p, JValType t) {
    JVal *v = arena_calloc(p->a, sizeof(JVal)); v->type = t; return v;
}

static JVal *parse_value(JP *p);

/* Парсим строку с inline-декодированием escape-последовательностей.
 * Выходной буфер аллоцируем с запасом (p->len - p->pos) — никогда не превысит входной длины. */
static JVal *parse_string(JP *p) {
    if (p->src[p->pos] != '"') return jv_new(p, JV_ERROR);
    p->pos++;
    size_t start = p->pos;
    char *out = arena_alloc(p->a, p->len - p->pos + 1);
    size_t outlen = 0;
    while (p->pos < p->len && p->src[p->pos] != '"') {
        if (p->src[p->pos] == '\\') {
            p->pos++;
            switch (p->src[p->pos]) {
                case '"': out[outlen++]='"'; break;
                case '\\': out[outlen++]='\\'; break;
                case '/': out[outlen++]='/'; break;
                case 'n': out[outlen++]='\n'; break;
                case 'r': out[outlen++]='\r'; break;
                case 't': out[outlen++]='\t'; break;
                case 'u': {
                    /* \uXXXX → UTF-8: BMP-кодпоинт до U+FFFF кодируем в 1–3 байта */
                    unsigned int cp = 0;
                    for (int _k = 0; _k < 4; _k++) {
                        if (p->pos+1 >= p->len) break;
                        p->pos++;
                        unsigned char h = (unsigned char)p->src[p->pos];
                        cp <<= 4;
                        if (h>='0'&&h<='9') cp|=(h-'0');
                        else if (h>='a'&&h<='f') cp|=(h-'a'+10);
                        else if (h>='A'&&h<='F') cp|=(h-'A'+10);
                    }
                    if (cp < 0x80) { out[outlen++]=(char)cp; }
                    else if (cp < 0x800) {
                        out[outlen++]=(char)(0xC0|(cp>>6));
                        out[outlen++]=(char)(0x80|(cp&0x3F));
                    } else {
                        out[outlen++]=(char)(0xE0|(cp>>12));
                        out[outlen++]=(char)(0x80|((cp>>6)&0x3F));
                        out[outlen++]=(char)(0x80|(cp&0x3F));
                    }
                    break;
                }
                default:  out[outlen++]=p->src[p->pos]; break;
            }
        } else { out[outlen++] = p->src[p->pos]; }
        p->pos++;
    }
    (void)start;
    if (p->pos < p->len) p->pos++; /* пропускаем закрывающую кавычку */
    out[outlen] = '\0';
    JVal *v = jv_new(p, JV_STRING); v->s = out; v->len = outlen;
    return v;
}

/* Массивы и объекты динамически расширяют буфер вдвое — классический amortized O(1). */
static JVal *parse_array(JP *p) {
    p->pos++; /* пропускаем '[' */
    JVal *v = jv_new(p, JV_ARRAY);
    size_t cap = 8;
    JVal **items = arena_alloc(p->a, cap * sizeof(JVal *));
    size_t n = 0;
    skip_ws(p);
    if (p->pos < p->len && p->src[p->pos] == ']') { p->pos++; v->items=items; v->nitems=0; return v; }
    for (;;) {
        skip_ws(p);
        if (n == cap) { cap*=2; JVal **nb=arena_alloc(p->a,cap*sizeof(JVal*)); memcpy(nb,items,n*sizeof(JVal*)); items=nb; }
        items[n++] = parse_value(p);
        skip_ws(p);
        if (p->pos >= p->len || p->src[p->pos] == ']') break;
        if (p->src[p->pos] == ',') p->pos++;
    }
    if (p->pos < p->len) p->pos++;
    v->items = items; v->nitems = n;
    return v;
}

static JVal *parse_object(JP *p) {
    p->pos++; /* пропускаем '{' */
    JVal *v = jv_new(p, JV_OBJECT);
    size_t cap = 8;
    const char **keys = arena_alloc(p->a, cap * sizeof(char *));
    JVal **vals = arena_alloc(p->a, cap * sizeof(JVal *));
    size_t n = 0;
    skip_ws(p);
    if (p->pos < p->len && p->src[p->pos] == '}') { p->pos++; v->keys=keys;v->vals=vals;v->nkeys=0; return v; }
    for (;;) {
        skip_ws(p);
        if (n == cap) {
            cap*=2;
            const char **nk=arena_alloc(p->a,cap*sizeof(char*)); memcpy(nk,keys,n*sizeof(char*)); keys=nk;
            JVal **nv=arena_alloc(p->a,cap*sizeof(JVal*)); memcpy(nv,vals,n*sizeof(JVal*)); vals=nv;
        }
        JVal *ks = parse_string(p); keys[n] = ks->s;
        skip_ws(p);
        if (p->pos < p->len && p->src[p->pos] == ':') p->pos++;
        skip_ws(p);
        vals[n] = parse_value(p); n++;
        skip_ws(p);
        if (p->pos >= p->len || p->src[p->pos] == '}') break;
        if (p->src[p->pos] == ',') p->pos++;
    }
    if (p->pos < p->len) p->pos++;
    v->keys=keys; v->vals=vals; v->nkeys=n;
    return v;
}

/* Точка входа рекурсивного спуска: определяем тип значения по первому символу. */
static JVal *parse_value(JP *p) {
    skip_ws(p);
    if (p->pos >= p->len) return jv_new(p, JV_ERROR);
    char c = p->src[p->pos];
    if (c == '"') return parse_string(p);
    if (c == '[') return parse_array(p);
    if (c == '{') return parse_object(p);
    if (strncmp(p->src+p->pos,"null",4)==0) { p->pos+=4; return jv_new(p,JV_NULL); }
    if (strncmp(p->src+p->pos,"true",4)==0) { p->pos+=4; JVal *v=jv_new(p,JV_BOOL); v->b=true; return v; }
    if (strncmp(p->src+p->pos,"false",5)==0){ p->pos+=5; JVal *v=jv_new(p,JV_BOOL); v->b=false; return v; }
    if (c=='-'||isdigit((unsigned char)c)) {
        /* Числа: используем strtod — он сам продвигает указатель до конца числа. */
        char *end; double d = strtod(p->src+p->pos, &end);
        p->pos = (size_t)(end - p->src);
        JVal *v = jv_new(p,JV_NUMBER); v->n=d; return v;
    }
    return jv_new(p, JV_ERROR);
}

JVal *json_parse(Arena *a, const char *src, size_t len) {
    JP p = {src, 0, len, a};
    return parse_value(&p);
}

/* Линейный поиск по ключам — для небольших объектов (до ~20 полей) быстрее хэш-таблицы. */
JVal *json_get(JVal *obj, const char *key) {
    if (!obj || obj->type != JV_OBJECT) return NULL;
    for (size_t i = 0; i < obj->nkeys; i++)
        if (strcmp(obj->keys[i], key)==0) return obj->vals[i];
    return NULL;
}

/* Типизированные аксессоры: возвращают значение нужного типа либо def,
 * если узел отсутствует (NULL) или имеет другой тип. */
const char *json_str(JVal *v, const char *def) {
    return (v && v->type==JV_STRING) ? v->s : def;
}
long long json_int(JVal *v, long long def) {
    return (v && v->type==JV_NUMBER) ? (long long)v->n : def;
}
double json_dbl(JVal *v, double def) {
    return (v && v->type==JV_NUMBER) ? v->n : def;
}
bool json_bool(JVal *v, bool def) {
    return (v && v->type==JV_BOOL) ? v->b : def;
}
