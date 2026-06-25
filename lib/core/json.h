#pragma once
/* Minimal JSON builder (streaming) + recursive-descent parser */
#include "arena.h"
#include <stddef.h>
#include <stdbool.h>

/* ── Builder ── */
/* Растущий буфер для потоковой сборки JSON; память берётся из арены `a`. */
typedef struct { char *buf; size_t len, cap; Arena *a; } JBuf;

/* Инициализирует буфер: привязывает арену и резервирует initial байт. */
void jb_init(JBuf *j, Arena *a, size_t initial);
void jb_obj_begin(JBuf *j);
void jb_obj_end(JBuf *j);
void jb_arr_begin(JBuf *j);
void jb_arr_end(JBuf *j);
void jb_key(JBuf *j, const char *k);
void jb_str(JBuf *j, const char *s);
void jb_strn(JBuf *j, const char *s, size_t n);
void jb_int(JBuf *j, long long v);
void jb_double(JBuf *j, double v);
void jb_bool(JBuf *j, bool v);
void jb_null(JBuf *j);
void jb_raw(JBuf *j, const char *raw);  /* append pre-built JSON verbatim */
/* returns NUL-terminated string in arena */
const char *jb_done(JBuf *j);

/* ── Parser ── */
/* Тип разобранного JSON-значения (JV_ERROR — признак ошибки разбора). */
typedef enum {
    JV_NULL, JV_BOOL, JV_NUMBER, JV_STRING, JV_ARRAY, JV_OBJECT, JV_ERROR
} JValType;

typedef struct JVal JVal;
/* Узел дерева разбора: union активен согласно полю type (см. метки ниже). */
struct JVal {
    JValType    type;
    union {
        bool        b;
        double      n;
        struct { const char *s; size_t len; };  /* JV_STRING */
        struct { JVal **items; size_t nitems; }; /* JV_ARRAY */
        struct { const char **keys; JVal **vals; size_t nkeys; }; /* JV_OBJECT */
    };
};

/* Разбирает src (len байт) в дерево JVal на арене; при ошибке тип JV_ERROR. */
JVal *json_parse(Arena *a, const char *src, size_t len);
JVal *json_get(JVal *obj, const char *key);   /* O(n) key lookup */
/* Аксессоры с дефолтом: возвращают def, если v не нужного типа/NULL. */
const char *json_str(JVal *v, const char *def);
long long   json_int(JVal *v, long long def);
double      json_dbl(JVal *v, double def);
bool        json_bool(JVal *v, bool def);
