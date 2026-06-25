#pragma once
/* Non-owning string slice — StringView */
#include <stddef.h>
#include <string.h>
#include <stdbool.h>

/* Указатель на чужую память + длина; SV не владеет данными и не копирует их. */
typedef struct { const char *ptr; size_t len; } SV;

/* SV из строкового литерала (длина известна на этапе компиляции, без \0). */
#define SV_LIT(s)       ((SV){(s), sizeof(s)-1})
#define SV_NULL         ((SV){NULL, 0})
/* Формат-строка и пара аргументов для printf: printf(SV_FMT, SV_ARG(sv)). */
#define SV_FMT          "%.*s"
#define SV_ARG(sv)      (int)(sv).len, (sv).ptr

/* Оборачивает C-строку в SV (NULL трактуется как пустой срез). */
static inline SV   sv_from(const char *s)              { return (SV){s, s ? strlen(s) : 0}; }
/* Подсрез длины n начиная с off; границы вызывающий контролирует сам. */
static inline SV   sv_slice(SV s, size_t off, size_t n){ return (SV){s.ptr+off, n}; }
/* Побайтовое сравнение двух срезов на равенство. */
static inline bool sv_eq(SV a, SV b)  { return a.len == b.len && memcmp(a.ptr, b.ptr, a.len) == 0; }
/* Сравнение среза с C-строкой. */
static inline bool sv_eqz(SV a, const char *b) { return sv_eq(a, sv_from(b)); }
static inline bool sv_empty(SV s)     { return s.len == 0; }
/* true, если срез начинается с префикса pre. */
static inline bool sv_starts(SV s, SV pre) {
    return s.len >= pre.len && memcmp(s.ptr, pre.ptr, pre.len) == 0;
}
/* Срезает пробельные символы с обоих концов, двигая границы (без копирования). */
static inline SV sv_trim(SV s) {
    /* пропускаем ведущие пробелы, сдвигая указатель вперёд */
    while (s.len && (s.ptr[0]==' '||s.ptr[0]=='\t'||s.ptr[0]=='\r'||s.ptr[0]=='\n'))
        { s.ptr++; s.len--; }
    /* затем отбрасываем хвостовые пробелы, уменьшая длину */
    while (s.len && (s.ptr[s.len-1]==' '||s.ptr[s.len-1]=='\t'||
                     s.ptr[s.len-1]=='\r'||s.ptr[s.len-1]=='\n'))
        s.len--;
    return s;
}
