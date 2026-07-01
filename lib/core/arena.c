/* Арена-аллокатор: выделяет память из непрерывных блоков, освобождение — только всей арены целиком.
 * Это даёт O(1) аллокацию без фрагментации, идеально для данных с одинаковым временем жизни
 * (один HTTP-запрос, один батч, одна операция). */
#include "arena.h"
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>

/* Выравнивание по границе ARENA_ALIGN байт: гарантирует корректный доступ к int64/double. */
static inline size_t align_up(size_t n, size_t a) { return (n + a - 1) & ~(a - 1); }

/* Блок хранится как: [ArenaBlock header][data байты].
 * malloc одним куском — один вызов, меньше накладных расходов. */
static ArenaBlock *block_new(size_t cap, ArenaBlock *prev) {
    ArenaBlock *b = malloc(sizeof(ArenaBlock) + cap);
    if (!b) return NULL;
    b->prev = prev; b->cap = cap; b->used = 0;
    return b;
}

Arena *arena_create(size_t sz) {
    if (!sz) sz = ARENA_DEFAULT_BLOCK;
    Arena *a = malloc(sizeof(Arena));
    if (!a) return NULL;
    a->top = block_new(sz, NULL);
    a->total_allocated = a->total_wasted = 0;
    if (!a->top) { free(a); return NULL; }
    return a;
}

/* Освобождаем цепочку блоков от вершины к основанию — порядок важен, т.к. блоки связаны односторонне. */
void arena_destroy(Arena *a) {
    for (ArenaBlock *b = a->top; b; ) { ArenaBlock *p = b->prev; free(b); b = p; }
    free(a);
}

void *arena_alloc(Arena *a, size_t sz) {
    if (!sz) sz = 1;
    sz = align_up(sz, ARENA_ALIGN);
    ArenaBlock *b = a->top;
    if (b) {
        /* Выравниваем возвращаемый АДРЕС, а не только размер: b->data не кратен
         * ARENA_ALIGN (заголовок блока — 24 байта на 64-битных платформах), поэтому
         * выравнивание одного лишь размера давало адреса, кратные 8, но не 16. */
        size_t off = (size_t)(align_up((uintptr_t)b->data + b->used, ARENA_ALIGN) - (uintptr_t)b->data);
        if (off + sz <= b->cap) {
            /* Быстрый путь: просто сдвигаем указатель внутри текущего блока. */
            void *p = b->data + off; b->used = off + sz; a->total_allocated += sz; return p;
        }
    }
    /* Текущий блок переполнен — фиксируем потери и выделяем новый.
     * total_wasted помогает диагностировать слишком маленький начальный размер. */
    if (b) a->total_wasted += (b->cap - b->used);
    /* Если запрос больше стандартного блока — выделяем блок с удвоенным запасом,
     * чтобы следующее выделение тоже поместилось без нового malloc. */
    size_t cap = sz > ARENA_DEFAULT_BLOCK ? sz * 2 : ARENA_DEFAULT_BLOCK;
    cap += ARENA_ALIGN;   /* запас на выравнивание nb->data (заголовок не кратен ARENA_ALIGN) */
    ArenaBlock *nb = block_new(cap, b);
    if (!nb) return NULL;
    size_t off = (size_t)(align_up((uintptr_t)nb->data, ARENA_ALIGN) - (uintptr_t)nb->data);
    a->top = nb; nb->used = off + sz; a->total_allocated += sz;
    return nb->data + off;
}

void *arena_calloc(Arena *a, size_t sz) {
    void *p = arena_alloc(a, sz); if (p) memset(p, 0, sz); return p;
}

char *arena_strdup(Arena *a, const char *s) {
    if (!s) return NULL;
    size_t n = strlen(s);
    char *p = arena_alloc(a, n + 1); if (p) memcpy(p, s, n + 1); return p;
}

char *arena_strndup(Arena *a, const char *s, size_t n) {
    if (!s) return NULL;
    size_t len = strnlen(s, n);
    char *p = arena_alloc(a, len + 1); if (!p) return NULL;
    memcpy(p, s, len); p[len] = '\0'; return p;
}

/* Двухпроходный vsnprintf: первый проход — узнаём точную длину без записи,
 * второй — пишем в заранее выделенный буфер арены. */
char *arena_sprintf(Arena *a, const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    int n = vsnprintf(NULL, 0, fmt, ap); va_end(ap);
    if (n < 0) return NULL;
    char *buf = arena_alloc(a, (size_t)n + 1); if (!buf) return NULL;
    va_start(ap, fmt); vsnprintf(buf, (size_t)n + 1, fmt, ap); va_end(ap);
    return buf;
}

/* Сброс арены без освобождения памяти: оставляем только первый блок, сбрасываем счётчик.
 * Позволяет переиспользовать арену (например, при переиспользовании соединения). */
void arena_reset(Arena *a) {
    ArenaBlock *b = a->top;
    while (b && b->prev) { ArenaBlock *p = b->prev; free(b); b = p; }
    a->top = b; if (b) b->used = 0;
    a->total_allocated = a->total_wasted = 0;
}

/* Метка фиксирует состояние арены (текущий блок + позицию в нём).
 * Нужна для временных аллокаций: выделили, использовали, откатились. */
ArenaMark arena_mark(Arena *a) { return (ArenaMark){a->top, a->top ? a->top->used : 0}; }

/* Откат к метке: освобождаем все блоки, выделенные после метки,
 * и восстанавливаем позицию записи в блоке-метке. */
void arena_restore(Arena *a, ArenaMark m) {
    while (a->top && a->top != m.block) { ArenaBlock *p = a->top->prev; free(a->top); a->top = p; }
    if (a->top) a->top->used = m.used;
}
