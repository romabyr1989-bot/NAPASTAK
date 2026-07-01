#pragma once
/*
 * arena.h — арена-аллокатор (bump allocator) для NAPASTAK.
 * Выделяет память крупными блоками и раздаёт её последовательно, без
 * пофрагментного free: вся память освобождается сразу через arena_destroy/reset.
 * Подходит для коротко живущих аллокаций с общим временем жизни.
 */
#include <stddef.h>
#include <stdint.h>

#define ARENA_DEFAULT_BLOCK (65536) /* размер блока по умолчанию, 64 KiB */
#define ARENA_ALIGN         16      /* выравнивание возвращаемых указателей */

/* Блок арены: единица выделения у ОС; блоки связаны в список через prev. */
typedef struct ArenaBlock {
    struct ArenaBlock *prev;   /* предыдущий блок (для обхода и освобождения) */
    size_t cap, used;          /* ёмкость блока и сколько уже занято */
    uint8_t data[];            /* гибкий массив — собственно область данных */
} ArenaBlock;

/* Дескриптор арены: вершина списка блоков и счётчики для статистики. */
typedef struct {
    ArenaBlock *top;                          /* текущий блок, из которого выделяем */
    size_t total_allocated, total_wasted;     /* всего выделено / потеряно на выравнивании и хвостах */
} Arena;

/* Создать арену; initial_size — стартовая ёмкость первого блока (0 = default). */
Arena    *arena_create(size_t initial_size);
/* Освободить арену и все её блоки. */
void      arena_destroy(Arena *a);
/* Выделить sz байт (с выравниванием ARENA_ALIGN); содержимое не инициализируется. */
void     *arena_alloc(Arena *a, size_t sz);
/* Как arena_alloc, но память обнулена. */
void     *arena_calloc(Arena *a, size_t sz);
/* Копия строки s в арене. */
char     *arena_strdup(Arena *a, const char *s);
/* Копия не более n символов строки s в арене. */
char     *arena_strndup(Arena *a, const char *s, size_t n);
/* printf-форматирование с размещением результата в арене. */
char     *arena_sprintf(Arena *a, const char *fmt, ...) __attribute__((format(printf,2,3)));
/* Сбросить арену для повторного использования, сохранив выделенные блоки. */
void      arena_reset(Arena *a);

/* Сохранённая позиция арены для отката (см. arena_mark/arena_restore). */
typedef struct { ArenaBlock *block; size_t used; } ArenaMark;
/* Запомнить текущую позицию арены. */
ArenaMark arena_mark(Arena *a);
/* Откатить арену к ранее снятой метке m, освободив всё, что выделено после неё. */
void      arena_restore(Arena *a, ArenaMark m);
