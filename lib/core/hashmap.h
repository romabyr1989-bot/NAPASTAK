#pragma once
/* hashmap.h — строковый хеш-мап на открытой адресации (Robin Hood).
   Память под копии ключей берётся из переданной арены (Arena). */
#include "arena.h"
#include <stdint.h>
#include <stdbool.h>

/* Open-addressing hashmap, Robin Hood probing, string keys */
typedef struct HMEntry {
    const char *key;
    void       *val;
    uint32_t    hash;
    uint32_t    psl;   /* probe sequence length */
} HMEntry;

/* Сам хеш-мап: массив слотов фиксированной ёмкости + счётчик и арена. */
typedef struct {
    HMEntry *slots;
    uint32_t cap;
    uint32_t count;
    Arena   *arena; /* for key copies */
} HashMap;

/* Инициализация: задаёт арену для ключей и стартовую ёмкость. */
void  hm_init(HashMap *m, Arena *a, uint32_t initial_cap);
/* Вставка/обновление значения по ключу (ключ копируется в арену). */
void  hm_set(HashMap *m, const char *key, void *val);
/* Поиск значения по ключу; NULL, если ключа нет. */
void *hm_get(HashMap *m, const char *key);
/* Удаление по ключу; true, если запись существовала. */
bool  hm_del(HashMap *m, const char *key);
/* Сброс всех записей (ёмкость и арена сохраняются). */
void  hm_clear(HashMap *m);

/* iterate: idx=0 start; returns next idx or -1 */
int   hm_next(const HashMap *m, int idx, const char **key_out, void **val_out);
