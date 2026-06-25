#pragma once
#include <stdint.h>
#include <stdbool.h>

/*
 * Page-based B+ tree — int64 keys, int64 row offsets.
 * File layout:
 *   Page 0  : file header (magic, root, nkeys, npages)
 *   Page 1+ : leaf and internal nodes (4 KB each)
 *
 * Leaf  : up to 255 (key, offset) pairs, linked for range scans.
 * Internal: up to 340 separator keys, 341 child pointers.
 */

#define BT_PAGE_SIZE   4096
#define BT_MAX_OFFSETS 131072   /* max results per range/lookup call */

/* Непрозрачный дескриптор открытого дерева; внутренности скрыты в .c */
typedef struct BTree BTree;

/* Жизненный цикл дерева: create — новый файл, open — существующий, close — закрыть и сбросить на диск */
BTree  *btree_create(const char *path);                              /* new file   */
BTree  *btree_open  (const char *path);                              /* existing   */
/* Вставка пары (ключ -> смещение строки); ключи не обязаны быть уникальными */
int     btree_insert(BTree *t, int64_t key, int64_t row_offset);
/* Точечный поиск: пишет в offs все смещения с данным ключом, n — вход/выход размер буфера */
int     btree_lookup(BTree *t, int64_t key, int64_t *offs, int *n); /* point      */
/* Поиск по диапазону [lo, hi] включительно через связанные листья */
int     btree_range (BTree *t, int64_t lo, int64_t hi,
                     int64_t *offs, int *n);                         /* inclusive  */
void    btree_close (BTree *t);
/* Возвращает суммарное число ключей в дереве */
int64_t btree_count (BTree *t);
