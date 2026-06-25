/* B+ дерево на страницах по 4 КБ: int64 ключи, int64 смещения в WAL-файле.
 * Листья связаны в цепочку для диапазонных сканирований без возврата к корню.
 * Страница 0 — заголовок файла, страница 1 — начальный корневой лист. */
#include "btree.h"
#include "../core/log.h"
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

/* ── Константы ── */
#define MAGIC    UINT64_C(0x44464F42545245FF)
#define VERSION  1

#define PT_LEAF  1
#define PT_INT   2

/* Ёмкости страниц:
 *   Лист: (4096 - 8 байт заголовка) / 16 байт на запись = 255 записей
 *   Внутренняя: 4 байта child[0] + N*(8 байт ключа + 4 байта child) ≤ 4088
 *               N ≤ (4088-4)/12 = 340 ключей */
#define LEAF_MAX  255
#define INT_MAX   340

typedef uint8_t Page[BT_PAGE_SIZE];

/* Дескриптор открытого дерева в памяти: fd файла + кэш полей заголовка. */
struct BTree {
    int     fd;
    int32_t root;    /* номер страницы текущего корня */
    int32_t npages;  /* общее количество выделенных страниц */
    int64_t nkeys;   /* количество вставленных ключей */
};

/* Заголовок файла — хранится в странице 0. Packed: нет паддинга между полями. */
typedef struct __attribute__((packed)) {
    uint64_t magic;
    uint32_t version;
    int32_t  root;
    int64_t  nkeys;
    int32_t  npages;
} FHdr;

/* Заголовок каждой B-tree страницы — первые 8 байт. */
typedef struct __attribute__((packed)) {
    uint8_t  type;   /* PT_LEAF или PT_INT */
    uint8_t  _pad;
    uint16_t n;      /* количество ключей на странице */
    int32_t  next;   /* лист: следующий сосед; внутренняя: -1 */
} PHdr;

/* Запись листа: 16 байт = 8 (ключ) + 8 (WAL-смещение). */
typedef struct __attribute__((packed)) { int64_t key; int64_t off; } LE;

/* Схема данных внутренней страницы (после 8-байтного PHdr):
 *   [child[0], 4 байта]
 *   [key[0], 8 байт][child[1], 4 байта]
 *   [key[1], 8 байт][child[2], 4 байта]
 *   ...
 * Смещения от начала данных (p+8):
 *   child[0]   → байт 0
 *   key[i]     → байт 4 + i*12
 *   child[i+1] → байт 4 + i*12 + 8 */

/* ── Низкоуровневый ввод-вывод страниц ── */
static int rd(int fd, int32_t pg, Page p) {
    ssize_t r = pread(fd, p, BT_PAGE_SIZE, (off_t)pg * BT_PAGE_SIZE);
    return r == BT_PAGE_SIZE ? 0 : -1;
}
static int wr(int fd, int32_t pg, const Page p) {
    ssize_t w = pwrite(fd, p, BT_PAGE_SIZE, (off_t)pg * BT_PAGE_SIZE);
    return w == BT_PAGE_SIZE ? 0 : -1;
}

/* Выделяем новую страницу: просто увеличиваем счётчик и записываем нули. */
static int32_t alloc_pg(BTree *t) {
    int32_t no = t->npages++;
    Page blank; memset(blank, 0, BT_PAGE_SIZE);
    wr(t->fd, no, blank);
    return no;
}

/* Сброс заголовка файла на диск — вызывается после каждой вставки. */
static void flush_hdr(BTree *t) {
    Page p; memset(p, 0, BT_PAGE_SIZE);
    FHdr *h = (FHdr *)p;
    h->magic = MAGIC; h->version = VERSION;
    h->root = t->root; h->nkeys = t->nkeys; h->npages = t->npages;
    wr(t->fd, 0, p);
}

/* ── Акцессоры листовой страницы ── */
static PHdr *L_hdr(Page p)       { return (PHdr *)p; }
static LE   *L_ent(Page p, int i){ return (LE *)(p + 8) + i; }
static void  L_init(Page p)      { memset(p, 0, BT_PAGE_SIZE); L_hdr(p)->type=PT_LEAF; L_hdr(p)->next=-1; }

/* ── Акцессоры внутренней страницы ── */
static PHdr*   I_hdr(Page p)          { return (PHdr *)p; }
static int32_t I_ch (Page p, int i)   {
    const uint8_t *d = p + 8;
    return i == 0 ? *(const int32_t *)d
                  : *(const int32_t *)(d + 4 + (size_t)(i-1)*12 + 8);
}
static int64_t I_key(Page p, int i)   { return *(int64_t *)(p + 8 + 4 + (size_t)i*12); }
static void I_set_ch(Page p, int i, int32_t c) {
    uint8_t *d = p + 8;
    if (i == 0) *(int32_t *)d = c;
    else *(int32_t *)(d + 4 + (size_t)(i-1)*12 + 8) = c;
}
static void I_set_key(Page p, int i, int64_t k) { *(int64_t *)(p + 8 + 4 + (size_t)i*12) = k; }
static void I_init(Page p) { memset(p, 0, BT_PAGE_SIZE); I_hdr(p)->type=PT_INT; I_hdr(p)->next=-1; }

/* ── Вставка в лист ──
 * Возвращает 0 = вставлено без разбиения.
 * Возвращает 1 = разбиение: левая половина остаётся в pgno, правая — в *rnew,
 *                первый ключ правой половины передаётся вверх через *mk. */
static int leaf_ins(BTree *t, int32_t pgno, int64_t key, int64_t off,
                    int64_t *mk, int32_t *rnew) {
    Page p; rd(t->fd, pgno, p);
    PHdr *h = L_hdr(p);
    int n = h->n;
    /* Двоичный поиск позиции вставки (здесь — линейный для простоты). */
    int pos = 0;
    while (pos < n && L_ent(p, pos)->key <= key) pos++;

    if (n < LEAF_MAX) {
        memmove(L_ent(p, pos+1), L_ent(p, pos), (size_t)(n-pos) * sizeof(LE));
        L_ent(p, pos)->key = key; L_ent(p, pos)->off = off;
        h->n++;
        wr(t->fd, pgno, p);
        return 0;
    }

    /* Страница полна: собираем LEAF_MAX+1 записей во временный массив и делим пополам. */
    LE tmp[LEAF_MAX + 1];
    memcpy(tmp, L_ent(p, 0), (size_t)pos * sizeof(LE));
    tmp[pos].key = key; tmp[pos].off = off;
    memcpy(&tmp[pos+1], L_ent(p, pos), (size_t)(n-pos) * sizeof(LE));

    int mid = (LEAF_MAX + 1) / 2;   /* = 128 */

    /* Левая половина остаётся на месте. */
    memcpy(L_ent(p, 0), tmp, (size_t)mid * sizeof(LE));
    h->n = mid;

    /* Правая половина — в новую страницу; вставляем её в цепочку листьев. */
    int32_t rno = alloc_pg(t);
    Page rp; L_init(rp);
    int rn = LEAF_MAX + 1 - mid;
    memcpy(L_ent(rp, 0), &tmp[mid], (size_t)rn * sizeof(LE));
    L_hdr(rp)->n = rn;
    L_hdr(rp)->next = h->next;
    h->next = rno;

    wr(t->fd, pgno, p);
    wr(t->fd, rno,  rp);
    *mk = tmp[mid].key; *rnew = rno;
    return 1;
}

/* ── Вставка в внутреннюю страницу ──
 * Работает с in-memory буфером страницы (p уже прочитан вызывающим).
 * Возвращает 0 = вставлено. Возвращает 1 = разбиение: левая в p, правая в rp_out, *mk вверх. */
static int int_ins(Page p, int64_t key, int32_t rc, int64_t *mk, Page rp_out) {
    PHdr *h = I_hdr(p);
    int n = h->n;
    int pos = 0;
    while (pos < n && key >= I_key(p, pos)) pos++;

    if (n < INT_MAX) {
        for (int i = n-1; i >= pos; i--) {
            I_set_key(p, i+1, I_key(p, i));
            I_set_ch(p, i+2, I_ch(p, i+1));
        }
        I_set_key(p, pos, key);
        I_set_ch(p, pos+1, rc);
        h->n++;
        return 0;
    }

    /* Разбиение: средний ключ поднимается вверх, не сохраняется ни в одной из половин.
     * Это отличает B+ дерево от B-дерева в обработке внутренних узлов. */
    int64_t tkeys[INT_MAX + 1];
    int32_t tchs [INT_MAX + 2];
    for (int i = 0; i <= n; i++) tchs[i]  = I_ch(p, i);
    for (int i = 0; i < n;  i++) tkeys[i] = I_key(p, i);

    memmove(&tkeys[pos+1], &tkeys[pos], (size_t)(n-pos) * sizeof(int64_t));
    memmove(&tchs[pos+2],  &tchs[pos+1], (size_t)(n-pos) * sizeof(int32_t));
    tkeys[pos] = key; tchs[pos+1] = rc;

    int mid = (n + 1) / 2;  /* ключ в позиции mid выталкивается вверх */

    I_init(p); h = I_hdr(p); h->n = mid;
    I_set_ch(p, 0, tchs[0]);
    for (int i = 0; i < mid; i++) { I_set_key(p, i, tkeys[i]); I_set_ch(p, i+1, tchs[i+1]); }

    I_init(rp_out);
    PHdr *rh = I_hdr(rp_out);
    int rn = n - mid;
    rh->n = rn;
    I_set_ch(rp_out, 0, tchs[mid+1]);
    for (int i = 0; i < rn; i++) {
        I_set_key(rp_out, i, tkeys[mid+1+i]);
        I_set_ch(rp_out, i+1, tchs[mid+2+i]);
    }
    *mk = tkeys[mid];
    return 1;
}

/* ── Рекурсивный спуск для вставки ── */
static int do_insert(BTree *t, int32_t pgno, int64_t key, int64_t off,
                     int64_t *mk, int32_t *rnew) {
    Page p; rd(t->fd, pgno, p);
    PHdr *h = (PHdr *)p;

    if (h->type == PT_LEAF)
        return leaf_ins(t, pgno, key, off, mk, rnew);

    /* Внутренний узел: выбираем нужного ребёнка и рекурсируем. */
    int n = h->n, ci = 0;
    while (ci < n && key >= I_key(p, ci)) ci++;
    int32_t child = I_ch(p, ci);

    int64_t cmk; int32_t crnew;
    int split = do_insert(t, child, key, off, &cmk, &crnew);
    if (!split) return 0;

    /* Ребёнок разбился — вставляем разделитель в текущий узел.
     * Перечитываем страницу: дочерняя запись не затронула нас, но это защита от кэша. */
    rd(t->fd, pgno, p);
    Page rp;
    int me_split = int_ins(p, cmk, crnew, mk, rp);
    wr(t->fd, pgno, p);
    if (!me_split) return 0;

    *rnew = alloc_pg(t);
    wr(t->fd, *rnew, rp);
    return 1;
}

/* ── Публичный интерфейс: вставка ── */
int btree_insert(BTree *t, int64_t key, int64_t off) {
    if (!t) return -1;
    int64_t mk; int32_t rnew;
    int split = do_insert(t, t->root, key, off, &mk, &rnew);

    if (split) {
        /* Корень разбился — создаём новый корень на уровень выше. */
        int32_t new_root = alloc_pg(t);
        Page p; I_init(p);
        PHdr *h = I_hdr(p); h->n = 1;
        I_set_ch(p, 0, t->root);
        I_set_key(p, 0, mk);
        I_set_ch(p, 1, rnew);
        wr(t->fd, new_root, p);
        t->root = new_root;
    }
    t->nkeys++;
    flush_hdr(t);
    return 0;
}

/* ── Поиск крайнего левого листа, содержащего ключ >= target ── */
static int32_t find_leaf_ge(BTree *t, int64_t target) {
    int32_t cur = t->root;
    Page p;
    for (;;) {
        rd(t->fd, cur, p);
        PHdr *h = (PHdr *)p;
        if (h->type == PT_LEAF) return cur;
        /* В внутреннем узле идём в первый дочерний узел, ключ которого > target. */
        int n = h->n, ci = 0;
        while (ci < n && target > I_key(p, ci)) ci++;
        cur = I_ch(p, ci);
    }
}

/* ── Публичный интерфейс: диапазонный поиск [lo, hi] ──
 * Находим левый лист и идём по цепочке, пока ключи ≤ hi или не заполнен буфер. */
int btree_range(BTree *t, int64_t lo, int64_t hi,
                int64_t *offs, int *n_out) {
    *n_out = 0;
    if (!t || t->nkeys == 0) return 0;

    int32_t lpg = find_leaf_ge(t, lo);
    Page p;

    while (lpg >= 0 && lpg < t->npages && *n_out < BT_MAX_OFFSETS) {
        rd(t->fd, lpg, p);
        PHdr *h = L_hdr(p);
        int n = h->n;
        for (int i = 0; i < n && *n_out < BT_MAX_OFFSETS; i++) {
            LE *e = L_ent(p, i);
            if (e->key > hi) { lpg = -1; goto done; }
            if (e->key >= lo) offs[(*n_out)++] = e->off;
        }
        lpg = h->next;  /* переходим к следующему листу по связному списку */
    }
done:
    return 0;
}

/* Точечный поиск — частный случай диапазона [key, key]. */
int btree_lookup(BTree *t, int64_t key, int64_t *offs, int *n_out) {
    return btree_range(t, key, key, offs, n_out);
}

/* ── Создание нового дерева (новый файл) ── */
BTree *btree_create(const char *path) {
    int fd = open(path, O_RDWR|O_CREAT|O_TRUNC|O_CLOEXEC, 0644);
    if (fd < 0) { LOG_ERROR("btree_create %s: %s", path, strerror(errno)); return NULL; }

    BTree *t = calloc(1, sizeof(BTree));
    t->fd = fd; t->root = 1; t->npages = 1; t->nkeys = 0;

    /* Страница 0 — заголовок (пока пустая, flush_hdr запишет). */
    Page blank; memset(blank, 0, BT_PAGE_SIZE);
    wr(fd, 0, blank);

    /* Страница 1 — начальный корневой лист. */
    Page root; L_init(root);
    wr(fd, 1, root);
    t->npages = 2;

    flush_hdr(t);
    return t;
}

/* ── Открытие существующего дерева ── */
BTree *btree_open(const char *path) {
    int fd = open(path, O_RDWR|O_CLOEXEC);
    if (fd < 0) return NULL;

    Page p;
    if (pread(fd, p, BT_PAGE_SIZE, 0) != BT_PAGE_SIZE) { close(fd); return NULL; }
    FHdr *h = (FHdr *)p;
    /* Проверяем magic: защита от случайного открытия чужого файла. */
    if (h->magic != MAGIC) { close(fd); return NULL; }

    BTree *t = calloc(1, sizeof(BTree));
    t->fd = fd; t->root = h->root; t->npages = h->npages; t->nkeys = h->nkeys;
    return t;
}

/* Закрытие дерева: сбрасываем заголовок, закрываем fd и освобождаем дескриптор. */
void btree_close(BTree *t) {
    if (!t) return;
    flush_hdr(t);
    close(t->fd);
    free(t);
}

int64_t btree_count(BTree *t) { return t ? t->nkeys : 0; }
