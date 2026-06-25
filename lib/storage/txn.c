/*
 * txn.c — менеджер транзакций для слоя хранения.
 *
 * Поддерживает фиксированный пул активных транзакций (TXN_MAX_ACTIVE).
 * Каждая транзакция буферизует операции (insert/delete/update) в собственной
 * арене и применяет их только при commit через переданный callback.
 * Доступ к пулу сериализуется одним мьютексом tm->mu.
 */
#include "txn.h"
#include "../core/log.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ── Helpers ── */

/* Найти активную транзакцию по id; NULL, если такой нет. */
static Txn *find_slot(TxnManager *tm, TxnId id) {
    for (int i = 0; i < TXN_MAX_ACTIVE; i++) {
        if (tm->txns[i].id == id && tm->txns[i].status == TXN_ACTIVE)
            return &tm->txns[i];
    }
    return NULL;
}

/* Найти свободный слот в пуле (пустой или с неактивной транзакцией). */
static Txn *find_free_slot(TxnManager *tm) {
    for (int i = 0; i < TXN_MAX_ACTIVE; i++) {
        if (tm->txns[i].id == TXN_ID_NONE || tm->txns[i].status != TXN_ACTIVE)
            return &tm->txns[i];
    }
    return NULL;
}

/* Deep-copy a ColBatch into dst arena. */
static ColBatch *colbatch_copy(const ColBatch *src, Arena *dst) {
    ColBatch *copy = arena_calloc(dst, sizeof(ColBatch));
    copy->ncols = src->ncols;
    copy->nrows = src->nrows;

    if (src->schema) {
        Schema *sc = arena_calloc(dst, sizeof(Schema));
        sc->ncols = src->schema->ncols;
        sc->cols  = arena_alloc(dst, (size_t)sc->ncols * sizeof(ColDef));
        for (int i = 0; i < sc->ncols; i++) {
            sc->cols[i].type     = src->schema->cols[i].type;
            sc->cols[i].nullable = src->schema->cols[i].nullable;
            sc->cols[i].name     = src->schema->cols[i].name
                ? arena_strdup(dst, src->schema->cols[i].name) : NULL;
        }
        copy->schema = sc;
    }

    for (int c = 0; c < src->ncols; c++) {
        if (src->null_bitmap[c]) {
            int nb = (src->nrows + 7) / 8; /* размер null-битмапа в байтах */
            copy->null_bitmap[c] = arena_alloc(dst, (size_t)nb);
            memcpy(copy->null_bitmap[c], src->null_bitmap[c], (size_t)nb);
        }
        if (!src->values[c]) continue;

        /* Тип столбца берём из схемы; без схемы трактуем как текст. */
        ColType type = (copy->schema && c < copy->schema->ncols)
                       ? copy->schema->cols[c].type : COL_TEXT;
        switch (type) {
        case COL_INT64:
        case COL_BOOL: {
            int64_t *v = arena_alloc(dst, (size_t)src->nrows * sizeof(int64_t));
            memcpy(v, src->values[c], (size_t)src->nrows * sizeof(int64_t));
            copy->values[c] = v;
            break;
        }
        case COL_DOUBLE: {
            double *v = arena_alloc(dst, (size_t)src->nrows * sizeof(double));
            memcpy(v, src->values[c], (size_t)src->nrows * sizeof(double));
            copy->values[c] = v;
            break;
        }
        default: { /* COL_TEXT, COL_NULL */
            char **sv = (char **)src->values[c];
            char **dv = arena_alloc(dst, (size_t)src->nrows * sizeof(char *));
            for (int r = 0; r < src->nrows; r++)
                dv[r] = (sv && sv[r]) ? arena_strdup(dst, sv[r]) : NULL;
            copy->values[c] = dv;
            break;
        }
        }
    }
    return copy;
}

/* ── Public API ── */

/* Создать менеджер транзакций с инициализированным мьютексом и счётчиком id. */
TxnManager *txn_manager_create(void) {
    TxnManager *tm = calloc(1, sizeof(TxnManager));
    pthread_mutex_init(&tm->mu, NULL);
    tm->next_id = 1;
    return tm;
}

/* Уничтожить менеджер: освободить арены всех слотов и сам мьютекс. */
void txn_manager_destroy(TxnManager *tm) {
    if (!tm) return;
    pthread_mutex_lock(&tm->mu);
    for (int i = 0; i < TXN_MAX_ACTIVE; i++) {
        if (tm->txns[i].arena) {
            arena_destroy(tm->txns[i].arena);
            tm->txns[i].arena = NULL;
        }
    }
    pthread_mutex_unlock(&tm->mu);
    pthread_mutex_destroy(&tm->mu);
    free(tm);
}

/* Начать транзакцию: занять свободный слот, выдать новый id и арену.
 * Возвращает TXN_ID_NONE, если все слоты заняты. */
TxnId txn_begin(TxnManager *tm) {
    pthread_mutex_lock(&tm->mu);
    Txn *slot = find_free_slot(tm);
    if (!slot) {
        pthread_mutex_unlock(&tm->mu);
        LOG_ERROR("txn_begin: no free transaction slots (max %d)", TXN_MAX_ACTIVE);
        return TXN_ID_NONE;
    }
    if (slot->arena) { arena_destroy(slot->arena); }
    memset(slot, 0, sizeof(Txn));
    slot->id       = tm->next_id++;
    slot->status   = TXN_ACTIVE;
    slot->begin_ts = (int64_t)time(NULL);
    slot->arena    = arena_create(65536);
    TxnId id = slot->id;
    pthread_mutex_unlock(&tm->mu);
    return id;
}

/* Откатить транзакцию: отбросить буферизованные операции и освободить слот. */
int txn_rollback(TxnManager *tm, TxnId id) {
    if (!tm || id == TXN_ID_NONE) return -1;
    pthread_mutex_lock(&tm->mu);
    Txn *txn = find_slot(tm, id);
    if (!txn) { pthread_mutex_unlock(&tm->mu); return -1; }
    txn->status = TXN_ABORTED;
    if (txn->arena) { arena_destroy(txn->arena); txn->arena = NULL; }
    txn->nentries = 0;
    txn->id       = TXN_ID_NONE; /* free the slot */
    pthread_mutex_unlock(&tm->mu);
    return 0;
}

/* Зафиксировать транзакцию: применить все буферизованные операции через fn.
 * Применение идёт вне мьютекса (операции с таблицами могут блокировать).
 * При первой ошибке (rc != 0) применение прекращается; слот всё равно освобождается. */
int txn_commit(TxnManager *tm, TxnId id, TxnApplyFn fn, void *userdata) {
    if (!tm || id == TXN_ID_NONE || !fn) return -1;

    pthread_mutex_lock(&tm->mu);
    Txn *txn = find_slot(tm, id);
    if (!txn) { pthread_mutex_unlock(&tm->mu); return -1; }

    /* Mark committed so no new operations can be buffered. */
    txn->status = TXN_COMMITTED;
    int nentries = txn->nentries;
    /* Make local copy of entries so we can release the mutex during I/O. */
    TxnEntry *entries = txn->entries; /* pointer into the Txn struct — safe while mutex held */
    pthread_mutex_unlock(&tm->mu);

    /* Apply entries outside the global mutex — table operations may block. */
    int rc = 0;
    for (int i = 0; i < nentries && rc == 0; i++) {
        TxnEntry *e = &entries[i];
        rc = fn(e->table, e->op, e->batch,
                e->orig_offset, e->new_csv, e->csv_len, userdata);
        if (rc != 0)
            LOG_ERROR("txn %llu commit: entry %d failed (table=%s op=%d)",
                      (unsigned long long)id, i, e->table, (int)e->op);
    }

    /* Clean up */
    pthread_mutex_lock(&tm->mu);
    if (txn->arena) { arena_destroy(txn->arena); txn->arena = NULL; }
    txn->nentries = 0;
    txn->id       = TXN_ID_NONE;
    pthread_mutex_unlock(&tm->mu);

    return rc;
}

/* Найти активную транзакцию по id под защитой мьютекса. */
Txn *txn_find(TxnManager *tm, TxnId id) {
    if (!tm || id == TXN_ID_NONE) return NULL;
    pthread_mutex_lock(&tm->mu);
    Txn *found = find_slot(tm, id);
    pthread_mutex_unlock(&tm->mu);
    return found;
}

/* Буферизовать вставку: глубоко копируем batch в арену транзакции. */
int txn_buffer_insert(TxnManager *tm, TxnId id,
                      const char *table, const ColBatch *batch) {
    if (!tm || id == TXN_ID_NONE || !batch || !table) return -1;
    pthread_mutex_lock(&tm->mu);
    Txn *txn = find_slot(tm, id);
    if (!txn || txn->nentries >= TXN_MAX_ENTRIES) {
        pthread_mutex_unlock(&tm->mu);
        return -1;
    }
    TxnEntry *e = &txn->entries[txn->nentries++];
    strncpy(e->table, table, sizeof(e->table) - 1);
    e->op    = TXN_OP_INSERT;
    e->batch = colbatch_copy(batch, txn->arena);
    pthread_mutex_unlock(&tm->mu);
    return 0;
}

/* Буферизовать удаление строки по её исходному смещению в таблице. */
int txn_buffer_delete(TxnManager *tm, TxnId id,
                      const char *table, int64_t orig_offset) {
    if (!tm || id == TXN_ID_NONE || !table) return -1;
    pthread_mutex_lock(&tm->mu);
    Txn *txn = find_slot(tm, id);
    if (!txn || txn->nentries >= TXN_MAX_ENTRIES) {
        pthread_mutex_unlock(&tm->mu);
        return -1;
    }
    TxnEntry *e = &txn->entries[txn->nentries++];
    strncpy(e->table, table, sizeof(e->table) - 1);
    e->op          = TXN_OP_DELETE;
    e->orig_offset = orig_offset;
    pthread_mutex_unlock(&tm->mu);
    return 0;
}

/* Буферизовать обновление: новая CSV-строка копируется в арену транзакции. */
int txn_buffer_update(TxnManager *tm, TxnId id,
                      const char *table, int64_t orig_offset,
                      const char *new_csv, size_t csv_len) {
    if (!tm || id == TXN_ID_NONE || !table) return -1;
    pthread_mutex_lock(&tm->mu);
    Txn *txn = find_slot(tm, id);
    if (!txn || txn->nentries >= TXN_MAX_ENTRIES) {
        pthread_mutex_unlock(&tm->mu);
        return -1;
    }
    TxnEntry *e = &txn->entries[txn->nentries++];
    strncpy(e->table, table, sizeof(e->table) - 1);
    e->op          = TXN_OP_UPDATE;
    e->orig_offset = orig_offset;
    if (new_csv && csv_len > 0) {
        e->new_csv = arena_alloc(txn->arena, csv_len + 1);
        memcpy(e->new_csv, new_csv, csv_len);
        e->new_csv[csv_len] = '\0';
        e->csv_len = csv_len;
    }
    pthread_mutex_unlock(&tm->mu);
    return 0;
}

/* Откатить активные транзакции, висящие дольше timeout_sec секунд.
 * Вызывается периодически для защиты от «забытых» транзакций. */
void txn_manager_timeout_check(TxnManager *tm, int timeout_sec) {
    if (!tm || timeout_sec <= 0) return;
    int64_t now = (int64_t)time(NULL);
    pthread_mutex_lock(&tm->mu);
    for (int i = 0; i < TXN_MAX_ACTIVE; i++) {
        Txn *txn = &tm->txns[i];
        if (txn->status != TXN_ACTIVE || txn->id == TXN_ID_NONE) continue;
        if (now - txn->begin_ts > (int64_t)timeout_sec) {
            LOG_WARN("txn %llu timed out after %d s — rolling back",
                     (unsigned long long)txn->id, timeout_sec);
            txn->status = TXN_ABORTED;
            if (txn->arena) { arena_destroy(txn->arena); txn->arena = NULL; }
            txn->nentries = 0;
            txn->id = TXN_ID_NONE;
        }
    }
    pthread_mutex_unlock(&tm->mu);
}
