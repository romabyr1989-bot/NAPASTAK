/*
 * storage.c — слой хранения NAPASTAK.
 *
 * Содержит три подсистемы:
 *   1. WAL — append-only журнал записи (вставки строк, тумбстоны DELETE/UPDATE),
 *      с опциональной репликацией через callback.
 *   2. Table — таблица поверх WAL: запись батчей (CSV или сжатый формат),
 *      B-tree индексы по INT64-столбцам, компакция (применение тумбстонов).
 *   3. Catalog — метаданные в SQLite: таблицы, пайплайны, запуски,
 *      сохранённые результаты и реестр индексов.
 */
#include "storage.h"
#include "compress.h"
#include "../core/log.h"
#include "../index/btree.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sqlite3.h>
#include <time.h>

/* WAL versioning.
 * version=0x00 is implicit for old records (first byte is printable ASCII >=0x20).
 * version=0x01 signals a CompressedBatch record: [0x01][serialized CompressedBatch]. */
#define WAL_VERSION_PLAIN      0x00  /* prepended to legacy row for explicit labelling */
#define WAL_VERSION_COMPRESSED 0x01  /* CompressedBatch follows */

/* ── WAL ── */
struct WAL {
    int     fd;
    char    path[512];
    int64_t write_pos;
    /* replication callback */
    WalWriteCallback repl_cb;
    void            *repl_ud;
    char             table_name[128];
    uint64_t         next_lsn;
};

/* Открыть/создать WAL-файл для дозаписи; write_pos выставляется по размеру файла. */
WAL *wal_open(const char *path) {
    WAL *w = calloc(1, sizeof(WAL));
    strncpy(w->path, path, sizeof(w->path)-1);
    w->fd = open(path, O_WRONLY|O_CREAT|O_APPEND|O_CLOEXEC, 0644);
    if (w->fd < 0) { LOG_ERROR("wal_open %s: %s", path, strerror(errno)); free(w); return NULL; }
    struct stat st;
    if (fstat(w->fd, &st) == 0) w->write_pos = st.st_size;
    return w;
}

/* Дозаписать запись формата [uint32_t len][data]; при наличии репликации
   передать её копию callback'у с инкрементом LSN. */
int wal_append(WAL *w, const void *data, size_t len) {
    uint32_t l = (uint32_t)len;
    if (write(w->fd, &l, 4) != 4) return -1;
    if (write(w->fd, data, len) != (ssize_t)len) return -1;
    w->write_pos += 4 + (int64_t)len;
    if (w->repl_cb)
        w->repl_cb(w->table_name, w->next_lsn++, data, len, w->repl_ud);
    return 0;
}

int64_t wal_tell(WAL *w) { return w ? w->write_pos : 0; }
int     wal_sync(WAL *w) { return fdatasync(w->fd); }
void    wal_close(WAL *w){ close(w->fd); free(w); }

/* Тумбстон удаления: [op=DELETE][8 байт offset исходной записи, big-endian]. */
int wal_append_delete(WAL *w, int64_t orig_offset) {
    uint8_t buf[9];
    buf[0] = WAL_OP_DELETE;
    for (int i = 0; i < 8; i++) buf[1+i] = (uint8_t)((uint64_t)orig_offset >> (56 - i*8));
    return wal_append(w, buf, 9);
}

/* Тумбстон обновления: [op=UPDATE][8 байт offset][новая CSV-строка].
   При компакции исходная запись заменяется на new_csv. */
int wal_append_update(WAL *w, int64_t orig_offset, const char *new_csv, size_t csv_len) {
    size_t total = 9 + csv_len;
    uint8_t *buf = malloc(total);
    if (!buf) return -1;
    buf[0] = WAL_OP_UPDATE;
    for (int i = 0; i < 8; i++) buf[1+i] = (uint8_t)((uint64_t)orig_offset >> (56 - i*8));
    memcpy(buf + 9, new_csv, csv_len);
    int r = wal_append(w, buf, total);
    free(buf);
    return r;
}

/* ── ColBatch helpers ── */
/* Чтение/установка i-го бита в упакованном битмапе (для null-масок столбцов). */
static bool bit_get(const uint8_t *bm, int i) {
    return !!(bm[i/8] & (1u << (i%8)));
}
static void bit_set(uint8_t *bm, int i) { bm[i/8] |= (1u << (i%8)); }

/* ── Table ── */
struct Table {
    char    name[128];
    char    dir[512];
    Schema *schema;
    int64_t row_count;
    WAL    *wal;
    /* B-tree indexes: one per indexed column */
    BTree **indexes;        /* malloc'd array of BTree* */
    int    *indexed_cols;   /* malloc'd array of column indices */
    int     nindexes;
};

/* Scan table dir for idx_N.btree files and open them */
static void table_load_indexes(Table *t) {
    DIR *d = opendir(t->dir);
    if (!d) return;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        int col_idx = -1;
        if (sscanf(ent->d_name, "idx_%d.btree", &col_idx) != 1 || col_idx < 0) continue;
        char idx_path[700];
        snprintf(idx_path, sizeof(idx_path), "%s/%s", t->dir, ent->d_name);
        BTree *bt = btree_open(idx_path);
        if (!bt) continue;
        t->indexes     = realloc(t->indexes,     (size_t)(t->nindexes+1) * sizeof(BTree*));
        t->indexed_cols= realloc(t->indexed_cols,(size_t)(t->nindexes+1) * sizeof(int));
        t->indexes[t->nindexes]      = bt;
        t->indexed_cols[t->nindexes] = col_idx;
        t->nindexes++;
    }
    closedir(d);
}

/* Сохранить схему таблицы в dir/schema.json (отладочная копия рядом с WAL). */
static void table_write_schema_file(const char *dir, Schema *sc) {
    if (!sc) return;
    char path[600]; snprintf(path, sizeof(path), "%s/schema.json", dir);
    FILE *f = fopen(path, "w");
    if (!f) return;
    fprintf(f, "{\"ncols\":%d,\"cols\":[", sc->ncols);
    for (int i = 0; i < sc->ncols; i++) {
        if (i > 0) fputc(',', f);
        fprintf(f, "{\"name\":\"%s\",\"type\":%d,\"nullable\":%s}",
                sc->cols[i].name ? sc->cols[i].name : "",
                (int)sc->cols[i].type,
                sc->cols[i].nullable ? "true" : "false");
    }
    fputs("]}", f);
    fclose(f);
}

/* Создать новую таблицу: каталог dir/name, файл схемы и пустой WAL. */
Table *table_create(const char *name, Schema *schema, const char *dir) {
    Table *t = calloc(1, sizeof(Table));
    strncpy(t->name, name, sizeof(t->name)-1);
    snprintf(t->dir, sizeof(t->dir), "%s/%s", dir, name);
    mkdir(t->dir, 0755);
    t->schema = schema;
    table_write_schema_file(t->dir, schema);
    char wal_path[600]; snprintf(wal_path, sizeof(wal_path), "%s/wal.bin", t->dir);
    t->wal = wal_open(wal_path);
    if (t->wal) strncpy(t->wal->table_name, name, sizeof(t->wal->table_name)-1);
    table_load_indexes(t);
    return t;
}

/* Открыть существующую таблицу: переоткрыть WAL и загрузить B-tree индексы. */
Table *table_open(const char *name, const char *dir) {
    Table *t = calloc(1, sizeof(Table));
    strncpy(t->name, name, sizeof(t->name)-1);
    snprintf(t->dir, sizeof(t->dir), "%s/%s", dir, name);
    /* Каталог таблицы может отсутствовать: запись есть в catalog.db, а данные
     * удалены мимо гейтвея. Раньше это давало ERROR «wal_open … No such file or
     * directory» на КАЖДУЮ такую таблицу при старте, а сама таблица оставалась
     * без WAL — то есть молча непригодной для записи. Создаём каталог и
     * сообщаем об этом один раз: таблица открывается пустой и снова рабочей. */
    struct stat dst;
    if (stat(t->dir, &dst) != 0) {
        if (mkdir(t->dir, 0755) == 0)
            LOG_WARN("таблица '%s': каталог данных отсутствовал, создан заново — таблица пуста", name);
    }
    char wal_path[600]; snprintf(wal_path, sizeof(wal_path), "%s/wal.bin", t->dir);
    t->wal = wal_open(wal_path);
    if (t->wal) strncpy(t->wal->table_name, name, sizeof(t->wal->table_name)-1);
    table_load_indexes(t);
    return t;
}

/* Подключить callback репликации к WAL таблицы. */
void table_set_wal_callback(Table *t, WalWriteCallback cb, void *userdata) {
    if (!t || !t->wal) return;
    t->wal->repl_cb = cb;
    t->wal->repl_ud = userdata;
}

int table_wal_append(Table *t, const void *data, size_t len) {
    if (!t || !t->wal) return -1;
    return wal_append(t->wal, data, len);
}

/* Write one batch to WAL — row-by-row CSV format (default / debug).
   ENABLE_COMPRESSION path: serialize entire batch as one versioned WAL record. */
int table_append(Table *t, ColBatch *batch) {
    if (!batch || batch->nrows == 0) return 0;

#ifdef ENABLE_COMPRESSION
    /* Compress the whole batch and write as a single versioned WAL record.
     * Note: with batch records, tombstone offsets reference the start of the
     * batch record. DML on compressed tables is handled via full-batch rewrite
     * during compaction. */
    Arena *ca = arena_create(65536);
    CompressedBatch *cb = compress_batch(batch, ca);
    void *ser = NULL; size_t ser_len = 0;
    compressed_batch_serialize(cb, &ser, &ser_len, ca);

    /* Record: [uint32_t total_len][uint8_t version=0x01][serialized_data] */
    size_t rec_len = 1 + ser_len;
    uint8_t *rec = arena_alloc(ca, rec_len);
    rec[0] = WAL_VERSION_COMPRESSED;
    memcpy(rec + 1, ser, ser_len);

    int64_t batch_offset = wal_tell(t->wal);
    wal_append(t->wal, rec, rec_len);
    t->row_count += batch->nrows;
    cb->compressed_bytes = ser_len;

    /* Update B-tree indexes for each row in the batch */
    if (t->nindexes > 0 && batch->schema) {
        for (int r = 0; r < batch->nrows; r++) {
            for (int idx = 0; idx < t->nindexes; idx++) {
                int ci = t->indexed_cols[idx];
                if (ci >= batch->ncols) continue;
                if (batch->schema->cols[ci].type != COL_INT64) continue;
                bool is_null = batch->null_bitmap[ci] && bit_get(batch->null_bitmap[ci], r);
                if (is_null) continue;
                int64_t key = ((int64_t*)batch->values[ci])[r];
                btree_insert(t->indexes[idx], key, batch_offset);
            }
        }
    }

    arena_destroy(ca);
    return wal_sync(t->wal);
#else
    enum { ROW_BUF_CAP = 262144 };
    char *row_buf = malloc(ROW_BUF_CAP);
    if (!row_buf) return -1;
    int ncols = batch->ncols ? batch->ncols : (batch->schema ? batch->schema->ncols : 0);
    for (int r = 0; r < batch->nrows; r++) {
        int off = 0;
        for (int c = 0; c < ncols; c++) {
            if (off >= ROW_BUF_CAP - 2) { free(row_buf); return -1; }
            if (c) row_buf[off++] = ',';
            if (batch->null_bitmap[c] && bit_get(batch->null_bitmap[c], r)) {
                int n = snprintf(row_buf+off, ROW_BUF_CAP-off, "NULL");
                if (n < 0 || off + n >= ROW_BUF_CAP) { free(row_buf); return -1; }
                off += n; continue;
            }
            switch (batch->schema->cols[c].type) {
                case COL_INT64: {
                    int n = snprintf(row_buf+off, ROW_BUF_CAP-off, "%lld", ((int64_t*)batch->values[c])[r]);
                    if (n < 0 || off + n >= ROW_BUF_CAP) { free(row_buf); return -1; }
                    off += n; break;
                }
                case COL_DOUBLE: {
                    int n = snprintf(row_buf+off, ROW_BUF_CAP-off, "%.10g", ((double*)batch->values[c])[r]);
                    if (n < 0 || off + n >= ROW_BUF_CAP) { free(row_buf); return -1; }
                    off += n; break;
                }
                case COL_TEXT: {
                    int n = snprintf(row_buf+off, ROW_BUF_CAP-off, "%s", ((char**)batch->values[c])[r]?:"");
                    if (n < 0 || off + n >= ROW_BUF_CAP) { free(row_buf); return -1; }
                    off += n; break;
                }
                case COL_BOOL: {
                    int n = snprintf(row_buf+off, ROW_BUF_CAP-off, "%s", ((int*)batch->values[c])[r]?"true":"false");
                    if (n < 0 || off + n >= ROW_BUF_CAP) { free(row_buf); return -1; }
                    off += n; break;
                }
                default: break;
            }
        }
        if (off >= ROW_BUF_CAP - 1) { free(row_buf); return -1; }
        row_buf[off++] = '\n';
        int64_t row_offset = wal_tell(t->wal);
        wal_append(t->wal, row_buf, (size_t)off);
        t->row_count++;
        if (t->nindexes > 0 && batch->schema) {
            for (int idx = 0; idx < t->nindexes; idx++) {
                int ci = t->indexed_cols[idx];
                if (ci >= batch->ncols) continue;
                if (batch->schema->cols[ci].type != COL_INT64) continue;
                bool is_null = batch->null_bitmap[ci] && bit_get(batch->null_bitmap[ci], r);
                if (is_null) continue;
                int64_t key = ((int64_t*)batch->values[ci])[r];
                btree_insert(t->indexes[idx], key, row_offset);
            }
        }
    }
    free(row_buf);
    return wal_sync(t->wal);
#endif
}

int64_t table_row_count(Table *t) { return t->row_count; }
Schema *table_schema(Table *t)    { return t->schema; }

int table_scan(Table *t, ColBatch **out, Arena *a) {
    (void)t; (void)out; (void)a;
    return (int)t->row_count;
}

int table_delete(Table *t, int64_t orig_offset) {
    if (!t || !t->wal) return -1;
    return wal_append_delete(t->wal, orig_offset);
}

int table_update(Table *t, int64_t orig_offset, const char *new_csv, size_t csv_len) {
    if (!t || !t->wal) return -1;
    return wal_append_update(t->wal, orig_offset, new_csv, csv_len);
}

/* Компакция WAL: за два прохода применить тумбстоны.
   Pass 1 — собрать удалённые offset'ы и UPDATE-замены; Pass 2 — переписать
   живые INSERT'ы (для обновлённых строк подставить новый CSV) в новый файл,
   затем атомарно заменить wal.bin и переоткрыть журнал. */
int table_compact(Table *t, Arena *a) {
    if (!t) return -1;
    (void)a;
    char tmp_path[600];
    snprintf(tmp_path, sizeof(tmp_path), "%s/wal_compact.bin", t->dir);

    char wal_path[600];
    snprintf(wal_path, sizeof(wal_path), "%s/wal.bin", t->dir);

    /* Pass 1: collect tombstones */
    FILE *rf = fopen(wal_path, "rb");
    if (!rf) return -1;

    /* Use a simple sorted array of deleted/updated offsets */
    int64_t *dead = NULL;
    int ndead = 0, dead_cap = 0;
    /* Map from orig_offset → new_csv for UPDATEs (store as linked list nodes) */
    struct UpdNode { int64_t off; char *csv; size_t csv_len; struct UpdNode *next; } *upd_list = NULL;

    int64_t file_off = 0;
    char row_buf[262144];
    while (1) {
        uint32_t l = 0;
        if (fread(&l, 4, 1, rf) != 1) break;
        if (l == 0 || l > sizeof(row_buf)-1) { fseek(rf, (long)l, SEEK_CUR); file_off += 4+(int64_t)l; continue; }
        if (fread(row_buf, 1, l, rf) != l) break;
        row_buf[l] = '\0';
        file_off += 4 + (int64_t)l;

        uint8_t op = (uint8_t)row_buf[0];
        if (op == WAL_OP_DELETE && l == 9) {
            int64_t orig = 0;
            for (int b=0;b<8;b++) orig = (orig<<8)|((uint8_t)row_buf[1+b]);
            if (ndead == dead_cap) {
                dead_cap = dead_cap ? dead_cap*2 : 64;
                dead = realloc(dead, (size_t)dead_cap * sizeof(int64_t));
            }
            dead[ndead++] = orig;
        } else if (op == WAL_OP_UPDATE && l >= 9) {
            int64_t orig = 0;
            for (int b=0;b<8;b++) orig = (orig<<8)|((uint8_t)row_buf[1+b]);
            size_t csv_len = l - 9;
            struct UpdNode *un = malloc(sizeof(*un) + csv_len + 1);
            un->off = orig; un->csv = (char*)(un+1);
            memcpy(un->csv, row_buf+9, csv_len); un->csv[csv_len] = '\0';
            un->csv_len = csv_len; un->next = upd_list; upd_list = un;
            /* also tombstone the original */
            if (ndead == dead_cap) { dead_cap = dead_cap ? dead_cap*2 : 64; dead = realloc(dead,(size_t)dead_cap*sizeof(int64_t)); }
            dead[ndead++] = orig;
        }
    }
    rewind(rf);

    /* Pass 2: write compacted WAL */
    FILE *wf = fopen(tmp_path, "wb");
    if (!wf) { fclose(rf); free(dead); return -1; }

    file_off = 0;
    while (1) {
        uint32_t l = 0;
        if (fread(&l, 4, 1, rf) != 1) break;
        int64_t rec_off = file_off;
        if (l == 0 || l > sizeof(row_buf)-1) { fseek(rf, (long)l, SEEK_CUR); file_off += 4+(int64_t)l; continue; }
        if (fread(row_buf, 1, l, rf) != l) break;
        row_buf[l] = '\0';
        file_off += 4 + (int64_t)l;

        uint8_t op = (uint8_t)row_buf[0];
        if (op == WAL_OP_DELETE || op == WAL_OP_UPDATE) continue; /* drop tombstones */

        /* check if this INSERT is tombstoned */
        bool tombstoned = false;
        for (int d=0;d<ndead;d++) if (dead[d]==rec_off) { tombstoned=true; break; }
        if (tombstoned) {
            /* Check if this is an UPDATE replacement */
            for (struct UpdNode *un=upd_list; un; un=un->next) {
                if (un->off == rec_off) {
                    uint32_t cl = (uint32_t)un->csv_len;
                    fwrite(&cl, 4, 1, wf);
                    fwrite(un->csv, 1, un->csv_len, wf);
                    break;
                }
            }
            continue;
        }
        fwrite(&l, 4, 1, wf);
        fwrite(row_buf, 1, l, wf);
    }
    fclose(rf); fclose(wf);

    /* Replace WAL */
    rename(tmp_path, wal_path);

    /* Reopen WAL for appending */
    wal_close(t->wal);
    t->wal = wal_open(wal_path);

    /* Free update nodes */
    for (struct UpdNode *un=upd_list; un; ) { struct UpdNode *nx=un->next; free(un); un=nx; }
    free(dead);
    return 0;
}

void table_close(Table *t) {
    if (!t) return;
    if (t->wal) wal_close(t->wal);
    for (int i = 0; i < t->nindexes; i++) btree_close(t->indexes[i]);
    free(t->indexes);
    free(t->indexed_cols);
    free(t);
}

/* Вернуть открытый B-tree индекс по столбцу col_idx, либо NULL если его нет. */
BTree *table_get_index(Table *t, int col_idx) {
    if (!t) return NULL;
    for (int i = 0; i < t->nindexes; i++)
        if (t->indexed_cols[i] == col_idx) return t->indexes[i];
    return NULL;
}

/* Построить B-tree индекс по INT64-столбцу: сбросить старый при наличии,
   просканировать WAL и заполнить дерево (ключ → offset записи), затем
   зарегистрировать в каталоге. */
int table_create_index(Table *t, int col_idx, Catalog *c) {
    if (!t || col_idx < 0) return -1;
    /* Only COL_INT64 supported */
    if (t->schema && col_idx < t->schema->ncols &&
        t->schema->cols[col_idx].type != COL_INT64) {
        LOG_ERROR("table_create_index: col %d is not INT64", col_idx);
        return -1;
    }

    /* Drop old index if any */
    for (int i = 0; i < t->nindexes; i++) {
        if (t->indexed_cols[i] == col_idx) {
            btree_close(t->indexes[i]);
            /* shift remaining entries */
            memmove(&t->indexes[i],      &t->indexes[i+1],
                    (size_t)(t->nindexes-i-1)*sizeof(BTree*));
            memmove(&t->indexed_cols[i], &t->indexed_cols[i+1],
                    (size_t)(t->nindexes-i-1)*sizeof(int));
            t->nindexes--;
            break;
        }
    }

    char idx_path[700];
    snprintf(idx_path, sizeof(idx_path), "%s/idx_%d.btree", t->dir, col_idx);
    BTree *bt = btree_create(idx_path);
    if (!bt) return -1;

    /* Scan WAL to populate index */
    char wal_path[700];
    snprintf(wal_path, sizeof(wal_path), "%s/wal.bin", t->dir);
    FILE *wf = fopen(wal_path, "rb");
    if (wf) {
        int64_t file_off = 0;
        char row_buf[262144];
        while (1) {
            uint32_t l = 0;
            if (fread(&l, 4, 1, wf) != 1) break;
            if (l == 0 || l >= sizeof(row_buf)) {
                fseek(wf, (long)l, SEEK_CUR);
                file_off += 4 + (int64_t)l;
                continue;
            }
            int64_t rec_off = file_off;
            if (fread(row_buf, 1, l, wf) != l) break;
            row_buf[l] = '\0';
            file_off += 4 + (int64_t)l;

            /* Skip to col_idx-th comma-separated field */
            char *p = row_buf;
            for (int ci = 0; ci < col_idx; ci++) {
                p = strchr(p, ',');
                if (!p) { p = NULL; break; }
                p++;
            }
            if (!p || !*p || strncmp(p, "NULL", 4) == 0) continue;
            int64_t key = strtoll(p, NULL, 10);
            btree_insert(bt, key, rec_off);
        }
        fclose(wf);
    }

    /* Register the open handle */
    t->indexes      = realloc(t->indexes,      (size_t)(t->nindexes+1)*sizeof(BTree*));
    t->indexed_cols = realloc(t->indexed_cols, (size_t)(t->nindexes+1)*sizeof(int));
    t->indexes[t->nindexes]      = bt;
    t->indexed_cols[t->nindexes] = col_idx;
    t->nindexes++;

    /* Register in catalog if provided */
    if (c && t->schema && col_idx < t->schema->ncols) {
        const char *col_name = t->schema->cols[col_idx].name;
        catalog_register_index(c, t->name, col_name, col_idx);
    }
    return 0;
}

/* ── Catalog ── */
struct Catalog { sqlite3 *db; pthread_mutex_t mu; };

/* The gateway executes pipelines on a worker pool, so several threads call
 * catalog_* on this one connection concurrently (worker write_rs_to_table /
 * catalog_log_run vs the HTTP thread's queries). The system libsqlite3 corrupts
 * its internal state under that and SIGSEGVs. CAT_GUARD serializes every public
 * catalog entry point with a recursive mutex; __attribute__((cleanup)) releases
 * it on EVERY return path. Recursive because some entries nest (register_index
 * -> ensure_index_table). */
static inline void _cat_unlock(pthread_mutex_t **m) { pthread_mutex_unlock(*m); }
#define CAT_GUARD(c) pthread_mutex_t *_catg __attribute__((cleanup(_cat_unlock))) = &(c)->mu; pthread_mutex_lock(_catg)

/* Обёртка над sqlite3_prepare_v2, которая НЕ молчит при отказе.
 *
 * Отказ здесь особенно коварен: prepare записывает в *st NULL, а
 * sqlite3_step(NULL) возвращает SQLITE_MISUSE, а не SQLITE_ROW. Поэтому циклы
 * вида while (sqlite3_step(st) == SQLITE_ROW) просто не выполняются ни разу, и
 * функция отдаёт пустой результат с признаком успеха. На практике это выглядит
 * как «данные пропали» — без единой строки в журнале; первое, что делает
 * оператор, это переингест, а catalog_register_table делает INSERT OR REPLACE,
 * то есть затирает метаданные. Так выглядела бы любая неудачная миграция схемы.
 *
 * Сигнатура совпадает с sqlite3_prepare_v2 — вызовы отличаются только именем,
 * поток управления не менялся. Полноценная передача ошибки наружу (каждая
 * функция каталога со своим кодом возврата) остаётся отдельной работой; здесь
 * задача в том, чтобы отказ перестал быть невидимым. */
static int cat_prep(sqlite3 *db, const char *sql, int nbyte,
                    sqlite3_stmt **st, const char **tail) {
    int rc = sqlite3_prepare_v2(db, sql, nbyte, st, tail);
    if (rc != SQLITE_OK)
        LOG_ERROR("catalog: prepare не удался [%d]: %s — SQL: %s",
                  rc, sqlite3_errmsg(db), sql ? sql : "(null)");
    return rc;
}

static const char *CATALOG_SCHEMA =
    "CREATE TABLE IF NOT EXISTS tables("
    "  name TEXT PRIMARY KEY, schema_json TEXT, created_at INTEGER);"
    "CREATE TABLE IF NOT EXISTS pipelines("
    "  id TEXT PRIMARY KEY, json TEXT, updated_at INTEGER);"
    "CREATE TABLE IF NOT EXISTS connections("
    "  id TEXT PRIMARY KEY, json TEXT, updated_at INTEGER);"
    "CREATE TABLE IF NOT EXISTS pipeline_runs("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  pipeline_id TEXT, started_at INTEGER, finished_at INTEGER,"
    "  status INTEGER, error_msg TEXT, retry_count INTEGER DEFAULT 0);"
    "CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON pipeline_runs(pipeline_id);"
    "CREATE TABLE IF NOT EXISTS saved_results("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  name TEXT NOT NULL,"
    "  sql_text TEXT,"
    "  columns_json TEXT,"
    "  rows_json TEXT,"
    "  row_count INTEGER,"
    "  created_at INTEGER);"
    "CREATE INDEX IF NOT EXISTS idx_saved_results_created ON saved_results(created_at DESC);";

/* Открыть SQLite-каталог: режим WAL, применить базовую схему и миграции
   (добавление новых столбцов в существующие таблицы — ошибки игнорируются). */
Catalog *catalog_open(const char *path) {
    Catalog *c = calloc(1, sizeof(Catalog));
    { pthread_mutexattr_t _a; pthread_mutexattr_init(&_a);
      pthread_mutexattr_settype(&_a, PTHREAD_MUTEX_RECURSIVE);
      pthread_mutex_init(&c->mu, &_a); pthread_mutexattr_destroy(&_a); }
    /* FULLMUTEX: the gateway runs pipelines on a worker pool, so several threads
     * share this one catalog connection (catalog_log_run etc.). The serialized
     * threading mode makes every call on the connection internally mutex-guarded,
     * preventing the SQLite-internal corruption / SIGSEGV that concurrent runs hit
     * with the default open. busy_timeout absorbs WAL writer-lock contention. */
    if (sqlite3_open_v2(path, &c->db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
            NULL) != SQLITE_OK) {
        LOG_ERROR("catalog_open %s: %s", path, sqlite3_errmsg(c->db));
        free(c); return NULL;
    }
    sqlite3_busy_timeout(c->db, 5000);
    sqlite3_exec(c->db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(c->db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
    /* Self-heal: an unclean shutdown can corrupt indexes (e.g. a duplicate
     * sqlite_autoindex), which makes existing tables invisible ("no such
     * table") even though their data is intact. Detect on open and REINDEX so
     * the platform recovers instead of silently losing the catalog. */
    {
        sqlite3_stmt *qst = NULL; int healthy = 1;
        if (cat_prep(c->db, "PRAGMA quick_check", -1, &qst, NULL) == SQLITE_OK) {
            if (sqlite3_step(qst) == SQLITE_ROW) {
                const char *r = (const char*)sqlite3_column_text(qst, 0);
                healthy = (r && strcmp(r, "ok") == 0);
                if (!healthy) LOG_ERROR("catalog integrity: %s — rebuilding", r ? r : "?");
            }
            sqlite3_finalize(qst);
        }
        if (!healthy) {
            if (sqlite3_exec(c->db, "REINDEX; VACUUM;", NULL, NULL, NULL) == SQLITE_OK)
                LOG_INFO("catalog rebuilt (REINDEX+VACUUM) — recovered");
            else
                LOG_ERROR("catalog rebuild failed — manual recovery may be needed");
        }
    }
    char *err = NULL;
    if (sqlite3_exec(c->db, CATALOG_SCHEMA, NULL, NULL, &err) != SQLITE_OK) {
        LOG_ERROR("catalog schema: %s", err); sqlite3_free(err);
    }
    /* migrate: add columns if they don't exist yet */
    sqlite3_exec(c->db, "ALTER TABLE tables ADD COLUMN source TEXT DEFAULT 'ingest';", NULL, NULL, NULL);
    sqlite3_exec(c->db, "ALTER TABLE tables ADD COLUMN row_count INTEGER DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(c->db, "ALTER TABLE pipeline_runs ADD COLUMN retry_count INTEGER DEFAULT 0;", NULL, NULL, NULL);
    return c;
}

void catalog_close(Catalog *c) { sqlite3_close(c->db); pthread_mutex_destroy(&c->mu); free(c); }

/* Schema → JSON helper */
static char *schema_to_json(Schema *s, Arena *a) {
    JBuf jb; jb_init(&jb, a, 512);
    jb_arr_begin(&jb);
    for (int i = 0; i < s->ncols; i++) {
        if (!s->cols[i].name) continue;
        jb_obj_begin(&jb);
        jb_key(&jb,"name"); jb_str(&jb, s->cols[i].name);
        const char *t = "text";
        if (s->cols[i].type==COL_INT64) t="int64";
        else if (s->cols[i].type==COL_DOUBLE) t="double";
        else if (s->cols[i].type==COL_BOOL) t="bool";
        jb_key(&jb,"type"); jb_str(&jb, t);
        jb_key(&jb,"nullable"); jb_bool(&jb, s->cols[i].nullable);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);
    return (char*)jb_done(&jb);
}

int catalog_register_table(Catalog *c, const char *name, Schema *schema) {
    CAT_GUARD(c);
    Arena *a = arena_create(4096);
    char *sj = schema_to_json(schema, a);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "INSERT OR REPLACE INTO tables(name,schema_json,created_at) VALUES(?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,sj,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(st,3,(int64_t)time(NULL));
    int rc = sqlite3_step(st); sqlite3_finalize(st); arena_destroy(a);
    return rc == SQLITE_DONE ? 0 : -1;
}

int catalog_list_tables(Catalog *c, char ***names_out, int *count_out, Arena *a) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,"SELECT name FROM tables ORDER BY name",-1,&st,NULL);
    int cap=16,n=0;
    char **names = arena_alloc(a, cap*sizeof(char*));
    while (sqlite3_step(st)==SQLITE_ROW) {
        if (n==cap){cap*=2;char**nb=arena_alloc(a,cap*sizeof(char*));memcpy(nb,names,n*sizeof(char*));names=nb;}
        names[n++] = arena_strdup(a,(const char*)sqlite3_column_text(st,0));
    }
    sqlite3_finalize(st);
    *names_out = names; *count_out = n;
    return 0;
}

int catalog_get_schema(Catalog *c, const char *table, Schema **out, Arena *a) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,"SELECT schema_json FROM tables WHERE name=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,table,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW){sqlite3_finalize(st);return -1;}
    const char *sj=(const char*)sqlite3_column_text(st,0);
    JVal *arr = json_parse(a, sj, strlen(sj));
    sqlite3_finalize(st);
    if (!arr || arr->type!=JV_ARRAY) return -1;
    Schema *schema = arena_calloc(a, sizeof(Schema));
    schema->ncols = (int)arr->nitems;
    schema->cols  = arena_alloc(a, schema->ncols * sizeof(ColDef));
    for (int i=0;i<schema->ncols;i++){
        JVal *col = arr->items[i];
        schema->cols[i].name = arena_strdup(a, json_str(json_get(col,"name"),""));
        const char *t = json_str(json_get(col,"type"),"text");
        if (!strcmp(t,"int64"))  schema->cols[i].type=COL_INT64;
        else if (!strcmp(t,"double")) schema->cols[i].type=COL_DOUBLE;
        else if (!strcmp(t,"bool"))   schema->cols[i].type=COL_BOOL;
        else schema->cols[i].type=COL_TEXT;
        schema->cols[i].nullable = json_bool(json_get(col,"nullable"),true);
    }
    *out = schema;
    return 0;
}

int catalog_drop_table(Catalog *c, const char *name) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,"DELETE FROM tables WHERE name=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}

int catalog_update_table_meta(Catalog *c, const char *name, const char *source, int64_t row_count) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "UPDATE tables SET source=?, row_count=? WHERE name=?", -1, &st, NULL);
    sqlite3_bind_text(st, 1, source, -1, SQLITE_STATIC);
    sqlite3_bind_int64(st, 2, row_count);
    sqlite3_bind_text(st, 3, name, -1, SQLITE_STATIC);
    int rc = sqlite3_step(st); sqlite3_finalize(st);
    return rc == SQLITE_DONE ? 0 : -1;
}

/* Список таблиц с метаданными для UI (имя, источник, число строк, столбцы). */
int catalog_list_tables_full(Catalog *c, char **json_out, Arena *a) {
    CAT_GUARD(c);
    /* Returns JSON array: [{name, source, row_count, columns:[{name,type}]}] */
    JBuf jb; jb_init(&jb, a, 4096);
    jb_arr_begin(&jb);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "SELECT name, COALESCE(source,'ingest'), COALESCE(row_count,0), schema_json"
        " FROM tables ORDER BY name", -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name  = (const char*)sqlite3_column_text(st, 0);
        const char *src   = (const char*)sqlite3_column_text(st, 1);
        int64_t     rows  = sqlite3_column_int64(st, 2);
        const char *sjson = (const char*)sqlite3_column_text(st, 3);
        jb_obj_begin(&jb);
        jb_key(&jb,"name");   jb_str(&jb, name ? name : "");
        jb_key(&jb,"source"); jb_str(&jb, src  ? src  : "ingest");
        jb_key(&jb,"rows");   jb_int(&jb, rows);
        /* parse schema_json to emit columns array */
        if (sjson && *sjson) {
            Arena *ta = arena_create(4096);
            JVal *arr = json_parse(ta, sjson, strlen(sjson));
            if (arr && arr->type == JV_ARRAY) {
                jb_key(&jb,"columns"); jb_arr_begin(&jb);
                for (size_t i = 0; i < arr->nitems; i++) {
                    JVal *col = arr->items[i];
                    const char *cname = json_str(json_get(col,"name"),"");
                    const char *ctype = json_str(json_get(col,"type"),"text");
                    if (!cname || !*cname) continue;
                    jb_obj_begin(&jb);
                    jb_key(&jb,"name"); jb_str(&jb, cname);
                    jb_key(&jb,"type"); jb_str(&jb, ctype);
                    jb_obj_end(&jb);
                }
                jb_arr_end(&jb);
            }
            arena_destroy(ta);
        }
        jb_obj_end(&jb);
    }
    sqlite3_finalize(st);
    jb_arr_end(&jb);
    *json_out = (char*)jb_done(&jb);
    return 0;
}

int catalog_save_pipeline(Catalog *c, const char *id, const char *json) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "INSERT OR REPLACE INTO pipelines(id,json,updated_at) VALUES(?,?,?)",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,json,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,3,(int64_t)time(NULL));
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}

int catalog_load_pipeline(Catalog *c, const char *id, char **out, Arena *a) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,"SELECT json FROM pipelines WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW){sqlite3_finalize(st);return -1;}
    *out = arena_strdup(a,(const char*)sqlite3_column_text(st,0));
    sqlite3_finalize(st); return 0;
}

int catalog_list_pipelines(Catalog *c, char ***ids_out, int *count_out, Arena *a) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,"SELECT id FROM pipelines ORDER BY updated_at DESC",-1,&st,NULL);
    int cap=16,n=0; char **ids=arena_alloc(a,cap*sizeof(char*));
    while(sqlite3_step(st)==SQLITE_ROW){
        if(n==cap){cap*=2;char**nb=arena_alloc(a,cap*sizeof(char*));memcpy(nb,ids,n*sizeof(char*));ids=nb;}
        ids[n++]=arena_strdup(a,(const char*)sqlite3_column_text(st,0));
    }
    sqlite3_finalize(st); *ids_out=ids; *count_out=n; return 0;
}

int catalog_delete_pipeline(Catalog *c, const char *id) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,"DELETE FROM pipelines WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    /* Drop the run history together with the pipeline. */
    sqlite3_stmt *rd;
    if(cat_prep(c->db,"DELETE FROM pipeline_runs WHERE pipeline_id=?",-1,&rd,NULL)==SQLITE_OK){
        sqlite3_bind_text(rd,1,id,-1,SQLITE_STATIC);
        sqlite3_step(rd); sqlite3_finalize(rd);
    }
    return rc==SQLITE_DONE?0:-1;
}

/* ─── Справочник подключений к внешним системам (источники/приёмники) ─────
 * Хранятся как конвейеры: id + JSON целиком, вида
 *   {id,name,type,config,created_at,updated_at}
 * config лежит в TEXT без потолка — в отличие от PipelineStep.connector_config[1024],
 * который молча обрезается на длинных конфигах Kafka с TLS/SASL. */

int catalog_save_connection(Catalog *c, const char *id, const char *json) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "INSERT OR REPLACE INTO connections(id,json,updated_at) VALUES(?,?,?)",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,json,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,3,(int64_t)time(NULL));
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}

int catalog_load_connection(Catalog *c, const char *id, char **out, Arena *a) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,"SELECT json FROM connections WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW){sqlite3_finalize(st);return -1;}
    *out = arena_strdup(a,(const char*)sqlite3_column_text(st,0));
    sqlite3_finalize(st); return 0;
}

/* Весь справочник одним JSON-массивом — UI забирает его одним запросом. */
int catalog_list_connections(Catalog *c, char **out, Arena *a) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "SELECT json FROM connections ORDER BY updated_at DESC",-1,&st,NULL);
    JBuf jb; jb_init(&jb,a,4096); jb_arr_begin(&jb);
    while(sqlite3_step(st)==SQLITE_ROW){
        const char *j=(const char*)sqlite3_column_text(st,0);
        if (j && j[0]) jb_raw(&jb, j);
    }
    jb_arr_end(&jb); sqlite3_finalize(st);
    *out=(char*)jb_done(&jb); return 0;
}

int catalog_delete_connection(Catalog *c, const char *id) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,"DELETE FROM connections WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}

int catalog_log_run(Catalog *c, const char *pid, int64_t s, int64_t f, int status, const char *err, int retry_count) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "INSERT INTO pipeline_runs(pipeline_id,started_at,finished_at,status,error_msg,retry_count) VALUES(?,?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,pid,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,2,s); sqlite3_bind_int64(st,3,f);
    sqlite3_bind_int(st,4,status);
    sqlite3_bind_text(st,5,err?err:"",-1,SQLITE_STATIC);
    sqlite3_bind_int(st,6,retry_count);
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}

int catalog_list_runs(Catalog *c, const char *pid, char **out, Arena *a) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "SELECT id,started_at,finished_at,status,error_msg,retry_count FROM pipeline_runs "
        "WHERE pipeline_id=? ORDER BY started_at DESC LIMIT 50",-1,&st,NULL);
    sqlite3_bind_text(st,1,pid,-1,SQLITE_STATIC);
    JBuf jb; jb_init(&jb,a,1024); jb_arr_begin(&jb);
    while(sqlite3_step(st)==SQLITE_ROW){
        jb_obj_begin(&jb);
        jb_key(&jb,"id");         jb_int(&jb,sqlite3_column_int64(st,0));
        jb_key(&jb,"started");    jb_int(&jb,sqlite3_column_int64(st,1));
        jb_key(&jb,"finished");   jb_int(&jb,sqlite3_column_int64(st,2));
        jb_key(&jb,"status");     jb_int(&jb,sqlite3_column_int(st,3));
        jb_key(&jb,"error");      jb_str(&jb,(const char*)sqlite3_column_text(st,4));
        jb_key(&jb,"retry_count"); jb_int(&jb,sqlite3_column_int(st,5));
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb); sqlite3_finalize(st);
    *out=(char*)jb_done(&jb); return 0;
}

/* Сохранить результат запроса (колонки и строки как JSON); вернуть rowid в out_id. */
int catalog_save_result(Catalog *c, const char *name, const char *sql_text,
                        const char *columns_json, const char *rows_json,
                        int row_count, int64_t *out_id) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "INSERT INTO saved_results(name,sql_text,columns_json,rows_json,row_count,created_at)"
        " VALUES(?,?,?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,sql_text?sql_text:"",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,columns_json?columns_json:"[]",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,rows_json?rows_json:"[]",-1,SQLITE_STATIC);
    sqlite3_bind_int(st,5,row_count);
    sqlite3_bind_int64(st,6,(int64_t)time(NULL));
    int rc = sqlite3_step(st); sqlite3_finalize(st);
    if (rc != SQLITE_DONE) return -1;
    if (out_id) *out_id = sqlite3_last_insert_rowid(c->db);
    return 0;
}

/* Список сохранённых результатов без тела строк (rows_json) — превью для UI. */
int catalog_list_results(Catalog *c, char **out, Arena *a) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "SELECT id,name,sql_text,columns_json,row_count,created_at FROM saved_results"
        " ORDER BY created_at DESC LIMIT 100", -1, &st, NULL);
    JBuf jb; jb_init(&jb,a,4096); jb_arr_begin(&jb);
    while(sqlite3_step(st)==SQLITE_ROW){
        jb_obj_begin(&jb);
        jb_key(&jb,"id");           jb_int(&jb,sqlite3_column_int64(st,0));
        jb_key(&jb,"name");         jb_str(&jb,(const char*)sqlite3_column_text(st,1));
        jb_key(&jb,"sql_text");     jb_str(&jb,(const char*)sqlite3_column_text(st,2));
        { const char *cj = (const char*)sqlite3_column_text(st,3);
          jb_key(&jb,"columns_json"); jb_raw(&jb, cj ? cj : "[]"); }
        jb_key(&jb,"row_count");    jb_int(&jb,sqlite3_column_int(st,4));
        jb_key(&jb,"created_at");   jb_int(&jb,sqlite3_column_int64(st,5));
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb); sqlite3_finalize(st);
    *out=(char*)jb_done(&jb); return 0;
}

/* Получить один сохранённый результат целиком, включая rows_json. */
int catalog_get_result(Catalog *c, int64_t id, char **out, Arena *a) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "SELECT id,name,sql_text,columns_json,rows_json,row_count,created_at"
        " FROM saved_results WHERE id=?", -1, &st, NULL);
    sqlite3_bind_int64(st,1,id);
    if (sqlite3_step(st)!=SQLITE_ROW){ sqlite3_finalize(st); return -1; }
    JBuf jb; jb_init(&jb,a,8192); jb_obj_begin(&jb);
    jb_key(&jb,"id");           jb_int(&jb,sqlite3_column_int64(st,0));
    jb_key(&jb,"name");         jb_str(&jb,(const char*)sqlite3_column_text(st,1));
    jb_key(&jb,"sql_text");     jb_str(&jb,(const char*)sqlite3_column_text(st,2));
    const char *cj=(const char*)sqlite3_column_text(st,3);
    const char *rj=(const char*)sqlite3_column_text(st,4);
    jb_key(&jb,"columns_json"); jb_str(&jb,cj?cj:"[]");
    jb_key(&jb,"rows_json");    jb_str(&jb,rj?rj:"[]");
    jb_key(&jb,"row_count");    jb_int(&jb,sqlite3_column_int(st,5));
    jb_key(&jb,"created_at");   jb_int(&jb,sqlite3_column_int64(st,6));
    jb_obj_end(&jb); sqlite3_finalize(st);
    *out=(char*)jb_done(&jb); return 0;
}

int catalog_delete_result(Catalog *c, int64_t id) {
    CAT_GUARD(c);
    sqlite3_stmt *st;
    cat_prep(c->db,"DELETE FROM saved_results WHERE id=?",-1,&st,NULL);
    sqlite3_bind_int64(st,1,id);
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}

/* ── Index registry ── */
/* The table is created lazily on first use via the migration pragma below */
static void catalog_ensure_index_table(Catalog *c) {
    CAT_GUARD(c);
    sqlite3_exec(c->db,
        "CREATE TABLE IF NOT EXISTS table_indexes("
        "  table_name TEXT NOT NULL,"
        "  col_name   TEXT NOT NULL,"
        "  col_idx    INTEGER NOT NULL,"
        "  created_at INTEGER,"
        "  PRIMARY KEY (table_name, col_name));",
        NULL, NULL, NULL);
}

int catalog_register_index(Catalog *c, const char *table,
                           const char *col, int col_idx) {
    CAT_GUARD(c);
    catalog_ensure_index_table(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "INSERT OR REPLACE INTO table_indexes(table_name,col_name,col_idx,created_at)"
        " VALUES(?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st,1,table,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,col,  -1,SQLITE_STATIC);
    sqlite3_bind_int (st,3,col_idx);
    sqlite3_bind_int64(st,4,(int64_t)time(NULL));
    int rc = sqlite3_step(st); sqlite3_finalize(st);
    return rc == SQLITE_DONE ? 0 : -1;
}

int catalog_list_indexes_json(Catalog *c, const char *table,
                              char **json_out, Arena *a) {
    CAT_GUARD(c);
    catalog_ensure_index_table(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "SELECT col_name, col_idx, created_at FROM table_indexes"
        " WHERE table_name=? ORDER BY col_idx", -1, &st, NULL);
    sqlite3_bind_text(st,1,table,-1,SQLITE_STATIC);
    JBuf jb; jb_init(&jb,a,512); jb_arr_begin(&jb);
    while (sqlite3_step(st)==SQLITE_ROW) {
        jb_obj_begin(&jb);
        jb_key(&jb,"column");  jb_str(&jb,(const char*)sqlite3_column_text(st,0));
        jb_key(&jb,"col_idx"); jb_int(&jb,sqlite3_column_int(st,1));
        jb_key(&jb,"type");    jb_str(&jb,"btree");
        jb_key(&jb,"created_at"); jb_int(&jb,sqlite3_column_int64(st,2));
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb); sqlite3_finalize(st);
    *json_out = (char*)jb_done(&jb);
    return 0;
}

int catalog_drop_indexes(Catalog *c, const char *table) {
    CAT_GUARD(c);
    catalog_ensure_index_table(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "DELETE FROM table_indexes WHERE table_name=?", -1, &st, NULL);
    sqlite3_bind_text(st,1,table,-1,SQLITE_STATIC);
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}

/* Проверить наличие индекса по столбцу; при наличии вернуть col_idx через out. */
int catalog_has_index(Catalog *c, const char *table, const char *col,
                      int *col_idx_out) {
    CAT_GUARD(c);
    catalog_ensure_index_table(c);
    sqlite3_stmt *st;
    cat_prep(c->db,
        "SELECT col_idx FROM table_indexes WHERE table_name=? AND col_name=?",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,table,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,col,  -1,SQLITE_STATIC);
    int found=0;
    if (sqlite3_step(st)==SQLITE_ROW) {
        if (col_idx_out) *col_idx_out = sqlite3_column_int(st,0);
        found = 1;
    }
    sqlite3_finalize(st);
    return found;
}
