#pragma once
/* storage.h — публичный интерфейс слоя хранения NAPASTAK:
 * колоночные батчи (ColBatch), журнал упреждающей записи (WAL),
 * таблицы с B-tree индексами и каталог метаданных на SQLite
 * (схемы таблиц, пайплайны, история запусков, сохранённые результаты). */
#include "../core/arena.h"
#include "../core/json.h"
#include "../index/btree.h"
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/* ── NULL sentinel (shared NULL contract across NAPASTAK) ──
 * A textual cell equal to this exact byte string represents a SQL NULL that
 * has been carried through the row/string layer (sources, materialization,
 * sinks, query engine, JSON output). The leading \x01\x02 bytes make it
 * collision-proof with real textual data and keep it distinct from the WAL
 * op bytes (DELETE=0x02 / UPDATE=0x03) and the compressed-record marker
 * (0x01 followed by a serialized batch, never by 0x02). The storage layer
 * treats it as ordinary opaque text — no special trimming or NULL forcing. */
#define DFO_NULL_SENTINEL "\x01\x02__DFO_NULL__"

/* ── Column types ── */
typedef enum { COL_INT64=0, COL_DOUBLE, COL_TEXT, COL_BOOL, COL_NULL } ColType;

/* Описание одной колонки: имя, тип и допустимость NULL. */
typedef struct {
    const char *name;
    ColType     type;
    bool        nullable;
} ColDef;

/* Схема таблицы — упорядоченный массив колонок. */
typedef struct {
    ColDef *cols;
    int     ncols;
} Schema;

/* ── Columnar batch (8192 rows per batch) ── */
#define BATCH_SIZE 8192
#define MAX_COLS   2048

/* Колоночный батч: до BATCH_SIZE строк, данные хранятся по колонкам
 * (column-major) для эффективного сканирования и векторизации. */
typedef struct {
    Schema     *schema;
    int         ncols;
    int         nrows;
    /* per column */
    uint8_t    *null_bitmap[MAX_COLS]; /* bit per row */
    void       *values[MAX_COLS];      /* int64_t*, double*, char** */
} ColBatch;

/* ── WAL op types (first byte of record data) ── */
/* Old INSERT records have no op byte (first byte is printable CSV char >= 0x20). */
#define WAL_OP_DELETE 0x02   /* [0x02][8-byte BE offset of deleted row] */
#define WAL_OP_UPDATE 0x03   /* [0x03][8-byte BE offset][new CSV row\n] */

/* ── WAL write callback (for replication) ── */
typedef void (*WalWriteCallback)(const char *table_name, uint64_t lsn,
                                  const void *data, size_t len, void *userdata);

/* ── WAL ── */
/* Журнал упреждающей записи: append-only лог изменений таблицы.
 * INSERT пишется как CSV-строки; DELETE/UPDATE — записи с op-байтом. */
typedef struct WAL WAL;
WAL     *wal_open  (const char *path);
int      wal_append(WAL *w, const void *data, size_t len);
int      wal_sync  (WAL *w);
int64_t  wal_tell  (WAL *w);  /* current write position (before next append) */
void     wal_close (WAL *w);
int      wal_append_delete(WAL *w, int64_t orig_offset);
int      wal_append_update(WAL *w, int64_t orig_offset, const char *new_csv, size_t csv_len);

/* ── Forward declarations ── */
typedef struct Catalog Catalog;

/* ── Table ── */
/* Таблица: WAL-хранилище + опциональные индексы. scan читает данные в батчи,
 * compact схлопывает delete/update-записи в новый компактный файл. */
typedef struct Table Table;
Table  *table_create(const char *name, Schema *schema, const char *dir);
Table  *table_open  (const char *name, const char *dir);
void    table_set_wal_callback(Table *t, WalWriteCallback cb, void *userdata);
int     table_wal_append(Table *t, const void *data, size_t len);
int     table_append(Table *t, ColBatch *batch);
int     table_scan  (Table *t, ColBatch **out, Arena *a);
int     table_delete(Table *t, int64_t orig_offset);
int     table_update(Table *t, int64_t orig_offset, const char *new_csv, size_t csv_len);
int     table_compact(Table *t, Arena *a);
int64_t table_row_count(Table *t);
Schema *table_schema   (Table *t);
void    table_close    (Table *t);

/* ── Index management ── */
/* Build a B-tree index on col_idx (COL_INT64 only) and register it in catalog.
 * Scans existing WAL data, then future appends update it automatically.
 * Returns 0 on success, -1 on error.                                       */
int     table_create_index(Table *t, int col_idx, Catalog *c);

/* Return open BTree* for col_idx, or NULL if not indexed. */
BTree  *table_get_index   (Table *t, int col_idx);

/* ── Catalog (SQLite-backed) ── */
/* Каталог хранит метаданные системы в SQLite-БД: схемы таблиц,
 * определения пайплайнов, историю запусков, сохранённые результаты и индексы. */
Catalog *catalog_open(const char *db_path);
void     catalog_close(Catalog *c);

int catalog_register_table(Catalog *c, const char *name, Schema *schema);
int catalog_update_table_meta(Catalog *c, const char *name, const char *source, int64_t row_count);
int catalog_list_tables(Catalog *c, char ***names_out, int *count_out, Arena *a);
int catalog_list_tables_full(Catalog *c, char **json_out, Arena *a);
int catalog_get_schema(Catalog *c, const char *table, Schema **out, Arena *a);
int catalog_drop_table(Catalog *c, const char *name);

/* Pipeline metadata */
int catalog_save_pipeline(Catalog *c, const char *id, const char *json);
int catalog_load_pipeline(Catalog *c, const char *id, char **json_out, Arena *a);
/* updated_at (unix seconds) записи пайплайна в каталоге, либо -1 если записи нет.
 * Нужно, чтобы GitOps-загрузка YAML не затирала более свежую правку из UI. */
int64_t catalog_pipeline_updated_at(Catalog *c, const char *id);
int catalog_list_pipelines(Catalog *c, char ***ids_out, int *count_out, Arena *a);
int catalog_delete_pipeline(Catalog *c, const char *id);

/* Run history */
int catalog_log_run(Catalog *c, const char *pipeline_id,
                    int64_t started_at, int64_t finished_at,
                    int status, const char *error_msg, int retry_count);
int catalog_list_runs(Catalog *c, const char *pipeline_id,
                      char **json_out, Arena *a);

/* Saved analytics results */
int catalog_save_result(Catalog *c, const char *name, const char *sql_text,
                        const char *columns_json, const char *rows_json,
                        int row_count, int64_t *out_id);
int catalog_list_results(Catalog *c, char **out, Arena *a);
int catalog_get_result(Catalog *c, int64_t id, char **out, Arena *a);
int catalog_delete_result(Catalog *c, int64_t id);

/* ── Catalog: index registry ── */
int catalog_register_index   (Catalog *c, const char *table,
                               const char *col, int col_idx);
int catalog_list_indexes_json(Catalog *c, const char *table,
                               char **json_out, Arena *a);
int catalog_drop_indexes     (Catalog *c, const char *table);
int catalog_has_index        (Catalog *c, const char *table,
                               const char *col, int *col_idx_out);
