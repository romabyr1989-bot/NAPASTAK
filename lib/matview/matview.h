#pragma once
/*
 * matview.h — хранилище материализованных представлений (datamart'ов).
 * Datamart описывается SQL-определением, материализуется в одноимённую таблицу
 * движка и может обновляться вручную, по записи в источник или по расписанию.
 */
#include "../storage/storage.h"
#include "../core/arena.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>

/* Режим обновления datamart'а: вручную, при записи в источник, по cron-расписанию */
typedef enum {
    MV_REFRESH_MANUAL   = 0,
    MV_REFRESH_ON_WRITE = 1,
    MV_REFRESH_SCHEDULE = 2,
} MvRefreshMode;

/* Описание одного datamart'а: определение, источники и текущее состояние обновления */
typedef struct {
    char          name[128];
    char          definition_sql[4096];
    char          source_tables[16][128];
    int           nsource_tables;
    MvRefreshMode refresh_mode;
    char          refresh_cron[64];
    int64_t       last_refreshed_at;
    int64_t       refresh_duration_ms;
    bool          is_stale;
    int64_t       row_count;
    bool          is_refreshing;
    char          last_error[512];   /* reason of the last failed refresh ("" = ok) */
} MatView;

/* Непрозрачный дескриптор хранилища всех datamart'ов (реализация в .c) */
typedef struct MatViewStore MatViewStore;

/* App forward declaration — избегаем циклических include */
struct App;

/* Создать/уничтожить хранилище datamart'ов (метаданные хранятся в data_dir) */
MatViewStore *mvs_create(Catalog *catalog, const char *data_dir);
void          mvs_destroy(MatViewStore *s);

/* CRUD по определениям datamart'ов; mvs_list отдаёт их как JSON в арене a */
int  mvs_create_view(MatViewStore *s, const MatView *mv);
int  mvs_drop_view(MatViewStore *s, const char *name);
int  mvs_get(MatViewStore *s, const char *name, MatView *out);
int  mvs_list(MatViewStore *s, char **json_out, Arena *a);
/* Принудительно пересчитать datamart `name` (синхронно, через зарегистрированный exec-fn) */
int  mvs_refresh(MatViewStore *s, const char *name, void *app);

/* Refresh history of one datamart (newest first) as a JSON array — for the
 * per-datamart metrics modal. History is deleted together with the datamart. */
int  mvs_list_refreshes(MatViewStore *s, const char *name, char **json_out, Arena *a);

/* True if any datamart except `exclude_name` reads `table` in its definition SQL.
 * Used so a shared engine table isn't dropped while a datamart still needs it. */
bool mvs_table_used(MatViewStore *s, const char *table, const char *exclude_name);

/* Отметить все view, использующие table_name, как устаревшие */
void mvs_invalidate(MatViewStore *s, const char *table_name);

/* Вызывается планировщиком: обрабатывает schedule + on_write refresh */
void mvs_tick(MatViewStore *s, void *app);

/* Materialise `def_sql` into the table `target_table` (= the view's own name, so
 * it is queryable by name). Returns the number of rows written, or -1 on error.
 * Registered from api.c, where the SQL engine + table writer live. */
typedef int (*MvExecFn)(const char *def_sql, const char *target_table, void *app);
void mvs_set_exec_fn(MatViewStore *s, MvExecFn fn, void *app);

/* Table that holds the datamart's data — its own name (a datamart IS a table,
 * referenceable by name in other queries / pipelines). */
static inline void mvs_storage_name(const char *name, char *out, size_t len) {
    snprintf(out, len, "%s", name);
}
