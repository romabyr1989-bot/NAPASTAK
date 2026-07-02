/*
 * matview.c — хранилище материализованных представлений (витрин/datamart).
 *
 * Определения витрин и история их обновлений хранятся в том же SQLite-файле
 * catalog.db (таблицы matviews и matview_runs). Сам результат витрины
 * материализуется в одноимённую таблицу через колбэк exec_fn (см. api.c),
 * поэтому витрина адресуется по имени в любых запросах/пайплайнах.
 *
 * Режимы обновления (MvRefreshMode): MANUAL, ON_WRITE (при записи в источник),
 * SCHEDULE (по интервалу). ON_WRITE и SCHEDULE обрабатываются в mvs_tick.
 * Доступ к БД сериализуется мьютексом mu; очередь on_write — отдельным pending_mu.
 */
#include "matview.h"
#include "../core/log.h"
#include "../core/arena.h"
#include <sqlite3.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <pthread.h>


/* DDL схемы: определения витрин (matviews) + журнал обновлений (matview_runs). */
static const char *MV_SCHEMA =
    "CREATE TABLE IF NOT EXISTS matviews ("
    "  name           TEXT PRIMARY KEY,"
    "  definition_sql TEXT NOT NULL,"
    "  source_tables  TEXT NOT NULL,"
    "  refresh_mode   INTEGER NOT NULL DEFAULT 0,"
    "  refresh_cron   TEXT DEFAULT '',"
    "  last_refreshed INTEGER DEFAULT 0,"
    "  is_stale       INTEGER DEFAULT 1,"
    "  row_count      INTEGER DEFAULT 0,"
    "  refresh_duration_ms INTEGER DEFAULT 0,"
    "  last_error     TEXT DEFAULT ''"
    ");"
    /* History of refreshes — one row per refresh (manual, scheduled, on-create),
     * so the metrics modal can show multiple rows. Removed with the datamart. */
    "CREATE TABLE IF NOT EXISTS matview_runs ("
    "  id          INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  mv_name     TEXT NOT NULL,"
    "  started_at  INTEGER,"
    "  finished_at INTEGER,"
    "  status      INTEGER,"           /* 0 = ok, 1 = error */
    "  row_count   INTEGER,"
    "  duration_ms INTEGER,"
    "  error_msg   TEXT DEFAULT ''"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_mvruns_name ON matview_runs(mv_name);";

struct MatViewStore {
    sqlite3        *db;
    pthread_mutex_t mu;
    char            data_dir[512];
    Catalog        *catalog;

    /* Callback для выполнения SQL (устанавливается из api.c) */
    MvExecFn        exec_fn;
    void           *exec_app;

    /* Очередь on_write refresh */
    char            pending_refresh[64][128];
    int             npending;
    pthread_mutex_t pending_mu;
};

/* Создать хранилище: открыть catalog.db, применить схему/миграции, инициализировать мьютексы. */
MatViewStore *mvs_create(Catalog *catalog, const char *data_dir) {
    MatViewStore *s = calloc(1, sizeof(MatViewStore));
    s->catalog = catalog;
    strncpy(s->data_dir, data_dir, sizeof(s->data_dir)-1);

    /* Открываем тот же catalog.db */
    char db_path[640];
    snprintf(db_path, sizeof(db_path), "%s/catalog.db", data_dir);
    if (sqlite3_open(db_path, &s->db) != SQLITE_OK) {
        LOG_ERROR("matview: failed to open db: %s", sqlite3_errmsg(s->db));
        free(s);
        return NULL;
    }
    sqlite3_exec(s->db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (sqlite3_exec(s->db, MV_SCHEMA, NULL, NULL, NULL) != SQLITE_OK) {
        LOG_ERROR("matview: schema init: %s", sqlite3_errmsg(s->db));
        sqlite3_close(s->db);
        free(s);
        return NULL;
    }
    /* Migrate older catalog.db that predates these columns (errors = already there). */
    sqlite3_exec(s->db, "ALTER TABLE matviews ADD COLUMN refresh_duration_ms INTEGER DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(s->db, "ALTER TABLE matviews ADD COLUMN last_error TEXT DEFAULT '';", NULL, NULL, NULL);
    pthread_mutex_init(&s->mu, NULL);
    pthread_mutex_init(&s->pending_mu, NULL);
    LOG_INFO("matview store initialized");
    return s;
}

/* Освободить хранилище: разрушить мьютексы, закрыть БД. */
void mvs_destroy(MatViewStore *s) {
    if (!s) return;
    pthread_mutex_destroy(&s->mu);
    pthread_mutex_destroy(&s->pending_mu);
    sqlite3_close(s->db);
    free(s);
}

/* Парсить JSON-массив source_tables */
static void parse_source_tables(const char *json, MatView *mv) {
    mv->nsource_tables = 0;
    if (!json || !*json) return;
    const char *p = json;
    while (*p && mv->nsource_tables < 16) {
        const char *q = strchr(p, '"');
        if (!q) break;
        q++;
        const char *e = strchr(q, '"');
        if (!e) break;
        size_t len = (size_t)(e - q);
        if (len >= 128) len = 127;
        memcpy(mv->source_tables[mv->nsource_tables], q, len);
        mv->source_tables[mv->nsource_tables][len] = '\0';
        mv->nsource_tables++;
        p = e + 1;
    }
}

/* Заполнить MatView из текущей строки выборки (порядок колонок = SELECT в mvs_get/mvs_list). */
static int load_mv(sqlite3_stmt *stmt, MatView *mv) {
    memset(mv, 0, sizeof(*mv));
    const char *name = (const char *)sqlite3_column_text(stmt, 0);
    const char *sql  = (const char *)sqlite3_column_text(stmt, 1);
    const char *srcs = (const char *)sqlite3_column_text(stmt, 2);
    if (name) strncpy(mv->name,           name, sizeof(mv->name)-1);
    if (sql)  strncpy(mv->definition_sql, sql,  sizeof(mv->definition_sql)-1);
    mv->refresh_mode    = (MvRefreshMode)sqlite3_column_int(stmt, 3);
    const char *cron = (const char *)sqlite3_column_text(stmt, 4);
    if (cron) strncpy(mv->refresh_cron, cron, sizeof(mv->refresh_cron)-1);
    mv->last_refreshed_at  = sqlite3_column_int64(stmt, 5);
    mv->is_stale           = sqlite3_column_int(stmt, 6) != 0;
    mv->row_count          = sqlite3_column_int64(stmt, 7);
    if (sqlite3_column_count(stmt) > 8)
        mv->refresh_duration_ms = sqlite3_column_int64(stmt, 8);
    if (sqlite3_column_count(stmt) > 9) {
        const char *err = (const char *)sqlite3_column_text(stmt, 9);
        if (err) strncpy(mv->last_error, err, sizeof(mv->last_error)-1);
    }
    parse_source_tables(srcs, mv);
    return 0;
}

/* Создать/обновить определение витрины (UPSERT по имени). Помечает её как stale. */
int mvs_create_view(MatViewStore *s, const MatView *mv) {
    if (!s || !mv || !mv->name[0] || !mv->definition_sql[0]) return -1;

    /* Построить JSON-массив source_tables */
    char srcs_json[2048]; int soff = 0;
    soff += snprintf(srcs_json, sizeof(srcs_json), "[");
    for (int i = 0; i < mv->nsource_tables; i++) {
        soff += snprintf(srcs_json+soff, sizeof(srcs_json)-(size_t)soff,
                         "%s\"%s\"", i?",":"", mv->source_tables[i]);
    }
    snprintf(srcs_json+soff, sizeof(srcs_json)-(size_t)soff, "]");

    pthread_mutex_lock(&s->mu);
    const char *sql =
        "INSERT INTO matviews (name,definition_sql,source_tables,refresh_mode,refresh_cron,is_stale) "
        "VALUES (?,?,?,?,?,1) "
        "ON CONFLICT(name) DO UPDATE SET "
        "  definition_sql=excluded.definition_sql,"
        "  source_tables=excluded.source_tables,"
        "  refresh_mode=excluded.refresh_mode,"
        "  refresh_cron=excluded.refresh_cron,"
        "  is_stale=1;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu); return -1;
    }
    sqlite3_bind_text(stmt, 1, mv->name, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, mv->definition_sql, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, srcs_json, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt,  4, (int)mv->refresh_mode);
    sqlite3_bind_text(stmt, 5, mv->refresh_cron, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);

    LOG_INFO("matview: created '%s' (mode=%d)", mv->name, (int)mv->refresh_mode);
    return rc == SQLITE_DONE ? 0 : -1;
}

/* Удалить определение витрины и её историю обновлений (data-таблицу удаляет вызывающий). */
int mvs_drop_view(MatViewStore *s, const char *name) {
    if (!s || !name) return -1;
    pthread_mutex_lock(&s->mu);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(s->db, "DELETE FROM matviews WHERE name=?;", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    /* Drop the refresh history along with the datamart. */
    sqlite3_stmt *hd;
    if (sqlite3_prepare_v2(s->db, "DELETE FROM matview_runs WHERE mv_name=?;", -1, &hd, NULL) == SQLITE_OK) {
        sqlite3_bind_text(hd, 1, name, -1, SQLITE_STATIC);
        sqlite3_step(hd);
        sqlite3_finalize(hd);
    }
    pthread_mutex_unlock(&s->mu);

    /* The data table is dropped by the caller (h_matview_drop) so it can first
     * check no pipeline / other datamart still uses it. */
    LOG_INFO("matview: dropped definition '%s'", name);
    return rc == SQLITE_DONE ? 0 : -1;
}

static void json_esc(const char *in, char *out, size_t outsz);   /* defined below */

/* Refresh history of one datamart, newest first → JSON array. */
int mvs_list_refreshes(MatViewStore *s, const char *name, char **json_out, Arena *a) {
    if (!s || !name) return -1;
    pthread_mutex_lock(&s->mu);
    sqlite3_stmt *st;
    if (sqlite3_prepare_v2(s->db,
            "SELECT id,started_at,finished_at,status,row_count,duration_ms,error_msg "
            "FROM matview_runs WHERE mv_name=? ORDER BY started_at DESC LIMIT 50;",
            -1, &st, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu); return -1;
    }
    sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
    size_t cap = 65536;   /* буфер под JSON в арене (до 50 строк истории) */
    char *buf = arena_alloc(a, cap);
    int off = snprintf(buf, cap, "[");
    int first = 1;
    char eerr[1040];
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *err = (const char *)sqlite3_column_text(st, 6);
        json_esc(err ? err : "", eerr, sizeof(eerr));
        off += snprintf(buf+off, cap-(size_t)off,
            "%s{\"id\":%lld,\"started\":%lld,\"finished\":%lld,\"status\":%d,"
            "\"row_count\":%lld,\"duration_ms\":%lld,\"error\":\"%s\"}",
            first ? "" : ",",
            (long long)sqlite3_column_int64(st, 0),
            (long long)sqlite3_column_int64(st, 1),
            (long long)sqlite3_column_int64(st, 2),
            sqlite3_column_int(st, 3),
            (long long)sqlite3_column_int64(st, 4),
            (long long)sqlite3_column_int64(st, 5),
            eerr);
        first = 0;
    }
    snprintf(buf+off, cap-(size_t)off, "]");
    sqlite3_finalize(st);
    pthread_mutex_unlock(&s->mu);
    *json_out = buf;
    return 0;
}

/* Whole-identifier occurrence of `tbl` in `hay` (so "orders" ≠ "orders_x"). */
static bool mv_sql_refs(const char *hay, const char *tbl) {
    if (!hay || !tbl || !tbl[0]) return false;
    size_t tl = strlen(tbl);
    for (const char *p = strstr(hay, tbl); p; p = strstr(p + tl, tbl)) {
        char b = (p == hay) ? ' ' : p[-1];
        char a = p[tl];
        #define MV_IDENT_CH(c) (((c)>='A'&&(c)<='Z')||((c)>='a'&&(c)<='z')||((c)>='0'&&(c)<='9')||(c)=='_')
        if (!MV_IDENT_CH(b) && !MV_IDENT_CH(a)) return true;
        #undef MV_IDENT_CH
    }
    return false;
}

/* Используется ли `table` в SQL какой-либо витрины (кроме exclude_name)? См. mvs_table_used в .h. */
bool mvs_table_used(MatViewStore *s, const char *table, const char *exclude_name) {
    if (!s || !table || !table[0]) return false;
    bool used = false;
    pthread_mutex_lock(&s->mu);
    sqlite3_stmt *st;
    if (sqlite3_prepare_v2(s->db, "SELECT name,definition_sql FROM matviews;", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *nm  = (const char *)sqlite3_column_text(st, 0);
            const char *sql = (const char *)sqlite3_column_text(st, 1);
            if (exclude_name && nm && !strcmp(nm, exclude_name)) continue;
            if (mv_sql_refs(sql, table)) { used = true; break; }
        }
        sqlite3_finalize(st);
    }
    pthread_mutex_unlock(&s->mu);
    return used;
}

/* Прочитать одну витрину по имени в `out`. Возврат 0 если найдена, иначе -1. */
int mvs_get(MatViewStore *s, const char *name, MatView *out) {
    if (!s || !name) return -1;
    pthread_mutex_lock(&s->mu);
    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT name,definition_sql,source_tables,refresh_mode,refresh_cron,"
        "last_refreshed,is_stale,row_count,refresh_duration_ms,last_error FROM matviews WHERE name=?;";
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu); return -1;
    }
    sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    int found = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        load_mv(stmt, out);
        found = 1;
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);
    return found ? 0 : -1;
}

/* Minimal JSON string escaper (quotes, backslash, newlines, control chars). */
static void json_esc(const char *in, char *out, size_t outsz) {
    size_t o = 0;
    for (const char *p = in ? in : ""; *p && o + 8 < outsz; p++) {
        unsigned char c = (unsigned char)*p;
        switch (c) {
            case '"':  out[o++]='\\'; out[o++]='"';  break;
            case '\\': out[o++]='\\'; out[o++]='\\'; break;
            case '\n': out[o++]='\\'; out[o++]='n';  break;
            case '\r': out[o++]='\\'; out[o++]='r';  break;
            case '\t': out[o++]='\\'; out[o++]='t';  break;
            default:
                if (c < 0x20) o += (size_t)snprintf(out+o, outsz-o, "\\u%04x", c);
                else out[o++] = (char)c;
        }
    }
    out[o] = '\0';
}

/* Все витрины как JSON-массив (для списка в UI). Буфер выделяется в арене. */
int mvs_list(MatViewStore *s, char **json_out, Arena *a) {
    if (!s) return -1;
    pthread_mutex_lock(&s->mu);
    const char *sql =
        "SELECT name,definition_sql,source_tables,refresh_mode,refresh_cron,"
        "last_refreshed,is_stale,row_count,refresh_duration_ms,last_error FROM matviews ORDER BY name;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu); return -1;
    }
    size_t cap = 262144;
    char *buf = arena_alloc(a, cap);
    int off = snprintf(buf, cap, "[");
    int first = 1;
    char esql[8200], eerr[1040];
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        MatView mv; load_mv(stmt, &mv);
        json_esc(mv.definition_sql, esql, sizeof(esql));
        json_esc(mv.last_error,     eerr, sizeof(eerr));
        if (!first) off += snprintf(buf+off, cap-(size_t)off, ",");
        off += snprintf(buf+off, cap-(size_t)off,
            "{\"name\":\"%s\",\"refresh_mode\":%d,\"is_stale\":%s,"
            "\"last_refreshed_at\":%lld,\"row_count\":%lld,"
            "\"refresh_duration_ms\":%lld,\"refresh_cron\":\"%s\","
            "\"definition_sql\":\"%s\",\"last_error\":\"%s\",\"source_tables\":[",
            mv.name, (int)mv.refresh_mode, mv.is_stale?"true":"false",
            (long long)mv.last_refreshed_at, (long long)mv.row_count,
            (long long)mv.refresh_duration_ms, mv.refresh_cron, esql, eerr);
        for (int i = 0; i < mv.nsource_tables; i++)
            off += snprintf(buf+off, cap-(size_t)off, "%s\"%s\"", i?",":"", mv.source_tables[i]);
        off += snprintf(buf+off, cap-(size_t)off, "]}");
        first = 0;
    }
    snprintf(buf+off, cap-(size_t)off, "]");
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);
    *json_out = buf;
    return 0;
}

/* Зарегистрировать exec callback из api.c */
void mvs_set_exec_fn(MatViewStore *s, MvExecFn fn, void *app) {
    s->exec_fn = fn;
    s->exec_app = app;
}

/* Обновить (материализовать) витрину: выполнить её SQL в data-таблицу, зафиксировать
 * итог (stale/ошибка/метрики) в matviews и дописать строку в matview_runs. */
int mvs_refresh(MatViewStore *s, const char *name, void *app) {
    (void)app;
    if (!s || !name) return -1;
    MatView mv;
    if (mvs_get(s, name, &mv) != 0) return -1;
    if (mv.is_refreshing) return 0;

    /* Target table = the view's own name → queryable by name everywhere. */
    char storage_name[160];
    mvs_storage_name(name, storage_name, sizeof(storage_name));

    struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
    int64_t started_at = (int64_t)time(NULL);

    int rows = -1;
    const char *errmsg = NULL;
    if (s->exec_fn) {
        /* Materialise: the callback runs the definition and writes the result
         * into `storage_name`, returning the row count (or -1 on error). */
        rows = s->exec_fn(mv.definition_sql, storage_name, s->exec_app);
        if (rows < 0) errmsg = "ошибка выполнения SQL витрины (проверьте запрос и источники)";
    } else {
        errmsg = "движок SQL не подключён (mvs_set_exec_fn не вызван)";
    }

    struct timespec t1; clock_gettime(CLOCK_MONOTONIC, &t1);
    int64_t dur_ms = (t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000;

    /* Persist outcome: success clears stale + error; failure keeps stale + reason. */
    pthread_mutex_lock(&s->mu);
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db,
            "UPDATE matviews SET is_stale=?,last_refreshed=?,row_count=?,"
            "refresh_duration_ms=?,last_error=? WHERE name=?;", -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int  (stmt, 1, errmsg ? 1 : 0);
        sqlite3_bind_int64(stmt, 2, (int64_t)time(NULL));
        sqlite3_bind_int64(stmt, 3, rows > 0 ? rows : 0);
        sqlite3_bind_int64(stmt, 4, dur_ms);
        sqlite3_bind_text (stmt, 5, errmsg ? errmsg : "", -1, SQLITE_STATIC);
        sqlite3_bind_text (stmt, 6, name, -1, SQLITE_STATIC);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    /* Append a history row — accumulates metrics for the datamart's modal. */
    sqlite3_stmt *hist;
    if (sqlite3_prepare_v2(s->db,
            "INSERT INTO matview_runs(mv_name,started_at,finished_at,status,row_count,duration_ms,error_msg)"
            " VALUES(?,?,?,?,?,?,?);", -1, &hist, NULL) == SQLITE_OK) {
        sqlite3_bind_text (hist, 1, name, -1, SQLITE_STATIC);
        sqlite3_bind_int64(hist, 2, started_at);
        sqlite3_bind_int64(hist, 3, (int64_t)time(NULL));
        sqlite3_bind_int  (hist, 4, errmsg ? 1 : 0);
        sqlite3_bind_int64(hist, 5, rows > 0 ? rows : 0);
        sqlite3_bind_int64(hist, 6, dur_ms);
        sqlite3_bind_text (hist, 7, errmsg ? errmsg : "", -1, SQLITE_STATIC);
        sqlite3_step(hist);
        sqlite3_finalize(hist);
    }
    pthread_mutex_unlock(&s->mu);

    if (errmsg) { LOG_ERROR("matview: refresh '%s' failed: %s (%lldms)", name, errmsg, (long long)dur_ms); return -1; }
    LOG_INFO("matview: refreshed '%s' → %d rows (%lldms)", name, rows, (long long)dur_ms);
    return 0;
}

/* Запись в table_name произошла: пометить зависящие витрины как stale; те из них,
 * что в режиме ON_WRITE, поставить в очередь pending для обновления в mvs_tick. */
void mvs_invalidate(MatViewStore *s, const char *table_name) {
    if (!s || !table_name) return;

    pthread_mutex_lock(&s->mu);
    /* Находим view, использующие эту таблицу */
    const char *sql = "SELECT name,source_tables FROM matviews WHERE is_stale=0;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu); return;
    }
    char to_invalidate[64][128]; int ninv = 0;
    char to_refresh[64][128];    int nref = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && ninv < 64) {
        const char *name = (const char *)sqlite3_column_text(stmt, 0);
        const char *srcs = (const char *)sqlite3_column_text(stmt, 1);
        if (!name || !srcs) continue;
        /* Совпадение по подстроке в JSON-массиве source_tables (не по целому
         * идентификатору, как в mv_sql_refs): "orders" зацепит и "orders_x". */
        if (strstr(srcs, table_name)) {
            strncpy(to_invalidate[ninv], name, 127);
            ninv++;
        }
    }
    sqlite3_finalize(stmt);

    /* Помечаем все найденные view как stale */
    for (int i = 0; i < ninv; i++) {
        sqlite3_stmt *upd;
        sqlite3_prepare_v2(s->db,
            "UPDATE matviews SET is_stale=1 WHERE name=?;", -1, &upd, NULL);
        sqlite3_bind_text(upd, 1, to_invalidate[i], -1, SQLITE_STATIC);
        sqlite3_step(upd);
        sqlite3_finalize(upd);

        /* Собираем список для ON_WRITE refresh */
        MatView mv;
        char mv_get_sql[256];
        snprintf(mv_get_sql, sizeof(mv_get_sql),
            "SELECT name,definition_sql,source_tables,refresh_mode,refresh_cron,"
            "last_refreshed,is_stale,row_count,refresh_duration_ms,last_error FROM matviews WHERE name='%s';",
            to_invalidate[i]);
        sqlite3_stmt *gs;
        if (sqlite3_prepare_v2(s->db, mv_get_sql, -1, &gs, NULL) == SQLITE_OK) {
            if (sqlite3_step(gs) == SQLITE_ROW) {
                load_mv(gs, &mv);
                if (mv.refresh_mode == MV_REFRESH_ON_WRITE && nref < 64) {
                    strncpy(to_refresh[nref++], mv.name, 127);
                }
            }
            sqlite3_finalize(gs);
        }
    }
    pthread_mutex_unlock(&s->mu);

    /* Ставим в очередь ON_WRITE refresh */
    if (nref > 0) {
        pthread_mutex_lock(&s->pending_mu);
        for (int i = 0; i < nref && s->npending < 64; i++) {
            /* Проверяем дубликаты */
            bool dup = false;
            for (int j = 0; j < s->npending; j++)
                if (!strcmp(s->pending_refresh[j], to_refresh[i])) { dup=true; break; }
            if (!dup) strncpy(s->pending_refresh[s->npending++], to_refresh[i], 127);
        }
        pthread_mutex_unlock(&s->pending_mu);
    }

    if (ninv > 0)
        LOG_INFO("matview: invalidated %d views due to write on '%s'", ninv, table_name);
}

/* Периодический тик планировщика: обновить накопленную ON_WRITE-очередь и
 * витрины SCHEDULE, у которых истёк интервал (refresh_cron трактуется как секунды). */
void mvs_tick(MatViewStore *s, void *app) {
    if (!s) return;

    /* Обрабатываем ON_WRITE pending queue */
    pthread_mutex_lock(&s->pending_mu);
    char names[64][128]; int n = s->npending;
    memcpy(names, s->pending_refresh, (size_t)n * sizeof(names[0]));
    s->npending = 0;
    pthread_mutex_unlock(&s->pending_mu);

    for (int i = 0; i < n; i++)
        mvs_refresh(s, names[i], app);

    /* Cron: простая проверка — если last_refreshed + period < now */
    /* Полный cron-парсер выходит за рамки; поддерживаем только refresh_cron как секунды */
    pthread_mutex_lock(&s->mu);
    const char *sql =
        "SELECT name,refresh_cron,last_refreshed FROM matviews WHERE refresh_mode=2 AND is_stale=0;";
    sqlite3_stmt *stmt;
    char sched_names[64][128]; int nsched = 0;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        int64_t now = (int64_t)time(NULL);
        while (sqlite3_step(stmt) == SQLITE_ROW && nsched < 64) {
            const char *nm   = (const char *)sqlite3_column_text(stmt, 0);
            const char *cron = (const char *)sqlite3_column_text(stmt, 1);
            int64_t     last = sqlite3_column_int64(stmt, 2);
            if (!nm || !cron) continue;
            int period = atoi(cron); /* cron как интервал в секундах */
            if (period > 0 && now - last >= (int64_t)period)
                strncpy(sched_names[nsched++], nm, 127);
        }
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&s->mu);

    for (int i = 0; i < nsched; i++)
        mvs_refresh(s, sched_names[i], app);
}
