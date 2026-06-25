/*
 * audit.c — подсистема журналирования аудита DataFlow OS.
 *
 * События аудита помещаются в кольцевой буфер без блокировок ввода-вывода,
 * а фоновый поток батчами сбрасывает их в SQLite и (опционально) в JSONL-файл.
 * Такая схема отделяет горячий путь (audit_log_event) от медленных записей на диск.
 */
#include "audit.h"
#include "../core/log.h"
#include "../core/arena.h"
#include <sqlite3.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

/* DDL таблицы аудита и индексов; выполняется один раз при инициализации. */
static const char *AUDIT_SCHEMA =
    "CREATE TABLE IF NOT EXISTS audit_log ("
    "  id             INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  ts             INTEGER NOT NULL,"
    "  event_type     INTEGER NOT NULL,"
    "  user_id        TEXT NOT NULL,"
    "  role           INTEGER NOT NULL,"
    "  resource       TEXT,"
    "  action_detail  TEXT,"
    "  correlation_id TEXT,"
    "  client_ip      TEXT,"
    "  result_code    INTEGER,"
    "  duration_ms    INTEGER"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_audit_ts   ON audit_log(ts);"
    "CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id, ts);";

#define FLUSH_BATCH 100  /* максимум записей, забираемых из кольца за один проход */

/* Персистит одну запись аудита: сначала в SQLite, затем в JSONL-файл.
 * Каждый приёмник защищён собственным мьютексом. */
static void flush_record(AuditLog *l, const AuditRecord *rec) {
    /* Write to SQLite */
    pthread_mutex_lock(&l->db_mu);
    const char *sql =
        "INSERT INTO audit_log "
        "(ts,event_type,user_id,role,resource,action_detail,correlation_id,client_ip,result_code,duration_ms) "
        "VALUES (?,?,?,?,?,?,?,?,?,?);";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(l->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, rec->ts);
        sqlite3_bind_int(stmt,   2, (int)rec->type);
        sqlite3_bind_text(stmt,  3, rec->user_id, -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt,   4, rec->role);
        sqlite3_bind_text(stmt,  5, rec->resource, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt,  6, rec->action_detail, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt,  7, rec->correlation_id, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt,  8, rec->client_ip, -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt,   9, rec->result_code);
        sqlite3_bind_int64(stmt, 10, rec->duration_ms);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&l->db_mu);

    /* Write to JSONL file */
    if (l->log_file) {
        pthread_mutex_lock(&l->file_mu);
        fprintf(l->log_file,
            "{\"ts\":%lld,\"event_type\":%d,\"user_id\":\"%s\","
            "\"role\":%d,\"resource\":\"%s\",\"result_code\":%d,"
            "\"duration_ms\":%lld,\"correlation_id\":\"%s\"}\n",
            (long long)rec->ts, (int)rec->type, rec->user_id,
            rec->role, rec->resource, rec->result_code,
            (long long)rec->duration_ms, rec->correlation_id);
        fflush(l->log_file);
        pthread_mutex_unlock(&l->file_mu);
    }
}

/* Тело фонового потока: пока сервис работает или в кольце есть данные,
 * ждёт появления записей, забирает их батчем под локом и сбрасывает на диск
 * уже вне лока, чтобы не блокировать продюсеров. */
static void *flush_thread_fn(void *arg) {
    AuditLog *l = (AuditLog *)arg;
    while (l->running || l->head != l->tail) {
        pthread_mutex_lock(&l->ring_mu);
        /* Ждём пока не появятся данные или не завершимся */
        while (l->head == l->tail && l->running) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;  /* таймаут 1с — чтобы периодически проверять флаг running */
            pthread_cond_timedwait(&l->ring_cond, &l->ring_mu, &ts);
        }
        /* Забираем батч */
        AuditRecord batch[FLUSH_BATCH];
        int n = 0;
        while (l->head != l->tail && n < FLUSH_BATCH) {
            batch[n++] = l->ring[l->tail % AUDIT_RING_SIZE];
            l->tail++;
        }
        pthread_mutex_unlock(&l->ring_mu);

        /* Сбрасываем без лока */
        for (int i = 0; i < n; i++)
            flush_record(l, &batch[i]);
    }
    return NULL;
}

/* Создаёт и инициализирует журнал аудита: открывает БД (WAL), применяет схему,
 * поднимает мьютексы/условные переменные, опционально открывает JSONL-файл
 * и запускает фоновый поток сброса. Возвращает NULL при ошибке. */
AuditLog *audit_log_create(const char *db_path, const char *log_file_path) {
    AuditLog *l = calloc(1, sizeof(AuditLog));
    if (!l) return NULL;

    if (sqlite3_open(db_path, &l->db) != SQLITE_OK) {
        LOG_ERROR("audit: failed to open db: %s", sqlite3_errmsg(l->db));
        free(l);
        return NULL;
    }
    sqlite3_exec(l->db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (sqlite3_exec(l->db, AUDIT_SCHEMA, NULL, NULL, NULL) != SQLITE_OK) {
        LOG_ERROR("audit: schema init: %s", sqlite3_errmsg(l->db));
        sqlite3_close(l->db);
        free(l);
        return NULL;
    }

    pthread_mutex_init(&l->db_mu, NULL);
    pthread_mutex_init(&l->ring_mu, NULL);
    pthread_mutex_init(&l->file_mu, NULL);
    pthread_cond_init(&l->ring_cond, NULL);

    if (log_file_path && *log_file_path) {
        l->log_file = fopen(log_file_path, "a");
        if (!l->log_file)
            LOG_WARN("audit: can't open log file '%s'", log_file_path);
    }

    l->running = 1;
    pthread_create(&l->flush_thread, NULL, flush_thread_fn, l);
    LOG_INFO("audit log started (db=%s)", db_path);
    return l;
}

/* Корректно останавливает журнал: сигналит потоку завершиться, дожидается
 * слива остатка кольца, закрывает файл/БД и освобождает все ресурсы. */
void audit_log_destroy(AuditLog *l) {
    if (!l) return;
    l->running = 0;
    pthread_mutex_lock(&l->ring_mu);
    pthread_cond_signal(&l->ring_cond);
    pthread_mutex_unlock(&l->ring_mu);
    pthread_join(l->flush_thread, NULL);

    if (l->log_file) fclose(l->log_file);
    pthread_mutex_destroy(&l->db_mu);
    pthread_mutex_destroy(&l->ring_mu);
    pthread_mutex_destroy(&l->file_mu);
    pthread_cond_destroy(&l->ring_cond);
    sqlite3_close(l->db);
    free(l);
}

/* Горячий путь: кладёт событие в кольцевой буфер и будит поток сброса.
 * Не делает дисковых операций. При переполнении буфера событие отбрасывается. */
void audit_log_event(AuditLog *l, const AuditEvent *ev) {
    if (!l || !ev) return;

    pthread_mutex_lock(&l->ring_mu);
    int next_head = (l->head + 1) % (AUDIT_RING_SIZE * 2); /* wrap-around counter */
    /* Заполненность = head - tail; если она достигла размера кольца — мест нет */
    if (next_head - l->tail >= AUDIT_RING_SIZE) {
        /* Буфер полон — теряем запись */
        LOG_WARN("audit: ring buffer full, dropping event type=%d", (int)ev->type);
        pthread_mutex_unlock(&l->ring_mu);
        return;
    }
    AuditRecord *rec = &l->ring[l->head % AUDIT_RING_SIZE];
    memset(rec, 0, sizeof(*rec));
    rec->ts         = (int64_t)time(NULL);
    rec->type       = ev->type;
    rec->role       = (int)ev->role;
    rec->result_code = ev->result_code;
    rec->duration_ms = ev->duration_ms;
    if (ev->user_id)        strncpy(rec->user_id,       ev->user_id,       sizeof(rec->user_id)-1);
    if (ev->resource)       strncpy(rec->resource,      ev->resource,      sizeof(rec->resource)-1);
    if (ev->correlation_id) strncpy(rec->correlation_id,ev->correlation_id,sizeof(rec->correlation_id)-1);
    if (ev->client_ip)      strncpy(rec->client_ip,     ev->client_ip,     sizeof(rec->client_ip)-1);
    if (ev->action_detail) {
        strncpy(rec->action_detail, ev->action_detail, sizeof(rec->action_detail)-1);
    }
    l->head++;
    pthread_cond_signal(&l->ring_cond);
    pthread_mutex_unlock(&l->ring_mu);
}

/* Выбирает записи аудита по фильтрам (user_id, временной диапазон, лимит)
 * и сериализует их в JSON-массив в арене. Результат через json_out. */
int audit_log_query(AuditLog *l, const char *user_id_filter,
                    int64_t from_ts, int64_t to_ts,
                    int limit, char **json_out, Arena *a) {
    if (!l) return -1;
    if (limit <= 0 || limit > 10000) limit = 100;  /* нормализуем лимит к безопасному значению */

    pthread_mutex_lock(&l->db_mu);

    /* Build query dynamically */
    char sql[1024];
    int off = snprintf(sql, sizeof(sql),
        "SELECT ts,event_type,user_id,role,resource,action_detail,"
        "correlation_id,client_ip,result_code,duration_ms "
        "FROM audit_log WHERE 1=1");
    if (from_ts > 0) off += snprintf(sql+off, sizeof(sql)-(size_t)off, " AND ts >= %lld", (long long)from_ts);
    if (to_ts > 0)   off += snprintf(sql+off, sizeof(sql)-(size_t)off, " AND ts <= %lld", (long long)to_ts);
    if (user_id_filter && *user_id_filter)
        off += snprintf(sql+off, sizeof(sql)-(size_t)off,
                        " AND user_id = '%s'", user_id_filter); /* user_id_filter is from admin API, validated above */
    off += snprintf(sql+off, sizeof(sql)-(size_t)off, " ORDER BY ts DESC LIMIT %d;", limit);

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(l->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&l->db_mu);
        return -1;
    }

    char *buf = arena_alloc(a, 131072);
    int boff = snprintf(buf, 131072, "[");
    int first = 1;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t ts     = sqlite3_column_int64(stmt, 0);
        int     etype  = sqlite3_column_int(stmt, 1);
        const char *uid = (const char *)sqlite3_column_text(stmt, 2);
        int     role   = sqlite3_column_int(stmt, 3);
        const char *res= (const char *)sqlite3_column_text(stmt, 4);
        const char *det= (const char *)sqlite3_column_text(stmt, 5);
        const char *cid= (const char *)sqlite3_column_text(stmt, 6);
        const char *ip = (const char *)sqlite3_column_text(stmt, 7);
        int     rc_val = sqlite3_column_int(stmt, 8);
        int64_t dur    = sqlite3_column_int64(stmt, 9);

        if (!first) boff += snprintf(buf+boff, 131072-(size_t)boff, ",");
        boff += snprintf(buf+boff, 131072-(size_t)boff,
            "{\"ts\":%lld,\"event_type\":%d,\"user_id\":\"%s\","
            "\"role\":%d,\"resource\":\"%s\",\"result_code\":%d,"
            "\"duration_ms\":%lld,\"correlation_id\":\"%s\",\"client_ip\":\"%s\"}",
            (long long)ts, etype, uid?uid:"", role, res?res:"",
            rc_val, (long long)dur, cid?cid:"", ip?ip:"");
        first = 0;
    }
    snprintf(buf+boff, 131072-(size_t)boff, "]");
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&l->db_mu);
    *json_out = buf;
    return 0;
}
