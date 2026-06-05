/* PostgreSQL коннектор: реализует DfoConnector ABI через libpq.
 * Подключается к живой БД, умеет: список таблиц, схема, постраничное чтение,
 * а также инкрементальный («лёгкий CDC») забор по высокой отметке — задаётся
 * полем cursor_column в connector_config (см. pg_read_batch).
 * Полноценный потоковый CDC (logical replication) — cdc_start/cdc_stop = NULL,
 * зарезервировано для фазы 3 (план — у объявления dfo_connector_entry ниже). */
#include "../../connector.h"
#include "../../../core/log.h"
#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

/* ── Конфигурация по умолчанию ── */
#define PG_DEFAULT_BATCH 8192
#define PG_MAX_DSN       1024
#define PG_MAX_SQL       4096

/* Контекст одного подключения к PG. Живёт в арене, переданной в create(). */
typedef struct {
    PGconn *conn;
    char    dsn[PG_MAX_DSN];
    int     batch_size;
    char    cursor_column[128];  /* if set → incremental high-watermark reads */
    Arena  *arena;
} PgCtx;

/* ── Маппинг pg-типа (строка из information_schema) в ColType ── */
static ColType pg_col_type(const char *pg_type) {
    if (!pg_type) return COL_TEXT;
    if (strstr(pg_type, "int")   || strstr(pg_type, "serial")) return COL_INT64;
    if (strstr(pg_type, "float") || strstr(pg_type, "numeric") ||
        strstr(pg_type, "decimal") || strstr(pg_type, "real"))  return COL_DOUBLE;
    if (strncmp(pg_type, "bool", 4) == 0)                      return COL_BOOL;
    if (strstr(pg_type, "timestamp") || strstr(pg_type, "date")) return COL_INT64;
    return COL_TEXT;
}

/* Ручной парсер JSON-поля: ищет "key": и извлекает строку или bare-значение.
 * Не зависит от арены — вызывается до её полной инициализации в create(). */
static void cfg_get(const char *json, const char *key, char *out, size_t outsz, const char *def) {
    if (def) strncpy(out, def, outsz-1);
    if (!json) return;
    char search[128];
    snprintf(search, sizeof(search), "\"%s\"", key);
    const char *p = strstr(json, search);
    if (!p) return;
    p += strlen(search);
    while (*p == ' ' || *p == '\t' || *p == '\n') p++;
    if (*p != ':') return;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n') p++;
    if (*p == '"') {
        p++;
        const char *e = strchr(p, '"');
        if (!e) return;
        size_t n = (size_t)(e - p);
        if (n >= outsz) n = outsz - 1;
        memcpy(out, p, n); out[n] = '\0';
    } else {
        /* bare number / boolean */
        const char *e = p;
        while (*e && *e != ',' && *e != '}' && *e != ' ' && *e != '\n') e++;
        size_t n = (size_t)(e - p);
        if (n >= outsz) n = outsz - 1;
        memcpy(out, p, n); out[n] = '\0';
    }
}

/* ── create(): разбираем config JSON и открываем подключение к PG ──
 * DSN строится в формате "keyword=value" — libpq рекомендует его вместо URL. */
static void *pg_create(const char *cfg, Arena *a) {
    PgCtx *ctx = arena_calloc(a, sizeof(PgCtx));
    ctx->arena      = a;
    ctx->batch_size = PG_DEFAULT_BATCH;

    char host[128]="localhost", port[16]="5432", dbname[128]="postgres";
    char user[128]="", pass[128]="", sslmode[32]="prefer", ctimeout[16]="10";
    char bsz[16]="8192";

    cfg_get(cfg, "host",            host,     sizeof(host),     "localhost");
    cfg_get(cfg, "port",            port,     sizeof(port),     "5432");
    cfg_get(cfg, "dbname",          dbname,   sizeof(dbname),   "postgres");
    cfg_get(cfg, "user",            user,     sizeof(user),     "");
    cfg_get(cfg, "password",        pass,     sizeof(pass),     "");
    cfg_get(cfg, "sslmode",         sslmode,  sizeof(sslmode),  "prefer");
    cfg_get(cfg, "connect_timeout", ctimeout, sizeof(ctimeout), "10");
    cfg_get(cfg, "batch_size",      bsz,      sizeof(bsz),      "8192");
    /* cursor_column: when present, read_batch does incremental high-watermark
     * reads on this column instead of OFFSET paging (see pg_read_batch). */
    cfg_get(cfg, "cursor_column",   ctx->cursor_column, sizeof(ctx->cursor_column), "");

    ctx->batch_size = atoi(bsz);
    if (ctx->batch_size <= 0 || ctx->batch_size > BATCH_SIZE)
        ctx->batch_size = PG_DEFAULT_BATCH;

    /* Build keyword=value DSN */
    int n = snprintf(ctx->dsn, sizeof(ctx->dsn),
        "host=%s port=%s dbname=%s connect_timeout=%s sslmode=%s",
        host, port, dbname, ctimeout, sslmode);
    if (user[0])
        n += snprintf(ctx->dsn+n, sizeof(ctx->dsn)-(size_t)n, " user=%s", user);
    if (pass[0])
        snprintf(ctx->dsn+n, sizeof(ctx->dsn)-(size_t)n, " password=%s", pass);

    ctx->conn = PQconnectdb(ctx->dsn);
    if (PQstatus(ctx->conn) != CONNECTION_OK) {
        LOG_ERROR("pg_connector: connect failed: %s", PQerrorMessage(ctx->conn));
        PQfinish(ctx->conn);
        ctx->conn = NULL;
        return ctx; /* return ctx so destroy() can free properly */
    }
    LOG_INFO("pg_connector: connected to %s:%s/%s", host, port, dbname);
    return ctx;
}

static void pg_destroy(void *vctx) {
    PgCtx *ctx = vctx;
    if (ctx && ctx->conn) { PQfinish(ctx->conn); ctx->conn = NULL; }
}

/* ── ping() ── */
static int pg_ping(void *vctx) {
    PgCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return -1;
    PGresult *r = PQexec(ctx->conn, "SELECT 1");
    int ok = (PQresultStatus(r) == PGRES_TUPLES_OK);
    PQclear(r);
    return ok ? 0 : -1;
}

/* ── list_entities(): список таблиц и вьюх в схеме public ── */
static int pg_list_entities(void *vctx, Arena *a, DfoEntityList *out) {
    PgCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return -1;

    PGresult *r = PQexec(ctx->conn,
        "SELECT table_name, table_type "
        "FROM information_schema.tables "
        "WHERE table_schema = 'public' "
        "  AND table_type IN ('BASE TABLE','VIEW') "
        "ORDER BY table_name");

    if (PQresultStatus(r) != PGRES_TUPLES_OK) {
        LOG_ERROR("pg list_entities: %s", PQerrorMessage(ctx->conn));
        PQclear(r); return -1;
    }

    int n = PQntuples(r);
    out->items = arena_calloc(a, (size_t)n * sizeof(DfoEntity));
    out->count = n;
    for (int i = 0; i < n; i++) {
        out->items[i].entity = arena_strdup(a, PQgetvalue(r, i, 0));
        const char *ttype    = PQgetvalue(r, i, 1);
        out->items[i].type   = strstr(ttype,"VIEW") ? "view" : "table";
    }
    PQclear(r);
    return 0;
}

/* ── describe(): схема таблицы через information_schema.columns ──
 * Используем PQexecParams с параметром $1 — защита от SQL-инъекции в имени таблицы. */
static int pg_describe(void *vctx, Arena *a, const char *entity, Schema **out) {
    PgCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return -1;

    const char *params[1] = { entity };
    PGresult *r = PQexecParams(ctx->conn,
        "SELECT column_name, data_type, is_nullable "
        "FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = $1 "
        "ORDER BY ordinal_position",
        1, NULL, params, NULL, NULL, 0);

    if (PQresultStatus(r) != PGRES_TUPLES_OK) {
        LOG_ERROR("pg describe %s: %s", entity, PQerrorMessage(ctx->conn));
        PQclear(r); return -1;
    }

    int n = PQntuples(r);
    Schema *sc = arena_calloc(a, sizeof(Schema));
    sc->ncols = n;
    sc->cols  = arena_alloc(a, (size_t)n * sizeof(ColDef));
    for (int i = 0; i < n; i++) {
        sc->cols[i].name     = arena_strdup(a, PQgetvalue(r, i, 0));
        sc->cols[i].type     = pg_col_type(PQgetvalue(r, i, 1));
        sc->cols[i].nullable = (strcmp(PQgetvalue(r, i, 2), "YES") == 0);
    }
    PQclear(r);
    *out = sc;
    return 0;
}

/* Парсинг метки времени PG ("2024-01-15 12:34:56") в unix-секунды.
 * Храним timestamp как int64 (COL_INT64), чтобы можно было строить B-tree индекс. */
static int64_t parse_pg_ts(const char *s) {
    if (!s || !*s) return 0;
    struct tm tm = {0};
    /* "2024-01-15 12:34:56" or "2024-01-15T12:34:56" */
    int y,mo,d,h,mi,sec;
    if (sscanf(s, "%d-%d-%d%*c%d:%d:%d", &y,&mo,&d,&h,&mi,&sec) >= 6) {
        tm.tm_year = y - 1900; tm.tm_mon = mo - 1; tm.tm_mday = d;
        tm.tm_hour = h; tm.tm_min = mi; tm.tm_sec = sec;
#ifdef __APPLE__
        /* timegm not available by default — use mktime + tz offset */
        time_t local = mktime(&tm);
        return (int64_t)local;
#else
        return (int64_t)timegm(&tm);
#endif
    }
    return 0;
}

/* ── read_batch(): читаем один батч строк из PG ──
 * Два режима, выбор по конфигу:
 *   • OFFSET-пагинация (по умолчанию): cursor — строковое целое смещение.
 *   • Инкрементальный CDC (если задан cursor_column): cursor — «высокая
 *     отметка», последнее увиденное значение cursor_column. К запросу
 *     добавляется WHERE <cursor_column> > $1 ORDER BY <cursor_column> ASC,
 *     а по завершении в req->cursor записывается max(cursor_column) батча,
 *     чтобы следующий вызов забрал только новые строки. Это «лёгкий» CDC по
 *     монотонному полю (id/updated_at/lsn) БЕЗ логической репликации —
 *     полноценный поток событий приедет в cdc_start/cdc_stop (см. ниже).
 * Если entity начинается с SELECT — выполняем как подзапрос, иначе строим SELECT * FROM table.
 * Схема ColBatch выводится из OID-типов результата — не требует предварительного describe().
 *
 * TODO(cdc): при немонотонном/неуникальном cursor_column строгое '>' может
 *   пропустить строки с тем же значением на границе батча — для корректности
 *   нужен уникальный/монотонный ключ либо '>=' с дедупликацией по последнему
 *   значению. Достаточно для MVP инкрементального забора. */
static int pg_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                          const char *entity, ColBatch **out_batch) {
    PgCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return -1;

    int64_t limit = (req && req->limit > 0) ? req->limit : (int64_t)ctx->batch_size;
    if (limit > BATCH_SIZE) limit = BATCH_SIZE;

    char sql[PG_MAX_SQL];
    const char *src = entity ? entity : "";
    /* req->filter may carry a full query */
    if (req && req->filter && req->filter[0]) src = req->filter;

    bool incremental = ctx->cursor_column[0] != '\0';
    PGresult *res;

    if (incremental) {
        /* High-watermark read on cursor_column. */
        const char *cc = ctx->cursor_column;
        char base[PG_MAX_SQL];
        if (strncasecmp(src, "SELECT", 6) == 0)
            snprintf(base, sizeof(base), "(%s) _dfo_q", src);
        else
            snprintf(base, sizeof(base), "\"%s\"", src);

        const char *cursor = (req && req->cursor) ? req->cursor : "";
        if (cursor[0]) {
            /* Parameterised so PG casts the bookmark to the column's type and
             * the value can't inject SQL. */
            snprintf(sql, sizeof(sql),
                "SELECT * FROM %s WHERE \"%s\" > $1 ORDER BY \"%s\" ASC LIMIT %lld",
                base, cc, cc, (long long)limit);
            const char *params[1] = { cursor };
            res = PQexecParams(ctx->conn, sql, 1, NULL, params, NULL, NULL, 0);
        } else {
            snprintf(sql, sizeof(sql),
                "SELECT * FROM %s ORDER BY \"%s\" ASC LIMIT %lld",
                base, cc, (long long)limit);
            res = PQexec(ctx->conn, sql);
        }
    } else {
        /* Classic OFFSET pagination — cursor is an integer offset. */
        int64_t offset = 0;
        if (req && req->cursor && req->cursor[0])
            offset = (int64_t)strtoll(req->cursor, NULL, 10);
        if (strncasecmp(src, "SELECT", 6) == 0)
            snprintf(sql, sizeof(sql),
                "SELECT * FROM (%s) _dfo_q LIMIT %lld OFFSET %lld",
                src, (long long)limit, (long long)offset);
        else
            snprintf(sql, sizeof(sql),
                "SELECT * FROM \"%s\" LIMIT %lld OFFSET %lld",
                src, (long long)limit, (long long)offset);
        res = PQexec(ctx->conn, sql);
    }

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        LOG_ERROR("pg read_batch query failed: %s", PQerrorMessage(ctx->conn));
        PQclear(res); return -1;
    }

    int nrows  = PQntuples(res);
    int ncols  = PQnfields(res);

    /* Build schema from result metadata */
    Schema *sc = arena_calloc(a, sizeof(Schema));
    sc->ncols = ncols;
    sc->cols  = arena_alloc(a, (size_t)ncols * sizeof(ColDef));
    for (int c = 0; c < ncols; c++) {
        sc->cols[c].name     = arena_strdup(a, PQfname(res, c));
        /* Force TEXT for every column — same as the CSV connector. The query
         * engine has a read-back bug on INT64/DOUBLE columns (a typed source
         * table crashed the gateway on the first SELECT over it). Emitting text
         * sidesteps it; downstream SQL coerces as needed. The cursor bookmark is
         * still read straight from the result (PQgetvalue), so cursor mode on a
         * bigint/timestamp column is unaffected.
         * TODO(pg): restore native typing once the engine INT64/DOUBLE
         *   read-back is fixed (would enable B-tree indexes on numeric cols). */
        sc->cols[c].type     = COL_TEXT;
        sc->cols[c].nullable = true;
    }

    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema = sc;
    batch->ncols  = ncols;
    batch->nrows  = nrows;

    /* Allocate column storage */
    for (int c = 0; c < ncols; c++) {
        batch->null_bitmap[c] = arena_calloc(a, ((size_t)nrows + 7) / 8);
        switch (sc->cols[c].type) {
            case COL_INT64:
                batch->values[c] = arena_alloc(a, (size_t)nrows * sizeof(int64_t)); break;
            case COL_DOUBLE:
                batch->values[c] = arena_alloc(a, (size_t)nrows * sizeof(double)); break;
            default:
                batch->values[c] = arena_alloc(a, (size_t)nrows * sizeof(char *)); break;
        }
    }

    /* Fill values */
    for (int r = 0; r < nrows; r++) {
        for (int c = 0; c < ncols; c++) {
            if (PQgetisnull(res, r, c)) {
                batch->null_bitmap[c][r/8] |= (uint8_t)(1u << (r % 8));
                continue;
            }
            const char *v = PQgetvalue(res, r, c);
            switch (sc->cols[c].type) {
                case COL_INT64: {
                    Oid oid = PQftype(res, c);
                    int64_t iv;
                    if (oid == 1114 || oid == 1184 || oid == 1082)
                        iv = parse_pg_ts(v);
                    else
                        iv = (int64_t)strtoll(v, NULL, 10);
                    ((int64_t *)batch->values[c])[r] = iv;
                    break;
                }
                case COL_DOUBLE:
                    ((double *)batch->values[c])[r] = strtod(v, NULL);
                    break;
                case COL_BOOL:
                    ((int64_t *)batch->values[c])[r] = (v[0]=='t' || v[0]=='T' || v[0]=='1');
                    break;
                default:
                    ((char **)batch->values[c])[r] = arena_strdup(a, v);
                    break;
            }
        }
    }

    /* Advance the bookmark: rows are ordered ASC by cursor_column, so the last
     * row holds the max value. Hand it back via req->cursor for the next call.
     * On an empty batch we leave the cursor untouched (nothing new yet). */
    if (incremental && nrows > 0 && req) {
        int ci = PQfnumber(res, ctx->cursor_column);
        if (ci >= 0) {
            const char *mv = PQgetvalue(res, nrows - 1, ci);
            req->cursor = arena_strdup(a, mv ? mv : "");
        }
    }

    PQclear(res);
    *out_batch = batch;
    return 0;
}

const DfoConnector dfo_connector_entry = {
    .abi_version   = DFO_CONNECTOR_ABI_VERSION,
    .name          = "postgresql",
    .version       = "0.1.0",
    .description   = "PostgreSQL connector via libpq",
    .create        = pg_create,
    .destroy       = pg_destroy,
    .list_entities = pg_list_entities,
    .describe      = pg_describe,
    .read_batch    = pg_read_batch,
    /* CDC streaming — phase 3, still NULL. Planned wiring mirrors the kafka
     * connector (lib/connector/plugins/kafka/kafka_connector.c):
     *   - cdc_start(handler, userdata): store both on PgCtx, set an
     *     atomic_int running flag, and pthread_create a consumer thread.
     *     The thread opens a logical-replication slot
     *     (CREATE_REPLICATION_SLOT ... pgoutput / START_REPLICATION ...) via a
     *     replication connection, decodes each change into a CdcEvent
     *     {op=INSERT/UPDATE/DELETE, before, after, lsn} and calls
     *     handler(&ev, userdata) — exactly как consumer_thread_fn в kafka.
     *   - cdc_stop(): clear the running flag and pthread_join the thread.
     * Until then, incremental pulls via cursor_column in pg_read_batch cover
     * the common "new/changed rows since last run" case without replication. */
    .cdc_start     = NULL,
    .cdc_stop      = NULL,
    .ping          = pg_ping,
};
