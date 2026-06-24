/* Greenplum коннектор: реализует DfoConnector ABI через libpq.
 *
 * Greenplum говорит на PostgreSQL wire protocol, поэтому транспорт — та же
 * libpq, что и в pg_connector.c (эталон). Отдельный коннектор нужен из-за
 * GP-специфики:
 *   • discovery через pg_catalog (быстрее information_schema на MPP);
 *   • версионная логика list_entities (5.x: pg_inherits-фильтр партиций;
 *     6.x/7.x: relispartition + relstorage-фильтр external-таблиц);
 *   • search_path выставляется на схему из конфига сразу после подключения;
 *   • расширенный маппинг типов (aclitem, gpxlogloc и пр. → TEXT);
 *   • OFFSET бьёт по мастер-сегменту → инкрементальный режим (cursor_column)
 *     рекомендован по умолчанию (см. gp_read_batch, LOG_WARN).
 *
 * read_batch, как и в pg_connector, эмитит ВСЕ колонки как COL_TEXT — у движка
 * есть баг чтения нативных INT64/DOUBLE (типизированный источник роняет gateway
 * на первом SELECT). Типизацию по GP-типам делает describe() (метаданные).
 *
 * Полноценный CDC (logical replication) в стандартной поставке GP отсутствует —
 * cdc_start/cdc_stop = NULL. Инкрементальный забор по cursor_column закрывает
 * кейс «новые/изменённые строки с прошлого запуска».
 *
 * write_batch = NULL: коннектор-источник, только чтение (sink не требуется). */
#include "../../connector.h"
#include "../../../core/log.h"
#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

/* ── Конфигурация по умолчанию ── */
#define GP_DEFAULT_BATCH 8192
#define GP_MAX_DSN       1024
#define GP_MAX_SQL       4096

/* Контекст одного подключения к GP. Живёт в арене, переданной в create(). */
typedef struct {
    PGconn *conn;
    char    dsn[GP_MAX_DSN];
    int     batch_size;
    char    schema[128];          /* GP schema (search_path + discovery filter) */
    char    cursor_column[128];   /* if set → incremental high-watermark reads */
    int     gp_major;             /* Greenplum major (5/6/7); 0 = unknown */
    Arena  *arena;
    char    last_err[512];        /* human-readable reason of the last failure */
} GpCtx;

/* ── Маппинг GP/PG-типа (typname из pg_type) в ColType ──
 * Расширяет pg-маппинг GP-специфичными типами. Используется только в describe()
 * как метаданные — read_batch всё равно эмитит TEXT (см. шапку файла). */
static ColType gp_col_type(const char *t) {
    if (!t) return COL_TEXT;
    /* целочисленные и системные «номерные» типы */
    if (!strcmp(t,"int2")  || !strcmp(t,"int4") || !strcmp(t,"int8") ||
        !strcmp(t,"oid")   || !strcmp(t,"xid")  || !strcmp(t,"cid")  ||
        !strcmp(t,"tid")   || !strcmp(t,"serial")|| !strcmp(t,"bigserial"))
        return COL_INT64;
    /* с плавающей точкой / произвольной точностью */
    if (!strcmp(t,"float4") || !strcmp(t,"float8") || !strcmp(t,"numeric") ||
        !strcmp(t,"decimal"))
        return COL_DOUBLE;
    if (!strcmp(t,"bool"))
        return COL_BOOL;
    /* дата/время → unix seconds (int64), как в pg_connector */
    if (!strcmp(t,"timestamp") || !strcmp(t,"timestamptz") ||
        !strcmp(t,"date")      || !strcmp(t,"abstime"))
        return COL_INT64;
    /* строковые */
    if (!strcmp(t,"varchar") || !strcmp(t,"text")  || !strcmp(t,"bpchar") ||
        !strcmp(t,"name")    || !strcmp(t,"citext"))
        return COL_TEXT;
    /* GP-специфика и всё неопознанное → строкой */
    return COL_TEXT;
}

/* Ручной парсер JSON-поля (копия из pg_connector.c): ищет "key": и извлекает
 * строку или bare-значение. Не зависит от арены — вызывается в create(). */
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
        const char *e = p;
        while (*e && *e != ',' && *e != '}' && *e != ' ' && *e != '\n') e++;
        size_t n = (size_t)(e - p);
        if (n >= outsz) n = outsz - 1;
        memcpy(out, p, n); out[n] = '\0';
    }
}

/* Парсинг major-версии из строки SELECT version():
 *   "PostgreSQL 9.4.26 (Greenplum Database 6.24.3 build ...)" → 6
 * Возвращает 0, если подстрока "Greenplum Database " не найдена. */
static int parse_gp_major(const char *ver_str) {
    if (!ver_str) return 0;
    const char *p = strstr(ver_str, "Greenplum Database ");
    if (!p) return 0;
    p += strlen("Greenplum Database ");
    return atoi(p);   /* "6.24.3" → 6 */
}

/* ── create(): config JSON → подключение к GP, search_path, автодетект версии ── */
static void *gp_create(const char *cfg, Arena *a) {
    GpCtx *ctx = arena_calloc(a, sizeof(GpCtx));
    ctx->arena      = a;
    ctx->batch_size = GP_DEFAULT_BATCH;

    char host[128]="localhost", port[16]="5432", dbname[128]="postgres";
    char user[128]="", pass[128]="", sslmode[32]="disable", ctimeout[16]="10";
    char bsz[16]="8192", gpver[16]="";

    cfg_get(cfg, "host",            host,     sizeof(host),     "localhost");
    cfg_get(cfg, "port",            port,     sizeof(port),     "5432");
    cfg_get(cfg, "dbname",          dbname,   sizeof(dbname),   "postgres");
    cfg_get(cfg, "user",            user,     sizeof(user),     "");
    cfg_get(cfg, "password",        pass,     sizeof(pass),     "");
    cfg_get(cfg, "sslmode",         sslmode,  sizeof(sslmode),  "disable");
    cfg_get(cfg, "connect_timeout", ctimeout, sizeof(ctimeout), "10");
    cfg_get(cfg, "batch_size",      bsz,      sizeof(bsz),      "8192");
    cfg_get(cfg, "schema",          ctx->schema, sizeof(ctx->schema), "public");
    cfg_get(cfg, "cursor_column",   ctx->cursor_column, sizeof(ctx->cursor_column), "");
    cfg_get(cfg, "gp_version",      gpver,    sizeof(gpver),    "");

    ctx->batch_size = atoi(bsz);
    if (ctx->batch_size <= 0 || ctx->batch_size > BATCH_SIZE)
        ctx->batch_size = GP_DEFAULT_BATCH;
    ctx->gp_major = gpver[0] ? atoi(gpver) : 0;   /* explicit overrides autodetect */

    int n = snprintf(ctx->dsn, sizeof(ctx->dsn),
        "host=%s port=%s dbname=%s connect_timeout=%s sslmode=%s",
        host, port, dbname, ctimeout, sslmode);
    if (user[0])
        n += snprintf(ctx->dsn+n, sizeof(ctx->dsn)-(size_t)n, " user=%s", user);
    if (pass[0])
        snprintf(ctx->dsn+n, sizeof(ctx->dsn)-(size_t)n, " password=%s", pass);

    ctx->conn = PQconnectdb(ctx->dsn);
    if (PQstatus(ctx->conn) != CONNECTION_OK) {
        const char *em = PQerrorMessage(ctx->conn);
        LOG_ERROR("gp_connector: connect failed: %s", em);
        snprintf(ctx->last_err, sizeof(ctx->last_err), "%s", em ? em : "connection failed");
        size_t L = strlen(ctx->last_err);
        while (L > 0 && (ctx->last_err[L-1] == '\n' || ctx->last_err[L-1] == '\r'))
            ctx->last_err[--L] = '\0';
        PQfinish(ctx->conn);
        ctx->conn = NULL;
        return ctx;
    }

    /* search_path → схема из конфига (+ public как фолбэк). Использует
     * параметризацию через quote_ident, чтобы имя схемы не инъектировалось. */
    {
        const char *sp_params[1] = { ctx->schema };
        PGresult *r = PQexecParams(ctx->conn,
            "SELECT set_config('search_path', quote_ident($1) || ',public', false)",
            1, NULL, sp_params, NULL, NULL, 0);
        if (PQresultStatus(r) != PGRES_TUPLES_OK)
            LOG_ERROR("gp_connector: set search_path failed: %s", PQerrorMessage(ctx->conn));
        PQclear(r);
    }

    /* Автодетект версии через version(). Обязателен признак "Greenplum" —
     * иначе это обычный PG, и GP-специфичные каталоги (relstorage) сломают
     * discovery. Явный gp_version из конфига перекрывает автодетект. */
    {
        PGresult *r = PQexec(ctx->conn, "SELECT version()");
        if (PQresultStatus(r) == PGRES_TUPLES_OK && PQntuples(r) > 0) {
            const char *vs = PQgetvalue(r, 0, 0);
            int detected = parse_gp_major(vs);
            if (!strstr(vs, "Greenplum")) {
                LOG_ERROR("gp_connector: server is not Greenplum (version: %s) — "
                          "use connector_type=postgresql for plain PG", vs);
                snprintf(ctx->last_err, sizeof(ctx->last_err),
                         "сервер не является Greenplum — используйте коннектор PostgreSQL");
                PQclear(r);
                PQfinish(ctx->conn);
                ctx->conn = NULL;
                return ctx;
            }
            if (!ctx->gp_major) ctx->gp_major = detected;
            LOG_INFO("gp_connector: connected to %s:%s/%s schema=%s — GP %d (%s)",
                     host, port, dbname, ctx->schema,
                     ctx->gp_major, vs);
        } else {
            /* version() недоступна — полагаемся на gp_version из конфига */
            if (!ctx->gp_major) ctx->gp_major = 6;   /* разумный дефолт */
            LOG_INFO("gp_connector: connected to %s:%s/%s schema=%s — "
                     "version() unavailable, using GP %d",
                     host, port, dbname, ctx->schema, ctx->gp_major);
        }
        PQclear(r);
    }
    return ctx;
}

static void gp_destroy(void *vctx) {
    GpCtx *ctx = vctx;
    if (ctx && ctx->conn) { PQfinish(ctx->conn); ctx->conn = NULL; }
}

/* ── ping() ── */
static int gp_ping(void *vctx) {
    GpCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return -1;
    PGresult *r = PQexec(ctx->conn, "SELECT 1");
    int ok = (PQresultStatus(r) == PGRES_TUPLES_OK);
    PQclear(r);
    return ok ? 0 : -1;
}

/* ── last_error(): human-readable reason of the last failure (ABI v3) ── */
static const char *gp_last_error(void *vctx) {
    GpCtx *ctx = vctx;
    return (ctx && ctx->last_err[0]) ? ctx->last_err : NULL;
}

/* ── list_entities(): таблицы и вьюхи схемы через pg_catalog ──
 * Версионная логика: GP 6+ умеет relispartition + relstorage; GP 5 — нет,
 * там партиции исключаются через pg_inherits. */
static int gp_list_entities(void *vctx, Arena *a, DfoEntityList *out) {
    GpCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return -1;

    const char *sql;
    int want_storage;   /* GP6+ возвращает 3-ю колонку storage_type */
    if (ctx->gp_major >= 6) {
        want_storage = 1;
        sql =
            "SELECT c.relname, "
            "       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' ELSE 'other' END, "
            "       CASE WHEN c.relstorage = 'a' THEN 'ao' "
            "            WHEN c.relstorage = 'c' THEN 'aocs' "
            "            ELSE 'heap' END AS storage_type "
            "FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 "
            "  AND c.relkind IN ('r','v') "
            "  AND c.relstorage <> 'x' "       /* исключить EXTERNAL TABLE */
            "  AND NOT c.relispartition "       /* исключить дочерние партиции (GP6+) */
            "ORDER BY c.relname";
    } else {
        /* GP 5.x: ни relispartition, ни надёжного relstorage='x' — партиции
         * фильтруем через pg_inherits (дочерние таблицы наследуют родителя). */
        want_storage = 0;
        sql =
            "SELECT c.relname, "
            "       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' ELSE 'other' END "
            "FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 "
            "  AND c.relkind IN ('r','v') "
            "  AND c.oid NOT IN (SELECT inhrelid FROM pg_catalog.pg_inherits) "
            "ORDER BY c.relname";
    }

    const char *params[1] = { ctx->schema };
    PGresult *r = PQexecParams(ctx->conn, sql, 1, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(r) != PGRES_TUPLES_OK) {
        LOG_ERROR("gp list_entities: %s", PQerrorMessage(ctx->conn));
        PQclear(r); return -1;
    }

    int nt = PQntuples(r);
    out->items = arena_calloc(a, (size_t)nt * sizeof(DfoEntity));
    out->count = nt;
    for (int i = 0; i < nt; i++) {
        out->items[i].entity = arena_strdup(a, PQgetvalue(r, i, 0));
        const char *ttype    = PQgetvalue(r, i, 1);
        out->items[i].type   = !strcmp(ttype,"view") ? "view" : "table";
        if (want_storage) {
            const char *storage = PQgetvalue(r, i, 2);
            if (storage && (storage[0]=='a'))  /* ao / aocs */
                LOG_DEBUG("gp list_entities: %s is append-optimized (%s)",
                          out->items[i].entity, storage);
        }
    }
    PQclear(r);
    return 0;
}

/* ── describe(): схема таблицы через pg_catalog.pg_attribute ──
 * Надёжнее information_schema на GP. $1=schema, $2=table. */
static int gp_describe(void *vctx, Arena *a, const char *entity, Schema **out) {
    GpCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return -1;

    const char *params[2] = { ctx->schema, entity };
    PGresult *r = PQexecParams(ctx->conn,
        "SELECT a.attname, t.typname, a.attnotnull "
        "FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_type t      ON t.oid = a.atttypid "
        "JOIN pg_catalog.pg_class c      ON c.oid = a.attrelid "
        "JOIN pg_catalog.pg_namespace n  ON n.oid = c.relnamespace "
        "WHERE n.nspname = $1 AND c.relname = $2 "
        "  AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY a.attnum",
        2, NULL, params, NULL, NULL, 0);

    if (PQresultStatus(r) != PGRES_TUPLES_OK) {
        LOG_ERROR("gp describe %s: %s", entity, PQerrorMessage(ctx->conn));
        PQclear(r); return -1;
    }

    int nc = PQntuples(r);
    Schema *sc = arena_calloc(a, sizeof(Schema));
    sc->ncols = nc;
    sc->cols  = arena_alloc(a, (size_t)nc * sizeof(ColDef));
    for (int i = 0; i < nc; i++) {
        sc->cols[i].name     = arena_strdup(a, PQgetvalue(r, i, 0));
        sc->cols[i].type     = gp_col_type(PQgetvalue(r, i, 1));
        /* attnotnull = 't' → NOT NULL → nullable=false */
        sc->cols[i].nullable = (PQgetvalue(r, i, 2)[0] != 't');
    }
    PQclear(r);
    *out = sc;
    return 0;
}

/* ── read_batch(): один батч строк из GP ──
 * Два режима (как в pg_connector):
 *   • OFFSET-пагинация (cursor_column не задан): cursor = строковое смещение.
 *     На GP OFFSET выполняется на мастер-сегменте и медленный на больших
 *     таблицах — пишем LOG_WARN и рекомендуем cursor_column.
 *   • Инкрементальный (cursor_column задан): WHERE "col" > $1 ORDER BY "col"
 *     ASC LIMIT n; bookmark = max(col) батча, кладётся в req->cursor.
 * Имена в кавычках — GP чувствителен к регистру. LIMIT используется вместо
 * FETCH NEXT (переносимо на все 5.x/6.x/7.x). */
static int gp_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                          const char *entity, ColBatch **out_batch) {
    GpCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return -1;

    int64_t limit = (req && req->limit > 0) ? req->limit : (int64_t)ctx->batch_size;
    if (limit > BATCH_SIZE) limit = BATCH_SIZE;

    char sql[GP_MAX_SQL];
    const char *src = entity ? entity : "";
    if (req && req->filter && req->filter[0]) src = req->filter;

    bool incremental = ctx->cursor_column[0] != '\0';
    PGresult *res;

    if (incremental) {
        const char *cc = ctx->cursor_column;
        char base[GP_MAX_SQL];
        if (strncasecmp(src, "SELECT", 6) == 0)
            snprintf(base, sizeof(base), "(%s) _dfo_q", src);
        else if (strchr(src, '.'))
            snprintf(base, sizeof(base), "%s", src);   /* caller qualified it */
        else
            snprintf(base, sizeof(base), "\"%s\".\"%s\"", ctx->schema, src);

        const char *cursor = (req && req->cursor) ? req->cursor : "";
        if (cursor[0]) {
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
        int64_t offset = 0;
        if (req && req->cursor && req->cursor[0])
            offset = (int64_t)strtoll(req->cursor, NULL, 10);
        if (offset == 0)
            LOG_WARN("gp_connector: OFFSET pagination on '%s' hits the master "
                     "segment and is slow on large tables — set cursor_column "
                     "for incremental reads", src);
        if (strncasecmp(src, "SELECT", 6) == 0)
            snprintf(sql, sizeof(sql),
                "SELECT * FROM (%s) _dfo_q LIMIT %lld OFFSET %lld",
                src, (long long)limit, (long long)offset);
        else if (strchr(src, '.'))
            snprintf(sql, sizeof(sql),
                "SELECT * FROM %s LIMIT %lld OFFSET %lld",
                src, (long long)limit, (long long)offset);
        else
            snprintf(sql, sizeof(sql),
                "SELECT * FROM \"%s\".\"%s\" LIMIT %lld OFFSET %lld",
                ctx->schema, src, (long long)limit, (long long)offset);
        res = PQexec(ctx->conn, sql);
    }

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        LOG_ERROR("gp read_batch query failed: %s", PQerrorMessage(ctx->conn));
        PQclear(res); return -1;
    }

    int nrows = PQntuples(res);
    int ncols = PQnfields(res);

    Schema *sc = arena_calloc(a, sizeof(Schema));
    sc->ncols = ncols;
    sc->cols  = arena_alloc(a, (size_t)ncols * sizeof(ColDef));
    for (int c = 0; c < ncols; c++) {
        sc->cols[c].name = arena_strdup(a, PQfname(res, c));
        /* Force TEXT for every column — same as pg_connector. The query engine
         * has a read-back bug on native INT64/DOUBLE columns; emitting text
         * sidesteps it, downstream SQL coerces as needed. The cursor bookmark
         * is read straight from PQgetvalue, so cursor mode is unaffected. */
        sc->cols[c].type     = COL_TEXT;
        sc->cols[c].nullable = true;
    }

    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema = sc;
    batch->ncols  = ncols;
    batch->nrows  = nrows;

    for (int c = 0; c < ncols; c++) {
        batch->null_bitmap[c] = arena_calloc(a, ((size_t)nrows + 7) / 8);
        batch->values[c]      = arena_alloc(a, (size_t)nrows * sizeof(char *));
    }

    for (int r = 0; r < nrows; r++) {
        for (int c = 0; c < ncols; c++) {
            if (PQgetisnull(res, r, c)) {
                batch->null_bitmap[c][r/8] |= (uint8_t)(1u << (r % 8));
                continue;
            }
            ((char **)batch->values[c])[r] = arena_strdup(a, PQgetvalue(res, r, c));
        }
    }

    /* Advance bookmark: rows are ASC by cursor_column, last row holds max. */
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

/* ── write_batch(): выгрузка строк в таблицу GP (sink, как у pg) ── */
#define GP_INS_CHUNK 250
static int gp_exec_ok(PGconn *c, const char *sql) {
    PGresult *r = PQexec(c, sql);
    int ok = (PQresultStatus(r) == PGRES_COMMAND_OK || PQresultStatus(r) == PGRES_TUPLES_OK);
    if (!ok) LOG_ERROR("gp sink: %s — %s", sql, PQerrorMessage(c));
    PQclear(r);
    return ok ? 0 : -1;
}

static int gp_write_batch(void *vctx, Arena *a, const char *entity,
                          const Schema *schema, const ColBatch *batch, int mode) {
    (void)a;
    GpCtx *ctx = vctx;
    if (!ctx || !ctx->conn) { LOG_ERROR("gp sink: no connection"); return -1; }
    if (!entity || !entity[0]) { LOG_ERROR("gp sink: empty target table"); return -1; }
    int ncols = batch->ncols;

    char *eident = PQescapeIdentifier(ctx->conn, entity, strlen(entity));
    if (!eident) return -1;

    /* CREATE TABLE IF NOT EXISTS <entity> (col TEXT, ...) */
    {
        size_t cap = 64 + (size_t)ncols * 80; char *ddl = malloc(cap); size_t off = 0;
        off += (size_t)snprintf(ddl+off, cap-off, "CREATE TABLE IF NOT EXISTS %s (", eident);
        for (int c = 0; c < ncols; c++) {
            char *col = PQescapeIdentifier(ctx->conn, schema->cols[c].name, strlen(schema->cols[c].name));
            off += (size_t)snprintf(ddl+off, cap-off, "%s%s TEXT", c?", ":"", col ? col : "\"col\"");
            if (col) PQfreemem(col);
        }
        snprintf(ddl+off, cap-off, ")");
        int rc = gp_exec_ok(ctx->conn, ddl); free(ddl);
        if (rc < 0) { PQfreemem(eident); return -1; }
    }
    if (mode == DFO_SINK_OVERWRITE) {
        char trunc[512]; snprintf(trunc, sizeof(trunc), "TRUNCATE TABLE %s", eident);
        if (gp_exec_ok(ctx->conn, trunc) < 0) { PQfreemem(eident); return -1; }
    }

    char collist[4096]; size_t co = 0; collist[0] = '\0';
    co += (size_t)snprintf(collist+co, sizeof(collist)-co, "(");
    for (int c = 0; c < ncols; c++) {
        char *col = PQescapeIdentifier(ctx->conn, schema->cols[c].name, strlen(schema->cols[c].name));
        co += (size_t)snprintf(collist+co, sizeof(collist)-co, "%s%s", c?", ":"", col ? col : "\"col\"");
        if (col) PQfreemem(col);
    }
    snprintf(collist+co, sizeof(collist)-co, ")");

    int written = 0;
    for (int start = 0; start < batch->nrows; start += GP_INS_CHUNK) {
        int end = start + GP_INS_CHUNK; if (end > batch->nrows) end = batch->nrows;
        size_t cap = 4096; char *sql = malloc(cap); size_t off = 0;
        #define GP_APP(...) do { \
            int need = snprintf(NULL,0,__VA_ARGS__); \
            if (off + (size_t)need + 1 > cap) { while (off+(size_t)need+1>cap) cap*=2; sql=realloc(sql,cap);} \
            off += (size_t)snprintf(sql+off, cap-off, __VA_ARGS__); } while(0)
        GP_APP("INSERT INTO %s %s VALUES ", eident, collist);
        for (int r = start; r < end; r++) {
            GP_APP("%s(", r>start?", ":"");
            for (int c = 0; c < ncols; c++) {
                const uint8_t *bm = batch->null_bitmap[c];
                int isnull = (bm && ((bm[r/8] >> (r%8)) & 1u));
                if (isnull) { GP_APP("%sNULL", c?", ":""); continue; }
                char numbuf[64]; const char *v;
                switch (schema->cols[c].type) {
                    case COL_INT64:  snprintf(numbuf,sizeof(numbuf),"%lld",(long long)((int64_t*)batch->values[c])[r]); v=numbuf; break;
                    case COL_DOUBLE: snprintf(numbuf,sizeof(numbuf),"%.10g",((double*)batch->values[c])[r]); v=numbuf; break;
                    default:         v = ((char**)batch->values[c])[r]; if(!v) v=""; break;
                }
                char *lit = PQescapeLiteral(ctx->conn, v, strlen(v));
                GP_APP("%s%s", c?", ":"", lit ? lit : "''");
                if (lit) PQfreemem(lit);
            }
            GP_APP(")");
        }
        #undef GP_APP
        int rc = gp_exec_ok(ctx->conn, sql); free(sql);
        if (rc < 0) { PQfreemem(eident); return -1; }
        written += (end - start);
    }
    PQfreemem(eident);
    LOG_INFO("gp sink: %d rows → %s (%s)", written, entity,
             mode==DFO_SINK_OVERWRITE?"overwrite":"append");
    return written;
}

const DfoConnector dfo_connector_entry = {
    .abi_version   = DFO_CONNECTOR_ABI_VERSION,
    .name          = "greenplum",
    .version       = "0.1.0",
    .description   = "Greenplum (MPP PostgreSQL) connector via libpq",
    .create        = gp_create,
    .destroy       = gp_destroy,
    .list_entities = gp_list_entities,
    .describe      = gp_describe,
    .read_batch    = gp_read_batch,
    /* CDC streaming — NULL. Greenplum has no logical replication in the stock
     * distribution, so a streaming slot (à la pg/kafka consumer_thread_fn) is
     * not available. Incremental pulls via cursor_column in gp_read_batch cover
     * the "new/changed rows since last run" case. */
    .cdc_start     = NULL,
    .cdc_stop      = NULL,
    .ping          = gp_ping,
    .write_batch   = gp_write_batch,
    .last_error    = gp_last_error,
};
