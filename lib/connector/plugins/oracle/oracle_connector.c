/* Oracle коннектор: реализует DfoConnector ABI через ODPI-C
 * (Oracle Database Programming Interface for C — тонкая обёртка над OCI).
 *
 * Эталон — lib/connector/plugins/pg/pg_connector.c. Совпадает логика двух
 * режимов read_batch (OFFSET-пагинация / инкрементальный high-watermark по
 * cursor_column) и приём «эмитить все колонки как COL_TEXT» — у движка есть
 * баг чтения нативных INT64/DOUBLE (типизированный источник роняет gateway).
 * Типизацию по Oracle-типам делает describe() как метаданные.
 *
 * Версионная матрица (выбор синтаксиса по ctx->ora_major):
 *   11g  → пагинация и инкремент через ROWNUM-subquery (нет OFFSET/FETCH)
 *   12c+ → OFFSET ... FETCH NEXT ... ROWS ONLY
 *   21c+ → тип JSON → COL_TEXT
 *   23ai → тип BOOLEAN → COL_BOOL, VECTOR → COL_TEXT
 * Версия определяется автоматически (dpiConn_getServerVersion); параметр
 * oracle_version в конфиге перекрывает автодетект (нужен, если у пользователя
 * нет прав на server version).
 *
 * write_batch = NULL: коннектор-источник, только чтение.
 * cdc_start/cdc_stop = NULL: потоковый CDC через Oracle LogMiner — отдельная
 * фаза (план — у объявления dfo_connector_entry ниже).
 *
 * ВНИМАНИЕ по сборке: требует ODPI-C в compile-time (dpi.h, -lodpic) и Oracle
 * Instant Client в runtime (ODPI-C грузит его через dlopen). См. README.md.
 * Собирать: `make oracle` (таргет включается, когда найден dpi.h). */
#include "../../connector.h"
#include "../../../core/log.h"
#include <dpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <pthread.h>

/* ── Конфигурация по умолчанию ── */
#define ORA_DEFAULT_BATCH 8192
#define ORA_MAX_SQL       4096
#define ORA_LOB_CHUNK     65536   /* чтение LOB порциями */

/* Единый ODPI-C контекст на процесс — создаётся один раз (pthread_once). */
static dpiContext  *gContext = NULL;
static int          gContextOk = 0;
static char         gInitErr[512] = "";   /* real ODPI-C init error (e.g. DPI-1047) */
static pthread_once_t gOnce = PTHREAD_ONCE_INIT;

static void ora_context_init(void) {
    dpiErrorInfo err;
    if (dpiContext_createWithParams(DPI_MAJOR_VERSION, DPI_MINOR_VERSION,
                                    NULL, &gContext, &err) == DPI_SUCCESS) {
        gContextOk = 1;
    } else {
        LOG_ERROR("oracle: dpiContext init failed: %.*s",
                  err.messageLength, err.message);
        snprintf(gInitErr, sizeof(gInitErr), "%.*s", err.messageLength, err.message);
        gContextOk = 0;
    }
}

/* Контекст одного подключения. Живёт в арене, переданной в create(). */
typedef struct {
    dpiConn *conn;
    char     schema[128];         /* = Oracle OWNER (UPPER) */
    char     cursor_column[128];  /* if set → incremental high-watermark reads */
    int      batch_size;
    int      ora_major;           /* 11/12/19/21/23 */
    Arena   *arena;
    char     last_err[512];       /* human-readable reason of the last failure */
} OraCtx;

/* ── Ручной парсер JSON-поля (копия из pg_connector.c). ── */
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

/* In-place ASCII upper/lower. */
static void str_upper(char *s) { for (; *s; s++) *s = (char)toupper((unsigned char)*s); }
static void str_lower(char *s) { for (; *s; s++) *s = (char)tolower((unsigned char)*s); }

/* Скопировать ODPI-C сообщение об ошибке в out, обрезав хвостовые CR/LF.
 * Безопасно при err.message == NULL (ODPI-C может вернуть пустой error при
 * отсутствии pending-ошибки — иначе %.*s по NULL даёт SIGSEGV). Если в out
 * ничего осмысленного не попало, пишем fallback. */
static void ora_copy_err(const dpiErrorInfo *err, char *out, size_t outsz,
                         const char *fallback) {
    if (!out || outsz == 0) return;
    if (err && err->message && err->messageLength > 0) {
        int n = (int)err->messageLength;
        if ((size_t)n >= outsz) n = (int)outsz - 1;
        memcpy(out, err->message, (size_t)n);
        out[n] = '\0';
    } else {
        snprintf(out, outsz, "%s", fallback ? fallback : "Oracle error");
    }
    size_t L = strlen(out);
    while (L > 0 && (out[L-1] == '\n' || out[L-1] == '\r')) out[--L] = '\0';
}

/* Последняя ошибка ODPI-C из глобального контекста → лог + (опц.) в ctx->last_err.
 * Текст сообщения ODPI-C уже содержит реальный ORA-код (напр. "ORA-01017: ...")
 * — пробрасываем его как есть, не обобщая. */
static void ora_capture_err(OraCtx *ctx, const char *what) {
    char msg[512];
    if (gContextOk) {
        dpiErrorInfo err;
        memset(&err, 0, sizeof(err));
        dpiContext_getError(gContext, &err);
        ora_copy_err(&err, msg, sizeof(msg),
                     "Oracle error (no diagnostic available)");
    } else {
        snprintf(msg, sizeof(msg), "%s",
                 gInitErr[0] ? gInitErr : "ODPI-C context unavailable");
    }
    LOG_ERROR("oracle %s: %s", what ? what : "error", msg);
    if (ctx) snprintf(ctx->last_err, sizeof(ctx->last_err), "%s", msg);
}

/* Старая сигнатура — только лог, без записи в ctx (для путей без ctx). */
static void ora_log_err(const char *what) { ora_capture_err(NULL, what); }

/* Гарантировать осмысленный last_err, когда подключения нет. ctx->last_err
 * обычно уже содержит реальный ORA из create() (напр. ORA-01017) — сохраняем
 * его. Если по какой-то причине пуст, ставим внятный fallback (а не пустоту,
 * которая downstream обобщается до "list_entities failed"). */
static int ora_no_conn(OraCtx *ctx, const char *what) {
    if (ctx && !ctx->last_err[0])
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "oracle %s: подключение не установлено", what ? what : "operation");
    return -1;
}

/* Маппинг Oracle data_type (строка из ALL_TAB_COLUMNS) + precision/scale в
 * ColType. Используется только describe() как метаданные (read_batch → TEXT).
 * ora_major нужен для версионных типов (BOOLEAN с 23ai). */
static ColType ora_map_type(const char *dt, int prec, int scale,
                            int has_prec, int has_scale, int ora_major) {
    if (!dt) return COL_TEXT;
    if (!strncmp(dt, "NUMBER", 6)) {
        /* NUMBER(p,0) / INTEGER → int64; NUMBER(p,s>0) → double;
         * NUMBER без точности → double (неопределённая → безопасно double). */
        if (has_scale && scale == 0 && has_prec) return COL_INT64;
        if (!has_prec && !has_scale)             return COL_DOUBLE;
        return COL_DOUBLE;
    }
    if (!strcmp(dt, "INTEGER") || !strcmp(dt, "INT") ||
        !strcmp(dt, "SMALLINT"))                          return COL_INT64;
    if (!strcmp(dt, "FLOAT")  || !strcmp(dt, "BINARY_FLOAT") ||
        !strcmp(dt, "BINARY_DOUBLE") || !strcmp(dt, "REAL")) return COL_DOUBLE;
    /* DATE / TIMESTAMP* → unix seconds (int64), как в pg_connector */
    if (!strcmp(dt, "DATE") || !strncmp(dt, "TIMESTAMP", 9)) return COL_INT64;
    /* BOOLEAN в SQL-колонках — только Oracle 23ai+ */
    if (!strcmp(dt, "BOOLEAN"))
        return (ora_major >= 23) ? COL_BOOL : COL_TEXT;
    /* строковые / LOB / спец-типы → TEXT */
    /* VARCHAR2, NVARCHAR2, CHAR, NCHAR, CLOB, NCLOB, BLOB(hex),
     * JSON(21c+), XMLTYPE, VECTOR(23ai), ROWID, RAW и пр. */
    (void)prec;
    return COL_TEXT;
}

/* dpiTimestamp → каноническая NLS-строка "YYYY-MM-DD HH:MI:SS[.FFF]".
 * Совпадает с NLS_TIMESTAMP_FORMAT, который мы ставим в create() — поэтому
 * значение годится и как cursor-bookmark (Oracle неявно конвертирует строку
 * обратно в DATE/TIMESTAMP при сравнении WHERE col > :1). */
static void fmt_ora_ts(const dpiTimestamp *ts, char *buf, size_t bufsz) {
    int ms = (int)(ts->fsecond / 1000000);   /* наносекунды → миллисекунды */
    if (ms > 0)
        snprintf(buf, bufsz, "%04d-%02d-%02d %02d:%02d:%02d.%03d",
                 ts->year, ts->month, ts->day, ts->hour, ts->minute, ts->second, ms);
    else
        snprintf(buf, bufsz, "%04d-%02d-%02d %02d:%02d:%02d",
                 ts->year, ts->month, ts->day, ts->hour, ts->minute, ts->second);
}

/* Выполнить простой DML/DDL-стейтмент без результата (для ALTER SESSION). */
static void ora_exec_simple(dpiConn *conn, const char *sql) {
    dpiStmt *st;
    uint32_t cols;
    if (dpiConn_prepareStmt(conn, 0, sql, (uint32_t)strlen(sql), NULL, 0, &st) != DPI_SUCCESS)
        return;
    dpiStmt_execute(st, DPI_MODE_EXEC_DEFAULT, &cols);
    dpiStmt_release(st);
}

/* ── create(): config JSON → ODPI-C контекст, подключение, NLS, автодетект ── */
static void *ora_create(const char *cfg, Arena *a) {
    pthread_once(&gOnce, ora_context_init);

    OraCtx *ctx = arena_calloc(a, sizeof(OraCtx));
    ctx->arena      = a;
    ctx->batch_size = ORA_DEFAULT_BATCH;
    ctx->ora_major  = 12;   /* дефолт до автодетекта */

    char host[128]="localhost", port[16]="1521", service[128]="ORCL";
    char user[128]="", pass[128]="", bsz[16]="8192", oraver[16]="";

    cfg_get(cfg, "host",          host,    sizeof(host),    "localhost");
    cfg_get(cfg, "port",          port,    sizeof(port),    "1521");
    cfg_get(cfg, "service_name",  service, sizeof(service), "ORCL");
    cfg_get(cfg, "user",          user,    sizeof(user),    "");
    cfg_get(cfg, "password",      pass,    sizeof(pass),    "");
    cfg_get(cfg, "batch_size",    bsz,     sizeof(bsz),     "8192");
    cfg_get(cfg, "schema",        ctx->schema, sizeof(ctx->schema), "");
    cfg_get(cfg, "cursor_column", ctx->cursor_column, sizeof(ctx->cursor_column), "");
    cfg_get(cfg, "oracle_version",oraver,  sizeof(oraver),  "");

    ctx->batch_size = atoi(bsz);
    if (ctx->batch_size <= 0 || ctx->batch_size > BATCH_SIZE)
        ctx->batch_size = ORA_DEFAULT_BATCH;
    if (oraver[0]) ctx->ora_major = atoi(oraver);
    /* Oracle хранит unquoted-идентификаторы в UPPER — owner для ALL_* запросов
     * и для квотирования "SCHEMA"."TABLE" в read_batch. */
    str_upper(ctx->schema);

    if (!gContextOk) {
        LOG_ERROR("oracle: ODPI-C context unavailable — cannot connect");
        snprintf(ctx->last_err, sizeof(ctx->last_err), "%s",
                 gInitErr[0] ? gInitErr
                             : "Oracle Instant Client недоступен (ODPI-C не инициализирован)");
        return ctx;   /* conn == NULL */
    }

    /* Easy Connect: host:port/service_name */
    char connstr[300];
    snprintf(connstr, sizeof(connstr), "%s:%s/%s", host, port, service);

    if (dpiConn_create(gContext,
                       user, (uint32_t)strlen(user),
                       pass, (uint32_t)strlen(pass),
                       connstr, (uint32_t)strlen(connstr),
                       NULL, NULL, &ctx->conn) != DPI_SUCCESS) {
        /* Реальная причина (напр. "ORA-01017: invalid username/password") →
         * ctx->last_err, чтобы last_error() отдал её downstream без обобщения. */
        ora_capture_err(ctx, "connect");
        ctx->conn = NULL;   /* ODPI-C не гарантирует conn==NULL после провала */
        return ctx;
    }

    /* Автодетект версии сервера (если есть права). */
    dpiVersionInfo srv_ver;
    if (dpiConn_getServerVersion(ctx->conn, NULL, NULL, &srv_ver) == DPI_SUCCESS) {
        ctx->ora_major = srv_ver.versionNum;
        LOG_INFO("oracle: server version %d.%d (schema=%s)",
                 srv_ver.versionNum, srv_ver.releaseNum, ctx->schema);
    } else {
        LOG_INFO("oracle: cannot detect version, using configured %d", ctx->ora_major);
    }

    /* NLS-форматы — критично: иначе DATE/TIMESTAMP приходят в формате сессии и
     * наша строковая сериализация / cursor-bind не совпадут. */
    static const char *nls_stmts[] = {
        "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
        "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'",
        "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM'",
        /* фикс десятичного разделителя '.' — NUMBER читаем как VARCHAR (см.
         * read_batch), и текст не должен зависеть от локали сессии. */
        "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'",
        NULL
    };
    for (int i = 0; nls_stmts[i]; i++)
        ora_exec_simple(ctx->conn, nls_stmts[i]);

    LOG_INFO("oracle: connected to %s as %s", connstr, user);
    return ctx;
}

/* ── destroy(): закрыть подключение (арена освобождается вызывающим) ── */
static void ora_destroy(void *vctx) {
    OraCtx *ctx = vctx;
    if (ctx && ctx->conn) { dpiConn_release(ctx->conn); ctx->conn = NULL; }
}

/* ── ping() ── */
static int ora_ping(void *vctx) {
    OraCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return -1;
    return (dpiConn_ping(ctx->conn) == DPI_SUCCESS) ? 0 : -1;
}

/* ── last_error(): human-readable reason of the last failure (ABI v3) ── */
static const char *ora_last_error(void *vctx) {
    OraCtx *ctx = vctx;
    return (ctx && ctx->last_err[0]) ? ctx->last_err : NULL;
}

/* Прочитать одну колонку текущей строки как строку (в арену). nativeType из
 * getQueryValue, oracleType из query info (для различения CLOB/BLOB). Возвращает
 * arena-строку (или "" для NULL — NULL ловится у вызывающего по data->isNull). */
static char *ora_cell_to_str(Arena *a, dpiConn *conn,
                             dpiNativeTypeNum nt, dpiOracleTypeNum ot,
                             dpiData *data) {
    char buf[256];
    if (!data) return arena_strdup(a, "");   /* защита от null-deref на путях ошибок */
    switch (nt) {
        case DPI_NATIVE_TYPE_INT64:
            snprintf(buf, sizeof(buf), "%lld", (long long)data->value.asInt64);
            return arena_strdup(a, buf);
        case DPI_NATIVE_TYPE_UINT64:
            snprintf(buf, sizeof(buf), "%llu", (unsigned long long)data->value.asUint64);
            return arena_strdup(a, buf);
        case DPI_NATIVE_TYPE_FLOAT:
            snprintf(buf, sizeof(buf), "%.9g", (double)data->value.asFloat);
            return arena_strdup(a, buf);
        case DPI_NATIVE_TYPE_DOUBLE:
            /* %.17g сохраняет round-trip double. NUMBER > 2^53 теряет точность —
             * см. README (ограничение нативной читки NUMBER через double). */
            snprintf(buf, sizeof(buf), "%.17g", data->value.asDouble);
            return arena_strdup(a, buf);
        case DPI_NATIVE_TYPE_BOOLEAN:
            return arena_strdup(a, data->value.asBoolean ? "true" : "false");
        case DPI_NATIVE_TYPE_TIMESTAMP:
            fmt_ora_ts(&data->value.asTimestamp, buf, sizeof(buf));
            return arena_strdup(a, buf);
        case DPI_NATIVE_TYPE_BYTES: {
            uint32_t len = data->value.asBytes.length;
            const char *ptr = data->value.asBytes.ptr;
            char *s = arena_alloc(a, (size_t)len + 1);
            if (ptr && len) memcpy(s, ptr, len);   /* ptr может быть NULL при len==0 */
            s[len] = '\0';
            return s;
        }
        case DPI_NATIVE_TYPE_LOB: {
            dpiLob *lob = data->value.asLOB;
            uint64_t size = 0;
            if (!lob || dpiLob_getSize(lob, &size) != DPI_SUCCESS) return arena_strdup(a, "");
            int is_blob = (ot == DPI_ORACLE_TYPE_BLOB);
            /* читаем порциями */
            uint64_t cap = is_blob ? size * 2 + 1 : size + 1;   /* BLOB → hex: 2 симв/байт */
            char *out = arena_alloc(a, (size_t)cap);
            size_t off = 0;
            uint64_t pos = 1;   /* ODPI-C: char/byte offset 1-based */
            char chunk[ORA_LOB_CHUNK];
            static const char hexd[] = "0123456789abcdef";
            while (pos <= size) {
                uint64_t want = ORA_LOB_CHUNK;
                uint64_t got = want;
                if (dpiLob_readBytes(lob, pos, want, chunk, &got) != DPI_SUCCESS) break;
                if (got == 0) break;
                if (is_blob) {
                    for (uint64_t i = 0; i < got && off + 2 < cap; i++) {
                        out[off++] = hexd[(unsigned char)chunk[i] >> 4];
                        out[off++] = hexd[(unsigned char)chunk[i] & 0xF];
                    }
                } else {
                    uint64_t n = got;
                    if (off + n >= cap) n = cap - off - 1;
                    memcpy(out + off, chunk, (size_t)n);
                    off += n;
                }
                pos += got;
            }
            out[off] = '\0';
            return out;
        }
        default:
            (void)conn;
            return arena_strdup(a, "");
    }
}

/* ── list_entities(): таблицы и вьюхи схемы через ALL_TABLES/ALL_VIEWS ── */
static int ora_list_entities(void *vctx, Arena *a, DfoEntityList *out) {
    OraCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return ora_no_conn(ctx, "list_entities");

    const char *sql =
        "SELECT table_name, 'table' AS etype FROM all_tables WHERE owner = :1 "
        "UNION ALL "
        "SELECT view_name,  'view'  AS etype FROM all_views  WHERE owner = :1 "
        "ORDER BY 1";

    dpiStmt *st;
    uint32_t ncols;
    if (dpiConn_prepareStmt(ctx->conn, 0, sql, (uint32_t)strlen(sql), NULL, 0, &st) != DPI_SUCCESS) {
        ora_capture_err(ctx, "list_entities prepare"); return -1;
    }
    dpiData bind; dpiData_setBytes(&bind, ctx->schema, (uint32_t)strlen(ctx->schema));
    dpiStmt_bindValueByPos(st, 1, DPI_NATIVE_TYPE_BYTES, &bind);
    if (dpiStmt_execute(st, DPI_MODE_EXEC_DEFAULT, &ncols) != DPI_SUCCESS) {
        ora_capture_err(ctx, "list_entities execute"); dpiStmt_release(st); return -1;
    }

    /* Соберём в динамический список (число строк заранее неизвестно). */
    int cap = 64, n = 0;
    DfoEntity *items = arena_alloc(a, (size_t)cap * sizeof(DfoEntity));
    for (;;) {
        int found = 0; uint32_t ri;
        if (dpiStmt_fetch(st, &found, &ri) != DPI_SUCCESS || !found) break;
        dpiNativeTypeNum nt; dpiData *d;
        char *name = NULL; const char *etype = "table";
        if (dpiStmt_getQueryValue(st, 1, &nt, &d) == DPI_SUCCESS && !d->isNull)
            name = ora_cell_to_str(a, ctx->conn, nt, DPI_ORACLE_TYPE_VARCHAR, d);
        if (dpiStmt_getQueryValue(st, 2, &nt, &d) == DPI_SUCCESS && !d->isNull) {
            char *t = ora_cell_to_str(a, ctx->conn, nt, DPI_ORACLE_TYPE_VARCHAR, d);
            etype = (t && !strcmp(t, "view")) ? "view" : "table";
        }
        if (!name) continue;
        if (n == cap) {
            int ncap = cap * 2;
            DfoEntity *ni = arena_alloc(a, (size_t)ncap * sizeof(DfoEntity));
            memcpy(ni, items, (size_t)n * sizeof(DfoEntity));
            items = ni; cap = ncap;
        }
        items[n].entity = name;   /* Oracle identifier — оставляем UPPER */
        items[n].type   = etype;
        n++;
    }
    dpiStmt_release(st);
    out->items = items;
    out->count = n;
    return 0;
}

/* ── describe(): схема через ALL_TAB_COLUMNS ── */
static int ora_describe(void *vctx, Arena *a, const char *entity, Schema **out) {
    OraCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return ora_no_conn(ctx, "describe");

    char ent[128];
    strncpy(ent, entity ? entity : "", sizeof(ent)-1); ent[sizeof(ent)-1]='\0';
    str_upper(ent);

    const char *sql =
        "SELECT column_name, data_type, nullable, data_precision, data_scale "
        "FROM all_tab_columns WHERE owner = :1 AND table_name = :2 "
        "ORDER BY column_id";

    dpiStmt *st; uint32_t ncols;
    if (dpiConn_prepareStmt(ctx->conn, 0, sql, (uint32_t)strlen(sql), NULL, 0, &st) != DPI_SUCCESS) {
        ora_capture_err(ctx, "describe prepare"); return -1;
    }
    dpiData b1, b2;
    dpiData_setBytes(&b1, ctx->schema, (uint32_t)strlen(ctx->schema));
    dpiData_setBytes(&b2, ent, (uint32_t)strlen(ent));
    dpiStmt_bindValueByPos(st, 1, DPI_NATIVE_TYPE_BYTES, &b1);
    dpiStmt_bindValueByPos(st, 2, DPI_NATIVE_TYPE_BYTES, &b2);
    if (dpiStmt_execute(st, DPI_MODE_EXEC_DEFAULT, &ncols) != DPI_SUCCESS) {
        ora_capture_err(ctx, "describe execute"); dpiStmt_release(st); return -1;
    }

    int cap = 32, n = 0;
    ColDef *cols = arena_alloc(a, (size_t)cap * sizeof(ColDef));
    for (;;) {
        int found = 0; uint32_t ri;
        if (dpiStmt_fetch(st, &found, &ri) != DPI_SUCCESS || !found) break;
        dpiNativeTypeNum nt; dpiData *d;
        char *cname = NULL, *dtype = NULL, *nullable = NULL;
        int prec = 0, scale = 0, has_prec = 0, has_scale = 0;
        if (dpiStmt_getQueryValue(st, 1, &nt, &d) == DPI_SUCCESS && !d->isNull)
            cname = ora_cell_to_str(a, ctx->conn, nt, DPI_ORACLE_TYPE_VARCHAR, d);
        if (dpiStmt_getQueryValue(st, 2, &nt, &d) == DPI_SUCCESS && !d->isNull)
            dtype = ora_cell_to_str(a, ctx->conn, nt, DPI_ORACLE_TYPE_VARCHAR, d);
        if (dpiStmt_getQueryValue(st, 3, &nt, &d) == DPI_SUCCESS && !d->isNull)
            nullable = ora_cell_to_str(a, ctx->conn, nt, DPI_ORACLE_TYPE_VARCHAR, d);
        if (dpiStmt_getQueryValue(st, 4, &nt, &d) == DPI_SUCCESS && !d->isNull) {
            has_prec = 1; prec = (int)d->value.asInt64;
            if (nt == DPI_NATIVE_TYPE_DOUBLE) prec = (int)d->value.asDouble;
        }
        if (dpiStmt_getQueryValue(st, 5, &nt, &d) == DPI_SUCCESS && !d->isNull) {
            has_scale = 1; scale = (int)d->value.asInt64;
            if (nt == DPI_NATIVE_TYPE_DOUBLE) scale = (int)d->value.asDouble;
        }
        if (!cname) continue;
        if (n == cap) {
            int ncap = cap * 2;
            ColDef *nc = arena_alloc(a, (size_t)ncap * sizeof(ColDef));
            memcpy(nc, cols, (size_t)n * sizeof(ColDef));
            cols = nc; cap = ncap;
        }
        str_lower(cname);   /* Oracle отдаёт UPPER — приводим к lower */
        cols[n].name     = cname;
        cols[n].type     = ora_map_type(dtype, prec, scale, has_prec, has_scale, ctx->ora_major);
        cols[n].nullable = (nullable && nullable[0] == 'Y');
        n++;
    }
    dpiStmt_release(st);

    Schema *sc = arena_calloc(a, sizeof(Schema));
    sc->ncols = n;
    sc->cols  = cols;
    *out = sc;
    return 0;
}

/* ── read_batch(): один батч строк из Oracle ──
 * Версионный SQL (11g ROWNUM-subquery / 12c+ OFFSET..FETCH), два режима
 * (OFFSET-пагинация / инкрементальный по cursor_column). Все колонки → TEXT.
 * 11g-пагинация добавляет служебную колонку DFO__RN — она отбрасывается. */
static int ora_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                          const char *entity, ColBatch **out_batch) {
    OraCtx *ctx = vctx;
    if (!ctx || !ctx->conn) return ora_no_conn(ctx, "read_batch");

    int64_t limit = (req && req->limit > 0) ? req->limit : (int64_t)ctx->batch_size;
    if (limit > BATCH_SIZE) limit = BATCH_SIZE;

    /* Источник: подзапрос (SELECT...) или "SCHEMA"."ENTITY". */
    const char *raw = entity ? entity : "";
    if (req && req->filter && req->filter[0]) raw = req->filter;
    char ent[160];
    int is_subquery = (strncasecmp(raw, "SELECT", 6) == 0);
    char src[260];
    if (is_subquery) {
        snprintf(src, sizeof(src), "(%s)", raw);
    } else {
        /* Имя НЕ цитируем: Oracle сам приводит unquoted-идентификатор к UPPER,
         * поэтому 'qa2_k5_sink' и 'QA2_K5_SINK' резолвятся одинаково. Это даёт
         * единообразие с sink-путём (write_batch тоже пишет без кавычек) —
         * иначе sink создаёт точно-регистровую таблицу, а read её не находит
         * (ORA-942). Схему оставляем без кавычек по той же причине. */
        strncpy(ent, raw, sizeof(ent)-1); ent[sizeof(ent)-1]='\0';
        if (ctx->schema[0])
            snprintf(src, sizeof(src), "%s.%s", ctx->schema, ent);
        else
            snprintf(src, sizeof(src), "%s", ent);
    }

    bool incremental = ctx->cursor_column[0] != '\0';
    const char *cc = ctx->cursor_column;
    const char *cursor = (req && req->cursor) ? req->cursor : "";
    int has_cursor_val = incremental && cursor[0];
    char sql[ORA_MAX_SQL];

    if (incremental) {
        if (ctx->ora_major >= 12) {
            if (has_cursor_val)
                snprintf(sql, sizeof(sql),
                    "SELECT * FROM %s WHERE \"%s\" > :1 ORDER BY \"%s\" ASC "
                    "FETCH NEXT %lld ROWS ONLY", src, cc, cc, (long long)limit);
            else
                snprintf(sql, sizeof(sql),
                    "SELECT * FROM %s ORDER BY \"%s\" ASC "
                    "FETCH NEXT %lld ROWS ONLY", src, cc, (long long)limit);
        } else {
            /* 11g: ROWNUM в WHERE применяется ДО ORDER BY → нужен subquery. */
            if (has_cursor_val)
                snprintf(sql, sizeof(sql),
                    "SELECT * FROM (SELECT * FROM %s WHERE \"%s\" > :1 "
                    "ORDER BY \"%s\" ASC) WHERE ROWNUM <= %lld",
                    src, cc, cc, (long long)limit);
            else
                snprintf(sql, sizeof(sql),
                    "SELECT * FROM (SELECT * FROM %s ORDER BY \"%s\" ASC) "
                    "WHERE ROWNUM <= %lld", src, cc, (long long)limit);
        }
    } else {
        int64_t offset = 0;
        if (req && req->cursor && req->cursor[0])
            offset = (int64_t)strtoll(req->cursor, NULL, 10);
        if (ctx->ora_major >= 12) {
            snprintf(sql, sizeof(sql),
                "SELECT * FROM %s OFFSET %lld ROWS FETCH NEXT %lld ROWS ONLY",
                src, (long long)offset, (long long)limit);
        } else {
            /* 11g: двойной subquery с ROWNUM. Внешняя колонка DFO__RN
             * отбрасывается при сборке батча. */
            snprintf(sql, sizeof(sql),
                "SELECT * FROM (SELECT t.*, ROWNUM dfo__rn FROM %s t "
                "WHERE ROWNUM <= %lld) WHERE dfo__rn > %lld",
                src, (long long)(offset + limit), (long long)offset);
        }
    }

    dpiStmt *st; uint32_t ncols_raw;
    if (dpiConn_prepareStmt(ctx->conn, 0, sql, (uint32_t)strlen(sql), NULL, 0, &st) != DPI_SUCCESS) {
        ora_capture_err(ctx, "read_batch prepare"); return -1;
    }
    if (has_cursor_val) {
        dpiData b; dpiData_setBytes(&b, (char *)cursor, (uint32_t)strlen(cursor));
        dpiStmt_bindValueByPos(st, 1, DPI_NATIVE_TYPE_BYTES, &b);
    }
    if (dpiStmt_execute(st, DPI_MODE_EXEC_DEFAULT, &ncols_raw) != DPI_SUCCESS) {
        ora_capture_err(ctx, "read_batch execute"); dpiStmt_release(st); return -1;
    }

    /* Метаданные колонок. Отбрасываем служебную DFO__RN (11g offset). */
    int ncols = (int)ncols_raw;
    dpiOracleTypeNum *otype = arena_alloc(a, (size_t)ncols * sizeof(dpiOracleTypeNum));
    Schema *sc = arena_calloc(a, sizeof(Schema));
    sc->cols = arena_alloc(a, (size_t)ncols * sizeof(ColDef));
    int ncols_out = 0, cursor_out_idx = -1;
    int *raw_to_out = arena_alloc(a, (size_t)ncols * sizeof(int));
    for (int c = 0; c < ncols; c++) {
        dpiQueryInfo qi;
        /* getQueryInfo на error-пути может вернуть DPI_FAILURE с реальным ORA —
         * не «глотаем» молча (это раньше прятало ошибку и шло читать сбойный
         * stmt), а захватываем текст, освобождаем stmt и выходим. */
        if (dpiStmt_getQueryInfo(st, (uint32_t)(c+1), &qi) != DPI_SUCCESS) {
            ora_capture_err(ctx, "read_batch query info");
            dpiStmt_release(st); return -1;
        }
        char nm[160];
        const char *qn = qi.name ? qi.name : "";
        uint32_t nlen = qi.nameLength < sizeof(nm)-1 ? qi.nameLength : (uint32_t)sizeof(nm)-1;
        if (nlen) memcpy(nm, qn, nlen);
        nm[nlen] = '\0';
        if (!strcasecmp(nm, "DFO__RN")) { raw_to_out[c] = -1; continue; }   /* служебная */
        otype[ncols_out] = qi.typeInfo.oracleTypeNum;
        /* NUMBER читаем нативной строкой (Oracle конвертит → точный десятичный
         * текст), а не через double — иначе 540.20 превращается в
         * 540.20000000000005 и теряется точность для NUMBER > 2^53. */
        if (qi.typeInfo.oracleTypeNum == DPI_ORACLE_TYPE_NUMBER) {
            /* Неуспешный defineValue оставляет колонку с невалидным буфером —
             * последующий getQueryValue вернёт мусорный/NULL dpiData и роняет
             * gateway. Проверяем результат и выходим по ошибке. */
            if (dpiStmt_defineValue(st, (uint32_t)(c+1), DPI_ORACLE_TYPE_VARCHAR,
                                    DPI_NATIVE_TYPE_BYTES, 128, 0, NULL) != DPI_SUCCESS) {
                ora_capture_err(ctx, "read_batch define value");
                dpiStmt_release(st); return -1;
            }
        }
        /* cursor bookmark column (сравнение без регистра) */
        if (incremental && !strcasecmp(nm, cc)) cursor_out_idx = ncols_out;
        str_lower(nm);
        sc->cols[ncols_out].name     = arena_strdup(a, nm);
        /* Logical type — value is read as text (NUMBER is defined as VARCHAR
         * above to keep precision), so no native read-back; the catalog type
         * renders numbers as JSON numbers on output. NUMBER with scale 0 -> INT64,
         * with a fractional scale -> DOUBLE; BOOLEAN -> BOOL; everything else
         * (dates, varchar, lob, ...) stays TEXT. */
        {
            dpiOracleTypeNum _ot = qi.typeInfo.oracleTypeNum;
            ColType _ct = COL_TEXT;
            if (_ot == DPI_ORACLE_TYPE_NUMBER)
                _ct = (qi.typeInfo.scale == 0) ? COL_INT64 : COL_DOUBLE;
            else if (_ot == DPI_ORACLE_TYPE_BOOLEAN)
                _ct = COL_BOOL;
            sc->cols[ncols_out].type = _ct;
        }
        sc->cols[ncols_out].nullable = true;
        raw_to_out[c] = ncols_out;
        ncols_out++;
    }
    sc->ncols = ncols_out;

    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema = sc;
    batch->ncols  = ncols_out;

    /* Колонки фиксированного размера limit (реальное nrows ≤ limit). */
    for (int c = 0; c < ncols_out; c++) {
        batch->null_bitmap[c] = arena_calloc(a, ((size_t)limit + 7) / 8);
        batch->values[c]      = arena_alloc(a, (size_t)limit * sizeof(char *));
    }

    int nrows = 0;
    char *last_cursor_val = NULL;
    while (nrows < (int)limit) {
        int found = 0; uint32_t ri;
        /* Разделяем «чистый конец данных» (found==0) и реальный сбой fetch
         * (DPI_FAILURE → может нести ORA, напр. ORA-942 на исчезнувшей вьюхе
         * посреди выборки). На ошибке — захват + release не-NULL stmt + return,
         * иначе раньше частичный батч молча выдавался как success. */
        int fr = dpiStmt_fetch(st, &found, &ri);
        if (fr != DPI_SUCCESS) {
            ora_capture_err(ctx, "read_batch fetch");
            dpiStmt_release(st); return -1;
        }
        if (!found) break;
        for (int c = 0; c < ncols; c++) {
            int oc = raw_to_out[c];
            if (oc < 0) continue;   /* DFO__RN или сбойная колонка */
            dpiNativeTypeNum nt; dpiData *d = NULL;
            /* NULL по контракту DFO: ставим и null_bitmap (для приёмников),
             * и сам сентинел в ячейку — материализация/движок ловят его по
             * строке, а не по указателю NULL (NULL-указатель downstream даёт
             * "" и теряет NULL-семантику). d может быть NULL при сбое
             * getQueryValue — проверяем перед разыменованием d->isNull. */
            if (dpiStmt_getQueryValue(st, (uint32_t)(c+1), &nt, &d) != DPI_SUCCESS
                || !d || d->isNull) {
                batch->null_bitmap[oc][nrows/8] |= (uint8_t)(1u << (nrows % 8));
                ((char **)batch->values[oc])[nrows] = arena_strdup(a, DFO_NULL_SENTINEL);
                continue;
            }
            char *s = ora_cell_to_str(a, ctx->conn, nt, otype[oc], d);
            ((char **)batch->values[oc])[nrows] = s;
            if (oc == cursor_out_idx) last_cursor_val = s;
        }
        nrows++;
    }
    batch->nrows = nrows;
    dpiStmt_release(st);

    /* Advance bookmark значением cursor_column последней строки. */
    if (incremental && nrows > 0 && req && last_cursor_val)
        req->cursor = arena_strdup(a, last_cursor_val);

    *out_batch = batch;
    return 0;
}

/* ── write_batch(): выгрузка строк в таблицу Oracle (sink) ── */
#define ORA_INS_CHUNK 100
/* Выполнить стейтмент с проверкой результата (для DDL/INSERT в sink). */
static int ora_exec_chk(dpiConn *conn, const char *sql) {
    dpiStmt *st;
    if (dpiConn_prepareStmt(conn, 0, sql, (uint32_t)strlen(sql), NULL, 0, &st) != DPI_SUCCESS) {
        ora_log_err("sink prepare"); return -1;
    }
    uint32_t cols;
    int rc = dpiStmt_execute(st, DPI_MODE_EXEC_DEFAULT, &cols);
    if (rc != DPI_SUCCESS) ora_log_err("sink execute");
    dpiStmt_release(st);
    return rc == DPI_SUCCESS ? 0 : -1;
}

/* Создаёт целевую таблицу при необходимости (все колонки VARCHAR2(4000)),
 * при OVERWRITE делает TRUNCATE, затем вставляет строки пачками по ORA_INS_CHUNK
 * через INSERT ALL. Значения экранируются вручную (одинарная кавычка → ''). */
static int ora_write_batch(void *vctx, Arena *a, const char *entity,
                           const Schema *schema, const ColBatch *batch, int mode) {
    (void)a;
    OraCtx *ctx = vctx;
    if (!ctx || !ctx->conn) { LOG_ERROR("oracle sink: no connection"); return -1; }
    if (!entity || !entity[0]) { LOG_ERROR("oracle sink: empty target table"); return -1; }
    int ncols = batch->ncols;

    /* CREATE TABLE IF NOT EXISTS через PL/SQL (в Oracle нет IF NOT EXISTS). */
    {
        size_t cap = 256 + (size_t)ncols * 80; char *cols = malloc(cap); size_t off = 0;
        for (int c = 0; c < ncols; c++)
            off += (size_t)snprintf(cols+off, cap-off, "%s\"%s\" VARCHAR2(4000)", c?", ":"", schema->cols[c].name);
        size_t dcap = cap + 600; char *ddl = malloc(dcap);
        /* Имя таблицы НЕ цитируем — Oracle приведёт его к UPPER, ровно так же
         * её потом находит read_batch (тоже без кавычек). С кавычками sink
         * создавал таблицу с точным регистром (напр. lower-case), которую
         * upper-резолвящий read не мог прочитать → ORA-942 на read-back. */
        snprintf(ddl, dcap,
            "BEGIN EXECUTE IMMEDIATE 'CREATE TABLE %s (%s)'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;",
            entity, cols);
        int rc = ora_exec_chk(ctx->conn, ddl); free(cols); free(ddl);
        if (rc < 0) return -1;
    }
    if (mode == DFO_SINK_OVERWRITE) {
        char trunc[600]; snprintf(trunc, sizeof(trunc), "TRUNCATE TABLE %s", entity);
        ora_exec_chk(ctx->conn, trunc);
    }

    char collist[4096]; size_t co = 0;
    co += (size_t)snprintf(collist+co, sizeof(collist)-co, "(");
    for (int c = 0; c < ncols; c++)
        co += (size_t)snprintf(collist+co, sizeof(collist)-co, "%s\"%s\"", c?", ":"", schema->cols[c].name);
    snprintf(collist+co, sizeof(collist)-co, ")");

    int written = 0;
    for (int start = 0; start < batch->nrows; start += ORA_INS_CHUNK) {
        int end = start + ORA_INS_CHUNK; if (end > batch->nrows) end = batch->nrows;
        size_t cap = 4096; char *sql = malloc(cap); size_t off = 0;
        #define ORA_APP(...) do { \
            int need = snprintf(NULL,0,__VA_ARGS__); \
            if (off + (size_t)need + 1 > cap) { while (off+(size_t)need+1>cap) cap*=2; sql=realloc(sql,cap);} \
            off += (size_t)snprintf(sql+off, cap-off, __VA_ARGS__); } while(0)
        ORA_APP("INSERT ALL ");   /* Oracle multi-row: INSERT ALL ... SELECT 1 FROM dual */
        for (int r = start; r < end; r++) {
            ORA_APP("INTO %s %s VALUES (", entity, collist);
            for (int c = 0; c < ncols; c++) {
                const uint8_t *bm = batch->null_bitmap[c];
                if (bm && ((bm[r/8] >> (r%8)) & 1u)) { ORA_APP("%sNULL", c?", ":""); continue; }
                /* TEXT-always sink: значения приходят как char* (текст). Числовые
                 * колонки пишем литералом БЕЗ кавычек. Раньше COL_INT64/COL_DOUBLE
                 * читались как native int64/double прямо из char*-массива → читался
                 * указатель как число → мусор в INSERT (R7-класс). */
                const char *v = ((char**)batch->values[c])[r]; if (!v) v = "";
                int is_num = (schema->cols[c].type==COL_INT64 || schema->cols[c].type==COL_DOUBLE);
                /* сентинел NULL или пустое число → настоящий NULL */
                if (strcmp(v, DFO_NULL_SENTINEL) == 0 || (is_num && !v[0])) {
                    ORA_APP("%sNULL", c?", ":""); continue;
                }
                if (is_num) { ORA_APP("%s%s", c?", ":"", v); continue; }
                ORA_APP("%s'", c?", ":"");
                for (const char *p=v; *p; p++) { if (*p=='\'') ORA_APP("''"); else ORA_APP("%c", *p); }
                ORA_APP("'");
            }
            ORA_APP(") ");
        }
        ORA_APP("SELECT 1 FROM dual");
        #undef ORA_APP
        int rc = ora_exec_chk(ctx->conn, sql); free(sql);
        if (rc < 0) return -1;
        written += (end - start);
    }
    dpiConn_commit(ctx->conn);
    LOG_INFO("oracle sink: %d rows → %s (%s)", written, entity,
             mode==DFO_SINK_OVERWRITE?"overwrite":"append");
    return written;
}

const DfoConnector dfo_connector_entry = {
    .abi_version   = DFO_CONNECTOR_ABI_VERSION,
    .name          = "oracle",
    .version       = "0.1.0",
    .description   = "Oracle Database connector via ODPI-C",
    .create        = ora_create,
    .destroy       = ora_destroy,
    .list_entities = ora_list_entities,
    .describe      = ora_describe,
    .read_batch    = ora_read_batch,
    /* CDC streaming — NULL. План (фаза 3) через Oracle LogMiner аналогичен
     * kafka consumer_thread_fn (lib/connector/plugins/kafka/kafka_connector.c):
     *   - cdc_start(handler, userdata): сохранить оба на OraCtx, atomic running
     *     flag, pthread_create фонового потока. Поток через DBMS_LOGMNR
     *     (ADD_LOGFILE/START_LOGMNR) или Continuous Mining читает V$LOGMNR_CONTENTS,
     *     декодирует каждую DML-операцию в CdcEvent {op,before,after,lsn=SCN} и
     *     вызывает handler(&ev, userdata).
     *   - cdc_stop(): сбросить flag, pthread_join.
     * До этого инкрементальный забор по cursor_column в ora_read_batch
     * закрывает кейс «новые/изменённые строки с прошлого запуска». */
    .cdc_start     = NULL,
    .cdc_stop      = NULL,
    .ping          = ora_ping,
    .write_batch   = ora_write_batch,
    .last_error    = ora_last_error,
};
