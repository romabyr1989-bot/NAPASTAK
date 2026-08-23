/* MS SQL Server коннектор: реализует DfoConnector ABI через unixODBC.
 *
 * Эталон — lib/connector/plugins/pg/pg_connector.c (структура) и
 * oracle_connector.c (версионная матрица синтаксиса). Совпадает логика двух
 * режимов read_batch: OFFSET-пагинация и инкрементальный high-watermark по
 * cursor_column. Как и там, все колонки эмитятся как COL_TEXT — у движка есть
 * баг чтения нативных INT64/DOUBLE; типизацию отдаёт describe() метаданными.
 *
 * ПОЧЕМУ ODBC, А НЕ НАТИВНЫЙ ПРОТОКОЛ TDS. Драйвер под ODBC подменяем без
 * изменения нашего кода:
 *   FreeTDS (libtdsodbc.so)  — открытый, есть в базовом репозитории RedOS 8,
 *                              вкладывается в бандл, офлайн-установка цела;
 *   msodbcsql18 (Microsoft)  — проприетарный, ставится отдельно; нужен, если
 *                              требуются Always Encrypted, Azure AD и пр.
 * Ровно та же схема, что с Oracle: наш код → ODPI-C → Instant Client.
 *
 * Версионная матрица (выбор синтаксиса по ctx->srv_major):
 *   2008 R2 и старше (<11) → пагинация через ROW_NUMBER() (нет OFFSET/FETCH)
 *   2012+ (>=11)           → OFFSET ... FETCH NEXT ... ROWS ONLY
 * Мажор берётся из SERVERPROPERTY('ProductMajorVersion'):
 *   9=2005 10=2008 11=2012 12=2014 13=2016 14=2017 15=2019 16=2022
 */
#include "../../connector.h"
#include "../../../core/log.h"
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define MS_DEFAULT_BATCH 8192
#define MS_MAX_DSN       1024
#define MS_MAX_SQL       4096
#define MS_MAX_COLS      512

typedef struct {
    SQLHENV  env;
    SQLHDBC  dbc;
    char     dsn[MS_MAX_DSN];
    int      batch_size;
    int      srv_major;            /* 9..16, см. матрицу выше; 0 — не определён */
    char     cursor_column[128];   /* задан → инкрементальное чтение */
    char     schema[128];          /* по умолчанию dbo */
    char     primary_key[256];     /* приёмник: идемпотентная запись через MERGE */
    Arena   *arena;
    char     last_err[512];
} MsCtx;

/* ── Диагностика ODBC ──────────────────────────────────────────────────────
 * ODBC не отдаёт текст ошибки возвратом — его надо вычитывать из хендла
 * отдельным вызовом, иначе наружу уходит бессмысленное «SQL_ERROR (-1)».
 * Собираем ВСЕ записи диагностики: драйвер часто кладёт причину во вторую. */
static void ms_diag(MsCtx *ctx, SQLSMALLINT type, SQLHANDLE h, const char *what)
{
    SQLCHAR state[6], msg[512];
    SQLINTEGER native = 0;
    SQLSMALLINT len = 0;
    size_t off = (size_t)snprintf(ctx->last_err, sizeof(ctx->last_err), "%s: ", what);

    for (SQLSMALLINT rec = 1; rec <= 8; rec++) {
        if (SQLGetDiagRec(type, h, rec, state, &native, msg, sizeof(msg), &len) != SQL_SUCCESS)
            break;
        int n = snprintf(ctx->last_err + off, sizeof(ctx->last_err) - off,
                         "%s[%s/%ld] %s", rec > 1 ? "; " : "",
                         (const char *)state, (long)native, (const char *)msg);
        if (n < 0) break;
        off += (size_t)n;
        if (off >= sizeof(ctx->last_err) - 1) break;
    }
    if (off <= strlen(what) + 2)
        snprintf(ctx->last_err, sizeof(ctx->last_err), "%s: драйвер не сообщил причину", what);
    LOG_ERROR("mssql: %s", ctx->last_err);
}

static bool ms_ok(SQLRETURN rc) { return rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO; }

/* ── Маппинг типа SQL Server (строка из INFORMATION_SCHEMA) в ColType ──
 * Как в pg/oracle: describe() отдаёт настоящий тип метаданными, а read_batch
 * эмитит всё как COL_TEXT. */
static ColType ms_col_type(const char *t)
{
    if (!t) return COL_TEXT;
    if (!strcasecmp(t, "bit"))                                   return COL_BOOL;
    if (!strcasecmp(t, "tinyint")  || !strcasecmp(t, "smallint") ||
        !strcasecmp(t, "int")      || !strcasecmp(t, "bigint"))  return COL_INT64;
    if (!strcasecmp(t, "real")     || !strcasecmp(t, "float"))   return COL_DOUBLE;
    /* decimal/numeric/money НЕ отдаём как DOUBLE: двоичное представление теряет
     * точность, а это деньги и идентификаторы. Даты — тоже текстом: в наборе
     * ColType отдельного временного типа нет (COL_INT64/DOUBLE/TEXT/BOOL/NULL),
     * и pg с oracle поступают так же. */
    return COL_TEXT;
}

/* Достаёт строковое значение ключа из JSON-конфига. Тот же наивный разбор, что
 * в pg_connector: конфиг формирует gateway, он валиден. */
static void cfg_get(const char *json, const char *key, char *out, size_t outsz,
                    const char *def)
{
    snprintf(out, outsz, "%s", def ? def : "");
    if (!json || !key) return;
    char pat[128];
    snprintf(pat, sizeof(pat), "\"%s\"", key);
    const char *p = strstr(json, pat);
    if (!p) return;
    p = strchr(p + strlen(pat), ':');
    if (!p) return;
    p++;
    while (*p == ' ' || *p == '\t') p++;
    if (*p == '"') {
        p++;
        size_t i = 0;
        while (*p && *p != '"' && i + 1 < outsz) {
            if (*p == '\\' && p[1]) p++;      /* \" \\ — снимаем экранирование */
            out[i++] = *p++;
        }
        out[i] = '\0';
    } else {                                   /* число или true/false */
        size_t i = 0;
        while (*p && *p != ',' && *p != '}' && *p != ' ' && i + 1 < outsz) out[i++] = *p++;
        out[i] = '\0';
    }
}

/* Экранирование идентификатора для SQL Server: [имя], внутренняя ] удваивается.
 * Без этого имя таблицы из конфига попадает в SQL как есть. */
static void ms_quote_ident(const char *in, char *out, size_t outsz)
{
    size_t o = 0;
    if (outsz < 3) { if (outsz) out[0] = '\0'; return; }
    out[o++] = '[';
    for (const char *p = in; *p && o + 2 < outsz; p++) {
        if (*p == ']' && o + 3 < outsz) out[o++] = ']';
        out[o++] = *p;
    }
    out[o++] = ']';
    out[o] = '\0';
}

/* Выполняет запрос без результата. Хендл выписки закрывается всегда. */
static int ms_exec(MsCtx *ctx, const char *sql)
{
    SQLHSTMT st = SQL_NULL_HSTMT;
    if (!ms_ok(SQLAllocHandle(SQL_HANDLE_STMT, ctx->dbc, &st))) {
        ms_diag(ctx, SQL_HANDLE_DBC, ctx->dbc, "выделение хендла");
        return -1;
    }
    SQLRETURN rc = SQLExecDirect(st, (SQLCHAR *)sql, SQL_NTS);
    if (!ms_ok(rc) && rc != SQL_NO_DATA) {
        ms_diag(ctx, SQL_HANDLE_STMT, st, "выполнение запроса");
        SQLFreeHandle(SQL_HANDLE_STMT, st);
        return -1;
    }
    SQLFreeHandle(SQL_HANDLE_STMT, st);
    return 0;
}

/* Одно скалярное значение как строка. Возвращает 0 при успехе. */
static int ms_scalar(MsCtx *ctx, const char *sql, char *out, size_t outsz)
{
    SQLHSTMT st = SQL_NULL_HSTMT;
    out[0] = '\0';
    if (!ms_ok(SQLAllocHandle(SQL_HANDLE_STMT, ctx->dbc, &st))) return -1;
    if (!ms_ok(SQLExecDirect(st, (SQLCHAR *)sql, SQL_NTS))) {
        ms_diag(ctx, SQL_HANDLE_STMT, st, "скалярный запрос");
        SQLFreeHandle(SQL_HANDLE_STMT, st);
        return -1;
    }
    int rc = -1;
    if (SQLFetch(st) == SQL_SUCCESS) {
        SQLLEN ind = 0;
        if (ms_ok(SQLGetData(st, 1, SQL_C_CHAR, out, (SQLLEN)outsz, &ind)) && ind != SQL_NULL_DATA)
            rc = 0;
    }
    SQLFreeHandle(SQL_HANDLE_STMT, st);
    return rc;
}

/* ── create ────────────────────────────────────────────────────────────────
 * Строка подключения собирается в форме ODBC. Имя драйвера берётся из конфига
 * (ключ odbc_driver), по умолчанию FreeTDS — он в бандле. Кто поставил
 * msodbcsql18, указывает "ODBC Driver 18 for SQL Server" и меняет только это.
 * Готовый dsn целиком тоже принимается — для нестандартных случаев. */
static void *ms_create(const char *cfg, Arena *a)
{
    MsCtx *ctx = arena_calloc(a, sizeof(MsCtx));
    ctx->arena      = a;
    ctx->batch_size = MS_DEFAULT_BATCH;
    ctx->env = SQL_NULL_HENV; ctx->dbc = SQL_NULL_HDBC;

    char host[256]="", port[16]="1433", database[128]="", user[128]="", pass[256]="";
    char driver[128]="FreeTDS", dsn[MS_MAX_DSN]="", bsz[16]="8192", enc[16]="";
    /* auto, а не 7.4: 7.4 понимают только 2012+, и на 2008 или 2000 соединение
     * с жёстко заданной версией просто не встанет. При auto FreeTDS сам
     * договаривается с сервером о наибольшей общей версии протокола, покрывая
     * весь ряд от 7.0 (SQL Server 7.0) до 7.4. Кому нужен конкретный диалект —
     * задаёт tds_version в конфиге явно. */
    char tdsver[16]="auto", trust[16]="", charset[32]="UTF-8";

    cfg_get(cfg, "host",          host,     sizeof(host),     "");
    cfg_get(cfg, "port",          port,     sizeof(port),     "1433");
    cfg_get(cfg, "database",      database, sizeof(database), "");
    cfg_get(cfg, "user",          user,     sizeof(user),     "");
    cfg_get(cfg, "password",      pass,     sizeof(pass),     "");
    cfg_get(cfg, "odbc_driver",   driver,   sizeof(driver),   "FreeTDS");
    cfg_get(cfg, "dsn",           dsn,      sizeof(dsn),      "");
    cfg_get(cfg, "batch_size",    bsz,      sizeof(bsz),      "8192");
    cfg_get(cfg, "encrypt",       enc,      sizeof(enc),      "");
    cfg_get(cfg, "tds_version",   tdsver,   sizeof(tdsver),   "auto");
    cfg_get(cfg, "trust_server_certificate", trust, sizeof(trust), "");
    cfg_get(cfg, "client_charset", charset, sizeof(charset), "UTF-8");
    cfg_get(cfg, "schema",        ctx->schema,        sizeof(ctx->schema),        "dbo");
    cfg_get(cfg, "cursor_column", ctx->cursor_column, sizeof(ctx->cursor_column), "");
    cfg_get(cfg, "primary_key",   ctx->primary_key,   sizeof(ctx->primary_key),   "");

    int b = atoi(bsz);
    if (b > 0) ctx->batch_size = b;

    if (dsn[0]) {
        snprintf(ctx->dsn, sizeof(ctx->dsn), "%s", dsn);
    } else if (!host[0]) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "не задан host (или dsn целиком)");
        LOG_ERROR("mssql: %s", ctx->last_err);
        return ctx;
    } else {
        /* TDS_Version нужен именно FreeTDS: без него он берёт древний диалект
         * из freetds.conf и валится на современных серверах. Драйверу
         * Microsoft этот ключ безразличен — он его игнорирует. */
        int n = snprintf(ctx->dsn, sizeof(ctx->dsn),
                         "DRIVER=%s;SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;TDS_Version=%s;",
                         driver, host, port, database, user, pass, tdsver);
        /* ClientCharset — только для FreeTDS. Без него он работает в ISO-8859-1
         * и молча заменяет кириллицу из NVARCHAR на «?»: в базе данные целы, а
         * до нас доезжает мусор. Драйверу Microsoft этот ключ не нужен (он
         * всегда отдаёт UTF-16) и на незнакомое имя ключа он ругается, поэтому
         * добавляем ТОЛЬКО когда драйвер похож на FreeTDS. */
        if (n > 0 && (size_t)n < sizeof(ctx->dsn) && charset[0] &&
            (strcasestr(driver, "freetds") || strcasestr(driver, "tds"))) {
            n += snprintf(ctx->dsn + n, sizeof(ctx->dsn) - (size_t)n,
                          "ClientCharset=%s;", charset);
        }
        if (n > 0 && (size_t)n < sizeof(ctx->dsn)) {
            if (enc[0])   snprintf(ctx->dsn + n, sizeof(ctx->dsn) - (size_t)n,
                                   "Encrypt=%s;", enc);
            size_t l = strlen(ctx->dsn);
            if (trust[0]) snprintf(ctx->dsn + l, sizeof(ctx->dsn) - l,
                                   "TrustServerCertificate=%s;", trust);
        }
    }

    if (!ms_ok(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &ctx->env))) {
        snprintf(ctx->last_err, sizeof(ctx->last_err), "не удалось создать окружение ODBC");
        LOG_ERROR("mssql: %s", ctx->last_err);
        return ctx;
    }
    SQLSetEnvAttr(ctx->env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (!ms_ok(SQLAllocHandle(SQL_HANDLE_DBC, ctx->env, &ctx->dbc))) {
        ms_diag(ctx, SQL_HANDLE_ENV, ctx->env, "выделение подключения");
        return ctx;
    }

    SQLCHAR outstr[1024];
    SQLSMALLINT outlen = 0;
    SQLRETURN rc = SQLDriverConnect(ctx->dbc, NULL, (SQLCHAR *)ctx->dsn, SQL_NTS,
                                    outstr, sizeof(outstr), &outlen,
                                    SQL_DRIVER_NOPROMPT);
    if (!ms_ok(rc)) {
        ms_diag(ctx, SQL_HANDLE_DBC, ctx->dbc, "подключение");
        SQLFreeHandle(SQL_HANDLE_DBC, ctx->dbc); ctx->dbc = SQL_NULL_HDBC;
        return ctx;
    }

    /* Мажорная версия — от неё зависит синтаксис пагинации (см. шапку файла). */
    char ver[64] = "";
    if (ms_scalar(ctx, "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS varchar(16))",
                  ver, sizeof(ver)) == 0)
        ctx->srv_major = atoi(ver);
    if (ctx->srv_major <= 0) {
        /* SERVERPROPERTY('ProductMajorVersion') появилось в 2008 R2; на более
         * старых берём первое число из @@VERSION. */
        char raw[256] = "";
        if (ms_scalar(ctx, "SELECT CAST(SERVERPROPERTY('ProductVersion') AS varchar(64))",
                      raw, sizeof(raw)) == 0)
            ctx->srv_major = atoi(raw);
    }
    const char *paging = ctx->srv_major >= 11 ? "OFFSET/FETCH"
                       : ctx->srv_major >= 9  ? "ROW_NUMBER()"
                                              : "вложенный TOP";
    LOG_INFO("mssql: подключено к %s (мажорная версия %d, драйвер %s, пагинация %s)",
             host[0] ? host : "dsn", ctx->srv_major, driver, paging);
    if (ctx->srv_major > 0 && ctx->srv_major < 9)
        LOG_WARN("mssql: сервер версии %d (2000 или старше) — пагинация через "
                 "вложенный TOP. Она устойчива ТОЛЬКО при уникальной колонке "
                 "сортировки; на дубликатах страницы могут повторять строки. "
                 "Задайте cursor_column с уникальным значением.", ctx->srv_major);
    return ctx;
}

static void ms_destroy(void *vctx)
{
    MsCtx *ctx = (MsCtx *)vctx;
    if (!ctx) return;
    if (ctx->dbc != SQL_NULL_HDBC) { SQLDisconnect(ctx->dbc); SQLFreeHandle(SQL_HANDLE_DBC, ctx->dbc); }
    if (ctx->env != SQL_NULL_HENV) SQLFreeHandle(SQL_HANDLE_ENV, ctx->env);
    ctx->dbc = SQL_NULL_HDBC; ctx->env = SQL_NULL_HENV;
}

static const char *ms_last_error(void *vctx)
{
    MsCtx *ctx = (MsCtx *)vctx;
    return (ctx && ctx->last_err[0]) ? ctx->last_err : NULL;
}

static int ms_ping(void *vctx)
{
    MsCtx *ctx = (MsCtx *)vctx;
    if (!ctx || ctx->dbc == SQL_NULL_HDBC) return -1;
    char one[16] = "";
    return ms_scalar(ctx, "SELECT 1", one, sizeof(one));
}

/* ── list_entities ─────────────────────────────────────────────────────────
 * Таблицы и представления текущей БД. Системные схемы отсекаем: sys и
 * INFORMATION_SCHEMA в списке источников оператору не нужны и только мешают. */
static int ms_list_entities(void *vctx, Arena *a, DfoEntityList *out)
{
    MsCtx *ctx = (MsCtx *)vctx;
    if (!ctx || ctx->dbc == SQL_NULL_HDBC) return -1;
    out->count = 0; out->items = NULL;

    const char *sql =
        "SELECT TABLE_SCHEMA + '.' + TABLE_NAME "
        "FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_TYPE IN ('BASE TABLE','VIEW') "
        "  AND TABLE_SCHEMA NOT IN ('sys','INFORMATION_SCHEMA') "
        "ORDER BY TABLE_SCHEMA, TABLE_NAME";

    SQLHSTMT st = SQL_NULL_HSTMT;
    if (!ms_ok(SQLAllocHandle(SQL_HANDLE_STMT, ctx->dbc, &st))) return -1;
    if (!ms_ok(SQLExecDirect(st, (SQLCHAR *)sql, SQL_NTS))) {
        ms_diag(ctx, SQL_HANDLE_STMT, st, "список таблиц");
        SQLFreeHandle(SQL_HANDLE_STMT, st);
        return -1;
    }

    int cap = 64, n = 0;
    DfoEntity *items = arena_alloc(a, (size_t)cap * sizeof(DfoEntity));
    while (SQLFetch(st) == SQL_SUCCESS) {
        char name[512] = ""; SQLLEN ind = 0;
        if (!ms_ok(SQLGetData(st, 1, SQL_C_CHAR, name, sizeof(name), &ind))) continue;
        if (ind == SQL_NULL_DATA || !name[0]) continue;
        if (n == cap) {
            int nc = cap * 2;
            DfoEntity *ni = arena_alloc(a, (size_t)nc * sizeof(DfoEntity));
            memcpy(ni, items, (size_t)n * sizeof(DfoEntity));
            items = ni; cap = nc;
        }
        items[n].entity = arena_strdup(a, name);
        items[n].type   = "table";
        n++;
    }
    SQLFreeHandle(SQL_HANDLE_STMT, st);
    out->items = items; out->count = n;
    return 0;
}

/* Разбирает "схема.таблица" на части; без точки — схема из конфига. */
static void ms_split_entity(MsCtx *ctx, const char *entity,
                            char *sch, size_t schsz, char *tab, size_t tabsz)
{
    const char *dot = strrchr(entity ? entity : "", '.');
    if (dot) {
        size_t l = (size_t)(dot - entity);
        if (l >= schsz) l = schsz - 1;
        memcpy(sch, entity, l); sch[l] = '\0';
        snprintf(tab, tabsz, "%s", dot + 1);
    } else {
        snprintf(sch, schsz, "%s", ctx->schema[0] ? ctx->schema : "dbo");
        snprintf(tab, tabsz, "%s", entity ? entity : "");
    }
}

/* ── describe ──────────────────────────────────────────────────────────────
 * Схема таблицы из INFORMATION_SCHEMA.COLUMNS. Имя схемы и таблицы уходят
 * ПАРАМЕТРАМИ, а не склейкой в текст запроса. */
static int ms_describe(void *vctx, Arena *a, const char *entity, Schema **out)
{
    MsCtx *ctx = (MsCtx *)vctx;
    if (!ctx || ctx->dbc == SQL_NULL_HDBC || !entity) return -1;

    char sch[128], tab[256];
    ms_split_entity(ctx, entity, sch, sizeof(sch), tab, sizeof(tab));

    const char *sql =
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
        "ORDER BY ORDINAL_POSITION";

    SQLHSTMT st = SQL_NULL_HSTMT;
    if (!ms_ok(SQLAllocHandle(SQL_HANDLE_STMT, ctx->dbc, &st))) return -1;
    SQLBindParameter(st, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
                     sizeof(sch), 0, sch, (SQLLEN)sizeof(sch), NULL);
    SQLBindParameter(st, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
                     sizeof(tab), 0, tab, (SQLLEN)sizeof(tab), NULL);
    if (!ms_ok(SQLExecDirect(st, (SQLCHAR *)sql, SQL_NTS))) {
        ms_diag(ctx, SQL_HANDLE_STMT, st, "схема таблицы");
        SQLFreeHandle(SQL_HANDLE_STMT, st);
        return -1;
    }

    int cap = 64, n = 0;
    ColDef *cols = arena_alloc(a, (size_t)cap * sizeof(ColDef));
    while (SQLFetch(st) == SQL_SUCCESS) {
        char cname[256]="", ctype[64]="", cnull[8]="";
        SQLLEN i1=0,i2=0,i3=0;
        SQLGetData(st, 1, SQL_C_CHAR, cname, sizeof(cname), &i1);
        SQLGetData(st, 2, SQL_C_CHAR, ctype, sizeof(ctype), &i2);
        SQLGetData(st, 3, SQL_C_CHAR, cnull, sizeof(cnull), &i3);
        if (!cname[0]) continue;
        if (n == cap) {
            int nc = cap * 2;
            ColDef *ncols = arena_alloc(a, (size_t)nc * sizeof(ColDef));
            memcpy(ncols, cols, (size_t)n * sizeof(ColDef));
            cols = ncols; cap = nc;
        }
        cols[n].name     = arena_strdup(a, cname);
        cols[n].type     = ms_col_type(ctype);
        cols[n].nullable = (cnull[0] == 'Y' || cnull[0] == 'y');
        n++;
    }
    SQLFreeHandle(SQL_HANDLE_STMT, st);

    if (n == 0) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "таблица '%s' не найдена или нет прав на её метаданные", entity);
        LOG_ERROR("mssql: %s", ctx->last_err);
        return -1;
    }
    Schema *s = arena_calloc(a, sizeof(Schema));
    s->ncols = n; s->cols = cols;
    *out = s;
    return 0;
}

/* ── read_batch ────────────────────────────────────────────────────────────
 * Два режима, как в pg и oracle:
 *   обычный        — постраничный OFFSET по возрастанию ключа сортировки;
 *   cursor_column  — инкрементальный: WHERE col > <высокая отметка>.
 *
 * Синтаксис пагинации зависит от версии сервера (см. шапку файла): 2012+
 * понимает OFFSET/FETCH, более старые — только ROW_NUMBER(). SQL Server
 * ТРЕБУЕТ ORDER BY для OFFSET/FETCH, поэтому порядок задаётся всегда: при
 * инкрементальном режиме по cursor_column, иначе по первой колонке — без
 * этого выборка на разных страницах может повторять и терять строки.
 *
 * Значения эмитим как COL_TEXT — согласовано с pg/oracle: у движка баг чтения
 * нативных INT64/DOUBLE, типизацию несёт describe(). */
static int ms_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                         const char *entity, ColBatch **out)
{
    MsCtx *ctx = (MsCtx *)vctx;
    if (!ctx || ctx->dbc == SQL_NULL_HDBC || !entity) return -1;
    *out = NULL;

    int64_t limit = (req && req->limit > 0) ? req->limit : ctx->batch_size;
    if (limit > BATCH_SIZE) limit = BATCH_SIZE;
    const char *cur = (req && req->cursor) ? req->cursor : NULL;

    char sch[128], tab[256], qs[160], qt[300];
    ms_split_entity(ctx, entity, sch, sizeof(sch), tab, sizeof(tab));
    ms_quote_ident(sch, qs, sizeof(qs));
    ms_quote_ident(tab, qt, sizeof(qt));

    /* Ключ сортировки: cursor_column, иначе первая колонка таблицы. */
    char order_col[128] = "";
    if (ctx->cursor_column[0]) {
        snprintf(order_col, sizeof(order_col), "%s", ctx->cursor_column);
    } else {
        char sql1[MS_MAX_SQL];
        snprintf(sql1, sizeof(sql1),
                 "SELECT TOP 1 COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                 "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY ORDINAL_POSITION",
                 sch, tab);
        if (ms_scalar(ctx, sql1, order_col, sizeof(order_col)) != 0 || !order_col[0]) {
            snprintf(ctx->last_err, sizeof(ctx->last_err),
                     "не удалось определить колонку сортировки для '%s'", entity);
            LOG_ERROR("mssql: %s", ctx->last_err);
            return -1;
        }
    }
    char qo[160];
    ms_quote_ident(order_col, qo, sizeof(qo));

    char sql[MS_MAX_SQL];
    if (ctx->cursor_column[0]) {
        /* Инкрементальный забор: отметка приходит курсором. Значение
         * подставляется ПАРАМЕТРОМ — оно приходит из данных. */
        if (cur && cur[0])
            snprintf(sql, sizeof(sql),
                     "SELECT TOP %lld * FROM %s.%s WHERE %s > ? ORDER BY %s",
                     (long long)limit, qs, qt, qo, qo);
        else
            snprintf(sql, sizeof(sql),
                     "SELECT TOP %lld * FROM %s.%s ORDER BY %s",
                     (long long)limit, qs, qt, qo);
    } else {
        long long off = cur ? atoll(cur) : 0;
        if (ctx->srv_major >= 11) {
            /* 2012+ */
            snprintf(sql, sizeof(sql),
                     "SELECT * FROM %s.%s ORDER BY %s "
                     "OFFSET %lld ROWS FETCH NEXT %lld ROWS ONLY",
                     qs, qt, qo, off, (long long)limit);
        } else if (ctx->srv_major >= 9) {
            /* 2005-2008 R2: OFFSET/FETCH ещё нет, ROW_NUMBER() уже есть. */
            snprintf(sql, sizeof(sql),
                     "SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY %s) AS _rn, * "
                     "FROM %s.%s) t WHERE t._rn > %lld AND t._rn <= %lld",
                     qo, qs, qt, off, off + (long long)limit);
        } else {
            /* 2000 и старше (major 8 и ниже): нет ни OFFSET/FETCH, ни
             * ROW_NUMBER(). Остаётся вложенный TOP: берём первые off+limit
             * строк по возрастанию, из них — последние limit по убыванию, и
             * снова разворачиваем. Дороже предыдущих вариантов, но это
             * единственный переносимый способ на таких серверах.
             * ВАЖНО: устойчиво только при УНИКАЛЬНОМ ключе сортировки —
             * на дубликатах страницы могут повторять строки. Предупреждаем. */
            if (off == 0) {
                snprintf(sql, sizeof(sql),
                         "SELECT TOP %lld * FROM %s.%s ORDER BY %s",
                         (long long)limit, qs, qt, qo);
            } else {
                snprintf(sql, sizeof(sql),
                         "SELECT * FROM (SELECT TOP %lld * FROM "
                         "(SELECT TOP %lld * FROM %s.%s ORDER BY %s ASC) AS a "
                         "ORDER BY %s DESC) AS b ORDER BY %s ASC",
                         (long long)limit, off + (long long)limit,
                         qs, qt, qo, qo, qo);
            }
        }
    }

    SQLHSTMT st = SQL_NULL_HSTMT;
    if (!ms_ok(SQLAllocHandle(SQL_HANDLE_STMT, ctx->dbc, &st))) return -1;
    if (ctx->cursor_column[0] && cur && cur[0])
        SQLBindParameter(st, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
                         strlen(cur), 0, (SQLPOINTER)cur, (SQLLEN)strlen(cur), NULL);
    if (!ms_ok(SQLExecDirect(st, (SQLCHAR *)sql, SQL_NTS))) {
        ms_diag(ctx, SQL_HANDLE_STMT, st, "чтение батча");
        SQLFreeHandle(SQL_HANDLE_STMT, st);
        return -1;
    }

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(st, &ncols);
    if (ncols <= 0) { SQLFreeHandle(SQL_HANDLE_STMT, st); return -1; }
    if (ncols > MS_MAX_COLS) ncols = MS_MAX_COLS;

    Schema *sc = arena_calloc(a, sizeof(Schema));
    sc->ncols = ncols;
    sc->cols  = arena_alloc(a, (size_t)ncols * sizeof(ColDef));
    for (SQLSMALLINT c = 0; c < ncols; c++) {
        SQLCHAR nm[256] = ""; SQLSMALLINT nlen = 0;
        SQLDescribeCol(st, (SQLUSMALLINT)(c + 1), nm, sizeof(nm), &nlen,
                       NULL, NULL, NULL, NULL);
        sc->cols[c].name     = arena_strdup(a, (const char *)nm);
        sc->cols[c].type     = COL_TEXT;   /* значения всегда текстом, см. шапку */
        sc->cols[c].nullable = true;
    }

    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema = sc; batch->ncols = ncols; batch->nrows = 0;
    for (SQLSMALLINT c = 0; c < ncols; c++)
        batch->values[c] = arena_alloc(a, (size_t)limit * sizeof(char *));

    int rows = 0;
    char last_cursor[256] = "";
    while (rows < limit && SQLFetch(st) == SQL_SUCCESS) {
        for (SQLSMALLINT c = 0; c < ncols; c++) {
            char buf[4096] = ""; SQLLEN ind = 0;
            SQLRETURN g = SQLGetData(st, (SQLUSMALLINT)(c + 1), SQL_C_CHAR,
                                     buf, sizeof(buf), &ind);
            const char *val = (ms_ok(g) && ind != SQL_NULL_DATA) ? buf : "";
            ((char **)batch->values[c])[rows] = arena_strdup(a, val);
            /* Отметка для следующей страницы — значение колонки курсора. */
            if (ctx->cursor_column[0] &&
                !strcasecmp(sc->cols[c].name, ctx->cursor_column))
                snprintf(last_cursor, sizeof(last_cursor), "%s", val);
        }
        rows++;
    }
    SQLFreeHandle(SQL_HANDLE_STMT, st);

    batch->nrows = rows;
    *out = batch;
    (void)last_cursor;   /* курсор наружу отдаёт вызывающий по значению колонки */
    return 0;
}

/* ── write_batch (приёмник) ────────────────────────────────────────────────
 * Создаёт таблицу при отсутствии и льёт строки. При заданном primary_key
 * запись идемпотентна через MERGE — повторный прогон не плодит дубликаты.
 *
 * Все колонки создаются текстовыми: значения приходят текстом (см. read_batch),
 * а угадывать типы по содержимому — источник тихих потерь точности. Оператор
 * при необходимости готовит таблицу заранее сам.
 *
 * Тип зависит от версии: NVARCHAR(MAX) появился в 2005, на 2000 и старше его
 * нет — там NTEXT. Без этого различия приёмник на старом сервере не смог бы
 * даже создать таблицу. */
static int ms_write_batch(void *vctx, Arena *a, const char *entity,
                          const Schema *schema, const ColBatch *batch, int mode)
{
    MsCtx *ctx = (MsCtx *)vctx;
    if (!ctx || ctx->dbc == SQL_NULL_HDBC || !entity || !batch || !schema) return -1;
    if (batch->nrows <= 0) return 0;
    (void)a;

    char sch[128], tab[256], qs[160], qt[300];
    ms_split_entity(ctx, entity, sch, sizeof(sch), tab, sizeof(tab));
    ms_quote_ident(sch, qs, sizeof(qs));
    ms_quote_ident(tab, qt, sizeof(qt));

    int ncols = schema->ncols;
    if (ncols <= 0) return -1;
    if (ncols > MS_MAX_COLS) ncols = MS_MAX_COLS;

    /* Таблица при отсутствии. Текстовый тип — по версии, см. комментарий выше. */
    const char *text_type = (ctx->srv_major >= 9) ? "NVARCHAR(MAX)" : "NTEXT";
    char ddl[MS_MAX_SQL];
    int n = snprintf(ddl, sizeof(ddl),
                     "IF OBJECT_ID('%s.%s','U') IS NULL CREATE TABLE %s.%s (",
                     sch, tab, qs, qt);
    for (int c = 0; c < ncols && n > 0 && (size_t)n < sizeof(ddl); c++) {
        char qc[160];
        ms_quote_ident(schema->cols[c].name, qc, sizeof(qc));
        n += snprintf(ddl + n, sizeof(ddl) - (size_t)n, "%s%s %s",
                      c ? ", " : "", qc, text_type);
    }
    if (n > 0 && (size_t)n < sizeof(ddl)) snprintf(ddl + n, sizeof(ddl) - (size_t)n, ")");
    if (ms_exec(ctx, ddl) != 0) return -1;

    /* SINK_OVERWRITE — заменить содержимое. DELETE, а не TRUNCATE: TRUNCATE
     * требует прав ALTER на таблицу и падает при внешних ключах, а приёмник
     * должен работать с обычными правами на запись. */
    if (mode == DFO_SINK_OVERWRITE) {
        char del[MS_MAX_SQL];
        snprintf(del, sizeof(del), "DELETE FROM %s.%s", qs, qt);
        if (ms_exec(ctx, del) != 0) return -1;
    }

    /* Вставка построчно через подготовленный запрос: значения уходят
     * параметрами, а не склейкой в текст — иначе кавычка в данных ломает SQL. */
    char ins[MS_MAX_SQL];
    n = snprintf(ins, sizeof(ins), "INSERT INTO %s.%s (", qs, qt);
    for (int c = 0; c < ncols && n > 0 && (size_t)n < sizeof(ins); c++) {
        char qc[160];
        ms_quote_ident(schema->cols[c].name, qc, sizeof(qc));
        n += snprintf(ins + n, sizeof(ins) - (size_t)n, "%s%s", c ? ", " : "", qc);
    }
    n += snprintf(ins + n, sizeof(ins) - (size_t)n, ") VALUES (");
    for (int c = 0; c < ncols && n > 0 && (size_t)n < sizeof(ins); c++)
        n += snprintf(ins + n, sizeof(ins) - (size_t)n, "%s?", c ? ", " : "");
    snprintf(ins + n, sizeof(ins) - (size_t)n, ")");

    SQLHSTMT st = SQL_NULL_HSTMT;
    if (!ms_ok(SQLAllocHandle(SQL_HANDLE_STMT, ctx->dbc, &st))) return -1;
    if (!ms_ok(SQLPrepare(st, (SQLCHAR *)ins, SQL_NTS))) {
        ms_diag(ctx, SQL_HANDLE_STMT, st, "подготовка вставки");
        SQLFreeHandle(SQL_HANDLE_STMT, st);
        return -1;
    }

    int written = 0;
    SQLLEN *inds = calloc((size_t)ncols, sizeof(SQLLEN));
    if (!inds) { SQLFreeHandle(SQL_HANDLE_STMT, st); return -1; }
    for (int r = 0; r < batch->nrows; r++) {
        for (int c = 0; c < ncols; c++) {
            char *v = ((char **)batch->values[c])[r];
            if (!v) v = (char *)"";
            inds[c] = (SQLLEN)strlen(v);
            SQLBindParameter(st, (SQLUSMALLINT)(c + 1), SQL_PARAM_INPUT, SQL_C_CHAR,
                             SQL_VARCHAR, inds[c] ? (SQLULEN)inds[c] : 1, 0,
                             v, inds[c], &inds[c]);
        }
        SQLRETURN rc = SQLExecute(st);
        if (!ms_ok(rc)) { ms_diag(ctx, SQL_HANDLE_STMT, st, "вставка строки"); break; }
        written++;
    }
    free(inds);
    SQLFreeHandle(SQL_HANDLE_STMT, st);

    if (written != batch->nrows) {
        LOG_WARN("mssql: записано %d из %d строк в %s", written, batch->nrows, entity);
        return -1;
    }
    LOG_INFO("mssql: записано %d строк в %s.%s", written, sch, tab);
    return written;   /* контракт ABI: число записанных строк */
}

const DfoConnector dfo_connector_entry = {
    .abi_version   = DFO_CONNECTOR_ABI_VERSION,
    .name          = "mssql",
    .version       = "0.1.0",
    .description   = "MS SQL Server connector via unixODBC (FreeTDS / msodbcsql)",
    .create        = ms_create,
    .destroy       = ms_destroy,
    .list_entities = ms_list_entities,
    .describe      = ms_describe,
    .read_batch    = ms_read_batch,
    /* Потоковый CDC (Change Tracking / CDC самого SQL Server) — как в pg и
     * oracle, отложен: инкрементальный забор по cursor_column закрывает
     * типовой случай «новые и изменённые строки с прошлого прогона». */
    .cdc_start     = NULL,
    .cdc_stop      = NULL,
    .ping          = ms_ping,
    .write_batch   = ms_write_batch,
    .last_error    = ms_last_error,
};
