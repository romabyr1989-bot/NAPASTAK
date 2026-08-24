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
/* strcasestr — расширение BSD/Darwin, которое глобальный _POSIX_C_SOURCE
 * на macOS скрывает. На Linux/glibc no-op (покрыто _GNU_SOURCE).
 * То же, что в src/gateway/api.c. */
#define _DARWIN_C_SOURCE
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
    char     driver_used[128];     /* имя драйвера, которым подключились */
    char     host[256];            /* для внятного текста в сообщениях об обрыве */
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

/* SQLSTATE первой записи диагностики. Нужен, чтобы отличить «такого драйвера
 * в системе нет» (IM002) от «драйвер есть, но сервер отказал»: перебирать
 * кандидатов имеет смысл только в первом случае — иначе неверный пароль
 * превратится в пять попыток подключения подряд. */
static void ms_sqlstate(SQLSMALLINT type, SQLHANDLE h, char out[6])
{
    SQLCHAR state[6] = "", msg[512];
    SQLINTEGER native = 0;
    SQLSMALLINT len = 0;
    out[0] = 0;
    if (SQLGetDiagRec(type, h, 1, state, &native, msg, sizeof(msg), &len) == SQL_SUCCESS)
        snprintf(out, 6, "%s", (const char *)state);
}

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
    char srvver[16]="";
    char tdsver[16]="auto", trust[16]="", charset[32]="UTF-8";

    cfg_get(cfg, "host",          host,     sizeof(host),     "");
    cfg_get(cfg, "port",          port,     sizeof(port),     "1433");
    cfg_get(cfg, "database",      database, sizeof(database), "");
    cfg_get(cfg, "user",          user,     sizeof(user),     "");
    cfg_get(cfg, "password",      pass,     sizeof(pass),     "");
    cfg_get(cfg, "odbc_driver",   driver,   sizeof(driver),   "");
    cfg_get(cfg, "dsn",           dsn,      sizeof(dsn),      "");
    cfg_get(cfg, "batch_size",    bsz,      sizeof(bsz),      "8192");
    cfg_get(cfg, "encrypt",       enc,      sizeof(enc),      "");
    cfg_get(cfg, "tds_version",   tdsver,   sizeof(tdsver),   "auto");
    cfg_get(cfg, "server_version", srvver,   sizeof(srvver),   "");
    cfg_get(cfg, "trust_server_certificate", trust, sizeof(trust), "");
    cfg_get(cfg, "client_charset", charset, sizeof(charset), "UTF-8");
    cfg_get(cfg, "schema",        ctx->schema,        sizeof(ctx->schema),        "dbo");
    cfg_get(cfg, "cursor_column", ctx->cursor_column, sizeof(ctx->cursor_column), "");
    cfg_get(cfg, "primary_key",   ctx->primary_key,   sizeof(ctx->primary_key),   "");

    int b = atoi(bsz);
    if (b > 0) ctx->batch_size = b;

    /* Имя драйвера ODBC. Если оператор указал своё — берём только его: чужой
     * драйвер молча подменять нельзя. Если поле пустое, перебираем кандидатов.
     * Первым идёт вложенный в поставку (его регистрирует install.sh под
     * именем NAPASTAK-FreeTDS) — он собран и проверен вместе с коннектором,
     * тогда как системный FreeTDS может оказаться сколь угодно старым. Дальше
     * системный FreeTDS и драйверы Microsoft: машина, где стоит msodbcsql18,
     * подключается без правки настроек. */
    static const char *const CANDIDATES[] = {
        "NAPASTAK-FreeTDS", "FreeTDS",
        "ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server",
        "SQL Server", NULL
    };
    const char *tried[8];
    int ntried = 0;

    if (!dsn[0] && !host[0]) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "не задан host (или dsn целиком)");
        LOG_ERROR("mssql: %s", ctx->last_err);
        return ctx;
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

    SQLRETURN rc = SQL_ERROR;
    for (int cand = 0; ; cand++) {
        if (dsn[0]) {
            snprintf(ctx->dsn, sizeof(ctx->dsn), "%s", dsn);
        } else {
            const char *drv = driver[0] ? driver : CANDIDATES[cand];
            if (!drv) break;                       /* кандидаты кончились */
            if (ntried < (int)(sizeof(tried)/sizeof(tried[0]))) tried[ntried++] = drv;
            snprintf(ctx->driver_used, sizeof(ctx->driver_used), "%s", drv);

            /* TDS_Version нужен именно FreeTDS: без него он берёт древний
             * диалект из freetds.conf и валится на современных серверах.
             * Драйверу Microsoft этот ключ безразличен — он его игнорирует. */
            int n = snprintf(ctx->dsn, sizeof(ctx->dsn),
                             "DRIVER=%s;SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;TDS_Version=%s;",
                             drv, host, port, database, user, pass, tdsver);
            /* ClientCharset — только для FreeTDS. Без него он работает в
             * ISO-8859-1 и молча заменяет кириллицу из NVARCHAR на «?»: в базе
             * данные целы, а до нас доезжает мусор. Драйверу Microsoft этот
             * ключ не нужен (он всегда отдаёт UTF-16) и на незнакомое имя
             * ключа он ругается, поэтому добавляем ТОЛЬКО когда драйвер похож
             * на FreeTDS. */
            if (n > 0 && (size_t)n < sizeof(ctx->dsn) && charset[0] &&
                (strcasestr(drv, "freetds") || strcasestr(drv, "tds"))) {
                n += snprintf(ctx->dsn + n, sizeof(ctx->dsn) - (size_t)n,
                              "ClientCharset=%s;", charset);
            }
            /* Шифрование канала называется у драйверов ПО-РАЗНОМУ, и чужой
             * ключ просто игнорируется — настройка молча не работает, что и
             * обнаружилось на проверке: «Encrypt=no» не отключал шифрование.
             * У Microsoft это Encrypt=yes|no плюс TrustServerCertificate,
             * у FreeTDS — Encryption=require|off (проверку сертификата он не
             * выполняет вовсе, поэтому доверие там задавать нечем). */
            bool is_tds = strcasestr(drv, "freetds") || strcasestr(drv, "tds");
            if (n > 0 && (size_t)n < sizeof(ctx->dsn) && enc[0]) {
                bool on = (strcasecmp(enc, "yes") == 0 || strcasecmp(enc, "true") == 0 ||
                           strcasecmp(enc, "require") == 0 || strcmp(enc, "1") == 0);
                n += snprintf(ctx->dsn + n, sizeof(ctx->dsn) - (size_t)n,
                              is_tds ? "Encryption=%s;" : "Encrypt=%s;",
                              is_tds ? (on ? "require" : "off") : (on ? "yes" : "no"));
            }
            if (n > 0 && (size_t)n < sizeof(ctx->dsn) && trust[0] && !is_tds)
                snprintf(ctx->dsn + n, sizeof(ctx->dsn) - (size_t)n,
                         "TrustServerCertificate=%s;", trust);
        }

        SQLCHAR outstr[1024];
        SQLSMALLINT outlen = 0;
        rc = SQLDriverConnect(ctx->dbc, NULL, (SQLCHAR *)ctx->dsn, SQL_NTS,
                              outstr, sizeof(outstr), &outlen, SQL_DRIVER_NOPROMPT);
        if (ms_ok(rc)) break;

        /* Следующего кандидата пробуем ТОЛЬКО если этого драйвера нет в
         * системе. Любой другой отказ — не наше дело перебирать: сервер
         * ответил, и повторять с другим драйвером бессмысленно и медленно. */
        char state[6];
        ms_sqlstate(SQL_HANDLE_DBC, ctx->dbc, state);
        if (dsn[0] || driver[0] || strcmp(state, "IM002") != 0) break;
    }

    if (!ms_ok(rc)) {
        ms_diag(ctx, SQL_HANDLE_DBC, ctx->dbc, "подключение");
        /* Перебрали всё и не нашли ни одного драйвера — подсказываем, что
         * именно искали: иначе «Data source name not found» ничего не говорит
         * о том, какие имена коннектор считает своими. */
        if (ntried > 1) {
            size_t l = strlen(ctx->last_err);
            l += (size_t)snprintf(ctx->last_err + l, sizeof(ctx->last_err) - l,
                                  ". Проверены драйверы ODBC:");
            for (int i = 0; i < ntried && l < sizeof(ctx->last_err) - 1; i++)
                l += (size_t)snprintf(ctx->last_err + l, sizeof(ctx->last_err) - l,
                                      "%s «%s»", i ? "," : "", tried[i]);
            snprintf(ctx->last_err + l, sizeof(ctx->last_err) - l,
                     ". Задайте своё имя в поле «Драйвер ODBC» или "
                     "зарегистрируйте драйвер в /etc/odbcinst.ini");
            LOG_ERROR("mssql: %s", ctx->last_err);
        }
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

    /* Ручное переопределение версии. Нужно в двух случаях. Первый рабочий:
     * сервер за прокси или с урезанными правами не отдаёт SERVERPROPERTY, и
     * без подсказки коннектор свалится в самый консервативный синтаксис.
     * Второй — проверочный: синтаксис для 2005-2008R2 и для 2000 иначе негде
     * исполнить. Образов SQL Server под Linux старше 2017 не существует, а
     * ROW_NUMBER() и вложенный TOP понимают и новые серверы — занизив версию,
     * мы гоняем ровно тот SQL, который поедет на старый сервер, но против
     * живой СУБД. */
    if (srvver[0]) {
        int forced = atoi(srvver);
        if (forced > 0) {
            LOG_WARN("mssql: версия сервера задана вручную: %d (определено %d). "
                     "Синтаксис пагинации и типы колонок выбираются по заданной.",
                     forced, ctx->srv_major);
            ctx->srv_major = forced;
        }
    }
    snprintf(ctx->host, sizeof(ctx->host), "%s", host[0] ? host : "по DSN");
    const char *paging = ctx->srv_major >= 11 ? "OFFSET/FETCH"
                       : ctx->srv_major >= 9  ? "ROW_NUMBER()"
                                              : "вложенный TOP";
    LOG_INFO("mssql: подключено к %s (мажорная версия %d, драйвер %s, пагинация %s)",
             host[0] ? host : "dsn", ctx->srv_major,
             ctx->driver_used[0] ? ctx->driver_used : "из DSN", paging);
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
    if (!ctx || ctx->dbc == SQL_NULL_HDBC) {
        if (ctx) snprintf(ctx->last_err, sizeof(ctx->last_err),
                          "нет подключения к серверу");
        return -1;
    }
    char one[16] = "";
    if (ms_scalar(ctx, "SELECT 1", one, sizeof(one)) == 0) return 0;

    /* Причина ложится в last_err и попадает прямо в интерфейс — рядом с
     * погасшим индикатором подключения. Драйвер на оборванной сессии отвечает
     * «[HY000] Unknown error», по которому оператору нечего понять; дописываем
     * то, что произошло на самом деле, сохраняя и текст драйвера. */
    char drv[320];
    snprintf(drv, sizeof(drv), "%s", ctx->last_err);
    snprintf(ctx->last_err, sizeof(ctx->last_err),
             "сервер %s не отвечает — связь потеряна (%s)",
             ctx->host[0] ? ctx->host : "SQL Server", drv);
    return -1;
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
/* ── Чтение значения любой длины ───────────────────────────────────────────
 * SQLGetData в фиксированный буфер молча режет длинные значения: драйвер
 * возвращает SQL_SUCCESS_WITH_INFO с состоянием 01004 («String data, right
 * truncated»), а такой код проходит проверку успеха. NVARCHAR(MAX) и NTEXT
 * при этом теряли бы всё после первых килобайт, и заметить это в конвейере
 * невозможно — данные просто приезжают короче. Поэтому дочитываем в цикле,
 * пока драйвер сообщает об усечении. */
static const char *ms_get_text(MsCtx *ctx, Arena *a, SQLHSTMT st,
                               SQLUSMALLINT col, SQLLEN *ind_out)
{
    char chunk[4096];
    SQLLEN ind = 0;
    SQLRETURN g = SQLGetData(st, col, SQL_C_CHAR, chunk, sizeof(chunk), &ind);
    *ind_out = ind;
    if (g == SQL_NO_DATA || !ms_ok(g) || ind == SQL_NULL_DATA) return NULL;

    /* Признак «влезло не всё» — сам код возврата: SQL_SUCCESS_WITH_INFO.
     * Сверяться с SQLSTATE 01004 не годится: драйвер вправе не заполнять
     * диагностику при усечении, и тогда мы принимали бы первый кусок за всё
     * значение — NVARCHAR(MAX) молча приезжал бы обрезанным до 4 КБ. */
    if (g != SQL_SUCCESS_WITH_INFO) return arena_strdup(a, chunk);

    size_t cap = sizeof(chunk) * 4, len = strlen(chunk);
    char *buf = arena_alloc(a, cap);
    memcpy(buf, chunk, len + 1);

    while (g == SQL_SUCCESS_WITH_INFO) {
        g = SQLGetData(st, col, SQL_C_CHAR, chunk, sizeof(chunk), &ind);
        if (g == SQL_NO_DATA || !ms_ok(g)) break;
        size_t add = strlen(chunk);
        if (add == 0) break;
        if (len + add + 1 > cap) {
            size_t ncap = cap * 2;
            while (len + add + 1 > ncap) ncap *= 2;
            char *nb = arena_alloc(a, ncap);
            memcpy(nb, buf, len + 1);
            buf = nb; cap = ncap;
        }
        memcpy(buf + len, chunk, add + 1);
        len += add;
    }
    (void)ctx;
    return buf;
}

/* Список колонок таблицы с префиксом алиаса — «t.[id], t.[name], …».
 * Нужен ветке ROW_NUMBER(): наружу из подзапроса нельзя выпускать «*», иначе
 * туда попадёт служебный номер строки. */
static int ms_column_list(MsCtx *ctx, const char *sch, const char *tab,
                          const char *alias, char *out, size_t outsz)
{
    char sql[MS_MAX_SQL];
    snprintf(sql, sizeof(sql),
             "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
             "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' "
             "ORDER BY ORDINAL_POSITION", sch, tab);

    SQLHSTMT st = SQL_NULL_HSTMT;
    if (!ms_ok(SQLAllocHandle(SQL_HANDLE_STMT, ctx->dbc, &st))) return -1;
    if (!ms_ok(SQLExecDirect(st, (SQLCHAR *)sql, SQL_NTS))) {
        ms_diag(ctx, SQL_HANDLE_STMT, st, "список колонок");
        SQLFreeHandle(SQL_HANDLE_STMT, st);
        return -1;
    }
    size_t off = 0;
    int n = 0;
    while (SQLFetch(st) == SQL_SUCCESS) {
        char nm[256] = ""; SQLLEN ind = 0;
        if (!ms_ok(SQLGetData(st, 1, SQL_C_CHAR, nm, sizeof(nm), &ind))) continue;
        char q[300];
        ms_quote_ident(nm, q, sizeof(q));
        int w = snprintf(out + off, outsz - off, "%s%s.%s", n ? ", " : "", alias, q);
        if (w < 0 || (size_t)w >= outsz - off) break;
        off += (size_t)w; n++;
    }
    SQLFreeHandle(SQL_HANDLE_STMT, st);
    if (n == 0) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "не удалось получить список колонок %s.%s", sch, tab);
        LOG_ERROR("mssql: %s", ctx->last_err);
        return -1;
    }
    return 0;
}

/* Исполняет готовый SELECT и собирает батч. Общая часть для чтения таблицы и
 * произвольного запроса: разбор результата, схема из выдачи, NULL и длинные
 * значения — всё одинаково, различается только сам SQL. */
static int ms_fetch_sql(MsCtx *ctx, Arena *a, const char *sql, const char *bind_cur,
                        int64_t limit, bool warn_truncate, ColBatch **out)
{
    SQLHSTMT st = SQL_NULL_HSTMT;
    if (!ms_ok(SQLAllocHandle(SQL_HANDLE_STMT, ctx->dbc, &st))) return -1;
    if (bind_cur && bind_cur[0])
        SQLBindParameter(st, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
                         strlen(bind_cur), 0, (SQLPOINTER)bind_cur,
                         (SQLLEN)strlen(bind_cur), NULL);
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

    for (SQLSMALLINT c = 0; c < ncols; c++)
        batch->null_bitmap[c] = arena_calloc(a, ((size_t)limit + 7) / 8);

    int rows = 0;
    char last_cursor[256] = "";
    while (rows < limit && SQLFetch(st) == SQL_SUCCESS) {
        for (SQLSMALLINT c = 0; c < ncols; c++) {
            SQLLEN ind = 0;
            const char *val = ms_get_text(ctx, a, st, (SQLUSMALLINT)(c + 1), &ind);
            if (ind == SQL_NULL_DATA) {
                /* NULL и пустая строка — разные значения. Отмечаем в битовой
                 * карте и кладём общий для проекта маркер: без него запись в
                 * приёмник превращала бы NULL в «», ломая IS NULL и внешние
                 * ключи (та же ошибка уже была в write_batch). */
                batch->null_bitmap[c][rows / 8] |= (uint8_t)(1u << (rows % 8));
                ((char **)batch->values[c])[rows] = (char *)DFO_NULL_SENTINEL;
                continue;
            }
            if (!val) val = "";
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

    /* Выборка уперлась в предел и продолжения не будет — молчать нельзя:
     * оператор считал бы, что получил все строки своего запроса. */
    if (warn_truncate && rows >= (int)limit)
        LOG_WARN("mssql: запрос вернул %d строк — это предел выборки. Остальные "
                 "строки НЕ прочитаны: у произвольного запроса нет устойчивого "
                 "порядка, поэтому постранично он читается только с заданным "
                 "полем-отметкой. Задайте отметку или сузьте запрос.", rows);
    return 0;
}
static int ms_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                         const char *entity, ColBatch **out)
{
    MsCtx *ctx = (MsCtx *)vctx;
    if (!ctx || ctx->dbc == SQL_NULL_HDBC || !entity) return -1;
    *out = NULL;

    /* Размер страницы. batch_size из настроек подключения ограничивает выборку
     * и тогда, когда вызывающий просит больше: иначе настройка не действовала
     * бы вовсе — конвейер всегда запрашивает целый BATCH_SIZE, — а именно ею
     * ограничивают выборку на серверах с жёсткими лимитами памяти и таймаутом
     * запроса. */
    int64_t limit = (req && req->limit > 0) ? req->limit : ctx->batch_size;
    if (ctx->batch_size > 0 && limit > ctx->batch_size) limit = ctx->batch_size;
    if (limit > BATCH_SIZE) limit = BATCH_SIZE;
    const char *cur = (req && req->cursor) ? req->cursor : NULL;

    /* ── Произвольный запрос вместо имени таблицы ──────────────────────────
     * Когда в поле источника написан SQL, а не имя таблицы, вызывающий
     * передаёт его отдельным полем filter, а entity оставляет пустым (так же
     * устроен коннектор PostgreSQL). Раньше mssql это поле игнорировал:
     * подставлялось пустое имя таблицы, и запрос оператора молча не
     * исполнялся — при том что форма прямо предлагает писать сюда SELECT.
     *
     * Постранично читать произвольный запрос можно только при заданном
     * поле-отметке: срез по нему устойчив на любой версии сервера. Без
     * отметки порядок строк в запросе не определён, поэтому OFFSET разъезжался
     * бы между страницами — вместо этого берём одну порцию и предупреждаем,
     * если она уперлась в предел. */
    const char *filter = (req && req->filter && req->filter[0]) ? req->filter : NULL;
    if (filter) {
        char sql[MS_MAX_SQL];
        if (ctx->cursor_column[0]) {
            char qc[160];
            ms_quote_ident(ctx->cursor_column, qc, sizeof(qc));
            if (cur && cur[0] && strcmp(cur, "0") != 0)
                snprintf(sql, sizeof(sql),
                         "SELECT TOP %lld * FROM (%s) AS q WHERE q.%s > ? ORDER BY q.%s",
                         (long long)limit, filter, qc, qc);
            else
                snprintf(sql, sizeof(sql),
                         "SELECT TOP %lld * FROM (%s) AS q ORDER BY q.%s",
                         (long long)limit, filter, qc);
        } else {
            snprintf(sql, sizeof(sql), "SELECT TOP %lld * FROM (%s) AS q",
                     (long long)limit, filter);
        }
        return ms_fetch_sql(ctx, a, sql,
                            (ctx->cursor_column[0] && cur && cur[0] && strcmp(cur, "0") != 0) ? cur : NULL,
                            limit, !ctx->cursor_column[0], out);
    }

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
            /* 2005-2008 R2: OFFSET/FETCH ещё нет, ROW_NUMBER() уже есть.
             *
             * Служебный номер строки ОБЯЗАН остаться внутри подзапроса.
             * Схема батча строится из самой выдачи (SQLDescribeCol ниже), так
             * что «SELECT *» поверх подзапроса протащил бы _rn наружу лишней
             * колонкой: приёмник создал бы её у себя, а describe() о ней не
             * знает. Поэтому наружу перечисляем колонки поимённо.
             *
             * И ORDER BY снаружи обязателен: WHERE отбирает нужный диапазон
             * номеров, но не задаёт порядок выдачи — без него сервер вправе
             * вернуть страницу в любом порядке. */
            char cols[MS_MAX_SQL / 2] = "";
            if (ms_column_list(ctx, sch, tab, "t", cols, sizeof(cols)) != 0)
                return -1;
            snprintf(sql, sizeof(sql),
                     "SELECT %s FROM (SELECT ROW_NUMBER() OVER (ORDER BY %s) AS _rn, * "
                     "FROM %s.%s) t WHERE t._rn > %lld AND t._rn <= %lld "
                     "ORDER BY t._rn",
                     cols, qo, qs, qt, off, off + (long long)limit);
        } else {
            /* 2000 и старше (major 8 и ниже): нет ни OFFSET/FETCH, ни
             * ROW_NUMBER(). Берём срез по КЛЮЧУ: пропускаем off первых
             * значений ключа подзапросом и читаем следующие limit.
             *
             * Раньше здесь был приём «TOP off+limit ASC → TOP limit DESC →
             * развернуть». Он даёт верную страницу, пока страницы есть, но у
             * него нет конца: когда off перерастает число строк, внутренний
             * TOP всё равно возвращает всю таблицу, а средний — её последние
             * limit строк. Читающий цикл останавливается только на пустом
             * батче, поэтому чтение зацикливалось и гнало последнюю страницу
             * снова и снова (на проверке: 7 строк источника превратились в
             * 8696 строк приёмника, из них id=6 — 4351 раз).
             *
             * Со срезом по ключу конец наступает сам: когда пропущены все
             * значения, MAX подзапроса равен последнему ключу и условие
             * «ключ > MAX» не выбирает ничего.
             *
             * ВАЖНО: устойчиво только при УНИКАЛЬНОМ ключе сортировки — на
             * дубликатах строки с одинаковым ключом попадут в один срез или
             * будут пропущены. Об этом предупреждаем при подключении. */
            if (off == 0) {
                snprintf(sql, sizeof(sql),
                         "SELECT TOP %lld * FROM %s.%s ORDER BY %s",
                         (long long)limit, qs, qt, qo);
            } else {
                snprintf(sql, sizeof(sql),
                         "SELECT TOP %lld * FROM %s.%s WHERE %s > "
                         "(SELECT MAX(%s) FROM (SELECT TOP %lld %s FROM %s.%s "
                         "ORDER BY %s ASC) AS a) ORDER BY %s ASC",
                         (long long)limit, qs, qt, qo,
                         qo, off, qo, qs, qt, qo, qo);
            }
        }
    }

    return ms_fetch_sql(ctx, a, sql,
                        (ctx->cursor_column[0] && cur && cur[0]) ? cur : NULL,
                        limit, false, out);
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
/* NVARCHAR(4000) — предел широкой строки в параметре; всё, что длиннее,
 * должно уходить как WLONGVARCHAR, иначе драйвер обрежет или откажет. */
#define MS_TEXT_SQL_TYPE(len) ((SQLSMALLINT)((len) > 4000 ? SQL_WLONGVARCHAR : SQL_WVARCHAR))

/* Возврат автокоммита. Соединение живёт между батчами (mssql держит сессию),
 * поэтому оставить его в ручном режиме — значит подвесить чужие запросы в
 * незакрытой транзакции. */
static void ms_autocommit_back(MsCtx *ctx, int had_tx) {
    if (had_tx)
        SQLSetConnectAttr(ctx->dbc, SQL_ATTR_AUTOCOMMIT,
                          (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
}

/* Ключ идемпотентности: список колонок через запятую. Возвращает их число,
 * имена — в out[]. Пустая настройка означает обычную вставку. */
static int ms_split_keys(const char *csv, char out[][160], int maxn)
{
    int n = 0;
    const char *p = csv;
    while (*p && n < maxn) {
        while (*p == ' ' || *p == ',') p++;
        const char *b = p;
        while (*p && *p != ',') p++;
        const char *e = p;
        while (e > b && e[-1] == ' ') e--;
        if (e > b) {
            size_t len = (size_t)(e - b);
            if (len > 159) len = 159;
            memcpy(out[n], b, len); out[n][len] = 0;
            n++;
        }
    }
    return n;
}

static bool ms_is_key_col(const char *name, char keys[][160], int nkeys)
{
    for (int i = 0; i < nkeys; i++)
        if (strcasecmp(name, keys[i]) == 0) return true;
    return false;
}

static int ms_write_batch(void *vctx, Arena *a, const char *entity,
                          const Schema *schema, const ColBatch *batch, int mode)
{
    MsCtx *ctx = (MsCtx *)vctx;
    if (!ctx || ctx->dbc == SQL_NULL_HDBC || !batch || !schema) return -1;
    if (batch->nrows <= 0) return 0;
    (void)a;

    /* Пустое имя назначения ловим здесь, а не отдаём в SQL. Иначе идентификатор
     * сворачивается в «[]», и SQL Server отвечает «An object or column name is
     * missing or empty» — по этому тексту невозможно понять, что не заполнено
     * поле «Таблица назначения» у шага. */
    if (!entity || !entity[0]) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "не задана таблица назначения: заполните «Таблица назначения» "
                 "у шага-приёмника (например dbo.orders_export)");
        LOG_ERROR("mssql: %s", ctx->last_err);
        return -1;
    }

    char sch[128], tab[256], qs[160], qt[300];
    ms_split_entity(ctx, entity, sch, sizeof(sch), tab, sizeof(tab));
    ms_quote_ident(sch, qs, sizeof(qs));
    ms_quote_ident(tab, qt, sizeof(qt));

    int ncols = schema->ncols;
    if (ncols <= 0) return -1;
    if (ncols > MS_MAX_COLS) ncols = MS_MAX_COLS;

    char keys[8][160];
    int nkeys = ctx->primary_key[0]
              ? ms_split_keys(ctx->primary_key, keys, 8) : 0;

    /* Ключ обязан существовать среди колонок батча, иначе запрос не соберётся,
     * а оператор получит ошибку сервера вместо внятного объяснения. */
    for (int k = 0; k < nkeys; k++) {
        bool found = false;
        for (int c = 0; c < ncols; c++)
            if (strcasecmp(schema->cols[c].name, keys[k]) == 0) { found = true; break; }
        if (!found) {
            snprintf(ctx->last_err, sizeof(ctx->last_err),
                     "колонка ключа «%s» отсутствует в данных источника; "
                     "исправьте «Ключ идемпотентности» в настройках подключения",
                     keys[k]);
            LOG_ERROR("mssql: %s", ctx->last_err);
            return -1;
        }
    }


    /* Таблица при отсутствии. Текстовый тип — по версии, см. комментарий выше.
     *
     * Колонки КЛЮЧА — исключение: их нельзя создавать ни NTEXT, ни
     * NVARCHAR(MAX). NTEXT вообще запрещено сравнивать («The text, ntext, and
     * image data types cannot be compared or sorted»), из-за чего на сервере
     * 2000 идемпотентная запись падала бы на первом же WHERE по ключу; а
     * NVARCHAR(MAX) сравнивать можно, но нельзя проиндексировать, и поиск
     * совпадения шёл бы полным перебором таблицы. NVARCHAR(450) сравнивается,
     * индексируется и существует во всех поддерживаемых версиях. */
    const char *text_type = (ctx->srv_major >= 9) ? "NVARCHAR(MAX)" : "NTEXT";
    const char *key_type  = "NVARCHAR(450)";
    char ddl[MS_MAX_SQL];
    int n = snprintf(ddl, sizeof(ddl),
                     "IF OBJECT_ID('%s.%s','U') IS NULL CREATE TABLE %s.%s (",
                     sch, tab, qs, qt);
    for (int c = 0; c < ncols && n > 0 && (size_t)n < sizeof(ddl); c++) {
        char qc[160];
        ms_quote_ident(schema->cols[c].name, qc, sizeof(qc));
        n += snprintf(ddl + n, sizeof(ddl) - (size_t)n, "%s%s %s",
                      c ? ", " : "", qc,
                      ms_is_key_col(schema->cols[c].name, keys, nkeys) ? key_type : text_type);
    }
    if (n > 0 && (size_t)n < sizeof(ddl)) snprintf(ddl + n, sizeof(ddl) - (size_t)n, ")");
    if (ms_exec(ctx, ddl) != 0) return -1;

    /* SINK_OVERWRITE — заменить содержимое. DELETE, а не TRUNCATE: TRUNCATE
     * требует прав ALTER на таблицу и падает при внешних ключах, а приёмник
     * должен работать с обычными правами на запись. */
    /* Батч пишем одной транзакцией. Иначе оборванная на середине вставка
     * оставляет часть строк в таблице, а планировщик повторяет шаг — и
     * повтор дублирует уже записанное. Именно так и вышло на проверке:
     * строка с кириллицей упала, две предыдущие остались, следующий прогон
     * добавил их второй раз. Автокоммит возвращаем в любом исходе. */
    int had_tx = 0;
    if (ms_ok(SQLSetConnectAttr(ctx->dbc, SQL_ATTR_AUTOCOMMIT,
                                (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0)))
        had_tx = 1;

    /* Очистка — внутри той же транзакции, что и вставка. Снаружи она
     * означала бы, что упавший батч оставляет приёмник пустым: старых
     * строк уже нет, новых ещё нет. */
    if (mode == DFO_SINK_OVERWRITE) {
        char del[MS_MAX_SQL];
        snprintf(del, sizeof(del), "DELETE FROM %s.%s", qs, qt);
        if (ms_exec(ctx, del) != 0) {
            if (had_tx) SQLEndTran(SQL_HANDLE_DBC, ctx->dbc, SQL_ROLLBACK);
            ms_autocommit_back(ctx, had_tx);
            return -1;
        }
    }

    /* Стратегия записи. Без ключа — обычная вставка. С ключом — запись должна
     * быть идемпотентной: повторный прогон конвейера обязан обновлять строку,
     * а не плодить дубликаты.
     *
     * MERGE появился в SQL Server 2008. На 2005 и старше его нет, поэтому там
     * тот же результат достигается парой «UPDATE, и если ничего не обновилось —
     * INSERT». Подсказка HOLDLOCK в MERGE обязательна: без неё две записи в одну
     * таблицу могут разойтись между проверкой и вставкой и вставить дубликат
     * ключа, что MERGE как раз и должен предотвращать. */
    int nnonkey = 0;
    for (int c = 0; c < ncols; c++)
        if (!ms_is_key_col(schema->cols[c].name, keys, nkeys)) nnonkey++;

    /* Все колонки ключевые — обновлять нечего, достаточно не вставлять
     * повторно. MERGE без ветки MATCHED допустим. */
    bool merge_mode  = (nkeys > 0 && ctx->srv_major >= 10);
    bool upsert_pair = (nkeys > 0 && ctx->srv_major < 10);

    char ins[MS_MAX_SQL];
    if (merge_mode) {
        n = snprintf(ins, sizeof(ins), "MERGE INTO %s.%s WITH (HOLDLOCK) AS t USING (SELECT ", qs, qt);
        for (int c = 0; c < ncols && n > 0 && (size_t)n < sizeof(ins); c++) {
            char qc[160]; ms_quote_ident(schema->cols[c].name, qc, sizeof(qc));
            n += snprintf(ins + n, sizeof(ins) - (size_t)n, "%s? AS %s", c ? ", " : "", qc);
        }
        n += snprintf(ins + n, sizeof(ins) - (size_t)n, ") AS s ON (");
        for (int k = 0; k < nkeys && n > 0 && (size_t)n < sizeof(ins); k++) {
            char qk[160]; ms_quote_ident(keys[k], qk, sizeof(qk));
            n += snprintf(ins + n, sizeof(ins) - (size_t)n,
                          "%st.%s = s.%s", k ? " AND " : "", qk, qk);
        }
        n += snprintf(ins + n, sizeof(ins) - (size_t)n, ")");
        if (nnonkey > 0) {
            n += snprintf(ins + n, sizeof(ins) - (size_t)n, " WHEN MATCHED THEN UPDATE SET ");
            int w = 0;
            for (int c = 0; c < ncols && n > 0 && (size_t)n < sizeof(ins); c++) {
                if (ms_is_key_col(schema->cols[c].name, keys, nkeys)) continue;
                char qc[160]; ms_quote_ident(schema->cols[c].name, qc, sizeof(qc));
                n += snprintf(ins + n, sizeof(ins) - (size_t)n,
                              "%st.%s = s.%s", w++ ? ", " : "", qc, qc);
            }
        }
        n += snprintf(ins + n, sizeof(ins) - (size_t)n, " WHEN NOT MATCHED THEN INSERT (");
        for (int c = 0; c < ncols && n > 0 && (size_t)n < sizeof(ins); c++) {
            char qc[160]; ms_quote_ident(schema->cols[c].name, qc, sizeof(qc));
            n += snprintf(ins + n, sizeof(ins) - (size_t)n, "%s%s", c ? ", " : "", qc);
        }
        n += snprintf(ins + n, sizeof(ins) - (size_t)n, ") VALUES (");
        for (int c = 0; c < ncols && n > 0 && (size_t)n < sizeof(ins); c++) {
            char qc[160]; ms_quote_ident(schema->cols[c].name, qc, sizeof(qc));
            n += snprintf(ins + n, sizeof(ins) - (size_t)n, "%ss.%s", c ? ", " : "", qc);
        }
        snprintf(ins + n, sizeof(ins) - (size_t)n, ");");   /* MERGE требует точку с запятой */
    } else {
        /* Обычная вставка: значения уходят параметрами, а не склейкой в текст —
         * иначе кавычка в данных ломает SQL. */
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
    }

    /* Пара «UPDATE, затем INSERT» для серверов без MERGE. Порядок параметров у
     * UPDATE иной: сначала неключевые колонки, затем ключевые в WHERE. */
    char upd[MS_MAX_SQL] = "";
    if (upsert_pair && nnonkey > 0) {
        int m = snprintf(upd, sizeof(upd), "UPDATE %s.%s SET ", qs, qt);
        int w = 0;
        for (int c = 0; c < ncols && m > 0 && (size_t)m < sizeof(upd); c++) {
            if (ms_is_key_col(schema->cols[c].name, keys, nkeys)) continue;
            char qc[160]; ms_quote_ident(schema->cols[c].name, qc, sizeof(qc));
            m += snprintf(upd + m, sizeof(upd) - (size_t)m, "%s%s = ?", w++ ? ", " : "", qc);
        }
        m += snprintf(upd + m, sizeof(upd) - (size_t)m, " WHERE ");
        for (int k = 0; k < nkeys && m > 0 && (size_t)m < sizeof(upd); k++) {
            char qk[160]; ms_quote_ident(keys[k], qk, sizeof(qk));
            m += snprintf(upd + m, sizeof(upd) - (size_t)m, "%s%s = ?", k ? " AND " : "", qk);
        }
    }

    SQLHSTMT st = SQL_NULL_HSTMT;
    if (!ms_ok(SQLAllocHandle(SQL_HANDLE_STMT, ctx->dbc, &st))) {
        ms_autocommit_back(ctx, had_tx); return -1;
    }
    if (!ms_ok(SQLPrepare(st, (SQLCHAR *)ins, SQL_NTS))) {
        ms_diag(ctx, SQL_HANDLE_STMT, st, "подготовка вставки");
        SQLFreeHandle(SQL_HANDLE_STMT, st);
        ms_autocommit_back(ctx, had_tx);
        return -1;
    }

    int written = 0;
    SQLLEN *inds = calloc((size_t)ncols, sizeof(SQLLEN));
    /* Отдельные индикаторы для UPDATE: ODBC держит указатель на них до
     * исполнения, поэтому переиспользовать массив вставки нельзя. */
    SQLLEN *uinds = calloc((size_t)ncols, sizeof(SQLLEN));
    if (!inds || !uinds) {
        free(inds); free(uinds);
        SQLFreeHandle(SQL_HANDLE_STMT, st); ms_autocommit_back(ctx, had_tx); return -1;
    }
    for (int r = 0; r < batch->nrows; r++) {
        for (int c = 0; c < ncols; c++) {
            const uint8_t *bm = batch->null_bitmap[c];
            int isnull = (bm && ((bm[r / 8] >> (r % 8)) & 1u));
            char *v = isnull ? NULL : ((char **)batch->values[c])[r];

            /* NULL отдаём индикатором, а не пустой строкой: в приёмнике это
             * разные значения, и «пусто» вместо NULL ломает и IS NULL, и
             * внешние ключи. */
            if (!v) {
                inds[c] = SQL_NULL_DATA;
                if (!ms_ok(SQLBindParameter(st, (SQLUSMALLINT)(c + 1), SQL_PARAM_INPUT,
                                            SQL_C_CHAR, MS_TEXT_SQL_TYPE(0), 1, 0,
                                            (SQLPOINTER)"", 0, &inds[c]))) {
                    ms_diag(ctx, SQL_HANDLE_STMT, st, "привязка параметра");
                    goto insert_done;
                }
                continue;
            }

            /* Тип параметра — ШИРОКИЙ (SQL_WVARCHAR), хотя буфер узкий
             * (SQL_C_CHAR). Колонки создаются как NVARCHAR, и при SQL_VARCHAR
             * драйвер пытается свернуть UTF-8 в однобайтовую кодировку
             * сервера: на латинской сортировке кириллица не пролезает, и
             * FreeTDS отвечает невнятным «[HY000] Unknown error». Объявив
             * параметр широким, мы просим драйвер перевести UTF-8 в UCS-2 —
             * это он делает верно (и FreeTDS с ClientCharset=UTF-8, и
             * msodbcsql18). Длинные значения — WLONGVARCHAR, иначе упрёмся в
             * предел NVARCHAR(4000). */
            inds[c] = (SQLLEN)strlen(v);
            SQLSMALLINT sqltype = MS_TEXT_SQL_TYPE(inds[c]);
            /* Размер — в символах; байтовая длина UTF-8 их не меньше, а
             * завышение здесь безвредно (это заявленная ёмкость, не буфер). */
            SQLULEN colsize = inds[c] ? (SQLULEN)inds[c] : 1;
            if (!ms_ok(SQLBindParameter(st, (SQLUSMALLINT)(c + 1), SQL_PARAM_INPUT,
                                        SQL_C_CHAR, sqltype, colsize, 0,
                                        v, inds[c], &inds[c]))) {
                ms_diag(ctx, SQL_HANDLE_STMT, st, "привязка параметра");
                goto insert_done;
            }
        }
        /* Сервер без MERGE: сперва пробуем обновить существующую строку и лишь
         * при её отсутствии вставляем. Порядок параметров у UPDATE другой,
         * поэтому привязываем их отдельно. */
        if (upsert_pair && upd[0]) {
            SQLHSTMT us = SQL_NULL_HSTMT;
            if (!ms_ok(SQLAllocHandle(SQL_HANDLE_STMT, ctx->dbc, &us))) goto insert_done;
            if (!ms_ok(SQLPrepare(us, (SQLCHAR *)upd, SQL_NTS))) {
                ms_diag(ctx, SQL_HANDLE_STMT, us, "подготовка обновления");
                SQLFreeHandle(SQL_HANDLE_STMT, us); goto insert_done;
            }
            SQLUSMALLINT pi = 1;
            bool bind_ok = true;
            for (int pass = 0; pass < 2 && bind_ok; pass++) {
                for (int c = 0; c < ncols && bind_ok; c++) {
                    bool iskey = ms_is_key_col(schema->cols[c].name, keys, nkeys);
                    if ((pass == 0) == iskey) continue;      /* сначала неключевые, затем ключевые */
                    const uint8_t *bm = batch->null_bitmap[c];
                    int isnull = (bm && ((bm[r / 8] >> (r % 8)) & 1u));
                    char *v = isnull ? NULL : ((char **)batch->values[c])[r];
                    if (!v) {
                        uinds[pi - 1] = SQL_NULL_DATA;
                        bind_ok = ms_ok(SQLBindParameter(us, pi, SQL_PARAM_INPUT, SQL_C_CHAR,
                                        MS_TEXT_SQL_TYPE(0), 1, 0, (SQLPOINTER)"", 0, &uinds[pi - 1]));
                    } else {
                        uinds[pi - 1] = (SQLLEN)strlen(v);
                        bind_ok = ms_ok(SQLBindParameter(us, pi, SQL_PARAM_INPUT, SQL_C_CHAR,
                                        MS_TEXT_SQL_TYPE(uinds[pi - 1]),
                                        uinds[pi - 1] ? (SQLULEN)uinds[pi - 1] : 1, 0,
                                        v, uinds[pi - 1], &uinds[pi - 1]));
                    }
                    pi++;
                }
            }
            SQLLEN affected = 0;
            if (bind_ok && ms_ok(SQLExecute(us))) SQLRowCount(us, &affected);
            else { ms_diag(ctx, SQL_HANDLE_STMT, us, "обновление строки");
                   SQLFreeHandle(SQL_HANDLE_STMT, us); goto insert_done; }
            SQLFreeHandle(SQL_HANDLE_STMT, us);
            if (affected > 0) { written++; continue; }       /* строка была — обновили */
        }

        SQLRETURN rc = SQLExecute(st);
        if (!ms_ok(rc)) {
            ms_diag(ctx, SQL_HANDLE_STMT, st,
                    merge_mode ? "слияние строки" : "вставка строки");
            break;
        }
        written++;
    }
insert_done:
    free(inds); free(uinds);
    SQLFreeHandle(SQL_HANDLE_STMT, st);

    /* Успех фиксируем, неудачу откатываем целиком — приёмник либо принял
     * батч, либо остался в том состоянии, в каком был. */
    if (had_tx) {
        SQLRETURN trc = SQLEndTran(SQL_HANDLE_DBC, ctx->dbc,
                                   (written == batch->nrows) ? SQL_COMMIT : SQL_ROLLBACK);
        if (written == batch->nrows && !ms_ok(trc)) {
            ms_diag(ctx, SQL_HANDLE_DBC, ctx->dbc, "фиксация транзакции");
            written = -1;
        }
        ms_autocommit_back(ctx, had_tx);
        if (written < 0) return -1;
    }

    if (written != batch->nrows) {
        LOG_WARN("mssql: записано %d из %d строк в %s", written, batch->nrows, entity);
        return -1;
    }
    LOG_INFO("mssql: записано %d строк в %s.%s (%s)", written, sch, tab,
             merge_mode ? "MERGE по ключу" :
             upsert_pair ? "обновление или вставка по ключу" : "вставка");
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
