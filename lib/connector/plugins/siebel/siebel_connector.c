/* siebel_connector.c — Siebel/EIM sink-коннектор DataFlow OS (CDI → Siebel).
 *
 * Джокер пишет мастер-данные обратно в Siebel. Siebel принимает данные двумя
 * способами: (1) EIM batch (INSERT в EIM_*-таблицы Oracle + EIM-джоб) или
 * (2) Siebel EAI через HTTP/REST (POST в Inbound Web Service). Реализован
 * REST-вариант — проще, без Oracle-зависимости: write_batch на каждый батч
 * формирует Siebel-совместимый JSON-конверт вокруг Integration Object и POST-ит
 * на configurable siebel_url с опциональным Basic-auth.
 *
 * По сути это специализация HTTP-приёмника (см. json_http_connector.c) с
 * маппингом полей строки под Siebel IO (Integration Object): каждая строка
 * становится одним IO-инстансом
 *     { "<io_name>": { "field": "value", ... } }
 * а весь батч заворачивается в массив объектов под ключом io_name, что
 * соответствует Siebel EAI «list of integration object instances».
 *
 * Только sink (read_batch/describe возвращают «read-only»). ABI как у json_http:
 *   .name="siebel" .write_batch .last_error и т.д. */
#include "../../connector.h"
#include "../../../core/log.h"
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>

/* ── Конфиг коннектора ── */
typedef struct {
    char url[1024];          /* siebel_url — Inbound Web Service endpoint */
    char io_name[256];       /* io_name / eim_object — Siebel Integration Object */
    char user[256];          /* siebel_user — для Basic-auth */
    char password[256];      /* siebel_password — для Basic-auth */
    char last_err[512];      /* human-readable причина последнего сбоя */
    Arena *arena;
} SiebelCtx;

/* ── Буфер для libcurl write callback / ручной сборки тела ── */
typedef struct { char *data; size_t len, cap; } CurlBuf;

/* libcurl write callback: дописывает порцию в CurlBuf, удваивая ёмкость.
 * Также используется как ручной аппендер при сборке JSON-тела.
 * Возврат != n => libcurl прервёт передачу. */
static size_t curl_write(void *ptr, size_t size, size_t nmemb, void *ud) {
    CurlBuf *b=ud;
    size_t n=size*nmemb;
    if (b->len+n+1>b->cap) {
        size_t ncap=(b->cap?b->cap*2:4096);
        while (ncap<b->len+n+1) ncap*=2;
        b->data=realloc(b->data,ncap);
        if (!b->data) return 0;
        b->cap=ncap;
    }
    memcpy(b->data+b->len,ptr,n);
    b->len+=n;
    b->data[b->len]='\0';
    return n;
}

/* ── cfg_get — мини-парсер плоского JSON-конфига (как в json_http) ── */
/* Вытаскивает значение поля key из JSON-строки в out (строковое в кавычках либо
 * «голый» токен до , } пробела). При отсутствии поля сохраняется def. */
static void cfg_get(const char *json, const char *key,
                     char *out, size_t outsz, const char *def) {
    if (def) { strncpy(out,def,outsz-1); out[outsz-1]='\0'; }
    if (!json) return;
    char search[128]; snprintf(search,sizeof(search),"\"%s\"",key);
    const char *p=strstr(json,search);
    if (!p) return;
    p+=strlen(search);
    while(*p==' '||*p=='\t'||*p=='\n') p++;
    if(*p!=':') return; p++;
    while(*p==' '||*p=='\t'||*p=='\n') p++;
    if(*p=='"') {
        p++; const char *e=strchr(p,'"'); if(!e) return;
        size_t n=(size_t)(e-p); if(n>=outsz)n=outsz-1;
        memcpy(out,p,n); out[n]='\0';
    } else {
        const char *e=p;
        while(*e&&*e!=','&&*e!='}'&&*e!=' '&&*e!='\n') e++;
        size_t n=(size_t)(e-p); if(n>=outsz)n=outsz-1;
        memcpy(out,p,n); out[n]='\0';
    }
}

/* ── JSON-escape строки в растущий буфер ── */
static void sb_json_escape(CurlBuf *b, const char *s) {
    char tmp[8];
    for (const char *p = s ? s : ""; *p; p++) {
        const char *esc = NULL; int n = 0; char c = *p;
        switch (c) {
            case '"':  esc = "\\\""; n=2; break;
            case '\\': esc = "\\\\"; n=2; break;
            case '\n': esc = "\\n";  n=2; break;
            case '\r': esc = "\\r";  n=2; break;
            case '\t': esc = "\\t";  n=2; break;
            default:
                if ((unsigned char)c < 0x20) { snprintf(tmp,sizeof(tmp),"\\u%04x",c); esc=tmp; n=6; }
        }
        if (esc) curl_write((void*)esc, 1, (size_t)n, b);
        else     curl_write((void*)&c, 1, 1, b);
    }
}

/* ── NULL-контракт ──
 * Ячейка считается SQL NULL если: помечена в null_bitmap, либо текстовое
 * значение равно DFO_NULL_SENTINEL. Такие поля выводятся как JSON null. */
static int sb_cell_is_null(const ColBatch *b, int c, int r) {
    const uint8_t *bm = b->null_bitmap[c];
    if (bm && ((bm[r/8] >> (r%8)) & 1u)) return 1;
    /* Sink values are always char* text (text-always model); check the sentinel
     * for every column regardless of its logical type. */
    const char *s = ((char**)b->values[c])[r];
    if (s && strcmp(s, DFO_NULL_SENTINEL)==0) return 1;
    return 0;
}

/* ── Lifecycle ── */

/* create(): аллоцирует SiebelCtx в арене и заполняет из cfg.
 * Принимает io_name либо eim_object (синоним) как имя Integration Object. */
static void *siebel_create(const char *cfg, Arena *a) {
    SiebelCtx *ctx=arena_calloc(a,sizeof(SiebelCtx));
    ctx->arena=a;
    cfg_get(cfg,"siebel_url",     ctx->url,      sizeof(ctx->url),      "");
    /* fallback: общий ключ "url" если siebel_url не задан */
    if (!ctx->url[0]) cfg_get(cfg,"url", ctx->url, sizeof(ctx->url), "");
    cfg_get(cfg,"io_name",        ctx->io_name,  sizeof(ctx->io_name),  "");
    /* fallback: eim_object как синоним имени Integration Object */
    if (!ctx->io_name[0]) cfg_get(cfg,"eim_object", ctx->io_name, sizeof(ctx->io_name), "");
    if (!ctx->io_name[0]) { strncpy(ctx->io_name,"Integration Object",sizeof(ctx->io_name)-1); }
    cfg_get(cfg,"siebel_user",    ctx->user,     sizeof(ctx->user),     "");
    cfg_get(cfg,"siebel_password",ctx->password, sizeof(ctx->password), "");
    return ctx;
}

static void siebel_destroy(void *ctx) { (void)ctx; }

/* Накладывает Basic-auth на curl-хэндл, если заданы user/password. */
static void siebel_apply_auth(CURL *curl, SiebelCtx *ctx) {
    if (ctx->user[0]) {
        curl_easy_setopt(curl, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC);
        curl_easy_setopt(curl, CURLOPT_USERNAME, ctx->user);
        curl_easy_setopt(curl, CURLOPT_PASSWORD, ctx->password);
    }
}

/* ping(): HEAD/GET на siebel_url; 0 если транспорт ОК и код вне 5xx.
 * Inbound Web Service на GET часто отдаёт 4xx/405 — это «живой» сервер, поэтому
 * health-проба считает успехом транспорт + любой ответ ниже 500. */
static int siebel_ping(void *vctx) {
    SiebelCtx *ctx=vctx;
    if (!ctx || !ctx->url[0]) {
        if (ctx) snprintf(ctx->last_err,sizeof(ctx->last_err),
                          "siebel: siebel_url не задан");
        return -1;
    }
    CURL *curl=curl_easy_init();
    if (!curl) return -1;
    curl_easy_setopt(curl,CURLOPT_URL,ctx->url);
    curl_easy_setopt(curl,CURLOPT_NOBODY,1L);       /* HEAD */
    curl_easy_setopt(curl,CURLOPT_TIMEOUT,15L);
    curl_easy_setopt(curl,CURLOPT_SSL_VERIFYPEER,1L);
    curl_easy_setopt(curl,CURLOPT_FOLLOWLOCATION,1L);
    siebel_apply_auth(curl,ctx);
    CURLcode res=curl_easy_perform(curl);
    long code=0;
    curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&code);
    curl_easy_cleanup(curl);
    if (res!=CURLE_OK) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),
                 "Ошибка соединения с %s: %s", ctx->url, curl_easy_strerror(res));
        return -1;
    }
    if (code>=500) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),
                 "HTTP %ld от %s", code, ctx->url);
        return -1;
    }
    return 0;
}

/* last_error(): человекочитаемая причина последнего сбоя (ABI v3). */
static const char *siebel_last_error(void *vctx) {
    SiebelCtx *ctx=vctx;
    return (ctx && ctx->last_err[0]) ? ctx->last_err : NULL;
}

/* list_entities(): у Siebel-приёмника одна «сущность» — Integration Object. */
static int siebel_list_entities(void *vctx, Arena *a, DfoEntityList *out) {
    SiebelCtx *ctx=vctx;
    out->items=arena_calloc(a,sizeof(DfoEntity));
    out->items[0].entity=arena_strdup(a, ctx->io_name[0]?ctx->io_name:"Integration Object");
    out->items[0].type="table";
    out->count=1;
    return 0;
}

/* ── Sink: POST батча в Siebel Inbound Web Service ──
 * Тело — Siebel-совместимый конверт: один объект с ключом io_name, под которым
 * массив IO-инстансов (по строке на инстанс). Все ячейки — TEXT (sink input),
 * NULL-поля (битмап или DFO_NULL_SENTINEL) → JSON null.
 *
 *   { "<io_name>": [ { "<field>": "<value>"|null, ... }, ... ] }
 *
 * Целевой IO: `entity` если задан, иначе сконфигурированный io_name.
 * Basic-auth из siebel_user/siebel_password. Возвращает число записанных строк
 * на HTTP 2xx, иначе -1 с детальной причиной в ctx->last_err (транспортная
 * ошибка libcurl или "HTTP <code> from <url>" + начало тела ответа). mode
 * игнорируется (Siebel EAI сам решает upsert по match-критериям IO). */
static int siebel_write_batch(void *vctx, Arena *a, const char *entity,
                              const Schema *schema, const ColBatch *batch, int mode) {
    (void)a; (void)mode;
    SiebelCtx *ctx = vctx;
    const char *url = ctx->url;
    if (!url || !url[0]) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "siebel sink: siebel_url не задан");
        return -1;
    }
    const char *io = (entity && entity[0]) ? entity : ctx->io_name;
    if (!io || !io[0]) io = "Integration Object";

    /* Сборка тела: { "<io>": [ {row}, ... ] } */
    CurlBuf body = {NULL,0,0};
    curl_write((void*)"{\"", 1, 2, &body);
    sb_json_escape(&body, io);
    curl_write((void*)"\":[", 1, 3, &body);

    for (int r = 0; r < batch->nrows; r++) {
        if (r) curl_write((void*)",", 1, 1, &body);
        curl_write((void*)"{", 1, 1, &body);
        for (int c = 0; c < batch->ncols; c++) {
            if (c) curl_write((void*)",", 1, 1, &body);
            curl_write((void*)"\"", 1, 1, &body);
            sb_json_escape(&body, schema->cols[c].name);
            curl_write((void*)"\":", 1, 2, &body);
            if (sb_cell_is_null(batch, c, r)) {
                curl_write((void*)"null", 1, 4, &body);
                continue;
            }
            /* Sink values are always char* text (text-always model); the schema
             * type is metadata only. Reading the cell as a native int64/double
             * reinterpreted the char* pointer as the value (R7). */
            const char *v = ((char**)batch->values[c])[r];
            curl_write((void*)"\"", 1, 1, &body);
            sb_json_escape(&body, v ? v : "");
            curl_write((void*)"\"", 1, 1, &body);
        }
        curl_write((void*)"}", 1, 1, &body);
    }
    curl_write((void*)"]}", 1, 2, &body);

    CURL *curl = curl_easy_init();
    if (!curl) {
        snprintf(ctx->last_err, sizeof(ctx->last_err), "siebel sink: curl_easy_init failed");
        free(body.data); return -1;
    }
    struct curl_slist *hdrs = NULL;
    hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
    hdrs = curl_slist_append(hdrs, "Accept: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data ? body.data : "{}");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)body.len);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    siebel_apply_auth(curl, ctx);
    CurlBuf resp = {NULL,0,0};
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);

    CURLcode res = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    curl_slist_free_all(hdrs);
    curl_easy_cleanup(curl);
    free(body.data);

    /* Транспортная ошибка libcurl: connection refused, DNS, TLS и т.п. */
    if (res != CURLE_OK) {
        snprintf(ctx->last_err, sizeof(ctx->last_err),
                 "Ошибка соединения с %s: %s", url, curl_easy_strerror(res));
        LOG_ERROR("siebel sink: curl: %s (%s)", curl_easy_strerror(res), url);
        free(resp.data);
        return -1;
    }
    /* HTTP вне 2xx: "HTTP <code> from <url>" + первые ~200 символов тела. */
    if (code < 200 || code >= 300) {
        if (resp.data && resp.len) {
            char snippet[200];
            size_t n = resp.len < sizeof(snippet) - 1 ? resp.len : sizeof(snippet) - 1;
            memcpy(snippet, resp.data, n);
            snippet[n] = '\0';
            for (size_t i = 0; i < n; i++)
                if (snippet[i] == '\n' || snippet[i] == '\r') snippet[i] = ' ';
            snprintf(ctx->last_err, sizeof(ctx->last_err),
                     "HTTP %ld from %s: %s", code, url, snippet);
        } else {
            snprintf(ctx->last_err, sizeof(ctx->last_err),
                     "HTTP %ld from %s", code, url);
        }
        LOG_ERROR("siebel sink: HTTP %ld → %s", code, url);
        free(resp.data);
        return -1;
    }
    free(resp.data);
    LOG_INFO("siebel sink: POST %d rows → IO '%s' @ %s (HTTP %ld)",
             batch->nrows, io, url, code);
    return batch->nrows;
}

/* Таблица экспорта плагина: точка входа, по которой загрузчик находит коннектор.
 * ABI как у json_http: read-only поля (describe/read_batch/cdc) = NULL — это
 * чистый sink. */
const DfoConnector dfo_connector_entry = {
    .abi_version  = DFO_CONNECTOR_ABI_VERSION,
    .name         = "siebel",
    .version      = "0.1.0",
    .description  = "Siebel/EIM sink connector (EAI REST Inbound Web Service)",
    .create       = siebel_create,
    .destroy      = siebel_destroy,
    .list_entities= siebel_list_entities,
    .describe     = NULL,
    .read_batch   = NULL,
    .cdc_start    = NULL,
    .cdc_stop     = NULL,
    .ping         = siebel_ping,
    .write_batch  = siebel_write_batch,
    .last_error   = siebel_last_error,
};
