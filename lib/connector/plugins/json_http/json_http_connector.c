/* json_http_connector.c — HTTP/REST → DFO через libcurl.
 * Загружает JSON с произвольного URL, навигирует к массиву через data_path,
 * выводит схему из первого элемента, поддерживает offset- и cursor-пагинацию. */
#include "../../connector.h"
#include "../../../core/log.h"
#include "../../../core/json.h"
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>

/* ── Конфиг коннектора ── */
typedef struct {
    char url[1024];          /* базовый URL */
    char method[8];          /* GET или POST */
    char auth_type[32];      /* none, bearer, api_key */
    char auth_token[512];    /* токен или ключ */
    char auth_header[64];    /* заголовок для api_key (default: X-API-Key) */
    char data_path[256];     /* dot-path к массиву в ответе, напр. "data.items" */
    char page_param[64];     /* имя GET-параметра для страницы/курсора */
    char page_type[16];      /* "offset" или "cursor" */
    char total_field[64];    /* поле с общим числом записей (для offset-режима) */
    int  page_size;          /* записей на страницу */
    char post_body[2048];    /* тело POST (при method=POST) */
    char last_err[512];      /* human-readable reason of the last failure */
    Arena *arena;
} JHCtx;

/* ── Буфер для libcurl write callback ── */
typedef struct { char *data; size_t len, cap; } CurlBuf;

/* libcurl write callback: дописывает порцию ответа в CurlBuf, удваивая
 * ёмкость по мере роста. Также используется как ручной аппендер при сборке
 * JSON-тела (см. jh_write_batch). Возврат != n => libcurl прервёт передачу. */
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

/* ── HTTP запрос ── */
/* Выполняет один GET/POST к ctx->url, добавляя page_param=cursor_or_offset
 * (если заданы), и возвращает тело ответа, скопированное в арену a (NULL при
 * ошибке транспорта или коде вне 2xx; причина пишется в ctx->last_err). */
static char *jh_fetch(JHCtx *ctx, const char *cursor_or_offset, Arena *a) {
    CURL *curl=curl_easy_init();
    if (!curl) return NULL;

    /* Строим URL с курсором/оффсетом */
    char url[2048];
    if (ctx->page_param[0] && cursor_or_offset && cursor_or_offset[0]) {
        /* Добавляем page_param=value к URL */
        const char *sep=strchr(ctx->url,'?') ? "&" : "?";
        snprintf(url,sizeof(url),"%s%s%s=%s",ctx->url,sep,ctx->page_param,cursor_or_offset);
    } else {
        strncpy(url,ctx->url,sizeof(url)-1); url[sizeof(url)-1]='\0';
    }

    CurlBuf buf={NULL,0,0};

    curl_easy_setopt(curl,CURLOPT_URL,url);
    curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,curl_write);
    curl_easy_setopt(curl,CURLOPT_WRITEDATA,&buf);
    curl_easy_setopt(curl,CURLOPT_FOLLOWLOCATION,1L);
    curl_easy_setopt(curl,CURLOPT_TIMEOUT,30L);
    curl_easy_setopt(curl,CURLOPT_SSL_VERIFYPEER,1L);

    /* Заголовки авторизации */
    struct curl_slist *hdrs=NULL;
    hdrs=curl_slist_append(hdrs,"Accept: application/json");
    if (strcasecmp(ctx->auth_type,"bearer")==0 && ctx->auth_token[0]) {
        char h[600]; snprintf(h,sizeof(h),"Authorization: Bearer %s",ctx->auth_token);
        hdrs=curl_slist_append(hdrs,h);
    } else if (strcasecmp(ctx->auth_type,"api_key")==0 && ctx->auth_token[0]) {
        const char *hname=ctx->auth_header[0]?ctx->auth_header:"X-API-Key";
        char h[600]; snprintf(h,sizeof(h),"%s: %s",hname,ctx->auth_token);
        hdrs=curl_slist_append(hdrs,h);
    }

    if (strcasecmp(ctx->method,"POST")==0) {
        curl_easy_setopt(curl,CURLOPT_POST,1L);
        if (ctx->post_body[0]) {
            curl_easy_setopt(curl,CURLOPT_POSTFIELDS,ctx->post_body);
            hdrs=curl_slist_append(hdrs,"Content-Type: application/json");
        }
    }

    curl_easy_setopt(curl,CURLOPT_HTTPHEADER,hdrs);

    CURLcode res=curl_easy_perform(curl);
    long http_code=0;
    curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&http_code);
    curl_slist_free_all(hdrs);
    curl_easy_cleanup(curl);

    if (res!=CURLE_OK || http_code<200 || http_code>=300) {
        LOG_ERROR("json_http fetch %s → curl=%d http=%ld",url,(int)res,http_code);
        if (res!=CURLE_OK)
            snprintf(ctx->last_err,sizeof(ctx->last_err),"Ошибка соединения: %s",curl_easy_strerror(res));
        else
            snprintf(ctx->last_err,sizeof(ctx->last_err),"HTTP %ld от сервера",http_code);
        free(buf.data); return NULL;
    }

    if (!buf.data) return NULL;
    /* Копируем в арену, освобождаем malloc-буфер */
    char *result=arena_strdup(a,buf.data);
    free(buf.data);
    return result;
}

/* ── Навигация по data_path ── */
/* data_path = "key1.key2.key3" → последовательно get по ключам */
static JVal *jh_navigate(JVal *root, const char *path) {
    if (!path||!path[0]) return root;
    char buf[256]; strncpy(buf,path,sizeof(buf)-1); buf[sizeof(buf)-1]='\0';
    JVal *cur=root;
    char *tok=strtok(buf,".");
    while (tok&&cur) {
        if (cur->type!=JV_OBJECT) return NULL;
        cur=json_get(cur,tok);
        tok=strtok(NULL,".");
    }
    return cur;
}

/* ── Вывод типа из массива JVal-значений (первые N элементов) ── */
static ColType infer_col_type(JVal *arr, int col_idx) {
    if (!arr||arr->type!=JV_ARRAY) return COL_TEXT;
    ColType t=COL_NULL;
    /* семплируем до 10 первых строк; при конфликте типов откатываемся к TEXT */
    int samples=(int)arr->nitems<10?(int)arr->nitems:10;
    for (int i=0;i<samples;i++) {
        JVal *row=arr->items[i];
        if (!row||row->type!=JV_OBJECT||(size_t)col_idx>=row->nkeys) continue;
        JVal *v=row->vals[col_idx];
        if (!v||v->type==JV_NULL) continue;
        ColType ct;
        switch(v->type) {
            case JV_NUMBER: {
                /* double vs int64: целое число без дробной части → INT64 */
                double d=v->n; int64_t iv=(int64_t)d;
                ct=(d==(double)iv)?COL_INT64:COL_DOUBLE;
                break;
            }
            case JV_BOOL: ct=COL_INT64; break;
            default:      ct=COL_TEXT;  break;
        }
        if (t==COL_NULL) t=ct;
        else if (t!=ct) { t=COL_TEXT; break; }
    }
    return (t==COL_NULL)?COL_TEXT:t;
}

/* ── Схема из первого элемента массива ── */
static Schema *jh_infer_schema(JVal *arr, Arena *a) {
    if (!arr||arr->type!=JV_ARRAY||arr->nitems==0) return NULL;
    JVal *first=arr->items[0];
    if (!first||first->type!=JV_OBJECT) return NULL;
    Schema *sc=arena_calloc(a,sizeof(Schema));
    sc->ncols=(int)first->nkeys;
    sc->cols=arena_alloc(a,(size_t)sc->ncols*sizeof(ColDef));
    for (int c=0;c<sc->ncols;c++) {
        sc->cols[c].name=arena_strdup(a,first->keys[c]);
        sc->cols[c].type=infer_col_type(arr,c);
        sc->cols[c].nullable=true;
    }
    return sc;
}

/* ── Заполнение ColBatch из массива JVal ── */
/* Переносит до nmax строк (начиная с start_item) в колоночные массивы batch
 * согласно типам схемы; отсутствующие/NULL-поля отмечаются в null_bitmap.
 * Возвращает число заполненных строк. */
static int jh_fill_batch(JVal *arr, Schema *sc, ColBatch *batch,
                          int start_item, int nmax, Arena *a) {
    if (!arr||arr->type!=JV_ARRAY) return 0;
    int n=0;
    for (int i=start_item;i<(int)arr->nitems&&n<nmax;i++,n++) {
        JVal *row=arr->items[i];
        if (!row||row->type!=JV_OBJECT) continue;
        for (int c=0;c<sc->ncols;c++) {
            JVal *v=json_get(row,sc->cols[c].name);
            if (!v||v->type==JV_NULL) {
                batch->null_bitmap[c][n/8]|=(uint8_t)(1u<<(n%8)); continue;
            }
            switch(sc->cols[c].type) {
                case COL_INT64:
                    ((int64_t*)batch->values[c])[n]=(v->type==JV_BOOL)?(int64_t)v->b:(int64_t)v->n;
                    break;
                case COL_DOUBLE:
                    ((double*)batch->values[c])[n]=v->n;
                    break;
                default:
                    if (v->type==JV_STRING)
                        ((const char**)batch->values[c])[n]=arena_strndup(a,v->s,v->len);
                    else
                        ((const char**)batch->values[c])[n]=arena_strdup(a,"");
                    break;
            }
        }
    }
    return n;
}

/* ── cfg_get ── */
/* Мини-парсер конфига: вытаскивает значение поля key из JSON-строки в out
 * (строковое в кавычках либо «голый» токен до , } пробела). При отсутствии
 * поля сохраняется def. Не полноценный JSON-парсер — достаточно для плоского
 * конфига коннектора. */
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

/* ── Lifecycle ── */

/* create(): аллоцирует JHCtx в арене и заполняет его из cfg значениями
 * по умолчанию. */
static void *jh_create(const char *cfg, Arena *a) {
    JHCtx *ctx=arena_calloc(a,sizeof(JHCtx));
    ctx->arena=a;
    cfg_get(cfg,"url",       ctx->url,       sizeof(ctx->url),       "");
    cfg_get(cfg,"method",    ctx->method,    sizeof(ctx->method),    "GET");
    cfg_get(cfg,"auth_type", ctx->auth_type, sizeof(ctx->auth_type), "none");
    cfg_get(cfg,"auth_token",ctx->auth_token,sizeof(ctx->auth_token),"");
    cfg_get(cfg,"auth_header",ctx->auth_header,sizeof(ctx->auth_header),"X-API-Key");
    cfg_get(cfg,"data_path", ctx->data_path, sizeof(ctx->data_path), "");
    cfg_get(cfg,"page_param",ctx->page_param,sizeof(ctx->page_param),"");
    cfg_get(cfg,"page_type", ctx->page_type, sizeof(ctx->page_type), "offset");
    cfg_get(cfg,"total_field",ctx->total_field,sizeof(ctx->total_field),"total");
    cfg_get(cfg,"post_body", ctx->post_body, sizeof(ctx->post_body), "");
    char pgsz[16]; cfg_get(cfg,"page_size",pgsz,sizeof(pgsz),"100");
    ctx->page_size=atoi(pgsz); if(ctx->page_size<=0) ctx->page_size=100;
    return ctx;
}

static void jh_destroy(void *ctx) { (void)ctx; }

/* ping(): пробный запрос к URL во временной арене; 0 если ответ получен. */
static int jh_ping(void *vctx) {
    JHCtx *ctx=vctx;
    if (!ctx||!ctx->url[0]) return -1;
    Arena *a=arena_create(65536);
    char *body=jh_fetch(ctx,NULL,a);
    arena_destroy(a);
    return body?0:-1;
}

/* ── last_error(): human-readable reason of the last failure (ABI v3) ── */
static const char *jh_last_error(void *vctx) {
    JHCtx *ctx=vctx;
    return (ctx && ctx->last_err[0]) ? ctx->last_err : NULL;
}

/* list_entities(): у REST-эндпоинта одна «сущность»; имя выводим из последнего
 * сегмента пути URL (без query string). */
static int jh_list_entities(void *vctx, Arena *a, DfoEntityList *out) {
    JHCtx *ctx=vctx;
    out->items=arena_calloc(a,sizeof(DfoEntity));
    /* Имя сущности = последний сегмент URL-пути */
    const char *last=strrchr(ctx->url,'/');
    const char *name=last?last+1:ctx->url;
    /* Убираем query string */
    char nbuf[256]; strncpy(nbuf,name,sizeof(nbuf)-1); nbuf[sizeof(nbuf)-1]='\0';
    char *q=strchr(nbuf,'?'); if(q)*q='\0';
    out->items[0].entity=arena_strdup(a,nbuf[0]?nbuf:"endpoint");
    out->items[0].type="stream";
    out->count=1;
    return 0;
}

/* describe(): тянет первую страницу, навигирует к массиву и выводит схему из
 * его первого элемента. Парсинг — во временной арене pa, схема — в арене a. */
static int jh_describe(void *vctx, Arena *a, const char *entity, Schema **out) {
    (void)entity;
    JHCtx *ctx=vctx;
    char *body=jh_fetch(ctx,"0",a);
    if (!body) return -1;
    Arena *pa=arena_create(65536);
    JVal *root=json_parse(pa,body,strlen(body));
    if (!root) { arena_destroy(pa); return -1; }
    JVal *arr=jh_navigate(root,ctx->data_path);
    Schema *sc=jh_infer_schema(arr,a);
    arena_destroy(pa);
    if (!sc) return -1;
    *out=sc; return 0;
}

/* read_batch: курсор = "offset" (целое) или непрозрачный токен следующей страницы.
 * При offset-пагинации: если len(items) < page_size → конец данных.
 * При cursor-пагинации: ищем поле next_cursor/next_page в ответе. */
static int jh_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                          const char *entity, ColBatch **out_batch) {
    (void)entity;
    JHCtx *ctx=vctx;
    if (!ctx->url[0]) return 1;

    const char *cursor=(req&&req->cursor)?req->cursor:"0";

    /* При offset-режиме cursor = total строк уже возвращённых */
    char cur_str[64];
    snprintf(cur_str,sizeof(cur_str),"%s",cursor);

    char *body=jh_fetch(ctx,cur_str,a);
    if (!body) return -1;

    /* Парсим ответ */
    JVal *root=json_parse(a,body,strlen(body));
    if (!root) return -1;

    JVal *arr=jh_navigate(root,ctx->data_path);
    if (!arr||arr->type!=JV_ARRAY||arr->nitems==0) return 1; /* нет данных */

    /* Схема из первого батча (или from req->filter как подсказка) */
    Schema *sc=jh_infer_schema(arr,a);
    if (!sc) return -1;

    int ncols=sc->ncols;
    int nrows=(int)arr->nitems;
    if (nrows>BATCH_SIZE) nrows=BATCH_SIZE;

    /* Выделяем колоночные массивы и null-битмапы под выведённые типы */
    ColBatch *batch=arena_calloc(a,sizeof(ColBatch));
    batch->schema=sc; batch->ncols=ncols;
    for (int c=0;c<ncols;c++) {
        batch->null_bitmap[c]=arena_calloc(a,((size_t)nrows+7)/8);
        switch(sc->cols[c].type) {
            case COL_INT64:  batch->values[c]=arena_alloc(a,(size_t)nrows*sizeof(int64_t)); break;
            case COL_DOUBLE: batch->values[c]=arena_alloc(a,(size_t)nrows*sizeof(double));  break;
            default:         batch->values[c]=arena_alloc(a,(size_t)nrows*sizeof(char*));   break;
        }
    }

    batch->nrows=jh_fill_batch(arr,sc,batch,0,nrows,a);
    *out_batch=batch;

    /* Сигнализируем конец данных если получили меньше page_size элементов */
    return ((int)arr->nitems<ctx->page_size)?1:0;
}

/* ── JSON-escape a string into a growable buffer ── */
static void jh_json_escape(CurlBuf *b, const char *s) {
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

/* ── Sink: POST the batch as a JSON array of row-objects ──
 * Target URL = `entity` if it starts with http(s)://, else the configured
 * "url". Auth headers (bearer / api_key) from config are reused. Returns rows
 * written on HTTP 2xx, -1 otherwise. mode is ignored (every call POSTs its own
 * batch; the remote endpoint decides append vs replace). */
/* Проверка NULL по битмапу: возвращает "" (sentinel) если ячейка NULL, иначе NULL. */
static const char *bit_isnull(const ColBatch *b, int c, int r) {
    const uint8_t *bm = b->null_bitmap[c];
    return (bm && ((bm[r/8] >> (r%8)) & 1u)) ? "" : NULL; /* "" sentinel = null */
}
static int jh_write_batch(void *vctx, Arena *a, const char *entity,
                          const Schema *schema, const ColBatch *batch, int mode) {
    (void)a; (void)mode;
    JHCtx *ctx = vctx;
    const char *url = (entity && (strncmp(entity,"http://",7)==0 || strncmp(entity,"https://",8)==0))
                      ? entity : ctx->url;
    if (!url || !url[0]) return -1;

    /* Build JSON body: [{"col":"val",...},...] (cells are TEXT).
     * Собираем вручную через curl_write в растущий буфер; строки экранируем. */
    CurlBuf body = {NULL,0,0};
    curl_write((void*)"[", 1, 1, &body);
    char tmp[64];
    for (int r = 0; r < batch->nrows; r++) {
        if (r) curl_write((void*)",", 1, 1, &body);
        curl_write((void*)"{", 1, 1, &body);
        for (int c = 0; c < batch->ncols; c++) {
            if (c) curl_write((void*)",", 1, 1, &body);
            curl_write((void*)"\"", 1, 1, &body);
            jh_json_escape(&body, schema->cols[c].name);
            curl_write((void*)"\":", 1, 2, &body);
            int isnull = (bit_isnull(batch, c, r) != NULL);
            if (isnull) { curl_write((void*)"null", 1, 4, &body); continue; }
            const char *v;
            switch (schema->cols[c].type) {
                case COL_INT64:  snprintf(tmp,sizeof(tmp),"%lld",(long long)((int64_t*)batch->values[c])[r]); v=tmp; break;
                case COL_DOUBLE: snprintf(tmp,sizeof(tmp),"%.10g",((double*)batch->values[c])[r]); v=tmp; break;
                default:         v = ((char**)batch->values[c])[r]; break;
            }
            curl_write((void*)"\"", 1, 1, &body);
            jh_json_escape(&body, v ? v : "");
            curl_write((void*)"\"", 1, 1, &body);
        }
        curl_write((void*)"}", 1, 1, &body);
    }
    curl_write((void*)"]", 1, 1, &body);

    CURL *curl = curl_easy_init();
    if (!curl) { free(body.data); return -1; }
    struct curl_slist *hdrs = NULL;
    hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
    if (strcasecmp(ctx->auth_type,"bearer")==0 && ctx->auth_token[0]) {
        char h[600]; snprintf(h,sizeof(h),"Authorization: Bearer %s",ctx->auth_token);
        hdrs = curl_slist_append(hdrs, h);
    } else if (strcasecmp(ctx->auth_type,"api_key")==0 && ctx->auth_token[0]) {
        const char *hname = ctx->auth_header[0]?ctx->auth_header:"X-API-Key";
        char h[600]; snprintf(h,sizeof(h),"%s: %s",hname,ctx->auth_token);
        hdrs = curl_slist_append(hdrs, h);
    }
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data ? body.data : "[]");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)body.len);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    CurlBuf resp = {NULL,0,0};
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);

    CURLcode res = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    curl_slist_free_all(hdrs);
    curl_easy_cleanup(curl);
    free(body.data); free(resp.data);

    if (res != CURLE_OK) { LOG_ERROR("json_http sink: curl: %s", curl_easy_strerror(res)); return -1; }
    if (code < 200 || code >= 300) { LOG_ERROR("json_http sink: HTTP %ld", code); return -1; }
    LOG_INFO("json_http sink: POST %d rows → %s (HTTP %ld)", batch->nrows, url, code);
    return batch->nrows;
}

/* Таблица экспорта плагина: точка входа, по которой загрузчик находит коннектор. */
const DfoConnector dfo_connector_entry = {
    .abi_version  = DFO_CONNECTOR_ABI_VERSION,
    .name         = "json_http",
    .version      = "0.1.0",
    .description  = "HTTP/REST JSON connector via libcurl",
    .create       = jh_create,
    .destroy      = jh_destroy,
    .list_entities= jh_list_entities,
    .describe     = jh_describe,
    .read_batch   = jh_read_batch,
    .cdc_start    = NULL,
    .cdc_stop     = NULL,
    .ping         = jh_ping,
    .write_batch  = jh_write_batch,
    .last_error   = jh_last_error,
};
