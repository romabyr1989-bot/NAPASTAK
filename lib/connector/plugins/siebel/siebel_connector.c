/* siebel_connector.c — Siebel/EIM коннектор NAPASTAK (CDI ↔ Siebel).
 *
 * Двунаправленный коннектор поверх Siebel EAI / REST Inbound Web Service:
 *
 *   • SINK (CDI → Siebel). Джокер пишет мастер-данные обратно в Siebel. write_batch
 *     на каждый батч формирует Siebel-совместимый JSON-конверт вокруг Integration
 *     Object и POST-ит на configurable siebel_url с опциональным Basic-auth:
 *         { "<io_name>": [ { "field": "value", ... }, ... ] }
 *     что соответствует Siebel EAI «list of integration object instances».
 *
 *   • SOURCE (Siebel → DFO). read_batch вытягивает записи IO из Siebel REST/EAI
 *     (как исходный TSMDM-поток «collector»: Siebel S_CONTACT → …). GET на
 *     read_url (или siebel_url) с offset-пагинацией ?StartRowNum=&PageSize=,
 *     навигация к массиву записей по data_path (по умолчанию — ключ io_name,
 *     с фолбэком на голый массив или "items"), вывод схемы из первого элемента.
 *     Чтобы перешагнуть лимит ядра (батч = BATCH_SIZE строк, короткий батч =
 *     конец данных), read_batch склеивает несколько Siebel-страниц в один батч.
 *
 * Специализация HTTP-коннектора (см. json_http_connector.c) под маппинг Siebel IO.
 * ABI как у json_http: .name="siebel" .read_batch/.describe/.write_batch/.last_error. */
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
    char url[1024];          /* siebel_url — Inbound Web Service endpoint (sink) */
    char io_name[256];       /* io_name / eim_object — Siebel Integration Object */
    char user[256];          /* siebel_user — для Basic-auth */
    char password[256];      /* siebel_password — для Basic-auth */
    /* ── source (чтение) ── */
    char read_url[1024];     /* read_url — эндпоинт запроса записей (default: url) */
    char data_path[256];     /* dot-path к массиву записей (default: io_name) */
    char page_param[64];     /* GET-параметр оффсета строки (default StartRowNum) */
    char size_param[64];     /* GET-параметр размера страницы (default PageSize) */
    char read_method[8];     /* GET | POST (default GET) */
    char read_body[2048];    /* тело POST-запроса (EAI Query SiebelMessage) */
    int  page_size;          /* записей за один Siebel-fetch (default 1000) */
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
    if(*p=='"') {            /* строка: декодируем JSON-экранирование, стоп на НЕэкранированной " */
        p++; size_t o=0;
        while(*p && *p!='"' && o<outsz-1) {
            if(*p=='\\' && p[1]) { p++; char c=*p; if(c=='n')c='\n'; else if(c=='t')c='\t'; else if(c=='r')c='\r'; out[o++]=c; p++; }
            else out[o++]=*p++;
        }
        out[o]='\0';
    } else {
        const char *e=p;
        while(*e&&*e!=','&&*e!='}'&&*e!=' '&&*e!='\n') e++;
        size_t n=(size_t)(e-p); if(n>=outsz)n=outsz-1;
        memcpy(out,p,n); out[n]='\0';
    }
}

/* ── JSON-escape строки в растущий буфер ── */
/* Экранирует спецсимволы и управляющие коды (< 0x20 → \uXXXX) для вставки
 * значения/ключа в JSON-тело запроса к Siebel. */
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
    if (!ctx->user[0]) cfg_get(cfg,"user", ctx->user, sizeof(ctx->user), "");
    cfg_get(cfg,"siebel_password",ctx->password, sizeof(ctx->password), "");
    if (!ctx->password[0]) cfg_get(cfg,"password", ctx->password, sizeof(ctx->password), "");
    /* ── source (чтение) ── */
    cfg_get(cfg,"read_url",       ctx->read_url, sizeof(ctx->read_url), "");
    cfg_get(cfg,"data_path",      ctx->data_path,sizeof(ctx->data_path),"");
    cfg_get(cfg,"page_param",     ctx->page_param,sizeof(ctx->page_param),"StartRowNum");
    cfg_get(cfg,"size_param",     ctx->size_param,sizeof(ctx->size_param),"PageSize");
    cfg_get(cfg,"read_method",    ctx->read_method,sizeof(ctx->read_method),"GET");
    cfg_get(cfg,"read_body",      ctx->read_body, sizeof(ctx->read_body), "");
    char pgsz[16]; cfg_get(cfg,"page_size",pgsz,sizeof(pgsz),"1000");
    ctx->page_size=atoi(pgsz); if(ctx->page_size<=0) ctx->page_size=1000;
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

/* ════════════════════════════ SOURCE (чтение) ════════════════════════════ */

/* sb_fetch: один GET/POST запрос записей из Siebel, тело ответа → арена a.
 * GET (по умолчанию): read_url|url + ?<page_param>=<offset>&<size_param>=<page_size>.
 * POST: тело read_body (EAI Query SiebelMessage), без пагинации в URL.
 * NULL при ошибке транспорта или коде вне 2xx (причина → ctx->last_err). */
static char *sb_fetch(SiebelCtx *ctx, int64_t offset, Arena *a) {
    CURL *curl=curl_easy_init();
    if (!curl) return NULL;
    const char *base = ctx->read_url[0] ? ctx->read_url : ctx->url;
    int is_get = strcasecmp(ctx->read_method,"POST")!=0;
    char url[2400];
    if (is_get && ctx->page_param[0]) {
        const char *sep = strchr(base,'?') ? "&" : "?";
        if (ctx->size_param[0])
            snprintf(url,sizeof(url),"%s%s%s=%lld&%s=%d",
                     base,sep,ctx->page_param,(long long)offset,ctx->size_param,ctx->page_size);
        else
            snprintf(url,sizeof(url),"%s%s%s=%lld",base,sep,ctx->page_param,(long long)offset);
    } else {
        strncpy(url,base,sizeof(url)-1); url[sizeof(url)-1]='\0';
    }

    CurlBuf buf={NULL,0,0};
    struct curl_slist *hdrs=NULL;
    hdrs=curl_slist_append(hdrs,"Accept: application/json");
    curl_easy_setopt(curl,CURLOPT_URL,url);
    curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,curl_write);
    curl_easy_setopt(curl,CURLOPT_WRITEDATA,&buf);
    curl_easy_setopt(curl,CURLOPT_FOLLOWLOCATION,1L);
    curl_easy_setopt(curl,CURLOPT_TIMEOUT,60L);
    curl_easy_setopt(curl,CURLOPT_SSL_VERIFYPEER,1L);
    if (!is_get) {
        curl_easy_setopt(curl,CURLOPT_POST,1L);
        curl_easy_setopt(curl,CURLOPT_POSTFIELDS,ctx->read_body[0]?ctx->read_body:"{}");
        hdrs=curl_slist_append(hdrs,"Content-Type: application/json");
    }
    curl_easy_setopt(curl,CURLOPT_HTTPHEADER,hdrs);
    siebel_apply_auth(curl,ctx);

    CURLcode res=curl_easy_perform(curl);
    long code=0;
    curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&code);
    curl_slist_free_all(hdrs);
    curl_easy_cleanup(curl);

    if (res!=CURLE_OK || code<200 || code>=300) {
        if (res!=CURLE_OK)
            snprintf(ctx->last_err,sizeof(ctx->last_err),
                     "Ошибка соединения с %s: %s", url, curl_easy_strerror(res));
        else
            snprintf(ctx->last_err,sizeof(ctx->last_err),"HTTP %ld от %s", code, url);
        LOG_ERROR("siebel source: fetch %s → curl=%d http=%ld", url, (int)res, code);
        free(buf.data); return NULL;
    }
    if (!buf.data) return NULL;
    char *result=arena_strdup(a,buf.data);
    free(buf.data);
    return result;
}

/* sb_navigate: data_path = "key1.key2" → последовательный json_get по ключам.
 * Имя IO с пробелами (без точек) трактуется как один ключ — это норма для Siebel. */
static JVal *sb_navigate(JVal *root, const char *path) {
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

/* sb_locate_array: найти массив записей в ответе — по data_path, иначе голый
 * массив-корень, иначе ключ "items" (Siebel REST). NULL если массива нет. */
static JVal *sb_locate_array(JVal *root, const char *dp) {
    JVal *arr=sb_navigate(root,dp);
    if (arr&&arr->type==JV_ARRAY) return arr;
    if (root&&root->type==JV_ARRAY) return root;
    JVal *it=json_get(root,"items");
    if (it&&it->type==JV_ARRAY) return it;
    return NULL;
}

/* sb_infer_col_type: тип колонки key по выборке до 10 строк; конфликт → TEXT. */
static ColType sb_infer_col_type(JVal **rows, int nrows, const char *key) {
    ColType t=COL_NULL;
    int samples=nrows<10?nrows:10;
    for (int i=0;i<samples;i++) {
        JVal *row=rows[i];
        if (!row||row->type!=JV_OBJECT) continue;
        JVal *v=json_get(row,key);
        if (!v||v->type==JV_NULL) continue;
        ColType ct;
        switch(v->type) {
            case JV_NUMBER: { double d=v->n; int64_t iv=(int64_t)d; ct=(d==(double)iv)?COL_INT64:COL_DOUBLE; break; }
            case JV_BOOL:   ct=COL_INT64; break;
            default:        ct=COL_TEXT;  break;
        }
        if (t==COL_NULL) t=ct;
        else if (t!=ct) { t=COL_TEXT; break; }
    }
    return (t==COL_NULL)?COL_TEXT:t;
}

/* sb_infer_schema: схема из ключей первого объекта-строки, типы — по выборке. */
static Schema *sb_infer_schema(JVal **rows, int nrows, Arena *a) {
    if (nrows<=0) return NULL;
    JVal *first=rows[0];
    if (!first||first->type!=JV_OBJECT||first->nkeys==0) return NULL;
    int ncols=(int)first->nkeys;
    if (ncols>MAX_COLS) ncols=MAX_COLS;
    Schema *sc=arena_calloc(a,sizeof(Schema));
    sc->ncols=ncols;
    sc->cols=arena_alloc(a,(size_t)ncols*sizeof(ColDef));
    for (int c=0;c<ncols;c++) {
        sc->cols[c].name=arena_strdup(a,first->keys[c]);
        sc->cols[c].type=sb_infer_col_type(rows,nrows,first->keys[c]);
        sc->cols[c].nullable=true;
    }
    return sc;
}

/* sb_fill_batch: переносит nrows строк-объектов в колоночные массивы согласно
 * типам схемы; отсутствующие/NULL-поля → null_bitmap. Для TEXT-колонок числа и
 * bool строкуются (не теряем значение при смешанном типе). */
static int sb_fill_batch(JVal **rows, int nrows, Schema *sc, ColBatch *batch, Arena *a) {
    for (int n=0;n<nrows;n++) {
        JVal *row=rows[n];
        for (int c=0;c<sc->ncols;c++) {
            JVal *v=(row&&row->type==JV_OBJECT)?json_get(row,sc->cols[c].name):NULL;
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
                    else if (v->type==JV_NUMBER) {
                        char nb[32]; double d=v->n; int64_t iv=(int64_t)d;
                        if (d==(double)iv) snprintf(nb,sizeof(nb),"%lld",(long long)iv);
                        else               snprintf(nb,sizeof(nb),"%g",d);
                        ((const char**)batch->values[c])[n]=arena_strdup(a,nb);
                    } else if (v->type==JV_BOOL) {
                        ((const char**)batch->values[c])[n]=arena_strdup(a,v->b?"true":"false");
                    } else {
                        ((const char**)batch->values[c])[n]=arena_strdup(a,"");
                    }
                    break;
            }
        }
    }
    return nrows;
}

/* describe(): тянет первую страницу, выводит схему из первого элемента массива. */
static int siebel_describe(void *vctx, Arena *a, const char *entity, Schema **out) {
    SiebelCtx *ctx=vctx;
    const char *base = ctx->read_url[0] ? ctx->read_url : ctx->url;
    if (!base||!base[0]) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),
                 "siebel source: ни read_url, ни siebel_url не заданы");
        return -1;
    }
    char *body=sb_fetch(ctx,0,a);
    if (!body) return -1;
    JVal *root=json_parse(a,body,strlen(body));
    if (!root||root->type==JV_ERROR) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),"siebel source: некорректный JSON-ответ");
        return -1;
    }
    const char *io=(entity&&entity[0])?entity:ctx->io_name;
    const char *dp=ctx->data_path[0]?ctx->data_path:io;
    JVal *arr=sb_locate_array(root,dp);
    if (!arr||arr->nitems==0) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),
                 "siebel source: массив записей не найден (data_path='%s')", dp);
        return -1;
    }
    int n=(int)arr->nitems; if(n>10)n=10;
    JVal **rows=arena_alloc(a,(size_t)n*sizeof(JVal*));
    for (int i=0;i<n;i++) rows[i]=arr->items[i];
    Schema *sc=sb_infer_schema(rows,n,a);
    if (!sc) return -1;
    *out=sc; return 0;
}

/* read_batch: один батч = строки начиная с req->cursor (целочисленный оффсет,
 * cumulative — ядро двигает его на число прочитанных строк). Чтобы перешагнуть
 * правило ядра «короткий батч = конец», склеиваем несколько Siebel-страниц в
 * один батч до BATCH_SIZE строк (или до короткой страницы Siebel = конец данных).
 *   rc <0 ошибка; 0 = есть ещё (полный батч); 1 = последняя/единственная порция. */
static int siebel_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                             const char *entity, ColBatch **out_batch) {
    SiebelCtx *ctx=vctx;
    const char *base = ctx->read_url[0] ? ctx->read_url : ctx->url;
    if (!base||!base[0]) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),
                 "siebel source: ни read_url, ни siebel_url не заданы");
        return -1;
    }
    int64_t offset=0;
    if (req&&req->cursor&&req->cursor[0]) offset=strtoll(req->cursor,NULL,10);

    const char *io=(entity&&entity[0])?entity:ctx->io_name;
    const char *dp=ctx->data_path[0]?ctx->data_path:io;

    /* Склейка Siebel-страниц в один батч (до BATCH_SIZE строк). */
    JVal **rows=arena_alloc(a,(size_t)BATCH_SIZE*sizeof(JVal*));
    int nrows=0, done=0;
    while (nrows<BATCH_SIZE && !done) {
        char *body=sb_fetch(ctx,offset+nrows,a);   /* абсолютный StartRowNum */
        if (!body) return -1;                       /* транспорт/HTTP — наверх */
        JVal *root=json_parse(a,body,strlen(body));
        if (!root||root->type==JV_ERROR) {
            snprintf(ctx->last_err,sizeof(ctx->last_err),"siebel source: некорректный JSON-ответ");
            return -1;
        }
        JVal *arr=sb_locate_array(root,dp);
        if (!arr||arr->nitems==0) { done=1; break; }   /* данные кончились */
        int got=(int)arr->nitems;
        int take=got<(BATCH_SIZE-nrows)?got:(BATCH_SIZE-nrows);
        for (int i=0;i<take;i++) rows[nrows++]=arr->items[i];
        if (got<ctx->page_size) done=1;     /* короткая страница Siebel → конец */
        if (!ctx->page_param[0])   done=1;  /* пагинация выключена → один fetch */
        if (take<got) break;                /* батч заполнен посреди страницы */
    }
    if (nrows==0) return 1;                  /* на этом оффсете пусто → конец */

    Schema *sc=sb_infer_schema(rows,nrows,a);
    if (!sc) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),
                 "siebel source: не удалось вывести схему из записей");
        return -1;
    }
    int ncols=sc->ncols;
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
    batch->nrows=sb_fill_batch(rows,nrows,sc,batch,a);
    *out_batch=batch;
    LOG_INFO("siebel source: %d rows ← IO '%s' @ %s (offset %lld%s)",
             batch->nrows, io, base, (long long)offset, done?", end":"");
    return done?1:0;
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
 * Двунаправленный (ABI как у json_http): describe/read_batch — source,
 * write_batch — sink; cdc не поддерживается. */
const DfoConnector dfo_connector_entry = {
    .abi_version  = DFO_CONNECTOR_ABI_VERSION,
    .name         = "siebel",
    .version      = "0.2.0",
    .description  = "Siebel/EIM connector (EAI REST Inbound Web Service, source+sink)",
    .create       = siebel_create,
    .destroy      = siebel_destroy,
    .list_entities= siebel_list_entities,
    .describe     = siebel_describe,
    .read_batch   = siebel_read_batch,
    .cdc_start    = NULL,
    .cdc_stop     = NULL,
    .ping         = siebel_ping,
    .write_batch  = siebel_write_batch,
    .last_error   = siebel_last_error,
};
