/* xml_connector.c — XML коннектор NAPASTAK (источник + приёмник).
 *
 * SOURCE: читает XML из HTTP-эндпоинта (GET/POST) или из файла, находит
 *   повторяющийся элемент-запись (row_tag; если не задан — выводится по самому
 *   частому прямому потомку контейнера), и превращает прямые дочерние элементы
 *   записи в колонки (имя = локальное имя тега, значение = текст). Опциональная
 *   навигация data_path к контейнеру; offset-пагинация для HTTP. Значения —
 *   всегда TEXT (XML текстовый; приведение типов — в трансформации ниже).
 *
 * SINK: каждую строку пишет как <row_tag><col>value</col>…</row_tag> внутри
 *   <root_tag>…</root_tag>; документ уходит POST-запросом на url (Content-Type
 *   application/xml) либо записывается в файл path.
 *
 * Транспорт/паттерны как у json_http/siebel; разбор XML — lib/core/xml. */
#include "../../connector.h"
#include "../../../core/log.h"
#include "../../../core/xml.h"
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>

/* ── Конфиг ── */
typedef struct {
    char url[1024];          /* HTTP-эндпоинт (источник/приёмник) */
    char path[1024];         /* путь к файлу (источник/приёмник) */
    char method[8];          /* GET | POST (источник) */
    char post_body[2048];    /* тело POST (источник, method=POST) */
    char auth_type[16];      /* none | basic | bearer */
    char auth_token[512];    /* bearer-токен */
    char user[256];          /* basic-auth */
    char password[256];
    char row_tag[128];       /* тег записи (пусто = вывести автоматически) */
    char root_tag[128];      /* корневой тег вывода (приёмник, default "rows") */
    char data_path[256];     /* dot-path локальных имён к контейнеру записей */
    char page_param[64];     /* GET-параметр оффсета (источник) */
    int  page_size;          /* записей за один HTTP-fetch */
    int  sink_started;       /* применён ли mode на первом write_batch */
    char last_err[512];
    Arena *arena;
} XmlCtx;

/* ── Буфер libcurl + ручная сборка тела ── */
/* Растущий буфер: используется и как write-callback libcurl (приём ответа), и
 * как ручной аппендер при сборке XML-документа. Возврат != n прерывает передачу. */
typedef struct { char *data; size_t len, cap; } CurlBuf;
static size_t curl_write(void *ptr, size_t size, size_t nmemb, void *ud) {
    CurlBuf *b=ud; size_t n=size*nmemb;
    if (b->len+n+1>b->cap) { size_t nc=b->cap?b->cap*2:4096; while(nc<b->len+n+1) nc*=2; b->data=realloc(b->data,nc); if(!b->data) return 0; b->cap=nc; }
    memcpy(b->data+b->len,ptr,n); b->len+=n; b->data[b->len]='\0'; return n;
}

/* cfg_get — мини-парсер плоского JSON-конфига (как в других коннекторах). */
static void cfg_get(const char *json, const char *key, char *out, size_t outsz, const char *def) {
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
    }
    else { const char *e=p; while(*e&&*e!=','&&*e!='}'&&*e!=' '&&*e!='\n') e++; size_t n=(size_t)(e-p); if(n>=outsz)n=outsz-1; memcpy(out,p,n); out[n]='\0'; }
}

/* ── XML-экранирование текста и имени тега для сборки вывода ── */
static void xml_escape(CurlBuf *b, const char *s) {
    for (const char *p=s?s:""; *p; p++) {
        switch (*p) {
            case '&': curl_write((void*)"&amp;",1,5,b); break;
            case '<': curl_write((void*)"&lt;",1,4,b);  break;
            case '>': curl_write((void*)"&gt;",1,4,b);  break;
            case '"': curl_write((void*)"&quot;",1,6,b);break;
            default:  { char c=*p; curl_write((void*)&c,1,1,b); }
        }
    }
}
/* Безопасное имя тега из имени колонки (заменяет недопустимые символы на '_'). */
static void xml_tag(CurlBuf *b, const char *name) {
    const char *s = (name && name[0]) ? name : "col";
    char c0=s[0];
    if (!((c0>='A'&&c0<='Z')||(c0>='a'&&c0<='z')||c0=='_')) curl_write((void*)"_",1,1,b);
    for (const char *p=s; *p; p++) {
        char c=*p;
        if ((c>='A'&&c<='Z')||(c>='a'&&c<='z')||(c>='0'&&c<='9')||c=='_'||c=='-'||c=='.')
            curl_write((void*)&c,1,1,b);
        else curl_write((void*)"_",1,1,b);
    }
}

/* ── Lifecycle ── */
/* create(): аллоцирует XmlCtx в арене и заполняет из плоского JSON-конфига.
 * url задан => HTTP-режим; иначе path => файловый режим (см. xml_is_file). */
static void *xml_create(const char *cfg, Arena *a) {
    XmlCtx *ctx=arena_calloc(a,sizeof(XmlCtx));
    ctx->arena=a;
    cfg_get(cfg,"url",        ctx->url,       sizeof(ctx->url),       "");
    cfg_get(cfg,"path",       ctx->path,      sizeof(ctx->path),      "");
    cfg_get(cfg,"method",     ctx->method,    sizeof(ctx->method),    "GET");
    cfg_get(cfg,"post_body",  ctx->post_body, sizeof(ctx->post_body), "");
    cfg_get(cfg,"auth_type",  ctx->auth_type, sizeof(ctx->auth_type), "none");
    cfg_get(cfg,"auth_token", ctx->auth_token,sizeof(ctx->auth_token),"");
    cfg_get(cfg,"user",       ctx->user,      sizeof(ctx->user),      "");
    cfg_get(cfg,"password",   ctx->password,  sizeof(ctx->password),  "");
    cfg_get(cfg,"row_tag",    ctx->row_tag,   sizeof(ctx->row_tag),   "");
    cfg_get(cfg,"root_tag",   ctx->root_tag,  sizeof(ctx->root_tag),  "rows");
    cfg_get(cfg,"data_path",  ctx->data_path, sizeof(ctx->data_path), "");
    cfg_get(cfg,"page_param", ctx->page_param,sizeof(ctx->page_param),"");
    char pgsz[16]; cfg_get(cfg,"page_size",pgsz,sizeof(pgsz),"1000");
    ctx->page_size=atoi(pgsz); if(ctx->page_size<=0) ctx->page_size=1000;
    return ctx;
}
/* Вся память ctx — в арене; отдельного освобождения нет. */
static void xml_destroy(void *ctx) { (void)ctx; }
static const char *xml_last_error(void *vctx) { XmlCtx *c=vctx; return (c&&c->last_err[0])?c->last_err:NULL; }

/* Накладывает Basic- или Bearer-auth на запрос (basic → на хэндл, bearer →
 * заголовком Authorization). */
static void xml_apply_auth(CURL *curl, XmlCtx *ctx, struct curl_slist **hdrs) {
    if (strcasecmp(ctx->auth_type,"basic")==0 && ctx->user[0]) {
        curl_easy_setopt(curl,CURLOPT_HTTPAUTH,(long)CURLAUTH_BASIC);
        curl_easy_setopt(curl,CURLOPT_USERNAME,ctx->user);
        curl_easy_setopt(curl,CURLOPT_PASSWORD,ctx->password);
    } else if (strcasecmp(ctx->auth_type,"bearer")==0 && ctx->auth_token[0]) {
        char h[600]; snprintf(h,sizeof(h),"Authorization: Bearer %s",ctx->auth_token);
        *hdrs=curl_slist_append(*hdrs,h);
    }
}

/* ── Источник: получить XML-документ (HTTP-страница или файл) в арену ── */
/* Файловый режим: задан path и НЕ задан url (url имеет приоритет). */
static int xml_is_file(XmlCtx *ctx) { return ctx->path[0] && !ctx->url[0]; }

static char *xml_read_file(const char *path, Arena *a, XmlCtx *ctx) {
    FILE *f=fopen(path,"rb");
    if (!f) { snprintf(ctx->last_err,sizeof(ctx->last_err),"xml: не открыть файл %s",path); return NULL; }
    fseek(f,0,SEEK_END); long sz=ftell(f); fseek(f,0,SEEK_SET);
    if (sz<0) { fclose(f); return NULL; }
    char *buf=arena_alloc(a,(size_t)sz+1);
    size_t rd=fread(buf,1,(size_t)sz,f); fclose(f);
    buf[rd]='\0'; return buf;
}
/* xml_http_fetch: один GET/POST XML-документа, тело ответа → арена a.
 * GET c page_param: дописывает &page_param=offset к url (offset-пагинация).
 * POST: тело post_body, Content-Type application/xml. NULL при ошибке (код
 * вне 2xx или транспорт), причина → ctx->last_err. */
static char *xml_http_fetch(XmlCtx *ctx, int64_t offset, Arena *a) {
    CURL *curl=curl_easy_init(); if(!curl) return NULL;
    int is_get = strcasecmp(ctx->method,"POST")!=0;
    char url[2400];
    if (is_get && ctx->page_param[0]) {
        const char *sep=strchr(ctx->url,'?')?"&":"?";
        snprintf(url,sizeof(url),"%s%s%s=%lld",ctx->url,sep,ctx->page_param,(long long)offset);
    } else { strncpy(url,ctx->url,sizeof(url)-1); url[sizeof(url)-1]='\0'; }
    CurlBuf buf={NULL,0,0};
    struct curl_slist *hdrs=NULL;
    hdrs=curl_slist_append(hdrs,"Accept: application/xml, text/xml");
    curl_easy_setopt(curl,CURLOPT_URL,url);
    curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,curl_write);
    curl_easy_setopt(curl,CURLOPT_WRITEDATA,&buf);
    curl_easy_setopt(curl,CURLOPT_FOLLOWLOCATION,1L);
    curl_easy_setopt(curl,CURLOPT_TIMEOUT,60L);
    curl_easy_setopt(curl,CURLOPT_SSL_VERIFYPEER,1L);
    if (!is_get) {
        curl_easy_setopt(curl,CURLOPT_POST,1L);
        curl_easy_setopt(curl,CURLOPT_POSTFIELDS,ctx->post_body[0]?ctx->post_body:"");
        hdrs=curl_slist_append(hdrs,"Content-Type: application/xml");
    }
    xml_apply_auth(curl,ctx,&hdrs);
    curl_easy_setopt(curl,CURLOPT_HTTPHEADER,hdrs);
    CURLcode res=curl_easy_perform(curl);
    long code=0; curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&code);
    curl_slist_free_all(hdrs); curl_easy_cleanup(curl);
    if (res!=CURLE_OK || code<200 || code>=300) {
        if (res!=CURLE_OK) snprintf(ctx->last_err,sizeof(ctx->last_err),"Ошибка соединения с %s: %s",url,curl_easy_strerror(res));
        else snprintf(ctx->last_err,sizeof(ctx->last_err),"HTTP %ld от %s",code,url);
        free(buf.data); return NULL;
    }
    if (!buf.data) return NULL;
    char *r=arena_strdup(a,buf.data); free(buf.data); return r;
}

/* Контейнер записей: навигация по data_path (локальные имена через '.'). */
static XmlNode *xml_container(XmlCtx *ctx, XmlNode *root) {
    if (!ctx->data_path[0]) return root;
    char buf[256]; strncpy(buf,ctx->data_path,sizeof(buf)-1); buf[sizeof(buf)-1]='\0';
    XmlNode *cur=root;
    for (char *tok=strtok(buf,"."); tok && cur; tok=strtok(NULL,".")) cur=xml_child(cur,tok);
    return cur ? cur : root;
}

/* Найти записи: по row_tag (рекурсивно), иначе по самому частому прямому
 * потомку контейнера. Возвращает число (в out — до max). */
static size_t xml_locate_rows(XmlCtx *ctx, XmlNode *root, XmlNode **out, size_t max) {
    XmlNode *cont = xml_container(ctx, root);
    if (!cont) return 0;
    if (ctx->row_tag[0]) return xml_find_all(cont, ctx->row_tag, out, max);
    /* вывод: самый частый local-name среди прямых детей контейнера */
    const char *best=NULL; int bestc=0;
    for (size_t i=0;i<cont->nkids;i++) {
        const char *nm=xml_local(cont->kids[i]->name);
        int c=0; for (size_t j=0;j<cont->nkids;j++) if (strcmp(xml_local(cont->kids[j]->name),nm)==0) c++;
        if (c>bestc) { bestc=c; best=nm; }
    }
    if (!best) return 0;
    size_t n=0;
    for (size_t i=0;i<cont->nkids;i++)
        if (strcmp(xml_local(cont->kids[i]->name),best)==0) { if (n<max) out[n]=cont->kids[i]; n++; }
    return n;
}

/* Схема (TEXT-always) из прямых дочерних элементов первой записи. */
static Schema *xml_schema_from(XmlNode *rec, Arena *a) {
    if (!rec) return NULL;
    /* уникальные local-names прямых детей-элементов, в порядке появления */
    const char *names[MAX_COLS]; int nc=0;
    for (size_t i=0;i<rec->nkids && nc<MAX_COLS;i++) {
        const char *nm=xml_local(rec->kids[i]->name);
        int dup=0; for (int j=0;j<nc;j++) if (strcmp(names[j],nm)==0) { dup=1; break; }
        if (!dup) names[nc++]=nm;
    }
    if (nc==0) return NULL;
    Schema *sc=arena_calloc(a,sizeof(Schema));
    sc->ncols=nc; sc->cols=arena_alloc(a,(size_t)nc*sizeof(ColDef));
    for (int c=0;c<nc;c++) { sc->cols[c].name=arena_strdup(a,names[c]); sc->cols[c].type=COL_TEXT; sc->cols[c].nullable=true; }
    return sc;
}

/* describe(): тянет документ (файл или первую HTTP-страницу), находит первую
 * запись и выводит из неё TEXT-схему. entity игнорируется (одна сущность). */
static int xml_describe(void *vctx, Arena *a, const char *entity, Schema **out) {
    (void)entity;
    XmlCtx *ctx=vctx;
    char *body = xml_is_file(ctx) ? xml_read_file(ctx->path,a,ctx) : xml_http_fetch(ctx,0,a);
    if (!body) return -1;
    XmlNode *root=xml_parse(a,body,strlen(body));
    if (!root) { snprintf(ctx->last_err,sizeof(ctx->last_err),"xml: не разобран XML"); return -1; }
    XmlNode *rows[1]={0};
    if (xml_locate_rows(ctx,root,rows,1)==0 || !rows[0]) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),"xml: записи не найдены (row_tag/data_path)");
        return -1;
    }
    Schema *sc=xml_schema_from(rows[0],a);
    if (!sc) return -1;
    *out=sc; return 0;
}

/* Заполнить батч (TEXT) из массива записей по схеме; нет элемента → NULL. */
static int xml_fill(XmlNode **rows, int nrows, Schema *sc, ColBatch *batch, Arena *a) {
    for (int n=0;n<nrows;n++) {
        XmlNode *rec=rows[n];
        for (int c=0;c<sc->ncols;c++) {
            XmlNode *cell = rec ? xml_child(rec, sc->cols[c].name) : NULL;
            if (!cell) { batch->null_bitmap[c][n/8]|=(uint8_t)(1u<<(n%8)); continue; }
            ((const char**)batch->values[c])[n]=arena_strdup(a, cell->text ? cell->text : "");
        }
    }
    return nrows;
}

/* Собрать ColBatch: схема из первой записи, колоночные TEXT-массивы + битмапы. */
static ColBatch *xml_build_batch(XmlNode **rows, int nrows, Arena *a, XmlCtx *ctx) {
    Schema *sc=xml_schema_from(rows[0],a);
    if (!sc) { snprintf(ctx->last_err,sizeof(ctx->last_err),"xml: не вывести схему из записей"); return NULL; }
    ColBatch *b=arena_calloc(a,sizeof(ColBatch));
    b->schema=sc; b->ncols=sc->ncols;
    for (int c=0;c<sc->ncols;c++) {
        b->null_bitmap[c]=arena_calloc(a,((size_t)nrows+7)/8);
        b->values[c]=arena_alloc(a,(size_t)nrows*sizeof(char*));
    }
    b->nrows=xml_fill(rows,nrows,sc,b,a);
    return b;
}

/* read_batch: cursor = целочисленный оффсет (cumulative; ядро двигает его на
 * число прочитанных строк). File — читаем/парсим файл и берём срез; HTTP —
 * склеиваем страницы до BATCH_SIZE (см. siebel). rc: <0 ошибка, 0 ещё есть, 1 конец. */
static int xml_read_batch(void *vctx, Arena *a, DfoReadReq *req, const char *entity, ColBatch **out) {
    (void)entity;
    XmlCtx *ctx=vctx;
    if (!ctx->url[0] && !ctx->path[0]) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),"xml: не задан ни url, ни path");
        return -1;
    }
    int64_t offset=0;
    if (req && req->cursor && req->cursor[0]) offset=strtoll(req->cursor,NULL,10);

    XmlNode **rows=arena_alloc(a,(size_t)BATCH_SIZE*sizeof(XmlNode*));
    int nrows=0, done=0;

    if (xml_is_file(ctx)) {
        char *body=xml_read_file(ctx->path,a,ctx);
        if (!body) return -1;
        XmlNode *root=xml_parse(a,body,strlen(body));
        if (!root) { snprintf(ctx->last_err,sizeof(ctx->last_err),"xml: не разобран XML-файл"); return -1; }
        XmlNode **all=arena_alloc(a,(size_t)BATCH_SIZE*sizeof(XmlNode*));
        size_t total=xml_locate_rows(ctx,root,all,BATCH_SIZE);
        for (size_t i=(size_t)offset; i<total && nrows<BATCH_SIZE; i++) rows[nrows++]=all[i];
        done = ((size_t)offset + (size_t)nrows >= total);
    } else {
        while (nrows<BATCH_SIZE && !done) {
            char *body=xml_http_fetch(ctx,offset+nrows,a);
            if (!body) return -1;
            XmlNode *root=xml_parse(a,body,strlen(body));
            if (!root) { snprintf(ctx->last_err,sizeof(ctx->last_err),"xml: не разобран XML-ответ"); return -1; }
            XmlNode **page=arena_alloc(a,(size_t)BATCH_SIZE*sizeof(XmlNode*));
            size_t got=xml_locate_rows(ctx,root,page,BATCH_SIZE);
            if (got==0) { done=1; break; }
            size_t take = got < (size_t)(BATCH_SIZE-nrows) ? got : (size_t)(BATCH_SIZE-nrows);
            for (size_t i=0;i<take;i++) rows[nrows++]=page[i];
            if ((int)got < ctx->page_size) done=1;
            if (!ctx->page_param[0])       done=1;
            if (take < got) break;
        }
    }
    if (nrows==0) return 1;
    ColBatch *b=xml_build_batch(rows,nrows,a,ctx);
    if (!b) return -1;
    *out=b;
    LOG_INFO("xml source: %d rows (offset %lld%s)", b->nrows, (long long)offset, done?", end":"");
    return done?1:0;
}

/* list_entities(): одна сущность — имя тега записи (row_tag или "row"). */
static int xml_list_entities(void *vctx, Arena *a, DfoEntityList *out) {
    XmlCtx *ctx=vctx;
    out->items=arena_calloc(a,sizeof(DfoEntity));
    out->items[0].entity=arena_strdup(a, ctx->row_tag[0]?ctx->row_tag:"row");
    out->items[0].type="table"; out->count=1;
    return 0;
}
/* ping(): файл — проверка открытия; HTTP — пробный fetch первой страницы. */
static int xml_ping(void *vctx) {
    XmlCtx *ctx=vctx;
    if (xml_is_file(ctx)) { FILE *f=fopen(ctx->path,"rb"); if(!f){snprintf(ctx->last_err,sizeof(ctx->last_err),"xml: файл недоступен: %s",ctx->path);return -1;} fclose(f); return 0; }
    if (!ctx->url[0]) { snprintf(ctx->last_err,sizeof(ctx->last_err),"xml: не задан url/path"); return -1; }
    Arena *a=arena_create(65536);
    char *b=xml_http_fetch(ctx,0,a);
    arena_destroy(a);
    return b?0:-1;
}

/* ── NULL-контракт приёмника (битмап или sentinel) ── */
/* Ячейка = NULL если помечена в null_bitmap либо текст == DFO_NULL_SENTINEL;
 * такое поле в выводе даёт пустой <col></col>. */
static int xml_cell_null(const ColBatch *b, int c, int r) {
    const uint8_t *bm=b->null_bitmap[c];
    if (bm && ((bm[r/8]>>(r%8))&1u)) return 1;
    const char *s=((char**)b->values[c])[r];
    return (s && strcmp(s,DFO_NULL_SENTINEL)==0);
}

/* Собрать XML-документ батча: <root><row><col>val</col>…</row>…</root>. */
static void xml_build_doc(CurlBuf *body, const Schema *schema, const ColBatch *batch,
                          const char *root_tag, const char *row_tag) {
    curl_write((void*)"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",1,39,body);
    curl_write((void*)"<",1,1,body); xml_tag(body,root_tag); curl_write((void*)">\n",1,2,body);
    for (int r=0;r<batch->nrows;r++) {
        curl_write((void*)"  <",1,3,body); xml_tag(body,row_tag); curl_write((void*)">",1,1,body);
        for (int c=0;c<batch->ncols;c++) {
            curl_write((void*)"<",1,1,body); xml_tag(body,schema->cols[c].name); curl_write((void*)">",1,1,body);
            if (!xml_cell_null(batch,c,r)) {
                const char *v=((char**)batch->values[c])[r];
                xml_escape(body, v?v:"");
            }
            curl_write((void*)"</",1,2,body); xml_tag(body,schema->cols[c].name); curl_write((void*)">",1,1,body);
        }
        curl_write((void*)"</",1,2,body); xml_tag(body,row_tag); curl_write((void*)">\n",1,2,body);
    }
    curl_write((void*)"</",1,2,body); xml_tag(body,root_tag); curl_write((void*)">\n",1,2,body);
}

/* write_batch: POST документа на url, либо запись в файл path. mode применяется
 * на первом вызове (OVERWRITE → truncate файла); далее — append. Замечание: при
 * нескольких батчах в файл попадает несколько XML-документов (типичный приёмник
 * умещается в один батч; для больших объёмов используйте HTTP-приёмник). */
static int xml_write_batch(void *vctx, Arena *a, const char *entity,
                           const Schema *schema, const ColBatch *batch, int mode) {
    (void)a;
    XmlCtx *ctx=vctx;
    const char *root_tag = ctx->root_tag[0] ? ctx->root_tag : "rows";
    const char *row_tag  = (entity && entity[0]) ? entity : (ctx->row_tag[0] ? ctx->row_tag : "row");
    int first = !ctx->sink_started; ctx->sink_started = 1;

    CurlBuf body={NULL,0,0};
    xml_build_doc(&body, schema, batch, root_tag, row_tag);

    /* Файловый приёмник */
    if (xml_is_file(ctx)) {
        const char *fmode = (first && mode==DFO_SINK_OVERWRITE) ? "wb" : "ab";
        FILE *f=fopen(ctx->path,fmode);
        if (!f) { snprintf(ctx->last_err,sizeof(ctx->last_err),"xml sink: не открыть файл %s",ctx->path); free(body.data); return -1; }
        size_t wr=fwrite(body.data,1,body.len,f); fclose(f); free(body.data);
        if (wr!=body.len) { snprintf(ctx->last_err,sizeof(ctx->last_err),"xml sink: ошибка записи в %s",ctx->path); return -1; }
        LOG_INFO("xml sink: %d rows → file %s", batch->nrows, ctx->path);
        return batch->nrows;
    }

    /* HTTP-приёмник */
    if (!ctx->url[0]) { snprintf(ctx->last_err,sizeof(ctx->last_err),"xml sink: не задан url/path"); free(body.data); return -1; }
    CURL *curl=curl_easy_init();
    if (!curl) { snprintf(ctx->last_err,sizeof(ctx->last_err),"xml sink: curl init failed"); free(body.data); return -1; }
    struct curl_slist *hdrs=NULL;
    hdrs=curl_slist_append(hdrs,"Content-Type: application/xml");
    hdrs=curl_slist_append(hdrs,"Accept: application/xml, text/xml");
    xml_apply_auth(curl,ctx,&hdrs);
    curl_easy_setopt(curl,CURLOPT_URL,ctx->url);
    curl_easy_setopt(curl,CURLOPT_POST,1L);
    curl_easy_setopt(curl,CURLOPT_POSTFIELDS,body.data?body.data:"");
    curl_easy_setopt(curl,CURLOPT_POSTFIELDSIZE,(long)body.len);
    curl_easy_setopt(curl,CURLOPT_HTTPHEADER,hdrs);
    curl_easy_setopt(curl,CURLOPT_TIMEOUT,60L);
    curl_easy_setopt(curl,CURLOPT_SSL_VERIFYPEER,1L);
    CurlBuf resp={NULL,0,0};
    curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,curl_write);
    curl_easy_setopt(curl,CURLOPT_WRITEDATA,&resp);
    CURLcode res=curl_easy_perform(curl);
    long code=0; curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&code);
    curl_slist_free_all(hdrs); curl_easy_cleanup(curl);
    free(body.data);
    if (res!=CURLE_OK) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),"Ошибка соединения с %s: %s",ctx->url,curl_easy_strerror(res));
        free(resp.data); return -1;
    }
    if (code<200 || code>=300) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),"HTTP %ld from %s",code,ctx->url);
        free(resp.data); return -1;
    }
    free(resp.data);
    LOG_INFO("xml sink: POST %d rows → %s (HTTP %ld)", batch->nrows, ctx->url, code);
    return batch->nrows;
}

/* Точка входа плагина: по этому символу загрузчик находит ABI-таблицу.
 * describe/read_batch — source, write_batch — sink; CDC не поддерживается. */
const DfoConnector dfo_connector_entry = {
    .abi_version  = DFO_CONNECTOR_ABI_VERSION,
    .name         = "xml",
    .version      = "0.1.0",
    .description  = "XML connector (HTTP/file, source+sink)",
    .create       = xml_create,
    .destroy      = xml_destroy,
    .list_entities= xml_list_entities,
    .describe     = xml_describe,
    .read_batch   = xml_read_batch,
    .cdc_start    = NULL,
    .cdc_stop     = NULL,
    .ping         = xml_ping,
    .write_batch  = xml_write_batch,
    .last_error   = xml_last_error,
};
