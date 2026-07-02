/* soap_connector.c — SOAP/EAI коннектор NAPASTAK (источник + приёмник).
 *
 * SOAP = XML-конверт поверх HTTP POST. Этот коннектор:
 *
 *   SOURCE: POST-ит SOAP-запрос (request_template — тело/конверт запроса; в нём
 *     подставляются {StartRow}/{PageSize} для пагинации) на url с заголовком
 *     SOAPAction, парсит ответ, спускается в <soap:Body>, находит повторяющийся
 *     элемент-запись (row_tag; иначе — самый частый), прямые дочерние элементы
 *     которого становятся колонками (значения TEXT). Несколько страниц
 *     склеиваются в один батч до BATCH_SIZE (см. siebel/xml).
 *
 *   SINK: оборачивает строки батча в SOAP-конверт
 *       <soap:Envelope><soap:Body><Operation xmlns="ns">
 *         <row><col>value</col>…</row> … </Operation></soap:Body></soap:Envelope>
 *     и POST-ит на url с SOAPAction.
 *
 * Разбор XML — lib/core/xml; транспорт/паттерны — как у json_http/siebel/xml. */
#include "../../connector.h"
#include "../../../core/log.h"
#include "../../../core/xml.h"
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>

/* Namespace-URI SOAP-конверта: 1.1 vs 1.2 (задают заголовки и xmlns:soap). */
#define SOAP11_NS "http://schemas.xmlsoap.org/soap/envelope/"
#define SOAP12_NS "http://www.w3.org/2003/05/soap-envelope"

/* ── Конфиг ── */
typedef struct {
    char url[1024];              /* SOAP-эндпоинт */
    char soap_action[256];       /* значение заголовка SOAPAction (1.1) */
    char soap_version[8];        /* "1.1" | "1.2" (default 1.1) */
    char request_template[4096]; /* тело SOAP-запроса (источник) */
    char row_tag[128];           /* элемент-запись в ответе (пусто = авто) */
    char data_path[256];         /* dot-path локальных имён от Body к контейнеру */
    char operation[128];         /* тег-обёртка операции (приёмник, default "Import") */
    char namespace_uri[512];     /* xmlns операции (приёмник) */
    char sink_row_tag[128];      /* тег строки вывода (приёмник, default "row") */
    char auth_type[16];          /* none | basic | bearer */
    char auth_token[512];
    char user[256];
    char password[256];
    int  page_size;
    int  sink_started;
    char last_err[512];
    Arena *arena;
} SoapCtx;

/* ── Буфер libcurl / ручная сборка тела ── */
/* Растущий буфер: write-callback libcurl и ручной аппендер SOAP-конверта. */
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
/* Экранирование текстового значения для вставки в XML/SOAP-конверт. */
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
/* Безопасное имя тега из имени колонки: недопустимые символы → '_'. */
static void xml_tag(CurlBuf *b, const char *name) {
    const char *s=(name&&name[0])?name:"col";
    char c0=s[0];
    if (!((c0>='A'&&c0<='Z')||(c0>='a'&&c0<='z')||c0=='_')) curl_write((void*)"_",1,1,b);
    for (const char *p=s; *p; p++) {
        char c=*p;
        if ((c>='A'&&c<='Z')||(c>='a'&&c<='z')||(c>='0'&&c<='9')||c=='_'||c=='-'||c=='.') curl_write((void*)&c,1,1,b);
        else curl_write((void*)"_",1,1,b);
    }
}

/* ── Lifecycle ── */
/* create(): аллоцирует SoapCtx в арене и заполняет из плоского JSON-конфига. */
static void *soap_create(const char *cfg, Arena *a) {
    SoapCtx *ctx=arena_calloc(a,sizeof(SoapCtx));
    ctx->arena=a;
    cfg_get(cfg,"url",              ctx->url,              sizeof(ctx->url),              "");
    cfg_get(cfg,"soap_action",      ctx->soap_action,      sizeof(ctx->soap_action),      "");
    cfg_get(cfg,"soap_version",     ctx->soap_version,     sizeof(ctx->soap_version),     "1.1");
    cfg_get(cfg,"request_template", ctx->request_template, sizeof(ctx->request_template), "");
    cfg_get(cfg,"row_tag",          ctx->row_tag,          sizeof(ctx->row_tag),          "");
    cfg_get(cfg,"data_path",        ctx->data_path,        sizeof(ctx->data_path),        "");
    cfg_get(cfg,"operation",        ctx->operation,        sizeof(ctx->operation),        "Import");
    cfg_get(cfg,"namespace",        ctx->namespace_uri,    sizeof(ctx->namespace_uri),    "");
    cfg_get(cfg,"sink_row_tag",     ctx->sink_row_tag,     sizeof(ctx->sink_row_tag),     "row");
    cfg_get(cfg,"auth_type",        ctx->auth_type,        sizeof(ctx->auth_type),        "none");
    cfg_get(cfg,"auth_token",       ctx->auth_token,       sizeof(ctx->auth_token),       "");
    cfg_get(cfg,"user",             ctx->user,             sizeof(ctx->user),             "");
    cfg_get(cfg,"password",         ctx->password,         sizeof(ctx->password),         "");
    char pgsz[16]; cfg_get(cfg,"page_size",pgsz,sizeof(pgsz),"1000");
    ctx->page_size=atoi(pgsz); if(ctx->page_size<=0) ctx->page_size=1000;
    return ctx;
}
/* Вся память ctx — в арене; отдельного освобождения нет. */
static void soap_destroy(void *ctx) { (void)ctx; }
static const char *soap_last_error(void *vctx) { SoapCtx *c=vctx; return (c&&c->last_err[0])?c->last_err:NULL; }

/* Накладывает Basic/Bearer-auth на запрос (basic → хэндл, bearer → заголовок). */
static void soap_apply_auth(CURL *curl, SoapCtx *ctx, struct curl_slist **hdrs) {
    if (strcasecmp(ctx->auth_type,"basic")==0 && ctx->user[0]) {
        curl_easy_setopt(curl,CURLOPT_HTTPAUTH,(long)CURLAUTH_BASIC);
        curl_easy_setopt(curl,CURLOPT_USERNAME,ctx->user);
        curl_easy_setopt(curl,CURLOPT_PASSWORD,ctx->password);
    } else if (strcasecmp(ctx->auth_type,"bearer")==0 && ctx->auth_token[0]) {
        char h[600]; snprintf(h,sizeof(h),"Authorization: Bearer %s",ctx->auth_token);
        *hdrs=curl_slist_append(*hdrs,h);
    }
}
/* SOAP 1.2? (иначе — 1.1: разные Content-Type, заголовок SOAPAction и NS). */
static int soap_is12(SoapCtx *ctx) { return strcmp(ctx->soap_version,"1.2")==0; }

/* Подстановка {StartRow}/{PageSize} в шаблон запроса. */
static void soap_subst(CurlBuf *b, const char *tpl, int64_t start, int size) {
    for (const char *p = tpl ? tpl : ""; *p; ) {
        if (strncmp(p,"{StartRow}",10)==0) { char n[32]; int k=snprintf(n,sizeof(n),"%lld",(long long)start); curl_write((void*)n,1,(size_t)k,b); p+=10; }
        else if (strncmp(p,"{PageSize}",10)==0) { char n[32]; int k=snprintf(n,sizeof(n),"%d",size); curl_write((void*)n,1,(size_t)k,b); p+=10; }
        else { char c=*p; curl_write((void*)&c,1,1,b); p++; }
    }
}

/* POST SOAP-запроса; тело ответа → арена a (NULL при ошибке). */
static char *soap_post(SoapCtx *ctx, const char *req_body, size_t req_len, Arena *a) {
    CURL *curl=curl_easy_init(); if(!curl) return NULL;
    struct curl_slist *hdrs=NULL;
    if (soap_is12(ctx)) {
        char ct[512];
        if (ctx->soap_action[0]) snprintf(ct,sizeof(ct),"Content-Type: application/soap+xml; charset=utf-8; action=\"%s\"",ctx->soap_action);
        else snprintf(ct,sizeof(ct),"Content-Type: application/soap+xml; charset=utf-8");
        hdrs=curl_slist_append(hdrs,ct);
    } else {
        hdrs=curl_slist_append(hdrs,"Content-Type: text/xml; charset=utf-8");
        char sa[400]; snprintf(sa,sizeof(sa),"SOAPAction: \"%s\"",ctx->soap_action);
        hdrs=curl_slist_append(hdrs,sa);
    }
    hdrs=curl_slist_append(hdrs,"Accept: application/xml, text/xml, application/soap+xml");
    soap_apply_auth(curl,ctx,&hdrs);
    CurlBuf resp={NULL,0,0};
    curl_easy_setopt(curl,CURLOPT_URL,ctx->url);
    curl_easy_setopt(curl,CURLOPT_POST,1L);
    curl_easy_setopt(curl,CURLOPT_POSTFIELDS,req_body?req_body:"");
    curl_easy_setopt(curl,CURLOPT_POSTFIELDSIZE,(long)req_len);
    curl_easy_setopt(curl,CURLOPT_HTTPHEADER,hdrs);
    curl_easy_setopt(curl,CURLOPT_TIMEOUT,60L);
    curl_easy_setopt(curl,CURLOPT_SSL_VERIFYPEER,1L);
    curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,curl_write);
    curl_easy_setopt(curl,CURLOPT_WRITEDATA,&resp);
    CURLcode res=curl_easy_perform(curl);
    long code=0; curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&code);
    curl_slist_free_all(hdrs); curl_easy_cleanup(curl);
    if (res!=CURLE_OK) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),"Ошибка соединения с %s: %s",ctx->url,curl_easy_strerror(res));
        free(resp.data); return NULL;
    }
    /* SOAP-сбой обычно отдаётся как HTTP 500 с <soap:Fault> — выносим причину. */
    if (code<200 || code>=300) {
        if (resp.data && resp.len) {
            char snip[200]; size_t n=resp.len<sizeof(snip)-1?resp.len:sizeof(snip)-1;
            memcpy(snip,resp.data,n); snip[n]='\0';
            for (size_t i=0;i<n;i++) if(snip[i]=='\n'||snip[i]=='\r') snip[i]=' ';
            snprintf(ctx->last_err,sizeof(ctx->last_err),"HTTP %ld from %s: %s",code,ctx->url,snip);
        } else snprintf(ctx->last_err,sizeof(ctx->last_err),"HTTP %ld from %s",code,ctx->url);
        free(resp.data); return NULL;
    }
    char *r = resp.data ? arena_strdup(a,resp.data) : arena_strdup(a,"");
    free(resp.data); return r;
}

/* Контейнер записей: Envelope→Body→(data_path). */
static XmlNode *soap_container(SoapCtx *ctx, XmlNode *root) {
    XmlNode *body = xml_child(root,"Body");
    XmlNode *cont = body ? body : root;
    if (ctx->data_path[0]) {
        char buf[256]; strncpy(buf,ctx->data_path,sizeof(buf)-1); buf[sizeof(buf)-1]='\0';
        for (char *tok=strtok(buf,"."); tok && cont; tok=strtok(NULL,".")) cont=xml_child(cont,tok);
    }
    return cont;
}
/* Найти записи в контейнере: по row_tag (рекурсивно), иначе самый частый
 * элемент глубже операции-обёртки. */
static size_t soap_locate_rows(SoapCtx *ctx, XmlNode *root, XmlNode **out, size_t max) {
    XmlNode *cont = soap_container(ctx, root);
    if (!cont) return 0;
    if (ctx->row_tag[0]) return xml_find_all(cont, ctx->row_tag, out, max);
    /* нет row_tag: спускаемся в первого ребёнка (обёртка операции-ответа) и
     * берём самый частый local-name его прямых детей. */
    XmlNode *scope = cont->nkids==1 ? cont->kids[0] : cont;
    const char *best=NULL; int bestc=0;
    for (size_t i=0;i<scope->nkids;i++) {
        const char *nm=xml_local(scope->kids[i]->name);
        int c=0; for (size_t j=0;j<scope->nkids;j++) if (strcmp(xml_local(scope->kids[j]->name),nm)==0) c++;
        if (c>bestc) { bestc=c; best=nm; }
    }
    if (!best) return 0;
    size_t n=0;
    for (size_t i=0;i<scope->nkids;i++)
        if (strcmp(xml_local(scope->kids[i]->name),best)==0) { if (n<max) out[n]=scope->kids[i]; n++; }
    return n;
}

/* Схема (TEXT-always): уникальные local-names прямых детей записи, в порядке появления. */
static Schema *soap_schema_from(XmlNode *rec, Arena *a) {
    if (!rec) return NULL;
    const char *names[MAX_COLS]; int nc=0;
    for (size_t i=0;i<rec->nkids && nc<MAX_COLS;i++) {
        const char *nm=xml_local(rec->kids[i]->name);
        int dup=0; for (int j=0;j<nc;j++) if (strcmp(names[j],nm)==0){dup=1;break;}
        if (!dup) names[nc++]=nm;
    }
    if (nc==0) return NULL;
    Schema *sc=arena_calloc(a,sizeof(Schema));
    sc->ncols=nc; sc->cols=arena_alloc(a,(size_t)nc*sizeof(ColDef));
    for (int c=0;c<nc;c++){ sc->cols[c].name=arena_strdup(a,names[c]); sc->cols[c].type=COL_TEXT; sc->cols[c].nullable=true; }
    return sc;
}
/* Заполнить батч из массива записей по схеме; нет дочернего элемента → NULL-бит. */
static int soap_fill(XmlNode **rows, int nrows, Schema *sc, ColBatch *b, Arena *a) {
    for (int n=0;n<nrows;n++) {
        XmlNode *rec=rows[n];
        for (int c=0;c<sc->ncols;c++) {
            XmlNode *cell = rec ? xml_child(rec, sc->cols[c].name) : NULL;
            if (!cell) { b->null_bitmap[c][n/8]|=(uint8_t)(1u<<(n%8)); continue; }
            ((const char**)b->values[c])[n]=arena_strdup(a, cell->text?cell->text:"");
        }
    }
    return nrows;
}

/* describe(): POST-ит шаблон (StartRow=0), находит первую запись в ответе и
 * выводит из неё TEXT-схему. entity игнорируется (одна сущность). */
static int soap_describe(void *vctx, Arena *a, const char *entity, Schema **out) {
    (void)entity;
    SoapCtx *ctx=vctx;
    if (!ctx->url[0] || !ctx->request_template[0]) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),"soap: нужны url и request_template для чтения");
        return -1;
    }
    CurlBuf req={NULL,0,0};
    soap_subst(&req, ctx->request_template, 0, ctx->page_size);
    char *body=soap_post(ctx, req.data?req.data:"", req.len, a);
    free(req.data);
    if (!body) return -1;
    XmlNode *root=xml_parse(a,body,strlen(body));
    if (!root) { snprintf(ctx->last_err,sizeof(ctx->last_err),"soap: не разобран XML-ответ"); return -1; }
    XmlNode *rows[1]={0};
    if (soap_locate_rows(ctx,root,rows,1)==0 || !rows[0]) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),"soap: записи не найдены (row_tag/data_path)");
        return -1;
    }
    Schema *sc=soap_schema_from(rows[0],a);
    if (!sc) return -1;
    *out=sc; return 0;
}

/* read_batch: POST шаблона с {StartRow}=offset+nrows / {PageSize}; склейка
 * страниц в один батч до BATCH_SIZE. rc: <0 ошибка, 0 ещё есть, 1 конец. */
static int soap_read_batch(void *vctx, Arena *a, DfoReadReq *req, const char *entity, ColBatch **out) {
    (void)entity;
    SoapCtx *ctx=vctx;
    if (!ctx->url[0] || !ctx->request_template[0]) {
        snprintf(ctx->last_err,sizeof(ctx->last_err),"soap: нужны url и request_template для чтения");
        return -1;
    }
    int64_t offset=0;
    if (req && req->cursor && req->cursor[0]) offset=strtoll(req->cursor,NULL,10);
    int paged = (strstr(ctx->request_template,"{StartRow}")!=NULL);

    XmlNode **rows=arena_alloc(a,(size_t)BATCH_SIZE*sizeof(XmlNode*));
    int nrows=0, done=0;
    while (nrows<BATCH_SIZE && !done) {
        CurlBuf rb={NULL,0,0};
        soap_subst(&rb, ctx->request_template, offset+nrows, ctx->page_size);
        char *body=soap_post(ctx, rb.data?rb.data:"", rb.len, a);
        free(rb.data);
        if (!body) return -1;
        XmlNode *root=xml_parse(a,body,strlen(body));
        if (!root) { snprintf(ctx->last_err,sizeof(ctx->last_err),"soap: не разобран XML-ответ"); return -1; }
        XmlNode **page=arena_alloc(a,(size_t)BATCH_SIZE*sizeof(XmlNode*));
        size_t got=soap_locate_rows(ctx,root,page,BATCH_SIZE);
        if (got==0) { done=1; break; }
        size_t take = got < (size_t)(BATCH_SIZE-nrows) ? got : (size_t)(BATCH_SIZE-nrows);
        for (size_t i=0;i<take;i++) rows[nrows++]=page[i];
        if (!paged)                done=1;   /* без пагинации — один POST */
        if ((int)got < ctx->page_size) done=1;
        if (take < got) break;
    }
    if (nrows==0) return 1;
    Schema *sc=soap_schema_from(rows[0],a);
    if (!sc) { snprintf(ctx->last_err,sizeof(ctx->last_err),"soap: не вывести схему"); return -1; }
    ColBatch *b=arena_calloc(a,sizeof(ColBatch));
    b->schema=sc; b->ncols=sc->ncols;
    for (int c=0;c<sc->ncols;c++) { b->null_bitmap[c]=arena_calloc(a,((size_t)nrows+7)/8); b->values[c]=arena_alloc(a,(size_t)nrows*sizeof(char*)); }
    b->nrows=soap_fill(rows,nrows,sc,b,a);
    *out=b;
    LOG_INFO("soap source: %d rows (offset %lld%s)", b->nrows, (long long)offset, done?", end":"");
    return done?1:0;
}

/* list_entities(): одна сущность — имя SOAP-операции (operation или "Operation"). */
static int soap_list_entities(void *vctx, Arena *a, DfoEntityList *out) {
    SoapCtx *ctx=vctx;
    out->items=arena_calloc(a,sizeof(DfoEntity));
    out->items[0].entity=arena_strdup(a, ctx->operation[0]?ctx->operation:"Operation");
    out->items[0].type="table"; out->count=1;
    return 0;
}
/* ping(): HEAD на url; успех = транспорт ОК и код < 500 (4xx/405 у SOAP-эндпоинта
 * на HEAD/GET — норма: сервер жив, но метод не тот). */
static int soap_ping(void *vctx) {
    SoapCtx *ctx=vctx;
    if (!ctx->url[0]) { snprintf(ctx->last_err,sizeof(ctx->last_err),"soap: не задан url"); return -1; }
    CURL *curl=curl_easy_init(); if(!curl) return -1;
    curl_easy_setopt(curl,CURLOPT_URL,ctx->url);
    curl_easy_setopt(curl,CURLOPT_NOBODY,1L);
    curl_easy_setopt(curl,CURLOPT_TIMEOUT,15L);
    curl_easy_setopt(curl,CURLOPT_SSL_VERIFYPEER,1L);
    curl_easy_setopt(curl,CURLOPT_FOLLOWLOCATION,1L);
    struct curl_slist *hdrs=NULL; soap_apply_auth(curl,ctx,&hdrs);
    if (hdrs) curl_easy_setopt(curl,CURLOPT_HTTPHEADER,hdrs);
    CURLcode res=curl_easy_perform(curl);
    long code=0; curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&code);
    if (hdrs) curl_slist_free_all(hdrs);
    curl_easy_cleanup(curl);
    if (res!=CURLE_OK) { snprintf(ctx->last_err,sizeof(ctx->last_err),"Ошибка соединения с %s: %s",ctx->url,curl_easy_strerror(res)); return -1; }
    if (code>=500) { snprintf(ctx->last_err,sizeof(ctx->last_err),"HTTP %ld от %s",code,ctx->url); return -1; }
    return 0;
}

/* ── NULL-контракт приёмника ── */
static int soap_cell_null(const ColBatch *b, int c, int r) {
    const uint8_t *bm=b->null_bitmap[c];
    if (bm && ((bm[r/8]>>(r%8))&1u)) return 1;
    const char *s=((char**)b->values[c])[r];
    return (s && strcmp(s,DFO_NULL_SENTINEL)==0);
}

/* write_batch: оборачивает батч в SOAP-конверт и POST-ит. */
static int soap_write_batch(void *vctx, Arena *a, const char *entity,
                            const Schema *schema, const ColBatch *batch, int mode) {
    (void)a; (void)mode;
    SoapCtx *ctx=vctx;
    if (!ctx->url[0]) { snprintf(ctx->last_err,sizeof(ctx->last_err),"soap sink: не задан url"); return -1; }
    ctx->sink_started=1;
    const char *op  = (entity && entity[0]) ? entity : (ctx->operation[0]?ctx->operation:"Import");
    const char *rt  = ctx->sink_row_tag[0]?ctx->sink_row_tag:"row";
    const char *env = soap_is12(ctx) ? SOAP12_NS : SOAP11_NS;

    CurlBuf body={NULL,0,0};
    curl_write((void*)"<?xml version=\"1.0\" encoding=\"UTF-8\"?>",1,38,&body);
    curl_write((void*)"<soap:Envelope xmlns:soap=\"",1,27,&body);
    curl_write((void*)env,1,strlen(env),&body);
    curl_write((void*)"\"><soap:Body>",1,13,&body);
    curl_write((void*)"<",1,1,&body); xml_tag(&body,op);
    if (ctx->namespace_uri[0]) { curl_write((void*)" xmlns=\"",1,8,&body); xml_escape(&body,ctx->namespace_uri); curl_write((void*)"\"",1,1,&body); }
    curl_write((void*)">",1,1,&body);
    for (int r=0;r<batch->nrows;r++) {
        curl_write((void*)"<",1,1,&body); xml_tag(&body,rt); curl_write((void*)">",1,1,&body);
        for (int c=0;c<batch->ncols;c++) {
            curl_write((void*)"<",1,1,&body); xml_tag(&body,schema->cols[c].name); curl_write((void*)">",1,1,&body);
            if (!soap_cell_null(batch,c,r)) { const char *v=((char**)batch->values[c])[r]; xml_escape(&body, v?v:""); }
            curl_write((void*)"</",1,2,&body); xml_tag(&body,schema->cols[c].name); curl_write((void*)">",1,1,&body);
        }
        curl_write((void*)"</",1,2,&body); xml_tag(&body,rt); curl_write((void*)">",1,1,&body);
    }
    curl_write((void*)"</",1,2,&body); xml_tag(&body,op); curl_write((void*)">",1,1,&body);
    curl_write((void*)"</soap:Body></soap:Envelope>",1,28,&body);

    char *resp=soap_post(ctx, body.data?body.data:"", body.len, ctx->arena);
    free(body.data);
    if (!resp) { LOG_ERROR("soap sink: POST → %s failed: %s", ctx->url, ctx->last_err); return -1; }
    LOG_INFO("soap sink: POST %d rows → op '%s' @ %s", batch->nrows, op, ctx->url);
    return batch->nrows;
}

/* Точка входа плагина: по этому символу загрузчик находит ABI-таблицу.
 * describe/read_batch — source, write_batch — sink; CDC не поддерживается. */
const DfoConnector dfo_connector_entry = {
    .abi_version  = DFO_CONNECTOR_ABI_VERSION,
    .name         = "soap",
    .version      = "0.1.0",
    .description  = "SOAP/EAI connector (HTTP envelope, source+sink)",
    .create       = soap_create,
    .destroy      = soap_destroy,
    .list_entities= soap_list_entities,
    .describe     = soap_describe,
    .read_batch   = soap_read_batch,
    .cdc_start    = NULL,
    .cdc_stop     = NULL,
    .ping         = soap_ping,
    .write_batch  = soap_write_batch,
    .last_error   = soap_last_error,
};
