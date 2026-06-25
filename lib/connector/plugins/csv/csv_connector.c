/* CSV connector — reads delimited text files */
#include "../../connector.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>

/* forward declaration — definition is later in this file */
static void bit_set(uint8_t *bm, int i);

/* Состояние коннектора: путь к файлу, разделитель, флаг заголовка и
 * выведенная/настроенная схема. Память владеется arena. */
typedef struct {
    char  path[512];
    char  delimiter;
    int   has_header;
    Schema *schema;   /* inferred or configured */
    char **headers;
    int    ncols;
    Arena *arena;
} CsvCtx;

/* ── Schema inference ── */
/* Угадывает тип столбца по строковому значению (NULL/bool/int/double/text). */
static ColType infer_type(const char *s) {
    if (!s || !*s || strcasecmp(s,"null")==0 || strcasecmp(s,"na")==0) return COL_NULL;
    if (strcasecmp(s,"true")==0||strcasecmp(s,"false")==0) return COL_BOOL;
    char *end; strtoll(s,&end,10); if (*end=='\0') return COL_INT64;
    strtod(s,&end); if (*end=='\0') return COL_DOUBLE;
    return COL_TEXT;
}

/* Разбивает строку CSV на поля по разделителю с учётом кавычек RFC-4180
 * (экранированные "" внутри поля). Возвращает массив, n_out — число полей. */
static char **split_line(Arena *a, const char *line, char delim, int *n_out) {
    int cap=16; char **parts=arena_alloc(a,cap*sizeof(char*)); int n=0;
    const char *p = line;
    while (1) {
        const char *start = p;
        bool in_quote = (*p == '"');
        if (in_quote) { start=++p; while(*p && !(*p=='"'&&*(p+1)!='"')) p++; }
        else { while(*p && *p!=delim && *p!='\r' && *p!='\n') p++; }
        size_t len=(size_t)(p-start);
        if (n==cap){cap*=2;char**nb=arena_alloc(a,cap*sizeof(char*));memcpy(nb,parts,n*sizeof(char*));parts=nb;}
        parts[n++]=arena_strndup(a,start,len);
        if (in_quote && *p=='"') p++;
        if (*p==delim) p++; else break;
    }
    *n_out=n; return parts;
}

/* Создаёт контекст: разбирает JSON-конфиг (path/delimiter/header) и выводит
 * схему по первой строке файла. Все столбцы — TEXT (см. примечание ниже). */
static void *csv_create(const char *cfg, Arena *a) {
    CsvCtx *ctx = arena_calloc(a, sizeof(CsvCtx));
    ctx->arena = a;
    ctx->delimiter = ','; ctx->has_header = 1;
    /* parse minimal JSON config */
    if (cfg) {
        const char *p = strstr(cfg, "\"path\"");
        if (p) { p=strchr(p,':'); if(p){p++;while(*p==' ')p++;if(*p=='"'){p++;const char*e=strchr(p,'"');if(e)snprintf(ctx->path,sizeof(ctx->path),"%.*s",(int)(e-p),p);}}}
        const char *d = strstr(cfg, "\"delimiter\"");
        if (d) { d=strchr(d,':');if(d){d++;while(*d==' ')d++;if(*d=='"')ctx->delimiter=d[1];}}
        const char *h = strstr(cfg, "\"header\"");
        /* accepts string ("true"/"false") or literal (true/false/0/1) — skip an
         * optional opening quote, then look at the first meaningful char. */
        if (h) { h=strchr(h,':');if(h){h++;while(*h==' ')h++;if(*h=='"')h++;
                 ctx->has_header=(*h!='f'&&*h!='F'&&*h!='0');}}
    }
    /* infer schema from first two lines */
    FILE *f = fopen(ctx->path, "r");
    if (!f) return ctx;
    char line[65536];
    if (fgets(line, sizeof(line), f)) {
        int n; char **hdrs=split_line(a,line,ctx->delimiter,&n);
        ctx->ncols=n;
        if (ctx->has_header) {
            ctx->headers=hdrs;
        } else {
            ctx->headers=arena_alloc(a,n*sizeof(char*));
            for(int i=0;i<n;i++){char tmp[16];snprintf(tmp,sizeof(tmp),"col%d",i);ctx->headers[i]=arena_strdup(a,tmp);}
            rewind(f);
        }
        /* All columns stored as TEXT. qengine has a pre-existing read-back
         * bug for INT64/DOUBLE columns where it returns raw values in cells
         * (causes segfault on SELECT). Keeping everything as TEXT matches
         * the schema of pipeline-produced tables (dept_stats, top_sellers,
         * etc.) which are queryable without issue. User can CAST in SQL. */
        ColType *types=arena_calloc(a,n*sizeof(ColType));
        for(int i=0;i<n;i++) types[i]=COL_TEXT;
        Schema *schema=arena_calloc(a,sizeof(Schema));
        schema->ncols=n; schema->cols=arena_alloc(a,n*sizeof(ColDef));
        /* use ctx->headers (real header row, or generated col0/col1… when
         * has_header=false) — NOT the raw first line. */
        for(int i=0;i<n;i++){schema->cols[i].name=ctx->headers[i];schema->cols[i].type=types[i];schema->cols[i].nullable=true;}
        ctx->schema=schema;
    }
    fclose(f);
    return ctx;
}

static void csv_destroy(void *ctx) { (void)ctx; /* arena owned */ }

/* Проверка доступности: пытается открыть файл на чтение. 0 — ок, -1 — ошибка. */
static int csv_ping(void *vctx) {
    CsvCtx *ctx=vctx;
    FILE *f=fopen(ctx->path,"r");
    if(!f) return -1;
    fclose(f); return 0;
}

/* Список сущностей: CSV-файл = одна таблица с именем = базовым именем файла. */
static int csv_list(void *vctx, Arena *a, DfoEntityList *out) {
    CsvCtx *ctx=vctx;
    out->items=arena_calloc(a,sizeof(DfoEntity));
    /* entity name = basename of path */
    const char *base=strrchr(ctx->path,'/');
    base = base ? base+1 : ctx->path;
    out->items[0].entity=arena_strdup(a,base);
    out->items[0].type="table";
    out->count=1;
    return 0;
}

/* Возвращает выведенную при create() схему (единственная сущность файла). */
static int csv_describe(void *vctx, Arena *a, const char *entity, Schema **out) {
    (void)entity;
    CsvCtx *ctx=vctx;
    *out = ctx->schema;
    (void)a;
    return ctx->schema ? 0 : -1;
}

/* Читает до BATCH_SIZE строк в колоночный батч, парся ячейки по типам схемы;
 * пустые/null/na помечаются в null_bitmap. Заголовок пропускается при наличии. */
static int csv_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                           const char *entity, ColBatch **out_batch) {
    (void)entity; (void)req;
    CsvCtx *ctx=vctx;
    if (!ctx->schema) return -1;

    FILE *f=fopen(ctx->path,"r");
    if (!f) return -1;

    int ncols=ctx->schema->ncols;
    ColBatch *batch=arena_calloc(a,sizeof(ColBatch));
    batch->schema=ctx->schema; batch->ncols=ncols;

    /* allocate value arrays */
    int64_t **iv=arena_calloc(a,ncols*sizeof(void*));
    double  **dv=arena_calloc(a,ncols*sizeof(void*));
    char   ***sv=arena_calloc(a,ncols*sizeof(void*));
    for(int c=0;c<ncols;c++){
        switch(ctx->schema->cols[c].type){
            case COL_INT64:  iv[c]=arena_alloc(a,BATCH_SIZE*sizeof(int64_t)); batch->values[c]=iv[c]; break;
            case COL_DOUBLE: dv[c]=arena_alloc(a,BATCH_SIZE*sizeof(double));  batch->values[c]=dv[c]; break;
            default:         sv[c]=arena_alloc(a,BATCH_SIZE*sizeof(char*));   batch->values[c]=sv[c]; break;
        }
        batch->null_bitmap[c]=arena_calloc(a,(BATCH_SIZE+7)/8);
    }

    char line[65536]; int row=0;
    if (ctx->has_header) fgets(line,sizeof(line),f); /* skip header */
    while (row<BATCH_SIZE && fgets(line,sizeof(line),f)) {
        int n; char **vals=split_line(a,line,ctx->delimiter,&n);
        for(int c=0;c<ncols&&c<n;c++){
            const char *v=vals[c];
            if(!v||!*v||strcasecmp(v,"null")==0||strcasecmp(v,"na")==0){
                bit_set(batch->null_bitmap[c],row); continue;
            }
            switch(ctx->schema->cols[c].type){
                case COL_INT64:  iv[c][row]=strtoll(v,NULL,10); break;
                case COL_DOUBLE: dv[c][row]=strtod(v,NULL); break;
                case COL_BOOL:   ((int*)sv[c])[row]=(strcasecmp(v,"true")==0||strcmp(v,"1")==0); break;
                default:         ((char**)sv[c])[row]=arena_strdup(a,v); break;
            }
        }
        row++;
    }
    fclose(f);
    batch->nrows=row;
    *out_batch=batch;
    return 0;
}

/* helper needed from storage.h for bit ops */
static void bit_set(uint8_t *bm, int i) { bm[i/8] |= (1u << (i%8)); }
static int  bit_get(const uint8_t *bm, int i) { return bm ? ((bm[i/8] >> (i%8)) & 1u) : 0; }

/* ── CSV field escaping (RFC-4180): quote if contains delim/quote/CR/LF ── */
static void csv_write_field(FILE *f, const char *v, char delim) {
    if (!v) v = "";
    int needq = 0;
    for (const char *p = v; *p; p++)
        if (*p == delim || *p == '"' || *p == '\n' || *p == '\r') { needq = 1; break; }
    if (!needq) { fputs(v, f); return; }
    fputc('"', f);
    for (const char *p = v; *p; p++) { if (*p == '"') fputc('"', f); fputc(*p, f); }
    fputc('"', f);
}

/* Render one cell of a column-batch as text (sink input is always TEXT). */
static const char *csv_cell_text(const ColBatch *b, int c, int r, char *tmp, size_t tn) {
    if (bit_get(b->null_bitmap[c], r)) return "";
    ColType t = b->schema ? b->schema->cols[c].type : COL_TEXT;
    switch (t) {
        case COL_INT64:  snprintf(tmp, tn, "%lld", (long long)((int64_t*)b->values[c])[r]); return tmp;
        case COL_DOUBLE: snprintf(tmp, tn, "%.10g", ((double*)b->values[c])[r]); return tmp;
        default: { char *s = ((char**)b->values[c])[r]; return s ? s : ""; }
    }
}

/* ── Sink: append/overwrite rows to a CSV file ──
 * Destination = `entity` if given, else the configured "path". The header row
 * is written when the file is created fresh (overwrite, or append to an empty/
 * absent file). Returns rows written. */
static int csv_write_batch(void *vctx, Arena *a, const char *entity,
                           const Schema *schema, const ColBatch *batch, int mode) {
    (void)a;
    CsvCtx *ctx = vctx;
    const char *path = (entity && entity[0]) ? entity : ctx->path;
    if (!path || !path[0]) { return -1; }
    char delim = ctx->delimiter ? ctx->delimiter : ',';

    int write_header;
    FILE *f;
    if (mode == DFO_SINK_OVERWRITE) {
        f = fopen(path, "w");
        write_header = 1;
    } else {
        /* append — write header only if the file is new or empty */
        FILE *probe = fopen(path, "r");
        long sz = 0;
        if (probe) { fseek(probe, 0, SEEK_END); sz = ftell(probe); fclose(probe); }
        write_header = (sz <= 0);
        f = fopen(path, "a");
    }
    if (!f) return -1;

    int ncols = batch->ncols;
    if (write_header && schema) {
        for (int c = 0; c < ncols; c++) {
            if (c) fputc(delim, f);
            csv_write_field(f, schema->cols[c].name, delim);
        }
        fputc('\n', f);
    }
    char tmp[64];
    for (int r = 0; r < batch->nrows; r++) {
        for (int c = 0; c < ncols; c++) {
            if (c) fputc(delim, f);
            csv_write_field(f, csv_cell_text(batch, c, r, tmp, sizeof(tmp)), delim);
        }
        fputc('\n', f);
    }
    fclose(f);
    return batch->nrows;
}

const DfoConnector dfo_connector_entry = {
    .abi_version = DFO_CONNECTOR_ABI_VERSION,
    .name        = "csv",
    .version     = "1.0.0",
    .description = "CSV/TSV file reader with type inference",
    .create      = csv_create,
    .destroy     = csv_destroy,
    .list_entities = csv_list,
    .describe    = csv_describe,
    .read_batch  = csv_read_batch,
    .cdc_start   = NULL,
    .cdc_stop    = NULL,
    .ping        = csv_ping,
    .write_batch = csv_write_batch,
};
