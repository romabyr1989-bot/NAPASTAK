/* CSV connector — reads delimited text files */
/* Коннектор CSV: источник И приёмник для локальных файлов с разделителями
 * (CSV/TSV). Транспорт — прямой файловый ввод-вывод stdio (fopen/fgets),
 * внешних библиотек нет. Парсинг полей — по RFC-4180 (кавычки, экранирование
 * "" -> "). Значения хранятся как TEXT (см. csv_create). Реализует DfoConnector
 * ABI; CDC не поддерживается (cdc_start/cdc_stop = NULL). */
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
/* Угадывает тип столбца по строковому значению (NULL/bool/int/double/text).
 * Оставлен как утилита: csv_create принудительно назначает всем столбцам TEXT
 * (причина — в комментарии внутри csv_create), поэтому здесь тип не используется. */
static ColType infer_type(const char *s) {
    if (!s || !*s || strcasecmp(s,"null")==0 || strcasecmp(s,"na")==0) return COL_NULL;
    if (strcasecmp(s,"true")==0||strcasecmp(s,"false")==0) return COL_BOOL;
    char *end; strtoll(s,&end,10); if (*end=='\0') return COL_INT64;
    strtod(s,&end); if (*end=='\0') return COL_DOUBLE;
    return COL_TEXT;
}

/* Разбивает строку CSV на поля по разделителю с учётом кавычек RFC-4180.
 * Экранированные двойные кавычки "" внутри кавыченного поля сворачиваются в
 * одну ". Возвращает массив, n_out — число полей. Если err != NULL, в него
 * записывается ненулевое значение при ошибке парсинга (незакрытая кавычка,
 * мусор после закрывающей кавычки); в этом случае возвращается NULL. */
static char **split_line_ex(Arena *a, const char *line, char delim, int *n_out, int *err) {
    if (err) *err = 0;
    int cap=16; char **parts=arena_alloc(a,cap*sizeof(char*)); int n=0;
    const char *p = line;
    while (1) {
        bool in_quote = (*p == '"');
        char *field; size_t flen;
        if (in_quote) {
            /* Quoted field: scan, collapsing "" -> " into a freshly built buffer.
             * Worst case the unescaped content is no longer than the raw input. */
            p++; /* skip opening quote */
            char *buf = arena_alloc(a, strlen(p) + 1);
            size_t bi = 0;
            int closed = 0;
            while (*p) {
                if (*p == '"') {
                    if (*(p+1) == '"') { buf[bi++] = '"'; p += 2; continue; } /* escaped quote */
                    p++; closed = 1; break; /* closing quote */
                }
                buf[bi++] = *p++;
            }
            if (!closed) { if (err) *err = 1; return NULL; } /* unterminated quote */
            buf[bi] = '\0';
            /* After a closing quote only a delimiter or end-of-line is valid. */
            if (*p && *p != delim && *p != '\r' && *p != '\n') { if (err) *err = 1; return NULL; }
            field = buf; flen = bi;
        } else {
            const char *start = p;
            while (*p && *p!=delim && *p!='\r' && *p!='\n') {
                if (*p == '"') { if (err) *err = 1; return NULL; } /* stray quote in unquoted field */
                p++;
            }
            flen = (size_t)(p - start);
            field = arena_strndup(a, start, flen);
        }
        if (n==cap){cap*=2;char**nb=arena_alloc(a,cap*sizeof(char*));memcpy(nb,parts,n*sizeof(char*));parts=nb;}
        parts[n++]=field; (void)flen;
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
    /* Минимальный ручной разбор JSON-конфига (без парсера): ищем ключи
     * "path"/"delimiter"/"header" по подстроке и вытаскиваем значение вручную.
     * delimiter берётся как первый символ строкового значения. */
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
        int n; int perr=0; char **hdrs=split_line_ex(a,line,ctx->delimiter,&n,&perr);
        if (perr || !hdrs) { fclose(f); return ctx; } /* malformed header — leave schema NULL */
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

/* Освобождать нечего: всё состояние в арене — её чистит вызывающий. */
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
 * пустые/null/na помечаются в null_bitmap. Заголовок пропускается при наличии.
 * ВНИМАНИЕ: req (cursor/limit/filter) игнорируется — файл читается с начала,
 * пагинации между вызовами нет; отдаётся только первый батч файла. */
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
        int n; int perr=0; char **vals=split_line_ex(a,line,ctx->delimiter,&n,&perr);
        /* RFC-4180 strictness: a quote-parse error (unterminated/stray quote) is a
         * hard failure — do not silently succeed with a truncated/garbled row. */
        if (perr || !vals) { fclose(f); return -1; }
        /* More fields than the header advertises => probable schema shift or an
         * unescaped delimiter. Reject rather than silently dropping the tail. */
        if (n > ncols) { fclose(f); return -1; }
        for(int c=0;c<ncols;c++){
            /* Trailing fields absent in this short row => real NULL (sentinel). */
            const char *v = (c < n) ? vals[c] : DFO_NULL_SENTINEL;
            if(!v||!*v||strcasecmp(v,"null")==0||strcasecmp(v,"na")==0
               ||strcmp(v,DFO_NULL_SENTINEL)==0){
                bit_set(batch->null_bitmap[c],row);
                /* For TEXT-backed columns also materialise the sentinel so that
                 * downstream materialisation (api.c) sees the NULL contract value
                 * even if it reads the cell pointer directly. */
                switch(ctx->schema->cols[c].type){
                    case COL_INT64: case COL_DOUBLE: case COL_BOOL: break;
                    default: ((char**)sv[c])[row]=arena_strdup(a,DFO_NULL_SENTINEL); break;
                }
                continue;
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

/* Render one cell of a column-batch as text. Sink values are ALWAYS char* text
 * (text-always model); the schema's logical type is metadata only. Reading the
 * cell as a native int64/double (the old typed switch) reinterpreted the char*
 * pointer as the value and wrote the pointer ADDRESS into the CSV (R7). */
static const char *csv_cell_text(const ColBatch *b, int c, int r, char *tmp, size_t tn) {
    (void)tmp; (void)tn;
    if (bit_get(b->null_bitmap[c], r)) return "";
    char *s = ((char**)b->values[c])[r];
    /* NULL contract: a sentinel cell renders as an empty (unquoted) CSV field,
     * i.e. a real absent value — never the raw sentinel bytes. */
    if (s && strcmp(s, DFO_NULL_SENTINEL)==0) return "";
    return s ? s : "";
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
