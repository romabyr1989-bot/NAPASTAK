/* parquet_connector.c — минимальный ридер Apache Parquet v2 для DFO.
 * Поддерживает: INT32/64, INT96 (как INT64), FLOAT/DOUBLE, BYTE_ARRAY,
 * BOOLEAN; кодировка PLAIN; GZIP и UNCOMPRESSED; один файл или glob. */
#include "../../connector.h"
#include "../../../core/log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <zlib.h>
#include <glob.h>

/* ── LE-читалки (не зависят от endian.h) ── */
static uint32_t le32r(const uint8_t *p) {
    return (uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24);
}
static uint64_t le64r(const uint8_t *p) { return (uint64_t)le32r(p)|((uint64_t)le32r(p+4)<<32); }
static float    lef32(const uint8_t *p) { float f; memcpy(&f,p,4); return f; }
static double   lef64(const uint8_t *p) { double d; memcpy(&d,p,8); return d; }

/* ── Thrift Compact Protocol ── */
typedef struct { const uint8_t *d; size_t pos, len; } TBuf;

/* Беззнаковый varint (LEB128) */
static uint64_t tc_uv(TBuf *b) {
    uint64_t r=0; int sh=0;
    while (b->pos < b->len) {
        uint8_t c=b->d[b->pos++];
        r|=(uint64_t)(c&0x7F)<<sh;
        if(!(c&0x80)) return r;
        sh+=7;
    }
    return r;
}
/* Знаковый varint (zigzag-декодирование) */
static int64_t tc_i(TBuf *b) { uint64_t n=tc_uv(b); return (int64_t)((n>>1)^-(n&1)); }

/* Типы полей Compact Protocol */
#define TCT_BOOLT 1
#define TCT_BOOLF 2
#define TCT_BYTE  3
#define TCT_I16   4
#define TCT_I32   5
#define TCT_I64   6
#define TCT_DBL   7
#define TCT_BIN   8
#define TCT_LIST  9
#define TCT_SET   10
#define TCT_MAP   11
#define TCT_STRUCT 12

static void tc_skip(TBuf *b, int t);

/* Пропускает элемент типа LIST/SET без сохранения значений */
static void tc_skip_list(TBuf *b) {
    if (b->pos>=b->len) return;
    uint8_t h=b->d[b->pos++];
    int et=h&0x0F, cnt=(h>>4)&0x0F;
    if (cnt==15) cnt=(int)tc_uv(b);
    for (int i=0;i<cnt&&b->pos<b->len;i++) tc_skip(b,et);
}

/* Пропускает значение поля произвольного compact-типа t (рекурсивно для struct/map) */
static void tc_skip(TBuf *b, int t) {
    size_t l;
    switch(t) {
        case TCT_BOOLT: case TCT_BOOLF: break;
        case TCT_BYTE:  b->pos++; break;
        case TCT_I16: case TCT_I32: case TCT_I64: tc_uv(b); break;
        case TCT_DBL: if(b->pos+8<=b->len) b->pos+=8; break;
        case TCT_BIN: l=(size_t)tc_uv(b); b->pos+=l; break;
        case TCT_LIST: case TCT_SET: tc_skip_list(b); break;
        case TCT_MAP: {
            uint64_t cnt=tc_uv(b);
            if(!cnt) break;
            uint8_t types=b->d[b->pos++];
            int kt=(types>>4)&0x0F, vt=types&0x0F;
            for (uint64_t i=0;i<cnt&&b->pos<b->len;i++) { tc_skip(b,kt); tc_skip(b,vt); }
            break;
        }
        case TCT_STRUCT: {
            int last=0;
            while (b->pos<b->len) {
                uint8_t fh=b->d[b->pos++];
                if(!fh) break;
                int delta=(fh>>4)&0x0F, ft=fh&0x0F;
                if(delta) last+=delta; else last=(int)tc_i(b);
                (void)last;
                tc_skip(b,ft);
            }
            break;
        }
    }
}

/* Читает следующий field header; возвращает 0 на stop-байте или конце буфера */
static int tc_next(TBuf *b, int *last_fid, int *ftype) {
    if (b->pos>=b->len) return 0;
    uint8_t fh=b->d[b->pos++];
    if (!fh) return 0;
    int delta=(fh>>4)&0x0F;
    *ftype=fh&0x0F;
    if (delta) *last_fid+=delta; else *last_fid=(int)tc_i(b);
    return 1;
}

/* ── Константы Parquet ── */
#define PQ_BOOLEAN  0
#define PQ_INT32    1
#define PQ_INT64    2
#define PQ_INT96    3
#define PQ_FLOAT    4
#define PQ_DOUBLE   5
#define PQ_BYTEARRAY 6
#define PQ_FIXEDLEN  7

#define PQ_UNCOMP   0
#define PQ_SNAPPY   1
#define PQ_GZIP     2

#define PQ_DATA_PAGE   0
#define PQ_DICT_PAGE   2
#define PQ_DATA_V2     3

#define PQ_PLAIN        0

#define PQ_MAX_COLS 256
#define PQ_MAX_RG  4096

/* ── Внутренние структуры ── */
typedef struct {
    char name[128];
    int  pq_type;    /* -1 для групповых узлов */
    int  repetition; /* 0=REQUIRED, 1=OPTIONAL */
    int  num_children;
} PqRawSchema;

typedef struct {
    char name[128];
    int  pq_type;
    bool optional;
} PqColDef;

typedef struct {
    int64_t data_page_offset;
    int64_t dict_page_offset;   /* 0 = none; points to the dictionary page */
    int64_t num_values;
    int64_t total_compressed;
    int     pq_type;
    int     codec;
} PqColMeta;

typedef struct {
    int64_t   num_rows;
    PqColMeta cols[PQ_MAX_COLS];
    int       ncols;
} PqRowGroup;

typedef struct {
    FILE     *fp;
    char      path[512];
    PqColDef  cols[PQ_MAX_COLS];
    int       ncols;
    PqRowGroup *rgs;
    int        nrgs;
    Arena     *arena;
} PqFile;

/* Позиция чтения — хранится в PqCtx, не в курсоре */
typedef struct {
    char    **paths;
    int       npaths;
    PqFile   *files;
    Arena    *arena;
    int       pos_file;
    int       pos_rg;
    int64_t   pos_row; /* ряд внутри текущей row group */
} PqCtx;

/* ── GZIP деcompression ── */
static int gzip_decomp(const uint8_t *src, size_t slen,
                        uint8_t *dst, size_t dlen) {
    z_stream z={0};
    z.next_in=(Bytef*)src; z.avail_in=(uInt)slen;
    z.next_out=(Bytef*)dst; z.avail_out=(uInt)dlen;
    /* windowBits 47 = автодетект gzip/zlib */
    if (inflateInit2(&z,47)!=Z_OK) return -1;
    int r=inflate(&z,Z_FINISH);
    inflateEnd(&z);
    return (r==Z_STREAM_END)?0:-1;
}

/* ── RLE/Bit-Packing для уровней определения ── */
static int rle_bp_dec(const uint8_t *data, size_t dlen,
                       int bw, uint8_t *out, int nmax) {
    TBuf b={data,0,dlen};
    int n=0;
    while (b.pos<b.len && n<nmax) {
        uint64_t hdr=tc_uv(&b);
        if (!hdr) break;
        if (hdr&1) {
            int grps=(int)(hdr>>1);
            int mask=(1<<bw)-1;
            for (int g=0;g<grps&&b.pos<b.len&&n<nmax;g++) {
                uint8_t byte=b.d[b.pos++];
                for (int v=0;v<8&&n<nmax;v++)
                    out[n++]=(byte>>(v*bw))&mask;
            }
        } else {
            int cnt=(int)(hdr>>1);
            int vb=(bw+7)/8; uint32_t val=0;
            for (int i=0;i<vb&&b.pos<b.len;i++)
                val|=(uint32_t)b.d[b.pos++]<<(i*8);
            for (int i=0;i<cnt&&n<nmax;i++) out[n++]=(uint8_t)val;
        }
    }
    return n;
}

/* ── Парсер схемы ── */
/* Разбирает список SchemaElement в плоский массив raw (включая root и группы) */
static int pq_parse_schema_list(TBuf *b, PqRawSchema *raw, int maxn) {
    if (b->pos>=b->len) return 0;
    uint8_t h=b->d[b->pos++];
    int et=h&0x0F, cnt=(h>>4)&0x0F;
    if (cnt==15) cnt=(int)tc_uv(b);
    if (et!=TCT_STRUCT) { for(int i=0;i<cnt;i++) tc_skip(b,et); return 0; }
    int n=0;
    for (int i=0;i<cnt&&n<maxn&&b->pos<b->len;i++) {
        PqRawSchema *s=&raw[n]; memset(s,0,sizeof(*s)); s->pq_type=-1;
        int last=0,ft;
        while (tc_next(b,&last,&ft)) {
            switch(last) {
                case 1: s->pq_type=(int)tc_i(b); break;
                case 3: s->repetition=(int)tc_i(b); break;
                case 4: { size_t l=(size_t)tc_uv(b);
                    size_t cp=l<127?l:127; memcpy(s->name,b->d+b->pos,cp); s->name[cp]='\0';
                    b->pos+=l; break; }
                case 5: s->num_children=(int)tc_i(b); break;
                default: tc_skip(b,ft); break;
            }
        }
        n++;
    }
    return n;
}

/* Выбирает листовые узлы схемы (num_children==0) как реальные колонки */
static int pq_collect_leaves(PqRawSchema *raw, int nraw, PqColDef *cols, int max) {
    int n=0;
    for (int i=1;i<nraw&&n<max;i++) /* 0 = root */
        if (raw[i].num_children==0) {
            cols[n].pq_type=raw[i].pq_type;
            cols[n].optional=(raw[i].repetition==1);
            memcpy(cols[n].name,raw[i].name,128);
            n++;
        }
    return n;
}

/* ── Парсер ColumnMetaData ── */
static void pq_parse_col_meta(TBuf *b, PqColMeta *cm) {
    int last=0,ft;
    while (tc_next(b,&last,&ft)) {
        switch(last) {
            case 1: cm->pq_type=(int)tc_i(b); break;
            case 2: tc_skip(b,ft); break; /* encodings */
            case 3: tc_skip(b,ft); break; /* path_in_schema */
            case 4: cm->codec=(int)tc_i(b); break;
            case 5: cm->num_values=tc_i(b); break;
            case 6: tc_i(b); break; /* total_uncompressed */
            case 7: cm->total_compressed=tc_i(b); break;
            case 8: tc_skip(b,ft); break; /* kv_metadata */
            case 9: cm->data_page_offset=tc_i(b); break;
            case 11: cm->dict_page_offset=tc_i(b); break;
            default: tc_skip(b,ft); break;
        }
    }
}

/* Разбирает ColumnChunk: интересует только вложенный ColumnMetaData (поле 3) */
static void pq_parse_col_chunk(TBuf *b, PqColMeta *cm) {
    memset(cm,0,sizeof(*cm));
    int last=0,ft;
    while (tc_next(b,&last,&ft)) {
        switch(last) {
            case 1: tc_skip(b,ft); break; /* file_path */
            case 2: tc_i(b); break;       /* file_offset */
            case 3: pq_parse_col_meta(b,cm); break;
            default: tc_skip(b,ft); break;
        }
    }
}

/* Разбирает список ColumnChunk внутри row group */
static int pq_parse_col_list(TBuf *b, PqRowGroup *rg, int maxcols) {
    if (b->pos>=b->len) return 0;
    uint8_t h=b->d[b->pos++];
    int et=h&0x0F, cnt=(h>>4)&0x0F;
    if (cnt==15) cnt=(int)tc_uv(b);
    if (et!=TCT_STRUCT) { for(int i=0;i<cnt;i++) tc_skip(b,et); return 0; }
    int n=0;
    for (int i=0;i<cnt&&n<maxcols&&b->pos<b->len;i++)
        pq_parse_col_chunk(b,&rg->cols[n++]);
    return n;
}

/* Разбирает один RowGroup: список колонок и число строк */
static void pq_parse_row_group(TBuf *b, PqRowGroup *rg) {
    memset(rg,0,sizeof(*rg));
    int last=0,ft;
    while (tc_next(b,&last,&ft)) {
        switch(last) {
            case 1: rg->ncols=pq_parse_col_list(b,rg,PQ_MAX_COLS); break;
            case 2: tc_i(b); break; /* total_byte_size */
            case 3: rg->num_rows=tc_i(b); break;
            default: tc_skip(b,ft); break;
        }
    }
}

/* Разбирает список RowGroup из FileMetaData */
static int pq_parse_rg_list(TBuf *b, PqRowGroup *rgs, int maxrg) {
    if (b->pos>=b->len) return 0;
    uint8_t h=b->d[b->pos++];
    int et=h&0x0F, cnt=(h>>4)&0x0F;
    if (cnt==15) cnt=(int)tc_uv(b);
    if (et!=TCT_STRUCT) { for(int i=0;i<cnt;i++) tc_skip(b,et); return 0; }
    int n=0;
    for (int i=0;i<cnt&&n<maxrg&&b->pos<b->len;i++)
        pq_parse_row_group(b,&rgs[n++]);
    return n;
}

/* Разбирает FileMetaData (футер): схема + row groups; заполняет f->cols/f->rgs */
static int pq_parse_footer(const uint8_t *data, size_t len, PqFile *f) {
    TBuf b={data,0,len};
    PqRawSchema raw[PQ_MAX_COLS*2]; int nraw=0;
    int last=0,ft;
    while (tc_next(&b,&last,&ft)) {
        switch(last) {
            case 1: tc_i(&b); break; /* version */
            case 2: nraw=pq_parse_schema_list(&b,raw,PQ_MAX_COLS*2); break;
            case 3: tc_i(&b); break; /* num_rows */
            case 4: f->nrgs=pq_parse_rg_list(&b,f->rgs,PQ_MAX_RG); break;
            default: tc_skip(&b,ft); break;
        }
    }
    f->ncols=pq_collect_leaves(raw,nraw,f->cols,PQ_MAX_COLS);
    return (f->ncols>0)?0:-1;
}

/* ── Открытие файла и чтение футера ── */
static int pq_open(PqFile *f, Arena *a) {
    f->fp=fopen(f->path,"rb");
    if (!f->fp) return -1;
    uint8_t magic[4];
    fread(magic,1,4,f->fp);
    if (memcmp(magic,"PAR1",4)!=0) { fclose(f->fp); f->fp=NULL; return -1; }
    fseek(f->fp,-8,SEEK_END);
    uint8_t tail[8]; fread(tail,1,8,f->fp);
    if (memcmp(tail+4,"PAR1",4)!=0) { fclose(f->fp); f->fp=NULL; return -1; }
    uint32_t flen=le32r(tail);
    fseek(f->fp,-(long)(8+flen),SEEK_END);
    uint8_t *footer=arena_alloc(a,flen);
    fread(footer,1,flen,f->fp);
    f->rgs=arena_calloc(a,PQ_MAX_RG*sizeof(PqRowGroup));
    f->arena=a;
    return pq_parse_footer(footer,flen,f);
}

/* ── PageHeader (DATA_PAGE / DATA_V2) ── */
typedef struct {
    int     page_type;
    int32_t uncompressed_size;
    int32_t compressed_size;
    int32_t num_values;
    int     encoding;
    int     def_enc;
    /* V2 extras */
    int32_t def_bytes;
    int32_t rep_bytes;
    bool    v2_compressed;
    size_t  hdr_bytes; /* размер самого заголовка */
} PageHdr;

/* Разбирает PageHeader (общие поля + DataPage/DataPageV2/DictionaryPage);
 * ph->hdr_bytes = длина заголовка для последующего seek к телу страницы */
static void pq_parse_page_hdr(TBuf *b, PageHdr *ph) {
    memset(ph,0,sizeof(*ph)); ph->v2_compressed=true;
    int last=0,ft;
    while (tc_next(b,&last,&ft)) {
        switch(last) {
            case 1: ph->page_type=(int)tc_i(b); break;
            case 2: ph->uncompressed_size=(int32_t)tc_i(b); break;
            case 3: ph->compressed_size=(int32_t)tc_i(b); break;
            case 4: tc_i(b); break; /* crc */
            case 5: { /* DataPageHeader */
                int l2=0,ft2;
                while (tc_next(b,&l2,&ft2)) {
                    switch(l2) {
                        case 1: ph->num_values=(int32_t)tc_i(b); break;
                        case 2: ph->encoding=(int)tc_i(b); break;
                        case 3: ph->def_enc=(int)tc_i(b); break;
                        default: tc_skip(b,ft2); break;
                    }
                }
                break;
            }
            case 7: { /* DataPageHeaderV2 */
                int l2=0,ft2;
                while (tc_next(b,&l2,&ft2)) {
                    switch(l2) {
                        case 1: ph->num_values=(int32_t)tc_i(b); break;
                        case 4: ph->encoding=(int)tc_i(b); break;
                        case 5: ph->def_bytes=(int32_t)tc_i(b); break;
                        case 6: ph->rep_bytes=(int32_t)tc_i(b); break;
                        case 7: ph->v2_compressed=(ft2==TCT_BOOLT); break;
                        default: tc_skip(b,ft2); break;
                    }
                }
                break;
            }
            case 8: { /* DictionaryPageHeader */
                int l2=0,ft2;
                while(tc_next(b,&l2,&ft2)) {
                    switch(l2) {
                        case 1: ph->num_values=(int32_t)tc_i(b); break; /* dict entry count */
                        case 2: ph->encoding=(int)tc_i(b); break;
                        default: tc_skip(b,ft2); break;
                    }
                }
                break;
            }
            default: tc_skip(b,ft); break;
        }
    }
    ph->hdr_bytes=b->pos;
}

/* ── Пропуск n значений в PLAIN-потоке ── */
static const uint8_t *skip_plain(const uint8_t *p, const uint8_t *end, int pq_type, int n) {
    if (n<=0) return p;
    switch (pq_type) {
        case PQ_INT32: case PQ_FLOAT:   return p+4*(size_t)n<end?p+4*(size_t)n:end;
        case PQ_INT64: case PQ_DOUBLE:  return p+8*(size_t)n<end?p+8*(size_t)n:end;
        case PQ_INT96:                  return p+12*(size_t)n<end?p+12*(size_t)n:end;
        case PQ_BYTEARRAY:
            for (int i=0;i<n&&p+4<=end;i++) { uint32_t l=le32r(p); p+=4; p+=l; if(p>end)p=end; }
            return p;
        default: return end; /* BOOLEAN и неизвестные — пропустить страницу */
    }
}

/* ── Запись одного значения из PLAIN-потока в ColBatch ── */
/* Возвращает указатель за прочитанными байтами */
static const uint8_t *write_plain_val(const uint8_t *p, const uint8_t *end,
                                       int pq_type, int bool_bit,
                                       ColBatch *batch, int slot, int row, Arena *a) {
    switch (pq_type) {
        case PQ_INT32:
            if (p+4>end) return end;
            ((int64_t*)batch->values[slot])[row]=(int64_t)(int32_t)le32r(p);
            return p+4;
        case PQ_INT64:
            if (p+8>end) return end;
            ((int64_t*)batch->values[slot])[row]=(int64_t)le64r(p);
            return p+8;
        case PQ_INT96:
            if (p+12>end) return end;
            ((int64_t*)batch->values[slot])[row]=(int64_t)le64r(p);
            return p+12;
        case PQ_FLOAT:
            if (p+4>end) return end;
            ((double*)batch->values[slot])[row]=(double)lef32(p);
            return p+4;
        case PQ_DOUBLE:
            if (p+8>end) return end;
            ((double*)batch->values[slot])[row]=lef64(p);
            return p+8;
        case PQ_BYTEARRAY: {
            if (p+4>end) return end;
            uint32_t sl=le32r(p); p+=4;
            if (p+sl>end) sl=(uint32_t)(end-p);
            ((const char**)batch->values[slot])[row]=arena_strndup(a,(const char*)p,sl);
            return p+sl;
        }
        case PQ_BOOLEAN: {
            /* bool_bit = позиция бита в потоке */
            const uint8_t *bp=p+bool_bit/8;
            int v=(bp<end)?(((*bp)>>(bool_bit%8))&1):0;
            ((int64_t*)batch->values[slot])[row]=(int64_t)v;
            return p; /* указатель не меняем — продвигаем через bool_bit */
        }
        default:
            ((const char**)batch->values[slot])[row]="";
            return p;
    }
}

/* ── Snappy decompression (self-contained; libsnappy not required) ──
 * Implements the raw Snappy block format: leading varint length, then a stream
 * of literal/copy elements. Used because pandas/pyarrow/Spark default to snappy. */
static int snappy_uncompress(const uint8_t *src, size_t slen, uint8_t *dst, size_t dcap) {
    size_t sp = 0, dp = 0;
    int shift = 0;                              /* skip the uncompressed length varint */
    while (sp < slen) { uint8_t c = src[sp++]; (void)shift; shift += 7; if (!(c & 0x80)) break; }
    while (sp < slen) {
        uint8_t tag = src[sp++];
        if ((tag & 3) == 0) {                   /* literal */
            uint32_t len = (uint32_t)(tag >> 2) + 1;
            if (len > 60) {
                int nb = (int)len - 60; len = 0;
                for (int i = 0; i < nb && sp < slen; i++) len |= (uint32_t)src[sp++] << (i * 8);
                len += 1;
            }
            if (sp + len > slen || dp + len > dcap) return -1;
            memcpy(dst + dp, src + sp, len); dp += len; sp += len;
        } else {                                /* copy */
            uint32_t len, off;
            int t = tag & 3;
            if (t == 1)      { len = ((uint32_t)(tag >> 2) & 7) + 4; if (sp >= slen) return -1;
                               off = ((uint32_t)(tag >> 5) << 8) | src[sp++]; }
            else if (t == 2) { len = (uint32_t)(tag >> 2) + 1; if (sp + 2 > slen) return -1;
                               off = src[sp] | ((uint32_t)src[sp+1] << 8); sp += 2; }
            else             { len = (uint32_t)(tag >> 2) + 1; if (sp + 4 > slen) return -1;
                               off = src[sp] | ((uint32_t)src[sp+1]<<8) | ((uint32_t)src[sp+2]<<16) | ((uint32_t)src[sp+3]<<24); sp += 4; }
            if (off == 0 || off > dp || dp + len > dcap) return -1;
            for (uint32_t i = 0; i < len; i++) { dst[dp] = dst[dp - off]; dp++; }  /* may overlap */
        }
    }
    return (int)dp;
}

/* Decompress a page body by codec into dst (capacity dlen). Returns 0 on success. */
static int pq_decompress(int codec, const uint8_t *src, size_t slen, uint8_t *dst, size_t dlen) {
    if (codec == PQ_GZIP)   return gzip_decomp(src, slen, dst, dlen);
    if (codec == PQ_SNAPPY) return snappy_uncompress(src, slen, dst, dlen) >= 0 ? 0 : -1;
    return -1;
}

/* General RLE/bit-packed hybrid decoder → int32 values (dictionary indices).
 * Unlike rle_bp_dec (def levels, bit_width==1 only) this handles any bit_width. */
static int rle_dict_dec(const uint8_t *data, size_t dlen, int bw, int32_t *out, int nmax) {
    if (bw <= 0) { for (int i = 0; i < nmax; i++) out[i] = 0; return nmax; }
    TBuf b = { data, 0, dlen };
    int n = 0;
    while (b.pos < b.len && n < nmax) {
        uint64_t hdr = tc_uv(&b);
        if (hdr & 1) {                          /* bit-packed: (hdr>>1) groups of 8 */
            int groups = (int)(hdr >> 1);
            for (int g = 0; g < groups && n < nmax; g++) {
                if (b.pos + (size_t)bw > b.len) { b.pos = b.len; break; }
                const uint8_t *gp = &b.d[b.pos];
                for (int v = 0; v < 8 && n < nmax; v++) {
                    int bitpos = v * bw; int32_t val = 0;
                    for (int k = 0; k < bw; k++) { int bp = bitpos + k; val |= ((gp[bp >> 3] >> (bp & 7)) & 1) << k; }
                    out[n++] = val;
                }
                b.pos += (size_t)bw;
            }
        } else {                                /* RLE run */
            int cnt = (int)(hdr >> 1), vb = (bw + 7) / 8; uint32_t val = 0;
            for (int i = 0; i < vb && b.pos < b.len; i++) val |= (uint32_t)b.d[b.pos++] << (i * 8);
            for (int i = 0; i < cnt && n < nmax; i++) out[n++] = (int32_t)val;
        }
    }
    return n;
}

/* Write dictionary entry `di` (typed parallel arrays) into the batch cell. */
static void write_dict_val(ColBatch *batch, int slot, int row, int pq_type,
                           const int64_t *di64, const double *did, const char **dis,
                           int dict_n, int di) {
    if (di < 0 || di >= dict_n) { batch->null_bitmap[slot][row/8] |= (uint8_t)(1u<<(row%8)); return; }
    switch (pq_type) {
        case PQ_INT32: case PQ_INT64: case PQ_INT96: case PQ_BOOLEAN:
            ((int64_t*)batch->values[slot])[row] = di64[di]; break;
        case PQ_FLOAT: case PQ_DOUBLE:
            ((double*)batch->values[slot])[row] = did[di]; break;
        case PQ_BYTEARRAY:
            ((const char**)batch->values[slot])[row] = dis[di]; break;
        default: ((const char**)batch->values[slot])[row] = ""; break;
    }
}

/* ── Основная функция чтения одной колонки ── */
/* Читает [start_row, start_row+nrows) из row group rg_idx, колонка col_idx.
 * Записывает в batch->values[slot] начиная с batch_start. */
static void pq_read_col_range(PqFile *f, int rg_idx, int col_idx,
                               int64_t start_row, int nrows,
                               ColBatch *batch, int slot, int batch_start, Arena *a) {
    PqRowGroup *rg=&f->rgs[rg_idx];
    PqColMeta *cm=&rg->cols[col_idx];
    PqColDef  *cd=&f->cols[col_idx];

    if (nrows<=0) return;

    /* Dictionary page (when present) precedes the data pages. */
    int64_t start_off = cm->dict_page_offset > 0 ? cm->dict_page_offset : cm->data_page_offset;
    fseek(f->fp,start_off,SEEK_SET);

    int64_t rows_seen=0;   /* строки в уже обработанных data-страницах */
    int     rows_written=0;/* записано в batch */
    int     bool_bit=0;    /* для BOOLEAN: бит в потоке нон-нулл значений */

    /* Декодированный словарь (параллельные типизированные массивы). */
    int dict_n=0; int64_t *di64=NULL; double *did=NULL; const char **dis=NULL;

    while (rows_written<nrows && rows_seen<cm->num_values) {
        /* Читаем заголовок страницы */
        uint8_t hdr_buf[512];
        long hdr_pos=(long)ftell(f->fp);
        size_t hr=fread(hdr_buf,1,sizeof(hdr_buf),f->fp);
        if (!hr) break;
        TBuf tb={hdr_buf,0,hr};
        PageHdr ph; pq_parse_page_hdr(&tb,&ph);
        fseek(f->fp,hdr_pos+(long)ph.hdr_bytes,SEEK_SET);

        /* Распаковщик тела V1-страницы (data/dict целиком сжаты). */
        #define PQ_LOAD_BODY(BODY,BLEN) \
            uint8_t *comp=arena_alloc(a,(size_t)ph.compressed_size); \
            if (fread(comp,1,(size_t)ph.compressed_size,f->fp)!=(size_t)ph.compressed_size) break; \
            const uint8_t *BODY=comp; size_t BLEN=(size_t)ph.compressed_size; \
            if ((cm->codec==PQ_GZIP||cm->codec==PQ_SNAPPY) && ph.page_type!=PQ_DATA_V2) { \
                uint8_t *dd=arena_alloc(a,(size_t)ph.uncompressed_size+1); \
                if (pq_decompress(cm->codec,comp,(size_t)ph.compressed_size,dd,(size_t)ph.uncompressed_size)==0) { \
                    BODY=dd; BLEN=(size_t)ph.uncompressed_size; } }

        if (ph.page_type==PQ_DICT_PAGE) {
            PQ_LOAD_BODY(body,blen)
            dict_n=ph.num_values;
            const uint8_t *q=body, *qe=body+blen;
            if (cm->pq_type==PQ_BYTEARRAY) dis=arena_alloc(a,(size_t)(dict_n>0?dict_n:1)*sizeof(char*));
            else if (cm->pq_type==PQ_FLOAT||cm->pq_type==PQ_DOUBLE) did=arena_alloc(a,(size_t)(dict_n>0?dict_n:1)*sizeof(double));
            else di64=arena_alloc(a,(size_t)(dict_n>0?dict_n:1)*sizeof(int64_t));
            for (int i=0;i<dict_n && q<qe;i++) {
                switch (cm->pq_type) {
                    case PQ_INT32:  if(q+4<=qe){di64[i]=(int64_t)(int32_t)le32r(q);q+=4;} break;
                    case PQ_INT64:  if(q+8<=qe){di64[i]=(int64_t)le64r(q);q+=8;} break;
                    case PQ_INT96:  if(q+12<=qe){di64[i]=(int64_t)le64r(q);q+=12;} break;
                    case PQ_FLOAT:  if(q+4<=qe){did[i]=(double)lef32(q);q+=4;} break;
                    case PQ_DOUBLE: if(q+8<=qe){did[i]=lef64(q);q+=8;} break;
                    case PQ_BYTEARRAY: if(q+4<=qe){uint32_t sl=le32r(q);q+=4;if(q+sl>qe)sl=(uint32_t)(qe-q);dis[i]=arena_strndup(a,(const char*)q,sl);q+=sl;} break;
                    default: break;
                }
            }
            continue;
        }
        if (ph.page_type!=PQ_DATA_PAGE && ph.page_type!=PQ_DATA_V2) {
            fseek(f->fp,ph.compressed_size,SEEK_CUR); continue;
        }
        /* Страница полностью до start_row — пропускаем без чтения тела */
        if (rows_seen+ph.num_values <= start_row) {
            rows_seen+=ph.num_values; fseek(f->fp,ph.compressed_size,SEEK_CUR); continue;
        }

        PQ_LOAD_BODY(page_data,page_len)
        #undef PQ_LOAD_BODY
        const uint8_t *p=page_data, *end=page_data+page_len;

        /* V2: уровни повторения (для плоской схемы = 0 байт) */
        if (ph.page_type==PQ_DATA_V2) { p+=ph.rep_bytes; if(p>end)p=end; }

        /* Уровни определения для опциональных колонок */
        uint8_t *def_lvls=NULL;
        if (cd->optional) {
            if (ph.page_type==PQ_DATA_V2) {
                def_lvls=arena_alloc(a,(size_t)ph.num_values);
                rle_bp_dec(p,(size_t)ph.def_bytes,1,def_lvls,ph.num_values);
                p+=ph.def_bytes; if(p>end)p=end;
                if (cm->codec!=PQ_UNCOMP && ph.v2_compressed) {
                    size_t val_clen=(size_t)(end-p);
                    size_t val_ulen=(size_t)(ph.uncompressed_size-ph.def_bytes-ph.rep_bytes);
                    uint8_t *dv=arena_alloc(a,val_ulen+1);
                    if (pq_decompress(cm->codec,p,val_clen,dv,val_ulen)==0) { p=dv; end=dv+val_ulen; }
                }
            } else if (page_len>=4) {
                uint32_t def_len=le32r(p); p+=4; if(p+def_len>end) def_len=(uint32_t)(end-p);
                def_lvls=arena_alloc(a,(size_t)ph.num_values);
                rle_bp_dec(p,def_len,1,def_lvls,ph.num_values);
                p+=def_len; if(p>end)p=end;
            }
        }

        int skip_in_page=(int)(start_row>rows_seen ? start_row-rows_seen : 0);
        int avail=ph.num_values-skip_in_page;
        int take=(avail<nrows-rows_written)?avail:nrows-rows_written;

        int nn_skip=0;
        if (def_lvls) { for (int i=0;i<skip_in_page;i++) if(def_lvls[i]) nn_skip++; }
        else nn_skip=skip_in_page;

        bool is_dict = (ph.encoding==2 || ph.encoding==8);  /* PLAIN_DICTIONARY | RLE_DICTIONARY */
        if (is_dict && dict_n>0) {
            /* секция значений: 1 байт bit-width, затем RLE/bit-packed индексы */
            int bw=(p<end)?*p++:0;
            int nn_total=0;
            if (def_lvls) { for (int i=0;i<ph.num_values;i++) if(def_lvls[i]) nn_total++; }
            else nn_total=ph.num_values;
            int32_t *idx=arena_alloc(a,(size_t)(nn_total>0?nn_total:1)*sizeof(int32_t));
            rle_dict_dec(p,(size_t)(end-p),bw,idx,nn_total);
            int ipos=nn_skip;
            for (int i=0;i<take&&rows_written<nrows;i++) {
                int r=batch_start+rows_written;
                bool is_null=cd->optional&&def_lvls&&(def_lvls[skip_in_page+i]==0);
                if (is_null) batch->null_bitmap[slot][r/8]|=(uint8_t)(1u<<(r%8));
                else write_dict_val(batch,slot,r,cm->pq_type,di64,did,dis,dict_n,idx[ipos++]);
                rows_written++;
            }
        } else {
            /* PLAIN-значения */
            p=skip_plain(p,end,cm->pq_type,nn_skip);
            bool_bit=nn_skip;
            for (int i=0;i<take&&rows_written<nrows;i++) {
                int r=batch_start+rows_written;
                bool is_null=cd->optional&&def_lvls&&(def_lvls[skip_in_page+i]==0);
                if (is_null) batch->null_bitmap[slot][r/8]|=(uint8_t)(1u<<(r%8));
                else { p=write_plain_val(p,end,cm->pq_type,bool_bit,batch,slot,r,a); if (cm->pq_type==PQ_BOOLEAN) bool_bit++; }
                rows_written++;
            }
        }
        rows_seen+=ph.num_values;
    }
}

/* ── Маппинг Parquet → ColType ── */
/* Все целочисленные/булевы типы сводятся к COL_INT64, прочее (BYTE_ARRAY и т.п.) — к COL_TEXT */
static ColType pq_to_col_type(int pq_type) {
    switch(pq_type) {
        case PQ_INT32: case PQ_INT64: case PQ_INT96: case PQ_BOOLEAN: return COL_INT64;
        case PQ_FLOAT: case PQ_DOUBLE: return COL_DOUBLE;
        default: return COL_TEXT;
    }
}

/* ── cfg_get — простой парсер JSON-конфига ── */
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
        p++;
        const char *e=strchr(p,'"');
        if (!e) return;
        size_t n=(size_t)(e-p); if(n>=outsz)n=outsz-1;
        memcpy(out,p,n); out[n]='\0';
    } else {
        const char *e=p;
        while(*e&&*e!=','&&*e!='}'&&*e!=' '&&*e!='\n') e++;
        size_t n=(size_t)(e-p); if(n>=outsz)n=outsz-1;
        memcpy(out,p,n); out[n]='\0';
    }
}

/* ── Connector lifecycle ── */

/* create(): раскрывает glob из cfg.path и открывает/парсит футеры всех файлов */
static void *pq_create(const char *cfg, Arena *a) {
    PqCtx *ctx=arena_calloc(a,sizeof(PqCtx));
    ctx->arena=a;

    char path[512]=""; cfg_get(cfg,"path",path,sizeof(path),"");
    if (!path[0]) { LOG_ERROR("parquet_connector: no path in config"); return ctx; }

    /* Glob expansion */
    glob_t gl; memset(&gl,0,sizeof(gl));
    int gr=glob(path,0,NULL,&gl);
    if (gr!=0||gl.gl_pathc==0) { globfree(&gl); LOG_ERROR("parquet: no files for %s",path); return ctx; }

    ctx->npaths=(int)gl.gl_pathc;
    ctx->paths=arena_alloc(a,(size_t)ctx->npaths*sizeof(char*));
    ctx->files=arena_calloc(a,(size_t)ctx->npaths*sizeof(PqFile));

    for (int i=0;i<ctx->npaths;i++) {
        ctx->paths[i]=arena_strdup(a,gl.gl_pathv[i]);
        strncpy(ctx->files[i].path,gl.gl_pathv[i],511);
        if (pq_open(&ctx->files[i],a)!=0)
            LOG_WARN("parquet: failed to open %s",gl.gl_pathv[i]);
    }
    globfree(&gl);
    return ctx;
}

/* destroy(): закрывает открытые файловые дескрипторы (память — на арене) */
static void pq_destroy(void *vctx) {
    PqCtx *ctx=vctx;
    if (!ctx) return;
    for (int i=0;i<ctx->npaths;i++)
        if (ctx->files[i].fp) { fclose(ctx->files[i].fp); ctx->files[i].fp=NULL; }
}

/* ping(): проверяет доступность первого файла на чтение */
static int pq_ping(void *vctx) {
    PqCtx *ctx=vctx;
    if (!ctx||ctx->npaths==0) return -1;
    FILE *f=fopen(ctx->paths[0],"rb");
    if (!f) return -1;
    fclose(f); return 0;
}

/* list_entities(): каждый файл glob-набора — отдельная "таблица" (имя = basename) */
static int pq_list_entities(void *vctx, Arena *a, DfoEntityList *out) {
    PqCtx *ctx=vctx;
    out->items=arena_calloc(a,(size_t)ctx->npaths*sizeof(DfoEntity));
    out->count=ctx->npaths;
    for (int i=0;i<ctx->npaths;i++) {
        const char *base=strrchr(ctx->paths[i],'/');
        out->items[i].entity=arena_strdup(a,base?base+1:ctx->paths[i]);
        out->items[i].type="table";
    }
    return 0;
}

/* describe(): возвращает схему по первому файлу (entity игнорируется) */
static int pq_describe(void *vctx, Arena *a, const char *entity, Schema **out) {
    (void)entity;
    PqCtx *ctx=vctx;
    if (!ctx||ctx->npaths==0) return -1;
    PqFile *f=&ctx->files[0];
    Schema *sc=arena_calloc(a,sizeof(Schema));
    sc->ncols=f->ncols;
    sc->cols=arena_alloc(a,(size_t)f->ncols*sizeof(ColDef));
    for (int c=0;c<f->ncols;c++) {
        sc->cols[c].name=arena_strdup(a,f->cols[c].name);
        sc->cols[c].type=pq_to_col_type(f->cols[c].pq_type);
        sc->cols[c].nullable=f->cols[c].optional;
    }
    *out=sc; return 0;
}

/* read_batch использует внутреннее состояние pos_file/pos_rg/pos_row.
 * Курсор req->cursor игнорируется: каждый экземпляр коннектора живёт один сеанс. */
static int pq_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                          const char *entity, ColBatch **out_batch) {
    (void)entity; (void)req;
    PqCtx *ctx=vctx;
    if (!ctx||ctx->npaths==0) return 1;

    /* Находим текущий файл/rg */
    while (ctx->pos_file<ctx->npaths) {
        PqFile *f=&ctx->files[ctx->pos_file];
        if (ctx->pos_rg<f->nrgs && f->fp) break;
        ctx->pos_file++; ctx->pos_rg=0; ctx->pos_row=0;
    }
    if (ctx->pos_file>=ctx->npaths) return 1; /* конец данных */

    PqFile *f=&ctx->files[ctx->pos_file];
    int ncols=f->ncols;

    /* Строим batch, объединяя строки из нескольких rg при необходимости */
    Schema *sc=arena_calloc(a,sizeof(Schema));
    sc->ncols=ncols; sc->cols=arena_alloc(a,(size_t)ncols*sizeof(ColDef));
    for (int c=0;c<ncols;c++) {
        sc->cols[c].name=arena_strdup(a,f->cols[c].name);
        sc->cols[c].type=pq_to_col_type(f->cols[c].pq_type);
        sc->cols[c].nullable=f->cols[c].optional;
    }

    ColBatch *batch=arena_calloc(a,sizeof(ColBatch));
    batch->schema=sc; batch->ncols=ncols;
    for (int c=0;c<ncols;c++) {
        batch->null_bitmap[c]=arena_calloc(a,(BATCH_SIZE+7)/8);
        switch(sc->cols[c].type) {
            case COL_INT64:  batch->values[c]=arena_alloc(a,BATCH_SIZE*sizeof(int64_t)); break;
            case COL_DOUBLE: batch->values[c]=arena_alloc(a,BATCH_SIZE*sizeof(double));  break;
            default:         batch->values[c]=arena_alloc(a,BATCH_SIZE*sizeof(char*));   break;
        }
    }

    int batch_row=0;
    while (batch_row<BATCH_SIZE && ctx->pos_file<ctx->npaths) {
        f=&ctx->files[ctx->pos_file];
        if (!f->fp || ctx->pos_rg>=f->nrgs) {
            ctx->pos_file++; ctx->pos_rg=0; ctx->pos_row=0; continue;
        }
        PqRowGroup *rg=&f->rgs[ctx->pos_rg];
        int64_t avail_in_rg=rg->num_rows-ctx->pos_row;
        int take=(int)(avail_in_rg<BATCH_SIZE-batch_row ? avail_in_rg : BATCH_SIZE-batch_row);
        if (take<=0) { ctx->pos_rg++; ctx->pos_row=0; continue; }

        /* Нужно убедиться что rg->ncols совпадает с f->ncols */
        if (rg->ncols!=ncols) { ctx->pos_rg++; ctx->pos_row=0; continue; }

        for (int c=0;c<ncols;c++)
            pq_read_col_range(f,ctx->pos_rg,c,ctx->pos_row,take,batch,c,batch_row,a);

        batch_row+=take;
        ctx->pos_row+=take;
        if (ctx->pos_row>=rg->num_rows) { ctx->pos_rg++; ctx->pos_row=0; }
    }

    batch->nrows=batch_row;
    *out_batch=batch;
    return (batch_row>0)?0:1;
}

/* ── write_batch(): запись parquet-файла (sink) ──
 * Минимальный валидный parquet: одна row-group, по одной DATA_PAGE на колонку,
 * все колонки REQUIRED BYTE_ARRAY(UTF8), PLAIN, UNCOMPRESSED. Значения — строки
 * (числа форматируются, null → пустая строка). Метаданные — thrift compact. */
/* WBuf — растущий байтовый буфер для сериализации страниц и метаданных.
 * wb_*: запись; tw_*: примитивы thrift compact (zigzag-varint, field-header) */
typedef struct { uint8_t *d; size_t len, cap; } WBuf;
static void wb_init(WBuf *b){ b->cap=4096; b->d=malloc(b->cap); b->len=0; }
static void wb_ensure(WBuf *b,size_t n){ if(b->len+n>b->cap){ while(b->len+n>b->cap)b->cap*=2; b->d=realloc(b->d,b->cap);} }
static void wb_u8(WBuf *b,uint8_t v){ wb_ensure(b,1); b->d[b->len++]=v; }
static void wb_raw(WBuf *b,const void *p,size_t n){ wb_ensure(b,n); memcpy(b->d+b->len,p,n); b->len+=n; }
static void wb_uv(WBuf *b,uint64_t v){ while(v>=0x80){ wb_u8(b,(uint8_t)(v|0x80)); v>>=7;} wb_u8(b,(uint8_t)v); }
static void wb_zz32(WBuf *b,int32_t v){ wb_uv(b,((uint32_t)v<<1)^(uint32_t)(v>>31)); }
static void wb_zz64(WBuf *b,int64_t v){ wb_uv(b,((uint64_t)v<<1)^(uint64_t)(v>>63)); }
static void wb_le32(WBuf *b,uint32_t v){ uint8_t t[4]={(uint8_t)v,(uint8_t)(v>>8),(uint8_t)(v>>16),(uint8_t)(v>>24)}; wb_raw(b,t,4); }
/* thrift compact types */
#define TW_I32 5
#define TW_I64 6
#define TW_BIN 8
#define TW_LIST 9
#define TW_STRUCT 12
/* Field-header: дельта-кодирование id поля относительно *last, иначе long form */
static void tw_field(WBuf *b,int *last,int fid,int ct){
    int d=fid-*last;
    if (d>0&&d<=15) wb_u8(b,(uint8_t)((d<<4)|ct)); else { wb_u8(b,(uint8_t)ct); wb_zz32(b,fid); }
    *last=fid;
}
static void tw_i32(WBuf *b,int *last,int fid,int32_t v){ tw_field(b,last,fid,TW_I32); wb_zz32(b,v); }
static void tw_i64(WBuf *b,int *last,int fid,int64_t v){ tw_field(b,last,fid,TW_I64); wb_zz64(b,v); }
static void tw_str(WBuf *b,int *last,int fid,const char *s){ tw_field(b,last,fid,TW_BIN); size_t n=strlen(s); wb_uv(b,n); wb_raw(b,s,n); }
/* List-header: число элементов + тип элемента (long form при size>=15) */
static void tw_lh(WBuf *b,int size,int ect){ if(size<15) wb_u8(b,(uint8_t)((size<<4)|ect)); else { wb_u8(b,(uint8_t)(0xF0|ect)); wb_uv(b,(uint64_t)size);} }
static void tw_stop(WBuf *b){ wb_u8(b,0); }

/* Маппинг DFO ColType → Parquet physical type. */
static int pq_phys_type(ColType t) {
    switch (t) {
        case COL_INT64: return PQ_INT64;
        case COL_DOUBLE:return PQ_DOUBLE;
        case COL_BOOL:  return PQ_BOOLEAN;
        default:        return PQ_BYTEARRAY; /* COL_TEXT и прочее */
    }
}

/* true, если ячейка (col c, row r) — настоящий SQL NULL:
 * либо помечена в null_bitmap, либо текстовое значение равно DFO_NULL_SENTINEL.
 * В sink-пути ячейки всегда хранятся текстом, поэтому сентинел проверяем для
 * всех типов колонок (подстраховка, если null_bitmap не выставлен). */
static bool pq_cell_is_null(const Schema *schema, const ColBatch *batch, int c, int r) {
    (void)schema;
    const uint8_t *bm = batch->null_bitmap[c];
    if (bm && ((bm[r/8] >> (r%8)) & 1u)) return true;
    const char *v = ((const char**)batch->values[c])[r];
    if (v && strcmp(v, DFO_NULL_SENTINEL) == 0) return true;
    return false;
}

/* Кодирует уровни определения (bit_width=1) bit-packed RLE/hybrid-форматом и
 * пишет в out: [4-байтный LE размер блока][RLE-hybrid]. def[i]=1 -> значение,
 * def[i]=0 -> NULL. Совместимо с rle_bp_dec и внешними parquet-ридерами. */
static void pq_write_def_levels(WBuf *out, const uint8_t *def, int n) {
    WBuf lv; wb_init(&lv);
    int groups = (n + 7) / 8;                     /* округление вверх до 8 значений */
    wb_uv(&lv, ((uint64_t)groups << 1) | 1u);     /* bit-packed header */
    for (int g = 0; g < groups; g++) {
        uint8_t byte = 0;
        for (int v = 0; v < 8; v++) {
            int idx = g*8 + v;
            if (idx < n && def[idx]) byte |= (uint8_t)(1u << v);
        }
        wb_u8(&lv, byte);
    }
    wb_le32(out, (uint32_t)lv.len);
    wb_raw(out, lv.d, lv.len);
    free(lv.d);
}

/* Текстовое представление ячейки. В sink-пути gateway (run_sink_step) все
 * значения ColBatch хранятся как char* строки независимо от логического типа
 * колонки, поэтому числовые/булевы значения парсятся именно отсюда. */
static const char *pq_cell_text(const ColBatch *batch, int c, int r) {
    const char *v = ((const char**)batch->values[c])[r];
    return v ? v : "";
}

/* Парсит булеву строку ("true"/"false"/"t"/"1"/"0"...) в 0/1. */
static int pq_parse_bool(const char *v) {
    if (!v || !v[0]) return 0;
    if (v[0]=='t'||v[0]=='T'||v[0]=='y'||v[0]=='Y') return 1;
    if (v[0]=='f'||v[0]=='F'||v[0]=='n'||v[0]=='N') return 0;
    return strtoll(v,NULL,10)!=0;
}

/* Пишет одно НЕ-NULL значение колонки c, строки r в PLAIN-кодировке в vals.
 * Ячейка приходит текстом (sink-контракт), числовые/булевы колонки парсятся
 * в нативный Parquet-вид. BOOLEAN обрабатывается отдельно (бит-паковка). */
static void pq_write_plain_value(WBuf *vals, const Schema *schema,
                                  const ColBatch *batch, int c, int r) {
    ColType ct = schema->cols[c].type;
    const char *v = pq_cell_text(batch,c,r);
    switch (ct) {
        case COL_INT64: {
            int64_t iv = strtoll(v,NULL,10);
            uint8_t t[8]; for (int i=0;i<8;i++) t[i]=(uint8_t)((uint64_t)iv>>(i*8));
            wb_raw(vals,t,8); break;
        }
        case COL_DOUBLE: {
            double dv = strtod(v,NULL);
            uint8_t t[8]; memcpy(t,&dv,8); wb_raw(vals,t,8); break;
        }
        default: { /* COL_TEXT и прочее → BYTE_ARRAY(UTF8) */
            uint32_t L=(uint32_t)strlen(v); wb_le32(vals,L); wb_raw(vals,v,L); break;
        }
    }
}

static int pq_write_batch(void *vctx, Arena *a, const char *entity,
                          const Schema *schema, const ColBatch *batch, int mode) {
    (void)vctx; (void)a; (void)mode;   /* sink всегда пишет файл целиком */
    if (!entity || !entity[0]) { LOG_ERROR("parquet sink: empty target path"); return -1; }
    int ncols=batch->ncols, nrows=batch->nrows;
    FILE *f=fopen(entity,"wb");
    if (!f) { LOG_ERROR("parquet sink: cannot open %s", entity); return -1; }
    fwrite("PAR1",1,4,f);

    int64_t *coff=malloc((size_t)ncols*sizeof(int64_t)); /* смещение страницы каждой колонки */
    int64_t *csz =malloc((size_t)ncols*sizeof(int64_t)); /* размер chunk (hdr+vals) каждой колонки */
    int     *cpt =malloc((size_t)ncols*sizeof(int));     /* Parquet physical type каждой колонки */
    bool    *copt=malloc((size_t)ncols*sizeof(bool));    /* OPTIONAL (nullable либо есть NULL) */

    /* Предрасчёт физического типа и nullability по схеме + фактическим NULL. */
    for (int c=0;c<ncols;c++) {
        cpt[c]=pq_phys_type(schema->cols[c].type);
        bool opt=schema->cols[c].nullable;
        if (!opt) for (int r=0;r<nrows;r++) if (pq_cell_is_null(schema,batch,c,r)) { opt=true; break; }
        copt[c]=opt;
    }

    for (int c=0;c<ncols;c++) {
        coff[c]=(int64_t)ftell(f);

        /* Тело страницы: [def levels (если OPTIONAL)][PLAIN-значения не-NULL ячеек]. */
        WBuf body; wb_init(&body);
        if (copt[c]) {
            uint8_t *def=malloc((size_t)(nrows>0?nrows:1));
            for (int r=0;r<nrows;r++) def[r]=pq_cell_is_null(schema,batch,c,r)?0:1;
            pq_write_def_levels(&body,def,nrows);
            free(def);
        }
        if (cpt[c]==PQ_BOOLEAN) {
            /* PLAIN BOOLEAN: бит-паковка по 8 значений в байт (только не-NULL). */
            uint8_t acc=0; int nb=0;
            for (int r=0;r<nrows;r++) {
                if (copt[c] && pq_cell_is_null(schema,batch,c,r)) continue;
                if (pq_parse_bool(pq_cell_text(batch,c,r))) acc|=(uint8_t)(1u<<(nb&7));
                if ((++nb&7)==0) { wb_u8(&body,acc); acc=0; }
            }
            if (nb&7) wb_u8(&body,acc); /* хвостовые биты */
        } else {
            for (int r=0;r<nrows;r++) {
                if (copt[c] && pq_cell_is_null(schema,batch,c,r)) continue; /* NULL → нет значения */
                pq_write_plain_value(&body,schema,batch,c,r);
            }
        }

        WBuf hdr; wb_init(&hdr); int last=0;
        tw_i32(&hdr,&last,1,0);                 /* type = DATA_PAGE */
        tw_i32(&hdr,&last,2,(int32_t)body.len); /* uncompressed_page_size */
        tw_i32(&hdr,&last,3,(int32_t)body.len); /* compressed_page_size */
        tw_field(&hdr,&last,5,TW_STRUCT);       /* data_page_header */
        { int l2=0; tw_i32(&hdr,&l2,1,nrows); tw_i32(&hdr,&l2,2,0); /* num_values, PLAIN */
          tw_i32(&hdr,&l2,3,3); tw_i32(&hdr,&l2,4,3); tw_stop(&hdr); } /* def/rep enc = RLE */
        tw_stop(&hdr);
        fwrite(hdr.d,1,hdr.len,f); fwrite(body.d,1,body.len,f);
        csz[c]=(int64_t)(hdr.len+body.len);
        free(hdr.d); free(body.d);
    }

    /* FileMetaData */
    WBuf m; wb_init(&m); int last=0;
    tw_i32(&m,&last,1,1);                        /* version */
    tw_field(&m,&last,2,TW_LIST);                /* schema list */
    tw_lh(&m,ncols+1,TW_STRUCT);
    { int l2=0; tw_str(&m,&l2,4,"schema"); tw_i32(&m,&l2,5,ncols); tw_stop(&m); }  /* root */
    for (int c=0;c<ncols;c++){ int l2=0;
        tw_i32(&m,&l2,1,cpt[c]);                 /* physical type */
        tw_i32(&m,&l2,3,copt[c]?1:0);            /* repetition: 1=OPTIONAL, 0=REQUIRED */
        tw_str(&m,&l2,4,schema->cols[c].name);
        if (cpt[c]==PQ_BYTEARRAY) tw_i32(&m,&l2,6,0); /* converted_type = UTF8 (только для строк) */
        tw_stop(&m); }
    tw_i64(&m,&last,3,(int64_t)nrows);           /* num_rows */
    tw_field(&m,&last,4,TW_LIST);                /* row_groups list (1) */
    tw_lh(&m,1,TW_STRUCT);
    { int rg=0;
      tw_field(&m,&rg,1,TW_LIST);                /* columns */
      tw_lh(&m,ncols,TW_STRUCT);
      int64_t total=0;
      for (int c=0;c<ncols;c++){ int cc=0;
          tw_i64(&m,&cc,2,coff[c]);              /* file_offset */
          tw_field(&m,&cc,3,TW_STRUCT);          /* meta_data */
          { int md=0;
            tw_i32(&m,&md,1,cpt[c]);             /* physical type */
            tw_field(&m,&md,2,TW_LIST); tw_lh(&m,1,TW_I32); wb_zz32(&m,0);   /* encodings=[PLAIN] */
            tw_field(&m,&md,3,TW_LIST); tw_lh(&m,1,TW_BIN);
            { size_t n=strlen(schema->cols[c].name); wb_uv(&m,n); wb_raw(&m,schema->cols[c].name,n); } /* path */
            tw_i32(&m,&md,4,0);                  /* codec = UNCOMPRESSED */
            tw_i64(&m,&md,5,(int64_t)nrows);     /* num_values */
            tw_i64(&m,&md,6,csz[c]);             /* total_uncompressed_size */
            tw_i64(&m,&md,7,csz[c]);             /* total_compressed_size */
            tw_i64(&m,&md,9,coff[c]);            /* data_page_offset */
            tw_stop(&m); }
          tw_stop(&m);                           /* ColumnChunk stop */
          total+=csz[c]; }
      tw_i64(&m,&rg,2,total); tw_i64(&m,&rg,3,(int64_t)nrows); tw_stop(&m); }  /* RowGroup */
    tw_stop(&m);                                 /* FileMetaData stop */
    fwrite(m.d,1,m.len,f);
    uint32_t mlen=(uint32_t)m.len;
    uint8_t lb[4]={(uint8_t)mlen,(uint8_t)(mlen>>8),(uint8_t)(mlen>>16),(uint8_t)(mlen>>24)};
    fwrite(lb,1,4,f); fwrite("PAR1",1,4,f);
    free(m.d); free(coff); free(csz); free(cpt); free(copt); fclose(f);
    LOG_INFO("parquet sink: %d rows × %d cols → %s", nrows, ncols, entity);
    return nrows;
}

const DfoConnector dfo_connector_entry = {
    .abi_version  = DFO_CONNECTOR_ABI_VERSION,
    .name         = "parquet",
    .version      = "0.1.0",
    .description  = "Apache Parquet v2 reader (PLAIN, GZIP/uncompressed)",
    .create       = pq_create,
    .destroy      = pq_destroy,
    .list_entities= pq_list_entities,
    .describe     = pq_describe,
    .read_batch   = pq_read_batch,
    .cdc_start    = NULL,
    .cdc_stop     = NULL,
    .ping         = pq_ping,
    .write_batch  = pq_write_batch,
};
