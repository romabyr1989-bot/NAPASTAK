/*
 * compress.c — колоночное сжатие батчей хранилища NAPASTAK.
 *
 * Поддерживаемые кодировки (Encoding): PLAIN, RLE, DICT, DELTA.
 * Кодировка для столбца выбирается эвристически по выборке данных
 * (compress_choose_encoding), затем столбец сжимается (compress_col) и
 * может быть сериализован в компактный бинарный формат "DCMB".
 * Все аллокации идут через арену (Arena) — освобождение пакетное.
 */
#include "compress.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

/* ═══════════════════════════════════════════════════
   Вспомогательные функции: битовые операции
   ═══════════════════════════════════════════════════ */

/* Проверка i-го бита NULL-битмапа (LSB-first); NULL-битмап == "нет NULL". */
static bool null_bit(const uint8_t *bm, int i) {
    if (!bm) return false;
    return !!(bm[i/8] & (1u << (i%8)));
}

/* Размер NULL-битмапа в байтах для nrows строк (округление вверх). */
static int null_bm_bytes(int nrows) { return (nrows + 7) / 8; }

/* ═══════════════════════════════════════════════════
   Сериализаторы/десериализаторы для бинарного формата
   ═══════════════════════════════════════════════════ */

/* wN — запись little-endian значения в буфер с продвижением курсора *p; wb — сырые байты. */
static void w8 (uint8_t **p, uint8_t  v){ **p=v; (*p)++; }
static void w16(uint8_t **p, uint16_t v){ memcpy(*p,&v,2); (*p)+=2; }
static void w32(uint8_t **p, uint32_t v){ memcpy(*p,&v,4); (*p)+=4; }
static void w64(uint8_t **p, uint64_t v){ memcpy(*p,&v,8); (*p)+=8; }
static void wb (uint8_t **p, const void *d, size_t n){ memcpy(*p,d,n); (*p)+=n; }

/* rN — чтение little-endian значения с продвижением курсора *p. */
static uint8_t  r8 (const uint8_t **p){ uint8_t  v=**p; (*p)++;   return v; }
static uint16_t r16(const uint8_t **p){ uint16_t v; memcpy(&v,*p,2); (*p)+=2; return v; }
static uint32_t r32(const uint8_t **p){ uint32_t v; memcpy(&v,*p,4); (*p)+=4; return v; }
static uint64_t r64(const uint8_t **p){ uint64_t v; memcpy(&v,*p,8); (*p)+=8; return v; }

/* ═══════════════════════════════════════════════════
   compress_choose_encoding
   ═══════════════════════════════════════════════════ */

/*
 * Эвристический выбор кодировки по выборке (до 1000 строк):
 * оценивает кардинальность/монотонность/долю повторов и возвращает
 * наиболее выгодную Encoding. NULL не учитываются в статистике.
 */
Encoding compress_choose_encoding(void *values, uint8_t *null_bitmap,
                                  int nrows, ColType type) {
    if (nrows <= 0) return ENC_PLAIN;

    switch (type) {
    case COL_DOUBLE:
        return ENC_PLAIN;  /* плавающая точка плохо сжимается без потерь */

    case COL_BOOL:
        return ENC_RLE;    /* булев тип — максимальный эффект RLE */

    case COL_INT64: {
        int64_t *vs = (int64_t*)values;
        int sample = nrows < 1000 ? nrows : 1000;

        /* Подсчёт уникальных значений — простая сортировка sample */
        int64_t tmp[1000];
        int sn = 0;
        for (int i = 0; i < sample; i++) {
            if (!null_bit(null_bitmap, i)) tmp[sn++] = vs[i];
        }
        /* insertion sort для подсчёта уникальных */
        for (int i = 1; i < sn; i++) {
            int64_t k = tmp[i]; int j = i-1;
            while (j >= 0 && tmp[j] > k) { tmp[j+1]=tmp[j]; j--; }
            tmp[j+1] = k;
        }
        int uniq = sn > 0 ? 1 : 0;
        for (int i = 1; i < sn; i++) if (tmp[i] != tmp[i-1]) uniq++;

        if (uniq < 256)   return ENC_DICT;
        if (uniq < 65536) return ENC_DICT;

        /* Проверка монотонного возрастания для DELTA */
        bool mono = true;
        for (int i = 1; i < sample; i++) {
            if (!null_bit(null_bitmap, i) && !null_bit(null_bitmap, i-1)) {
                int64_t d = vs[i] - vs[i-1];
                if (d < -0x7FFFFFFF || d > 0x7FFFFFFF) { mono = false; break; }
            }
        }
        if (mono) return ENC_DELTA;
        return ENC_PLAIN;
    }

    case COL_TEXT: {
        char **vs = (char**)values;
        int sample = nrows < 1000 ? nrows : 1000;

        /* Считаем runs */
        int runs = 1;
        for (int i = 1; i < sample; i++) {
            const char *a = vs[i-1] ? vs[i-1] : "";
            const char *b = vs[i]   ? vs[i]   : "";
            if (strcmp(a, b) != 0) runs++;
        }

        /* Много повторений → RLE */
        if (runs <= sample / 3) return ENC_RLE;

        /* Подсчёт уникальных (naïve O(n²) для sample) */
        int uniq = 0;
        const char *seen[256]; /* max 256 из первых 256 строк */
        int seen_n = 0;
        for (int i = 0; i < sample; i++) {
            const char *s = vs[i] ? vs[i] : "";
            bool found = false;
            for (int j = 0; j < seen_n && j < 256; j++)
                if (strcmp(seen[j], s) == 0) { found = true; break; }
            if (!found) {
                if (seen_n < 256) seen[seen_n++] = s;
                uniq++;
            }
        }

        if (uniq < 256) return ENC_DICT;
        return ENC_PLAIN;
    }

    default:
        return ENC_PLAIN;
    }
}

/* ═══════════════════════════════════════════════════
   compress_col
   ═══════════════════════════════════════════════════ */

/* RLE для int64/bool: NULL кодируется как INT64_MIN; длина run ограничена 65535 (uint16). */
static int compress_col_rle_int(CompressedCol *out, int64_t *vs,
                                 uint8_t *bm, int nrows, Arena *a) {
    /* Первый проход: подсчёт числа run для аллокации массивов */
    int nruns = 0;
    for (int i = 0; i < nrows; ) {
        int64_t v = null_bit(bm, i) ? INT64_MIN : vs[i];
        int j = i+1;
        while (j < nrows) {
            int64_t vj = null_bit(bm, j) ? INT64_MIN : vs[j];
            if (vj != v) break;
            j++;
        }
        nruns++; i = j;
    }

    out->nruns      = nruns;
    out->rle_counts = arena_alloc(a, (size_t)nruns * sizeof(uint16_t));
    out->rle_values = arena_alloc(a, (size_t)nruns * sizeof(int64_t));
    int64_t *rv = (int64_t*)out->rle_values;

    int ri = 0;
    for (int i = 0; i < nrows; ) {
        int64_t v = null_bit(bm, i) ? INT64_MIN : vs[i];
        int j = i+1;
        while (j < nrows && j - i < 65535) {
            int64_t vj = null_bit(bm, j) ? INT64_MIN : vs[j];
            if (vj != v) break;
            j++;
        }
        out->rle_counts[ri] = (uint16_t)(j - i);
        rv[ri] = v;
        ri++; i = j;
    }
    return 0;
}

/* RLE для текста: NULL и пустые строки трактуются как ""; значения run копируются в арену. */
static int compress_col_rle_text(CompressedCol *out, char **vs,
                                  uint8_t *bm, int nrows, Arena *a) {
    int nruns = 0;
    for (int i = 0; i < nrows; ) {
        const char *v = (!null_bit(bm, i) && vs[i]) ? vs[i] : "";
        int j = i+1;
        while (j < nrows) {
            const char *vj = (!null_bit(bm, j) && vs[j]) ? vs[j] : "";
            if (strcmp(v, vj) != 0) break;
            j++;
        }
        nruns++; i = j;
    }

    out->nruns      = nruns;
    out->rle_counts = arena_alloc(a, (size_t)nruns * sizeof(uint16_t));
    out->rle_values = arena_alloc(a, (size_t)nruns * sizeof(char*));
    char **rv = (char**)out->rle_values;

    int ri = 0;
    for (int i = 0; i < nrows; ) {
        const char *v = (!null_bit(bm, i) && vs[i]) ? vs[i] : "";
        int j = i+1;
        while (j < nrows && j - i < 65535) {
            const char *vj = (!null_bit(bm, j) && vs[j]) ? vs[j] : "";
            if (strcmp(v, vj) != 0) break;
            j++;
        }
        out->rle_counts[ri] = (uint16_t)(j - i);
        rv[ri] = arena_strdup(a, v);
        ri++; i = j;
    }
    return 0;
}

/*
 * Словарная кодировка int64/bool: словарь сортируется, коды находятся
 * бинарным поиском. Ширина кода 1 или 2 байта (dict_is_u8). При >65535
 * уникальных значений возвращает -1 (вызов compress_col откатится в PLAIN).
 */
static int compress_col_dict_int(CompressedCol *out, int64_t *vs,
                                  uint8_t *bm, int nrows, Arena *a) {
    /* Собрать уникальные значения */
    int64_t *uniq = arena_alloc(a, (size_t)nrows * sizeof(int64_t));
    int nuniq = 0;
    for (int i = 0; i < nrows; i++) {
        if (null_bit(bm, i)) continue;
        int64_t v = vs[i];
        bool found = false;
        for (int j = 0; j < nuniq; j++) if (uniq[j] == v) { found = true; break; }
        if (!found) {
            if (nuniq >= 65535) { /* too many unique → fallback */ return -1; }
            uniq[nuniq++] = v;
        }
    }

    /* Sort dictionary */
    for (int i = 1; i < nuniq; i++) {
        int64_t k = uniq[i]; int j = i-1;
        while (j >= 0 && uniq[j] > k) { uniq[j+1]=uniq[j]; j--; }
        uniq[j+1] = k;
    }

    out->dict_size   = nuniq;
    out->dict_values = uniq;
    out->dict_is_u8  = (nuniq <= 256);

    if (out->dict_is_u8) {
        uint8_t *codes = arena_alloc(a, (size_t)nrows);
        for (int i = 0; i < nrows; i++) {
            if (null_bit(bm, i)) { codes[i] = 0; continue; }
            int64_t v = vs[i];
            /* binary search */
            int lo=0, hi=nuniq-1, code=0;
            while (lo <= hi) { int m=(lo+hi)/2; if (uniq[m]==v){code=m;break;} else if (uniq[m]<v) lo=m+1; else hi=m-1; }
            codes[i] = (uint8_t)code;
        }
        out->dict_codes = codes;
    } else {
        uint16_t *codes = arena_alloc(a, (size_t)nrows * sizeof(uint16_t));
        for (int i = 0; i < nrows; i++) {
            if (null_bit(bm, i)) { codes[i] = 0; continue; }
            int64_t v = vs[i];
            int lo=0, hi=nuniq-1, code=0;
            while (lo <= hi) { int m=(lo+hi)/2; if (uniq[m]==v){code=m;break;} else if (uniq[m]<v) lo=m+1; else hi=m-1; }
            codes[i] = (uint16_t)code;
        }
        out->dict_codes = codes;
    }
    return 0;
}

/*
 * Словарная кодировка текста: словарь не сортируется (поиск кода линейный),
 * строки копируются в арену. При >65535 уникальных значений возвращает -1.
 */
static int compress_col_dict_text(CompressedCol *out, char **vs,
                                   uint8_t *bm, int nrows, Arena *a) {
    char **uniq = arena_alloc(a, (size_t)nrows * sizeof(char*));
    int nuniq = 0;
    for (int i = 0; i < nrows; i++) {
        const char *v = (!null_bit(bm,i) && vs[i]) ? vs[i] : "";
        bool found = false;
        for (int j = 0; j < nuniq; j++) if (strcmp(uniq[j], v) == 0) { found=true; break; }
        if (!found) {
            if (nuniq >= 65535) return -1;
            uniq[nuniq++] = arena_strdup(a, v);
        }
    }

    out->dict_size   = nuniq;
    out->dict_values = uniq;
    out->dict_is_u8  = (nuniq <= 256);

    if (out->dict_is_u8) {
        uint8_t *codes = arena_alloc(a, (size_t)nrows);
        for (int i = 0; i < nrows; i++) {
            const char *v = (!null_bit(bm,i) && vs[i]) ? vs[i] : "";
            for (int j = 0; j < nuniq; j++) if (strcmp(uniq[j],v)==0) { codes[i]=(uint8_t)j; break; }
        }
        out->dict_codes = codes;
    } else {
        uint16_t *codes = arena_alloc(a, (size_t)nrows * sizeof(uint16_t));
        for (int i = 0; i < nrows; i++) {
            const char *v = (!null_bit(bm,i) && vs[i]) ? vs[i] : "";
            for (int j = 0; j < nuniq; j++) if (strcmp(uniq[j],v)==0) { codes[i]=(uint16_t)j; break; }
        }
        out->dict_codes = codes;
    }
    return 0;
}

/*
 * Дельта-кодировка int64: base_value = первое non-NULL значение, далее
 * хранятся 32-битные разности к предыдущему non-NULL. NULL дают дельту 0.
 * Если разность не влезает в int32 — возврат -1 (откат в PLAIN).
 */
static int compress_col_delta(CompressedCol *out, int64_t *vs,
                               uint8_t *bm, int nrows, Arena *a) {
    /* Найти первый non-null */
    int first = 0;
    while (first < nrows && null_bit(bm, first)) first++;
    if (first >= nrows) { out->base_value=0; out->deltas=NULL; return 0; }

    out->base_value = vs[first];
    out->deltas = arena_alloc(a, (size_t)nrows * sizeof(int32_t));

    int64_t prev = vs[first];
    for (int i = 0; i < nrows; i++) {
        if (null_bit(bm, i)) { out->deltas[i] = 0; continue; }
        int64_t d = vs[i] - prev;
        if (d < -0x7FFFFFFF || d > 0x7FFFFFFF) return -1; /* overflow → fallback */
        out->deltas[i] = (int32_t)d;
        prev = vs[i];
    }
    return 0;
}

/*
 * Сжимает один столбец заданной кодировкой enc, заполняя *out.
 * Копирует NULL-битмап в арену. При неприменимой кодировке или ошибке
 * сжатия (rc != 0) откатывается на ENC_PLAIN. Всегда возвращает 0.
 */
int compress_col(CompressedCol *out, void *values, uint8_t *null_bitmap,
                 int nrows, ColType type, Encoding enc, Arena *a) {
    memset(out, 0, sizeof(*out));
    out->enc = enc; out->col_type = type; out->nrows = nrows;
    out->has_nulls = (null_bitmap != NULL);

    if (null_bitmap) {
        int nbytes = null_bm_bytes(nrows);
        out->null_bitmap = arena_alloc(a, (size_t)nbytes);
        memcpy(out->null_bitmap, null_bitmap, (size_t)nbytes);
    }

    int rc = 0;
    switch (enc) {
    case ENC_RLE:
        if (type == COL_INT64 || type == COL_BOOL)
            rc = compress_col_rle_int(out, (int64_t*)values, null_bitmap, nrows, a);
        else if (type == COL_TEXT)
            rc = compress_col_rle_text(out, (char**)values, null_bitmap, nrows, a);
        else { enc = ENC_PLAIN; rc = -1; }
        break;
    case ENC_DICT:
        if (type == COL_INT64 || type == COL_BOOL)
            rc = compress_col_dict_int(out, (int64_t*)values, null_bitmap, nrows, a);
        else if (type == COL_TEXT)
            rc = compress_col_dict_text(out, (char**)values, null_bitmap, nrows, a);
        else { enc = ENC_PLAIN; rc = -1; }
        break;
    case ENC_DELTA:
        if (type == COL_INT64)
            rc = compress_col_delta(out, (int64_t*)values, null_bitmap, nrows, a);
        else { enc = ENC_PLAIN; rc = -1; }
        break;
    default: break;
    }

    if (rc != 0 || enc == ENC_PLAIN) {
        /* Fallback to PLAIN */
        out->enc = ENC_PLAIN;
        out->plain_values = values;
    }
    return 0;
}

/* ═══════════════════════════════════════════════════
   decompress_col
   ═══════════════════════════════════════════════════ */

/*
 * Восстанавливает значения столбца из CompressedCol в *values_out
 * (массив в арене) и при необходимости NULL-битмап в *null_bitmap_out.
 * Возвращает 0, либо -1 при неизвестной кодировке.
 */
int decompress_col(const CompressedCol *in, void **values_out,
                   uint8_t **null_bitmap_out, Arena *a) {
    int nrows = in->nrows;

    if (null_bitmap_out) {
        *null_bitmap_out = NULL;
        if (in->has_nulls && in->null_bitmap) {
            int nb = null_bm_bytes(nrows);
            uint8_t *bm = arena_alloc(a, (size_t)nb);
            memcpy(bm, in->null_bitmap, (size_t)nb);
            *null_bitmap_out = bm;
        }
    }

    switch (in->enc) {
    case ENC_PLAIN:
        *values_out = in->plain_values;
        return 0;

    case ENC_RLE: {
        if (in->col_type == COL_INT64 || in->col_type == COL_BOOL) {
            int64_t *out = arena_alloc(a, (size_t)nrows * sizeof(int64_t));
            int64_t *rv = (int64_t*)in->rle_values;
            int i = 0;
            for (int r = 0; r < in->nruns; r++)
                for (int k = 0; k < in->rle_counts[r]; k++, i++) out[i] = rv[r];
            *values_out = out;
        } else {
            char **out = arena_alloc(a, (size_t)nrows * sizeof(char*));
            char **rv = (char**)in->rle_values;
            int i = 0;
            for (int r = 0; r < in->nruns; r++)
                for (int k = 0; k < in->rle_counts[r]; k++, i++) out[i] = rv[r];
            *values_out = out;
        }
        return 0;
    }

    case ENC_DICT: {
        if (in->col_type == COL_INT64 || in->col_type == COL_BOOL) {
            int64_t *out = arena_alloc(a, (size_t)nrows * sizeof(int64_t));
            int64_t *dv = (int64_t*)in->dict_values;
            if (in->dict_is_u8) {
                uint8_t *codes = (uint8_t*)in->dict_codes;
                for (int i = 0; i < nrows; i++) out[i] = dv[codes[i]];
            } else {
                uint16_t *codes = (uint16_t*)in->dict_codes;
                for (int i = 0; i < nrows; i++) out[i] = dv[codes[i]];
            }
            *values_out = out;
        } else {
            char **out = arena_alloc(a, (size_t)nrows * sizeof(char*));
            char **dv = (char**)in->dict_values;
            if (in->dict_is_u8) {
                uint8_t *codes = (uint8_t*)in->dict_codes;
                for (int i = 0; i < nrows; i++) out[i] = dv[codes[i]];
            } else {
                uint16_t *codes = (uint16_t*)in->dict_codes;
                for (int i = 0; i < nrows; i++) out[i] = dv[codes[i]];
            }
            *values_out = out;
        }
        return 0;
    }

    case ENC_DELTA: {
        int64_t *out = arena_alloc(a, (size_t)nrows * sizeof(int64_t));
        int64_t cur = in->base_value;
        for (int i = 0; i < nrows; i++) {
            if (!in->has_nulls || !null_bit(in->null_bitmap, i)) {
                cur += in->deltas ? in->deltas[i] : 0;
                out[i] = cur;
            } else {
                out[i] = 0;
            }
        }
        *values_out = out;
        return 0;
    }
    }
    return -1;
}

/* ═══════════════════════════════════════════════════
   compress_batch / decompress_batch
   ═══════════════════════════════════════════════════ */

/*
 * Сжимает весь батч: для каждого столбца выбирает кодировку и вызывает
 * compress_col. Параллельно грубо оценивает original_bytes для compress_ratio.
 */
CompressedBatch *compress_batch(const ColBatch *batch, Arena *a) {
    CompressedBatch *cb = arena_calloc(a, sizeof(CompressedBatch));
    cb->schema = batch->schema;
    cb->ncols  = batch->ncols;
    cb->nrows  = batch->nrows;

    size_t orig = 0, comp = 0;
    for (int c = 0; c < batch->ncols; c++) {
        ColType t = (batch->schema && c < batch->schema->ncols)
                    ? batch->schema->cols[c].type : COL_TEXT;
        Encoding enc = compress_choose_encoding(batch->values[c],
                                                batch->null_bitmap[c],
                                                batch->nrows, t);
        compress_col(&cb->cols[c], batch->values[c], batch->null_bitmap[c],
                     batch->nrows, t, enc, a);

        /* Rough size estimate for ratio. Fixed-width numeric columns (INT64,
         * BOOL, DOUBLE) store 8-byte values, NOT char* — treating DOUBLE as text
         * here cast double* to char** and ran strlen() on a double's bit pattern,
         * faulting on any native floating-point source (Kafka JSON numbers,
         * Parquet, Greenplum). */
        if (t == COL_INT64 || t == COL_BOOL || t == COL_DOUBLE) orig += (size_t)batch->nrows * 8;
        else { char **vs = (char**)batch->values[c]; for (int i=0;i<batch->nrows;i++) orig += vs && vs[i] ? strlen(vs[i])+1 : 1; }
    }
    cb->original_bytes = orig > 0 ? orig : 1;
    /* compressed size estimated after serialization */
    cb->compressed_bytes = cb->original_bytes; /* updated after serialize */
    return cb;
}

/* Обратная операция к compress_batch: восстанавливает ColBatch постолбцово. */
ColBatch *decompress_batch(const CompressedBatch *cb, Arena *a) {
    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema = cb->schema;
    batch->ncols  = cb->ncols;
    batch->nrows  = cb->nrows;
    for (int c = 0; c < cb->ncols; c++) {
        decompress_col(&cb->cols[c], &batch->values[c], &batch->null_bitmap[c], a);
    }
    return batch;
}

/* ═══════════════════════════════════════════════════
   Сериализация / десериализация

   Формат:
   [4 bytes magic "DCMB"]
   [uint8 version=1]
   [int32 ncols]
   [int32 nrows]
   For each col:
     [uint8 col_type][uint8 encoding]
     [uint8 has_nulls]
     if has_nulls: [int32 bm_bytes][bm_bytes bytes]
     encoding-specific data (see below)
   ═══════════════════════════════════════════════════ */

/* Estimate max serialized size */
/* Верхняя оценка размера буфера сериализации (+64 байта запаса). */
static size_t ser_estimate(const CompressedBatch *cb) {
    size_t n = 14; /* magic + version + ncols + nrows */
    for (int c = 0; c < cb->ncols; c++) {
        const CompressedCol *cc = &cb->cols[c];
        n += 3; /* col_type + enc + has_nulls */
        if (cc->has_nulls) n += 4 + (size_t)null_bm_bytes(cc->nrows);
        switch (cc->enc) {
        case ENC_PLAIN:
            if (cc->col_type==COL_INT64||cc->col_type==COL_BOOL||cc->col_type==COL_DOUBLE)
                n += 4 + (size_t)cc->nrows * 8;
            else { char **vs=(char**)cc->plain_values; for (int i=0;i<cc->nrows;i++) n += 4 + (vs&&vs[i]?strlen(vs[i]):0); }
            break;
        case ENC_RLE:
            n += 4; /* nruns */
            if (cc->col_type==COL_INT64||cc->col_type==COL_BOOL)
                n += (size_t)cc->nruns * (2+8);
            else { char **rv=(char**)cc->rle_values; for (int r=0;r<cc->nruns;r++) n += 2+4+(rv&&rv[r]?strlen(rv[r]):0); }
            break;
        case ENC_DICT:
            n += 4+1; /* dict_size + dict_is_u8 */
            if (cc->col_type==COL_INT64||cc->col_type==COL_BOOL)
                n += (size_t)cc->dict_size * 8;
            else { char **dv=(char**)cc->dict_values; for (int i=0;i<cc->dict_size;i++) n += 4+(dv&&dv[i]?strlen(dv[i]):0); }
            n += (size_t)cc->nrows * (cc->dict_is_u8 ? 1 : 2);
            break;
        case ENC_DELTA:
            n += 8 + (size_t)cc->nrows * 4;
            break;
        }
    }
    return n + 64;
}

/*
 * Сериализует сжатый батч в бинарный формат "DCMB" v1 (см. описание выше).
 * Буфер выделяется в арене по верхней оценке ser_estimate; в *out_len
 * возвращается фактический размер.
 */
int compressed_batch_serialize(const CompressedBatch *cb,
                                void **out, size_t *out_len, Arena *a) {
    size_t cap = ser_estimate(cb);
    uint8_t *buf = arena_alloc(a, cap);
    uint8_t *p = buf;

    /* Header */
    wb(&p, "DCMB", 4);
    w8(&p, 1);  /* version */
    w32(&p, (uint32_t)cb->ncols);
    w32(&p, (uint32_t)cb->nrows);

    for (int c = 0; c < cb->ncols; c++) {
        const CompressedCol *cc = &cb->cols[c];
        w8(&p, (uint8_t)cc->col_type);
        w8(&p, (uint8_t)cc->enc);
        w8(&p, (uint8_t)cc->has_nulls);
        if (cc->has_nulls && cc->null_bitmap) {
            int nb = null_bm_bytes(cc->nrows);
            w32(&p, (uint32_t)nb);
            wb(&p, cc->null_bitmap, (size_t)nb);
        }

        switch (cc->enc) {
        case ENC_PLAIN:
            if (cc->col_type==COL_INT64||cc->col_type==COL_BOOL) {
                w32(&p, (uint32_t)(cc->nrows * 8));
                wb(&p, cc->plain_values, (size_t)cc->nrows * 8);
            } else if (cc->col_type==COL_DOUBLE) {
                w32(&p, (uint32_t)(cc->nrows * 8));
                wb(&p, cc->plain_values, (size_t)cc->nrows * 8);
            } else {
                char **vs = (char**)cc->plain_values;
                for (int i = 0; i < cc->nrows; i++) {
                    const char *s = vs && vs[i] ? vs[i] : "";
                    uint32_t sl = (uint32_t)strlen(s);
                    w32(&p, sl); wb(&p, s, sl);
                }
            }
            break;

        case ENC_RLE:
            w32(&p, (uint32_t)cc->nruns);
            if (cc->col_type==COL_INT64||cc->col_type==COL_BOOL) {
                int64_t *rv=(int64_t*)cc->rle_values;
                for (int r = 0; r < cc->nruns; r++) {
                    w16(&p, cc->rle_counts[r]);
                    w64(&p, (uint64_t)rv[r]);
                }
            } else {
                char **rv=(char**)cc->rle_values;
                for (int r = 0; r < cc->nruns; r++) {
                    w16(&p, cc->rle_counts[r]);
                    const char *s = rv && rv[r] ? rv[r] : "";
                    uint32_t sl = (uint32_t)strlen(s);
                    w32(&p, sl); wb(&p, s, sl);
                }
            }
            break;

        case ENC_DICT:
            w32(&p, (uint32_t)cc->dict_size);
            w8(&p,  (uint8_t)cc->dict_is_u8);
            if (cc->col_type==COL_INT64||cc->col_type==COL_BOOL) {
                int64_t *dv=(int64_t*)cc->dict_values;
                for (int i = 0; i < cc->dict_size; i++) w64(&p, (uint64_t)dv[i]);
            } else {
                char **dv=(char**)cc->dict_values;
                for (int i = 0; i < cc->dict_size; i++) {
                    const char *s = dv && dv[i] ? dv[i] : "";
                    uint32_t sl = (uint32_t)strlen(s);
                    w32(&p, sl); wb(&p, s, sl);
                }
            }
            if (cc->dict_is_u8) wb(&p, cc->dict_codes, (size_t)cc->nrows);
            else                wb(&p, cc->dict_codes, (size_t)cc->nrows*2);
            break;

        case ENC_DELTA:
            w64(&p, (uint64_t)cc->base_value);
            if (cc->deltas) wb(&p, cc->deltas, (size_t)cc->nrows * 4);
            break;
        }
    }

    *out_len = (size_t)(p - buf);
    *out = buf;
    return 0;
}

/*
 * Разбирает буфер формата "DCMB" обратно в CompressedBatch.
 * Возвращает NULL при неверной длине, чужой сигнатуре или некорректных
 * ncols/nrows. Курсор продвигается до end, превышение длины не проверяется.
 */
CompressedBatch *compressed_batch_deserialize(const void *data,
                                               size_t len, Arena *a) {
    const uint8_t *p = (const uint8_t*)data;
    const uint8_t *end = p + len;

    if (len < 14) return NULL;
    if (memcmp(p, "DCMB", 4) != 0) return NULL;
    p += 4;
    uint8_t ver = r8(&p); (void)ver;
    int ncols = (int)r32(&p);
    int nrows = (int)r32(&p);

    if (ncols < 0 || ncols > MAX_COLS || nrows < 0) return NULL;

    CompressedBatch *cb = arena_calloc(a, sizeof(CompressedBatch));
    cb->ncols = ncols; cb->nrows = nrows;

    for (int c = 0; c < ncols && p < end; c++) {
        CompressedCol *cc = &cb->cols[c];
        cc->col_type = (ColType)r8(&p);
        cc->enc      = (Encoding)r8(&p);
        cc->has_nulls = (int)r8(&p);
        cc->nrows    = nrows;

        if (cc->has_nulls) {
            uint32_t nb = r32(&p);
            cc->null_bitmap = arena_alloc(a, nb);
            memcpy(cc->null_bitmap, p, nb); p += nb;
        }

        switch (cc->enc) {
        case ENC_PLAIN:
            if (cc->col_type==COL_INT64||cc->col_type==COL_BOOL||cc->col_type==COL_DOUBLE) {
                uint32_t dl = r32(&p);
                void *v = arena_alloc(a, dl);
                memcpy(v, p, dl); p += dl;
                cc->plain_values = v;
            } else {
                char **vs = arena_alloc(a, (size_t)nrows * sizeof(char*));
                for (int i = 0; i < nrows; i++) {
                    uint32_t sl = r32(&p);
                    char *s = arena_alloc(a, sl+1);
                    memcpy(s, p, sl); s[sl]='\0'; p += sl;
                    vs[i] = s;
                }
                cc->plain_values = vs;
            }
            break;

        case ENC_RLE:
            cc->nruns = (int)r32(&p);
            cc->rle_counts = arena_alloc(a, (size_t)cc->nruns * sizeof(uint16_t));
            if (cc->col_type==COL_INT64||cc->col_type==COL_BOOL) {
                int64_t *rv = arena_alloc(a, (size_t)cc->nruns * sizeof(int64_t));
                for (int r = 0; r < cc->nruns; r++) {
                    cc->rle_counts[r] = r16(&p);
                    rv[r] = (int64_t)r64(&p);
                }
                cc->rle_values = rv;
            } else {
                char **rv = arena_alloc(a, (size_t)cc->nruns * sizeof(char*));
                for (int r = 0; r < cc->nruns; r++) {
                    cc->rle_counts[r] = r16(&p);
                    uint32_t sl = r32(&p);
                    char *s = arena_alloc(a, sl+1);
                    memcpy(s, p, sl); s[sl]='\0'; p += sl;
                    rv[r] = s;
                }
                cc->rle_values = rv;
            }
            break;

        case ENC_DICT:
            cc->dict_size = (int)r32(&p);
            cc->dict_is_u8 = (int)r8(&p);
            if (cc->col_type==COL_INT64||cc->col_type==COL_BOOL) {
                int64_t *dv = arena_alloc(a, (size_t)cc->dict_size * sizeof(int64_t));
                for (int i = 0; i < cc->dict_size; i++) dv[i] = (int64_t)r64(&p);
                cc->dict_values = dv;
            } else {
                char **dv = arena_alloc(a, (size_t)cc->dict_size * sizeof(char*));
                for (int i = 0; i < cc->dict_size; i++) {
                    uint32_t sl = r32(&p);
                    char *s = arena_alloc(a, sl+1);
                    memcpy(s, p, sl); s[sl]='\0'; p += sl;
                    dv[i] = s;
                }
                cc->dict_values = dv;
            }
            if (cc->dict_is_u8) {
                uint8_t *codes = arena_alloc(a, (size_t)nrows);
                memcpy(codes, p, (size_t)nrows); p += nrows;
                cc->dict_codes = codes;
            } else {
                uint16_t *codes = arena_alloc(a, (size_t)nrows * 2);
                memcpy(codes, p, (size_t)nrows*2); p += nrows*2;
                cc->dict_codes = codes;
            }
            break;

        case ENC_DELTA:
            cc->base_value = (int64_t)r64(&p);
            cc->deltas = arena_alloc(a, (size_t)nrows * sizeof(int32_t));
            memcpy(cc->deltas, p, (size_t)nrows * 4); p += (size_t)nrows * 4;
            break;
        }
    }
    return cb;
}

/* Коэффициент сжатия original_bytes / compressed_bytes (>=1 — лучше). */
float compress_ratio(const CompressedBatch *cb) {
    if (!cb || cb->compressed_bytes == 0) return 1.0f;
    return (float)cb->original_bytes / (float)cb->compressed_bytes;
}
