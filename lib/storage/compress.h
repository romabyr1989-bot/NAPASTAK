#pragma once
/* compress.h — поколоночное сжатие батчей хранилища.
 * Описывает форматы кодирования (RLE/Dictionary/Delta/Plain), структуры
 * сжатой колонки и батча, а также API сжатия/распаковки и (де)сериализации
 * для WAL. */
#include "storage.h"
#include "../core/arena.h"
#include <stdint.h>
#include <stddef.h>

/* ── Encoding types ── */
typedef enum {
    ENC_PLAIN = 0,  /* без сжатия */
    ENC_RLE   = 1,  /* Run-Length Encoding: (count, value) пары */
    ENC_DICT  = 2,  /* Dictionary: uint8/uint16 индексы в словарь уникальных значений */
    ENC_DELTA = 3,  /* Delta encoding для монотонных int64 (timestamps, ids) */
} Encoding;

/* ── Compressed column ── */
/* Одна сжатая колонка: активны только поля, относящиеся к выбранному enc;
 * null_bitmap хранится отдельно и никогда не сжимается. */
typedef struct {
    Encoding  enc;
    ColType   col_type;
    int       nrows;

    /* ENC_RLE */
    int       nruns;
    uint16_t *rle_counts;   /* uint16_t[nruns] */
    void     *rle_values;   /* int64_t[nruns] или char*[nruns] */

    /* ENC_DICT */
    int       dict_size;
    void     *dict_values;  /* int64_t[dict_size] или char*[dict_size] */
    int       dict_is_u8;   /* 1 = uint8 codes, 0 = uint16 codes */
    void     *dict_codes;   /* uint8_t[nrows] или uint16_t[nrows] */

    /* ENC_DELTA */
    int64_t   base_value;
    int32_t  *deltas;       /* int32_t[nrows-1] */

    /* ENC_PLAIN */
    void     *plain_values;

    /* null bitmap (never compressed) */
    uint8_t  *null_bitmap;  /* ceil(nrows/8) bytes, NULL if no nulls */
    int       has_nulls;
} CompressedCol;

/* ── Compressed batch ── */
/* Сжатый батч: набор колонок плюс учёт размеров до/после для compress_ratio. */
typedef struct {
    Schema        *schema;
    int            ncols;
    int            nrows;
    CompressedCol  cols[MAX_COLS];
    size_t         compressed_bytes;
    size_t         original_bytes;
} CompressedBatch;

/* ── API ── */

/* Выбрать лучший encoding для колонки */
Encoding compress_choose_encoding(void *values, uint8_t *null_bitmap,
                                  int nrows, ColType type);

/* Сжать одну колонку (результат размещается в arena) */
int compress_col(CompressedCol *out, void *values, uint8_t *null_bitmap,
                 int nrows, ColType type, Encoding enc, Arena *a);

/* Распаковать обратно в plain values */
int decompress_col(const CompressedCol *in, void **values_out,
                   uint8_t **null_bitmap_out, Arena *a);

/* Сжать весь ColBatch */
CompressedBatch *compress_batch(const ColBatch *batch, Arena *a);

/* Распаковать в ColBatch */
ColBatch *decompress_batch(const CompressedBatch *cb, Arena *a);

/* Сериализовать CompressedBatch в байты (для WAL) */
int compressed_batch_serialize(const CompressedBatch *cb,
                                void **out, size_t *out_len, Arena *a);

/* Десериализовать из байт WAL */
CompressedBatch *compressed_batch_deserialize(const void *data,
                                               size_t len, Arena *a);

/* original_bytes / compressed_bytes */
float compress_ratio(const CompressedBatch *cb);
