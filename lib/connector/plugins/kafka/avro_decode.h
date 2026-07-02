#pragma once
/* avro_decode.h — примитивные декодеры бинарного формата Avro.
 * Header-only so they can be unit-tested without linking librdkafka / libcurl.
 * Используется kafka-коннектором для разбора payload Avro-сообщений (после
 * 5-байтного префикса Confluent). Схема/модель записи — в avro_record.h.
 *
 * Avro binary encoding (the subset CDI flat records need):
 *   int/long  — zig-zag varint
 *   float     — 4 bytes little-endian
 *   double    — 8 bytes little-endian
 *   string    — long length prefix + raw bytes
 *   bytes     — long length prefix + raw bytes
 * Целые кодируются zig-zag varint: младшие 7 бит каждого байта несут данные,
 * старший бит (0x80) — флаг продолжения; zig-zag отображает знаковые в
 * беззнаковые так, что малые по модулю числа занимают мало байт.
 */
#include <stdint.h>
#include <stddef.h>

/* Decode an Avro long (zig-zag varint). Returns bytes consumed, sets *out.
 * `avail` bounds the read so a truncated/garbage buffer can't run away. */
static inline size_t avro_decode_long(const uint8_t *p, size_t avail, int64_t *out)
{
    uint64_t value = 0;
    int      shift = 0;      /* позиция текущей 7-битной группы в результате */
    size_t   i     = 0;
    while (i < avail) {
        uint8_t b = p[i++];
        value |= (uint64_t)(b & 0x7F) << shift;   /* 7 полезных бит на байт */
        if (!(b & 0x80)) break;   /* старший бит сброшен — это последний байт */
        shift += 7;
        if (shift >= 64) break;   /* malformed varint guard */
    }
    /* zig-zag decode: (n >> 1) ^ -(n & 1). (~x + 1) — это -x без warning на
     * беззнаковом типе: даёт маску 0 или ~0 по младшему биту n. */
    *out = (int64_t)((value >> 1) ^ (~(value & 1) + 1));
    return i;
}

/* Decode an Avro string/bytes: long length prefix + raw data.
 * *out points INTO the buffer (not copied); *len is the byte length.
 * Returns total bytes consumed (prefix + data). Callers must verify the
 * returned consumed count against the remaining buffer before trusting *out. */
static inline size_t avro_decode_bytes(const uint8_t *p, size_t avail,
                                       const uint8_t **out, int64_t *len)
{
    int64_t slen     = 0;
    size_t  consumed = avro_decode_long(p, avail, &slen);
    *out = p + consumed;    /* указатель на данные сразу за префиксом длины */
    *len = slen;
    /* отрицательную длину трактуем как пустую, чтобы не уйти в огромный size_t */
    return consumed + (slen > 0 ? (size_t)slen : 0);
}
