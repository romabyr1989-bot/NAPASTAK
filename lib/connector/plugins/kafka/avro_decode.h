#pragma once
/* Avro binary primitive decoders — header-only so they can be unit-tested
 * without linking librdkafka / libcurl.
 *
 * Avro binary encoding (the subset CDI flat records need):
 *   int/long  — zig-zag varint
 *   float     — 4 bytes little-endian
 *   double    — 8 bytes little-endian
 *   string    — long length prefix + raw bytes
 *   bytes     — long length prefix + raw bytes
 */
#include <stdint.h>
#include <stddef.h>

/* Decode an Avro long (zig-zag varint). Returns bytes consumed, sets *out.
 * `avail` bounds the read so a truncated/garbage buffer can't run away. */
static inline size_t avro_decode_long(const uint8_t *p, size_t avail, int64_t *out)
{
    uint64_t value = 0;
    int      shift = 0;
    size_t   i     = 0;
    while (i < avail) {
        uint8_t b = p[i++];
        value |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
        if (shift >= 64) break;   /* malformed varint guard */
    }
    /* zig-zag decode: (n >> 1) ^ -(n & 1) */
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
    *out = p + consumed;
    *len = slen;
    return consumed + (slen > 0 ? (size_t)slen : 0);
}
