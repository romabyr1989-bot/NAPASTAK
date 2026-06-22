/* Unit tests for the Avro binary primitive decoders (header-only).
 * No Kafka / Schema Registry needed. */
#include "../../lib/connector/plugins/kafka/avro_decode.h"
#include <stdio.h>
#include <string.h>

static int plan=0, pass=0, fail=0;
#define ok(c, desc) do{ plan++; \
    if (c) { pass++; printf("ok %d - %s\n", plan, desc); } \
    else   { fail++; printf("not ok %d - %s\n", plan, desc); } } while(0)

int main(void) {
    puts("TAP version 14");

    /* ── avro_decode_long: zig-zag varint ── */
    int64_t v; size_t n;
    { uint8_t b[] = {0x00}; n = avro_decode_long(b, sizeof b, &v);
      ok(v == 0 && n == 1, "long 0x00 -> 0, consumed 1"); }
    { uint8_t b[] = {0x02}; avro_decode_long(b, sizeof b, &v);
      ok(v == 1,  "long 0x02 -> 1"); }
    { uint8_t b[] = {0x01}; avro_decode_long(b, sizeof b, &v);
      ok(v == -1, "long 0x01 -> -1"); }
    { uint8_t b[] = {0x03}; avro_decode_long(b, sizeof b, &v);
      ok(v == -2, "long 0x03 -> -2"); }
    { uint8_t b[] = {0x54}; avro_decode_long(b, sizeof b, &v);
      ok(v == 42, "long 0x54 -> 42"); }
    /* 300 -> zig-zag 600 = 0x258 -> varint 0xD8 0x04 (multi-byte) */
    { uint8_t b[] = {0xD8, 0x04}; n = avro_decode_long(b, sizeof b, &v);
      ok(v == 300 && n == 2, "long 0xD8 0x04 -> 300, consumed 2"); }

    /* ── avro_decode_bytes: length prefix + data ── */
    { uint8_t s[] = {0x06, 'f','o','o'};   /* len 3 (zig-zag 0x06) + "foo" */
      const uint8_t *out; int64_t len;
      size_t consumed = avro_decode_bytes(s, sizeof s, &out, &len);
      ok(len == 3,                    "bytes len == 3");
      ok(memcmp(out, "foo", 3) == 0,  "bytes data == 'foo'");
      ok(consumed == 4,               "bytes consumed == 4 (1 prefix + 3 data)"); }

    { uint8_t s[] = {0x00};   /* empty string: len 0 */
      const uint8_t *out; int64_t len;
      size_t consumed = avro_decode_bytes(s, sizeof s, &out, &len);
      ok(len == 0 && consumed == 1,   "empty bytes -> len 0, consumed 1"); }

    printf("1..%d\n", plan);
    fprintf(stderr, "avro_decode: %d/%d passed\n", pass, plan);
    return fail ? 1 : 0;
}
