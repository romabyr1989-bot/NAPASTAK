/* Unit tests for Avro schema parsing + flat-record decoding (avro_record.h).
 * Needs json.o/arena.o/log.o but NO Kafka / Schema Registry. */
#include "../../lib/connector/plugins/kafka/avro_record.h"
#include <stdio.h>
#include <string.h>

static int plan=0, pass=0, fail=0;
#define ok(c, desc) do{ plan++; \
    if (c) { pass++; printf("ok %d - %s\n", plan, desc); } \
    else   { fail++; printf("not ok %d - %s\n", plan, desc); } } while(0)

static const char *PERSON =
    "{\"type\":\"record\",\"name\":\"Person\",\"fields\":["
    "{\"name\":\"id\",\"type\":\"long\"},"
    "{\"name\":\"name\",\"type\":\"string\"},"
    "{\"name\":\"inn\",\"type\":[\"null\",\"string\"]}]}";

int main(void) {
    puts("TAP version 14");
    Arena *a = arena_create(0);

    /* ── schema parse ── */
    AvroField *fields;
    int nf = avro_parse_schema(a, PERSON, &fields);
    ok(nf == 3, "Person schema -> 3 fields");
    ok(strcmp(fields[0].name, "id") == 0  && fields[0].type == AVRO_LONG,   "field 0 id:long");
    ok(strcmp(fields[1].name, "name") == 0 && fields[1].type == AVRO_STRING, "field 1 name:string");
    ok(fields[2].type == AVRO_UNION_NULL_T && fields[2].union_inner == AVRO_STRING,
       "field 2 inn:[null,string] union");
    ok(fields[2].union_null_idx == 0, "inn null branch index == 0");

    /* ── decode {id:42, name:"Ivanov", inn:null} ──
     * id long 42 -> 0x54 | name "Ivanov" len6 -> 0x0C + bytes | inn null -> 0x00 */
    uint8_t p1[] = {0x54, 0x0C, 'I','v','a','n','o','v', 0x00};
    char **v; uint8_t *n;
    int rc = avro_decode_record(a, fields, nf, p1, sizeof p1, &v, &n);
    ok(rc == 0, "decode #1 ok");
    ok(v[0] && strcmp(v[0], "42") == 0,      "id == 42");
    ok(v[1] && strcmp(v[1], "Ivanov") == 0,  "name == Ivanov");
    ok(v[2] == NULL && n[2] == 1,            "inn is NULL (union null)");

    /* ── decode {id:7, name:"Petrov", inn:"7707"} ──
     * id 7 -> 0x0E | name len6 -> 0x0C | inn branch 1 -> 0x02 | "7707" len4 -> 0x08 */
    uint8_t p2[] = {0x0E, 0x0C, 'P','e','t','r','o','v', 0x02, 0x08, '7','7','0','7'};
    rc = avro_decode_record(a, fields, nf, p2, sizeof p2, &v, &n);
    ok(rc == 0, "decode #2 ok");
    ok(v[0] && strcmp(v[0], "7") == 0,      "id == 7");
    ok(v[2] && strcmp(v[2], "7707") == 0 && n[2] == 0, "inn == 7707 (union value)");

    /* ── truncated buffer must fail cleanly (no crash) ── */
    uint8_t bad[] = {0x54};   /* only the id long, name/inn missing */
    rc = avro_decode_record(a, fields, nf, bad, sizeof bad, &v, &n);
    ok(rc == -1, "truncated payload -> rc -1 (graceful)");

    /* ── logical type resolves to its physical primitive ── */
    JVal *lt = json_parse(a, "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}", 47);
    ok(avro_type_from_jval(lt) == AVRO_LONG, "logical {type:long} -> AVRO_LONG");

    /* ── union with a logical-type branch: ["null",{type:long,...}] ── */
    const char *LSCHEMA =
        "{\"type\":\"record\",\"name\":\"T\",\"fields\":["
        "{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]}]}";
    AvroField *lf; int lnf = avro_parse_schema(a, LSCHEMA, &lf);
    ok(lnf == 1 && lf[0].type == AVRO_UNION_NULL_T && lf[0].union_inner == AVRO_LONG,
       "union [null, logical-long] inner -> AVRO_LONG");

    /* ── double decode: 1.5 = 0x3FF8000000000000, little-endian ── */
    const char *DSCHEMA =
        "{\"type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"x\",\"type\":\"double\"}]}";
    AvroField *df; int dnf = avro_parse_schema(a, DSCHEMA, &df);
    uint8_t pd[] = {0x00,0x00,0x00,0x00,0x00,0x00,0xF8,0x3F};
    rc = avro_decode_record(a, df, dnf, pd, sizeof pd, &v, &n);
    ok(rc == 0 && v[0] && strcmp(v[0], "1.5") == 0, "double 1.5 decoded");

    printf("1..%d\n", plan);
    fprintf(stderr, "avro_record: %d/%d passed\n", pass, plan);
    return fail ? 1 : 0;
}
