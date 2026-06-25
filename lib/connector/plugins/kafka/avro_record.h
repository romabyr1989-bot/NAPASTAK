#pragma once
/* Avro record schema model + flat-record decoder.
 *
 * Header-only (static inline) so it can be unit-tested with just json.o/arena.o/
 * log.o — no librdkafka, no libcurl. The Schema Registry client, the schema
 * cache, and the ColBatch builder stay in kafka_connector.c (they need curl /
 * the storage layer); everything here is pure schema-JSON → typed-value logic.
 *
 * Scope: flat records of primitives and unions of the form ["null", T] (in
 * either order). Logical types ({"type":"long","logicalType":...}) resolve to
 * their underlying physical type so the binary stream stays in sync. Nested
 * records / arrays / maps are out of scope and fall back to raw string (which
 * may desync the stream — documented MVP limit).
 */
#include "avro_decode.h"
#include "../../../core/arena.h"
#include "../../../core/json.h"
#include "../../../core/log.h"
#include <string.h>
#include <stdio.h>
#include <stdint.h>

/* Resolved primitive type of an Avro record field. */
typedef enum {
    AVRO_NULL, AVRO_BOOL, AVRO_INT, AVRO_LONG,
    AVRO_FLOAT, AVRO_DOUBLE, AVRO_STRING, AVRO_BYTES,
    AVRO_UNION_NULL_T,   /* ["null", T] — the common CDI nullable column */
} AvroType;

/* One pre-parsed Avro record field (flat schemas only). */
typedef struct AvroField {
    char     name[128];
    AvroType type;             /* a primitive, or AVRO_UNION_NULL_T */
    AvroType union_inner;      /* for unions: the non-null branch type */
    int      union_null_idx;   /* for unions: index of the "null" branch (-1 = none) */
} AvroField;

/* A parsed schema: Registry id → parsed field list (also the cache node). */
typedef struct AvroSchemaNode {
    int32_t                 schema_id;
    char                   *schema_json;   /* arena-owned, NUL-terminated */
    AvroField              *fields;
    int                     nfields;
    struct AvroSchemaNode  *next;          /* cache linkage (owned by the .c) */
} AvroSchemaNode;

/* Map an Avro primitive type name to AvroType; unknown/complex names → строка. */
static inline AvroType avro_type_from_str(const char *t)
{
    if (!t)                     return AVRO_STRING;
    if (!strcmp(t, "null"))     return AVRO_NULL;
    if (!strcmp(t, "boolean"))  return AVRO_BOOL;
    if (!strcmp(t, "int"))      return AVRO_INT;
    if (!strcmp(t, "long"))     return AVRO_LONG;
    if (!strcmp(t, "float"))    return AVRO_FLOAT;
    if (!strcmp(t, "double"))   return AVRO_DOUBLE;
    if (!strcmp(t, "string"))   return AVRO_STRING;
    if (!strcmp(t, "bytes"))    return AVRO_BYTES;
    return AVRO_STRING;  /* enum/fixed/record/array/map/unknown → raw text */
}

/* Resolve the physical Avro type of a "type" node, which may be a plain string
 * ("long") OR a logical-type object ({"type":"long","logicalType":...}).
 * Resolving the object's inner physical type keeps the binary stream in sync —
 * CDI timestamps/decimals are longs/bytes wrapped in logical types. */
static inline AvroType avro_type_from_jval(JVal *t)
{
    if (!t) return AVRO_STRING;
    if (t->type == JV_STRING) return avro_type_from_str(t->s);
    if (t->type == JV_OBJECT) {
        JVal *inner = json_get(t, "type");
        if (inner && inner->type == JV_STRING) return avro_type_from_str(inner->s);
    }
    return AVRO_STRING;  /* nested record/array/map → raw text (MVP limit) */
}

/* Parse an Avro record schema JSON into a flat AvroField list.
 * Handles record-of-primitives and union ["null", T] (in either order).
 * Returns field count (>=0) or -1 on a malformed schema. */
static inline int avro_parse_schema(Arena *a, const char *schema_json,
                                    AvroField **out_fields)
{
    JVal *root = json_parse(a, schema_json, strlen(schema_json));
    if (!root || root->type != JV_OBJECT) return -1;
    JVal *fields = json_get(root, "fields");
    if (!fields || fields->type != JV_ARRAY) return -1;

    int n = (int)fields->nitems;
    AvroField *flist = arena_calloc(a, (size_t)n * sizeof(AvroField));

    for (int i = 0; i < n; i++) {
        JVal *field = fields->items[i];
        const char *fname = json_str(json_get(field, "name"), "");
        snprintf(flist[i].name, sizeof(flist[i].name), "%s", fname);
        flist[i].union_null_idx = -1;

        JVal *ftype = json_get(field, "type");
        if (ftype && ftype->type == JV_ARRAY) {
            /* Union — usually ["null", T]; record which branch is null. */
            flist[i].type        = AVRO_UNION_NULL_T;
            flist[i].union_inner = AVRO_STRING;
            for (size_t u = 0; u < ftype->nitems; u++) {
                JVal *br = ftype->items[u];
                if (br->type == JV_STRING && !strcmp(br->s, "null"))
                    flist[i].union_null_idx = (int)u;
                else
                    flist[i].union_inner = avro_type_from_jval(br);
            }
        } else {
            flist[i].type = avro_type_from_jval(ftype);
        }
    }
    *out_fields = flist;
    return n;
}

/* Decode one Avro record payload into per-field string values (NULL for union
 * nulls). Returns 0 on success, -1 on any decode overrun (caller falls back). */
static inline int avro_decode_record(Arena *a, const AvroField *fields, int nfields,
                                     const uint8_t *payload, size_t plen,
                                     char ***out_values, uint8_t **out_nulls)
{
    char    **values = arena_calloc(a, (size_t)nfields * sizeof(char *));
    uint8_t  *nulls  = arena_calloc(a, (size_t)nfields);
    size_t    pos    = 0;
    char      buf[64];

    for (int f = 0; f < nfields; f++) {
        const AvroField *field    = &fields[f];
        AvroType         eff_type = field->type;  /* для union уточняется ниже */

        /* Every field needs at least one byte (a union branch index, a string
         * length prefix of 0x00, a varint, ...) — only the literal `null` type
         * is zero-width. No bytes left here means the payload was truncated. */
        if (pos >= plen && field->type != AVRO_NULL) {
            LOG_ERROR("avro: truncated payload before field %d", f);
            return -1;
        }

        if (field->type == AVRO_UNION_NULL_T) {
            int64_t branch = 0;
            pos += avro_decode_long(payload + pos, plen - pos, &branch);
            if (pos > plen) return -1;
            if ((int)branch == field->union_null_idx) {
                nulls[f] = 1; values[f] = NULL; continue;  /* выбрана ветка null */
            }
            eff_type = field->union_inner;  /* иначе декодируем непустую ветку */
        }

        switch (eff_type) {
            case AVRO_NULL:
                nulls[f] = 1; values[f] = NULL;
                break;
            case AVRO_BOOL: {
                if (pos + 1 > plen) return -1;
                uint8_t b = payload[pos++];
                values[f] = arena_strdup(a, b ? "true" : "false");
                break;
            }
            case AVRO_INT:
            case AVRO_LONG: {
                int64_t v = 0;
                pos += avro_decode_long(payload + pos, plen - pos, &v);
                if (pos > plen) return -1;
                snprintf(buf, sizeof(buf), "%lld", (long long)v);
                values[f] = arena_strdup(a, buf);
                break;
            }
            case AVRO_FLOAT: {
                if (pos + 4 > plen) return -1;
                float v; memcpy(&v, payload + pos, 4); pos += 4;
                snprintf(buf, sizeof(buf), "%g", (double)v);
                values[f] = arena_strdup(a, buf);
                break;
            }
            case AVRO_DOUBLE: {
                if (pos + 8 > plen) return -1;
                double v; memcpy(&v, payload + pos, 8); pos += 8;
                snprintf(buf, sizeof(buf), "%g", v);
                values[f] = arena_strdup(a, buf);
                break;
            }
            case AVRO_STRING:
            case AVRO_BYTES: {
                int64_t slen = 0;
                size_t  pfx  = avro_decode_long(payload + pos, plen - pos, &slen);
                if (slen < 0 || pos + pfx + (size_t)slen > plen) return -1;
                const uint8_t *sp = payload + pos + pfx;
                char *s = arena_alloc(a, (size_t)slen + 1);
                memcpy(s, sp, (size_t)slen);
                s[slen] = '\0';
                values[f] = s;
                pos += pfx + (size_t)slen;
                break;
            }
            default:
                values[f] = arena_strdup(a, "");
                break;
        }
        if (pos > plen) { LOG_ERROR("avro: decode overran at field %d", f); return -1; }
    }

    *out_values = values;
    *out_nulls  = nulls;
    return 0;
}
