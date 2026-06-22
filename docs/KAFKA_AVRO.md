# Kafka Avro support

The Kafka connector reads three message formats, selected by `data_format`:
`json`, `csv`, `avro`.

## Avro + Confluent Schema Registry

Set `data_format: avro` and provide `schema_registry_url`.

### Config

| Field                  | Required | Description |
|------------------------|----------|-------------|
| `data_format`          | yes      | `"avro"` |
| `schema_registry_url`  | yes      | Confluent Registry base URL (e.g. `http://registry:8081`) |
| `schema_registry_auth` | no       | `"user:pass"` HTTP basic auth |

Without `schema_registry_url` the connector logs an error and every message
falls back to the raw two-column batch (`kafka_offset`, `value`).

### Wire format

Confluent Avro messages are framed as:

```
byte 0     magic byte (0x00)
bytes 1-4  schema ID (big-endian int32)
bytes 5+   Avro binary payload, encoded against that schema
```

On each message the connector reads the schema ID, fetches the schema from the
Registry (`GET {url}/schemas/ids/{id}`), caches it in memory, and decodes the
payload. A bad magic byte, an unreachable Registry, or a decode overrun all fall
back to the raw batch — a malformed message never crashes the read loop.

### Result table

`kafka_offset` (INT64) + one column per Avro field, **all rendered as TEXT**.
Union `["null", T]` nulls become SQL NULL.

```yaml
steps:
  - id: read_cdi_avro
    connector_type: kafka
    connector_config: |
      {"brokers":"kafka.prod:9092",
       "topic":"cdi_physical_party",
       "group_id":"dfo_cdi_consumer",
       "data_format":"avro",
       "schema_registry_url":"http://registry.prod:8081",
       "schema_registry_auth":"${SR_USER}:${SR_PASSWORD}"}
    target_table: cdi_physical_party_raw
```

### Supported Avro types

Primitives `null, boolean, int, long, float, double, string, bytes` and unions
of the form `["null", T]` (in either branch order). **Logical types**
(`{"type":"long","logicalType":"timestamp-micros"}`) decode as their underlying
physical primitive, so timestamps/decimals keep the binary stream aligned.

### Limitations

- Flat record schemas only — nested records, arrays and maps are stored as raw
  text and may desync a record that contains them.
- Read-only: no Avro **serialization** (the Kafka sink still writes JSON).
- Only the Confluent **Avro** Registry format (not Protobuf / JSON Schema).
- The in-memory schema cache lives for the connector's lifetime (no disk cache).

### Caching

The first message of each schema ID triggers one Registry HTTP GET
(`kafka: cached avro schema id=… (N fields)` in the log); subsequent messages of
that schema decode without any HTTP call.

## Implementation / tests

- `lib/connector/plugins/kafka/avro_decode.h` — zig-zag varint + bytes primitives
  (header-only).
- `lib/connector/plugins/kafka/avro_record.h` — schema-JSON parsing + flat-record
  decoding (header-only; depends only on `json`/`arena`/`log`).
- `tests/unit/test_avro_decode.c` — varint/bytes primitives.
- `tests/unit/test_avro_record.c` — schema parse, record decode, union-null,
  logical types, truncation handling.
- `tests/integration/test_kafka_avro.sh` — end-to-end against a live broker +
  Registry (skips if env is unset).
