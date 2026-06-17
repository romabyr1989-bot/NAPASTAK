# Connector Plugin ABI — v2

Export symbol: `dfo_connector_entry` (a `const DfoConnector` **data symbol**, not a
function — the loader `dlsym()`s it and reads it directly as `DfoConnector*`).

Build: `gcc -shared -fPIC my.c -o my.so`

Required fields: `abi_version=2`, `name`, `create`, `destroy`,
`list_entities`, `describe`, `read_batch`, `ping`.

Optional: `cdc_start`, `cdc_stop`, `write_batch`.

Never reorder or remove struct fields. Add new optional fields at end only.
Bump `DFO_CONNECTOR_ABI_VERSION` on breaking changes.

## v2 — sinks / приёмники

ABI v2 adds an optional **sink** hook so a connector can write data OUT, not
just read it in:

```c
int (*write_batch)(void *ctx, Arena *a, const char *entity,
                   const Schema *schema, const ColBatch *batch, int mode);
```

- `entity` — destination name (table / object key / topic / file path).
- `schema` / `batch` — the rows to write (pipeline output cells are TEXT).
- `mode` — `DFO_SINK_APPEND` (0) keeps existing data, `DFO_SINK_OVERWRITE` (1)
  replaces it. On a multi-batch write only the first call applies `mode`;
  later calls always append.
- Returns rows written (`>=0`) or `<0` on error. Leave `NULL` for read-only
  connectors (the engine reports "коннектор не поддерживает запись").

A **sink pipeline step** sets `is_sink: true`, `connector_type`,
`connector_config`, `sink_entity`, `sink_mode`, and a `transform_sql` SELECT
that produces the rows to export. The gateway materializes the SELECT, builds a
batch, loads the connector, and streams it through `write_batch`.

Built-in sinks: **csv** (file), **json_http** (POST a JSON array),
**postgresql** (CREATE TABLE IF NOT EXISTS + INSERT; OVERWRITE truncates),
**s3** (PUT a CSV object, sigv4-signed), **kafka** (one JSON message per row).
