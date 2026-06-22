# Greenplum connector (`gp_connector.so`)

Reads data from **Greenplum** (MPP PostgreSQL) into DataFlow OS via libpq.
Greenplum speaks the PostgreSQL wire protocol, so the transport is the same
libpq used by the [pg connector](../pg/) — but discovery, version handling and
pagination are GP-specific, hence a separate plugin.

`connector_type` in a pipeline/YAML is **`greenplum`** (the gateway maps it to
`gp_connector.so`).

## Dependencies & install

Compile-time needs **libpq** (same as the pg plugin). Runtime needs network
access to a Greenplum master.

```bash
# Ubuntu/Debian
apt install libpq-dev
# Fedora/RHEL
dnf install libpq-devel
# macOS (Homebrew, keg-only)
brew install libpq        # Makefile auto-detects /usr/local/opt/libpq or /opt/homebrew/opt/libpq
```

Build:

```bash
make gp          # builds build/<release|debug>/lib/gp_connector.so
# or simply `make` — gp is built automatically whenever libpq is detected
```

## `connector_config` parameters

| Key             | Type   | Default     | Description                                                        |
|-----------------|--------|-------------|--------------------------------------------------------------------|
| `host`          | string | `localhost` | Greenplum master host                                              |
| `port`          | string | `5432`      | master port                                                        |
| `dbname`        | string | `postgres`  | database name                                                      |
| `user`          | string | `""`        | login user                                                         |
| `password`      | string | `""`        | password (use `${GP_PASSWORD}` env substitution in YAML)           |
| `schema`        | string | `public`    | schema for `search_path`, `list_entities`, `describe`              |
| `sslmode`       | string | `disable`   | libpq sslmode (`disable`/`require`/`verify-full`/…)                |
| `connect_timeout`| string| `10`        | connect timeout, seconds                                           |
| `batch_size`    | string | `8192`      | rows per `read_batch` (capped at 8192 = `BATCH_SIZE`)              |
| `cursor_column` | string | `""`        | if set → incremental high-watermark reads on this column           |
| `gp_version`    | string | autodetect  | GP major (`5`/`6`/`7`); overrides `SELECT version()` autodetect     |

On connect the plugin:
1. sets `search_path` to `<schema>,public` (parameterised via `quote_ident`);
2. runs `SELECT version()`, requires the string to contain `Greenplum`
   (otherwise it errors — use `connector_type: postgresql` for plain PG), and
   derives the GP major unless `gp_version` is given.

## Version compatibility

| Версия | PG база | `list_entities`        | External tables filter | Партиции          |
|--------|---------|------------------------|------------------------|-------------------|
| 5.x    | PG 8.4  | `pg_inherits` фильтр   | `relkind`/`pg_inherits`| via `pg_inherits` |
| 6.x    | PG 9.4  | `relispartition`       | `relstorage <> 'x'`    | `relispartition`  |
| 7.x    | PG 12   | `relispartition`       | `relstorage <> 'x'`    | `relispartition`  |

- GP 6+: `list_entities` also reports storage kind (`heap`/`ao`/`aocs`);
  append-optimized tables are logged at `LOG_DEBUG`.
- All versions: `read_batch` uses `LIMIT n` (portable) rather than
  `FETCH NEXT n ROWS ONLY`, so the same code path works on 5.x→7.x.
- `describe` reads `pg_catalog.pg_attribute` (faster/more reliable on MPP than
  `information_schema`).

## Type mapping (`describe`)

| GP/PG type                                   | ColType     |
|----------------------------------------------|-------------|
| `int2`,`int4`,`int8`,`oid`,`xid`,`tid`,`cid` | `COL_INT64` |
| `float4`,`float8`,`numeric`,`decimal`        | `COL_DOUBLE`|
| `bool`                                       | `COL_BOOL`  |
| `timestamp`,`timestamptz`,`date`,`abstime`   | `COL_INT64` (unix seconds) |
| `varchar`,`text`,`bpchar`,`name`,`citext`    | `COL_TEXT`  |
| `aclitem`,`gpxlogloc`, other GP-internal     | `COL_TEXT`  |

> **Note:** `describe` is metadata. `read_batch` itself emits **every** column
> as `COL_TEXT` — identical to the pg connector — because the query engine has a
> read-back bug on native INT64/DOUBLE columns. Downstream SQL coerces as needed;
> the cursor bookmark is read straight from the raw value, so cursor mode on a
> bigint/timestamp column is unaffected.

## Read modes

- **OFFSET pagination** (default, `cursor_column` unset): cursor is an integer
  offset. On GP, `OFFSET` executes on the master segment and is slow on large
  tables — the connector emits a `LOG_WARN` recommending `cursor_column`.
- **Incremental** (`cursor_column` set): `WHERE "col" > $1 ORDER BY "col" ASC
  LIMIT n`; the max value of the batch is handed back via `req->cursor` for the
  next call. Best on a monotonic key (`id` / `last_upd_ts`).

`entity` may also be a full `SELECT …` — it is wrapped as a subquery.

## Example pipeline (incremental + SCD2)

```yaml
name: gp_customers_cdh
steps:
  # 1. Pull only rows changed since last run from Greenplum CDH
  - id: extract
    connector_type: greenplum
    connector_config:
      host: gp-master.prod.internal
      port: "5432"
      dbname: tsmdm
      user: tsmdm_rw
      password: ${GP_PASSWORD}
      schema: tsmdm_cdh
      cursor_column: last_upd_ts
    target_table: stg_customers

  # 2. Historise into a type-2 dimension
  - id: historise
    transform_sql: SELECT * FROM stg_customers
    target_table: dim_customers
    scd2_business_key: customer_id
    scd2_effective_from_col: valid_from
    scd2_effective_to_col: valid_to
    scd2_compare_columns: name,email,city
    scd2_transaction_time: last_upd_ts
```

## Known limitations

- **CDC not implemented** (`cdc_start`/`cdc_stop` = `NULL`): stock Greenplum has
  no logical replication. Use `cursor_column` for incremental pulls.
- **Read-only** (`write_batch` = `NULL`): source connector, no sink.
- `read_batch` emits TEXT for all columns (see type-mapping note above).
- ABI: built against **Connector ABI v2** (`DFO_CONNECTOR_ABI_VERSION`); the
  loader rejects any other version.
