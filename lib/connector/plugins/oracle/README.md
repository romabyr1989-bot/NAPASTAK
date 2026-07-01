# Oracle connector (`oracle_connector.so`)

Reads data from **Oracle Database** into NAPASTAK via **ODPI-C** (Oracle
Database Programming Interface for C — a thin wrapper over OCI). Mirrors the
two read modes of the [pg connector](../pg/) (OFFSET pagination / incremental
high-watermark on `cursor_column`) with Oracle-version-aware SQL.

`connector_type` in a pipeline/YAML is **`oracle`**.

## Dependencies & install

- **Compile-time:** ODPI-C only (`dpi.h`, `-lodpic`). ODPI-C does *not* need the
  Oracle Instant Client at build time.
- **Runtime:** Oracle Instant Client (ODPI-C `dlopen`s it). Put it on the loader
  path (`LD_LIBRARY_PATH` on Linux, `DYLD_LIBRARY_PATH`/`~/lib` on macoS) or set
  `TNS_ADMIN` as usual.

```bash
# Ubuntu/Debian
apt install libodpi-dev          # or build ODPI-C from https://github.com/oracle/odpi
# Fedora/RHEL
dnf install odpi-c-devel
# macOS
brew install instantclient-basic # Instant Client (runtime)
# + build/install ODPI-C headers+lib from https://github.com/oracle/odpi
```

Build:

```bash
make oracle      # builds build/<release|debug>/lib/oracle_connector.so
# `make` builds it automatically only when dpi.h is detected (ODPI_PREFIX)
```

## `connector_config` parameters

| Key             | Type   | Default     | Description                                                       |
|-----------------|--------|-------------|-------------------------------------------------------------------|
| `host`          | string | `localhost` | Oracle host                                                       |
| `port`          | string | `1521`      | listener port                                                     |
| `service_name`  | string | `ORCL`      | service name (Easy Connect: `host:port/service_name`)             |
| `user`          | string | `""`        | login user                                                        |
| `password`      | string | `""`        | password (use `${ORACLE_PASSWORD}` env substitution in YAML)      |
| `schema`        | string | `""`        | Oracle OWNER for `ALL_TABLES`/`ALL_TAB_COLUMNS` + table quoting (upper-cased) |
| `batch_size`    | string | `8192`      | rows per `read_batch` (capped at 8192 = `BATCH_SIZE`)             |
| `cursor_column` | string | `""`        | if set → incremental high-watermark reads on this column          |
| `oracle_version`| string | autodetect  | major (`11`/`12`/`19`/`21`/`23`); overrides autodetect; default 12 |

On connect the plugin:
1. creates a single process-wide ODPI-C context (`pthread_once`);
2. opens the connection (`host:port/service_name`);
3. autodetects the server major via `dpiConn_getServerVersion` (when the user
   has the privilege) — `oracle_version` overrides it;
4. sets NLS formats so DATE/TIMESTAMP round-trip predictably:
   `NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'`,
   `NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF3'`,
   `NLS_TIMESTAMP_TZ_FORMAT='… TZH:TZM'`.

## Version compatibility

| Версия     | Пагинация          | Инкремент          | JSON тип   | BOOLEAN    | VECTOR     |
|------------|--------------------|--------------------|------------|------------|------------|
| 11g (11.2) | ROWNUM subquery    | ROWNUM subquery    | —          | —          | —          |
| 12c (12.1+)| OFFSET/FETCH       | FETCH NEXT         | —          | —          | —          |
| 19c        | OFFSET/FETCH       | FETCH NEXT         | —          | —          | —          |
| 21c        | OFFSET/FETCH       | FETCH NEXT         | `COL_TEXT` | —          | —          |
| 23ai       | OFFSET/FETCH       | FETCH NEXT         | `COL_TEXT` | `COL_BOOL` | `COL_TEXT` |

- **11g**: no `OFFSET … FETCH` — pagination uses a `ROWNUM` subquery
  (`SELECT * FROM (SELECT t.*, ROWNUM dfo__rn FROM … WHERE ROWNUM <= hi) WHERE
  dfo__rn > lo`); the helper `dfo__rn` column is stripped from the output batch.
  Incremental uses `SELECT * FROM (… WHERE col > :1 ORDER BY col ASC) WHERE
  ROWNUM <= n` so `ROWNUM` is applied *after* the sort.
- **12c+**: standard `OFFSET :o ROWS FETCH NEXT :n ROWS ONLY` and
  `… FETCH NEXT :n ROWS ONLY`.
- `BOOLEAN` SQL columns exist only on **23ai** → mapped to `COL_BOOL` only when
  `ora_major >= 23` (else `COL_TEXT`).

## Type mapping (`describe`, via `ALL_TAB_COLUMNS`)

| Oracle type                                       | ColType     |
|---------------------------------------------------|-------------|
| `NUMBER(p,0)`, `INTEGER`, `SMALLINT`              | `COL_INT64` |
| `NUMBER(p,s>0)`, `NUMBER` (no p/s), `FLOAT`, `BINARY_FLOAT/DOUBLE` | `COL_DOUBLE` |
| `VARCHAR2`,`NVARCHAR2`,`CHAR`,`NCHAR`,`CLOB`,`NCLOB` | `COL_TEXT` |
| `DATE`, `TIMESTAMP`, `TIMESTAMP WITH TIME ZONE`   | `COL_INT64` (unix seconds) |
| `BLOB`                                            | `COL_TEXT` (hex: 2 chars/byte) |
| `BOOLEAN` (23ai)                                  | `COL_BOOL` (else `COL_TEXT`) |
| `JSON` (21c+), `XMLTYPE`, `VECTOR` (23ai), other  | `COL_TEXT`  |

> **Note:** `describe` is metadata. `read_batch` itself emits **every** column
> as `COL_TEXT` — identical to the pg connector — because the query engine has a
> read-back bug on native INT64/DOUBLE columns. Cell stringification: integers →
> decimal, doubles → `%.17g`, TIMESTAMP/DATE → the canonical NLS string (also
> used as the cursor bookmark, so Oracle implicitly re-casts it on `WHERE col >
> :1`), BLOB → hex, CLOB/NCLOB → text via `dpiLob_readBytes`, BOOLEAN →
> `true`/`false`.

Column names are returned by Oracle in UPPER CASE and **lower-cased** when
building the schema.

## Read modes

- **OFFSET pagination** (default): cursor is an integer offset.
- **Incremental** (`cursor_column` set): high-watermark on the column; the max
  value of the batch is handed back via `req->cursor`. Best on a monotonic key
  (`id` / `LAST_UPD`). The bookmark is bound as a string and Oracle re-casts it
  to the column type using the session NLS formats.

`entity` may also be a full `SELECT …` (wrapped as a subquery).

## Example pipeline (incremental + SCD2)

```yaml
name: oracle_tsmdm_in
steps:
  - id: extract
    connector_type: oracle
    connector_config:
      host: oracle.prod.internal
      port: "1521"
      service_name: ORCL
      user: tsmdm_user
      password: ${ORACLE_PASSWORD}
      schema: TSMDM_IN
      cursor_column: LAST_UPD
      oracle_version: "19"
    target_table: stg_parties

  - id: historise
    transform_sql: SELECT * FROM stg_parties
    target_table: dim_parties
    scd2_business_key: party_id
    scd2_effective_from_col: valid_from
    scd2_effective_to_col: valid_to
    scd2_compare_columns: name,inn,address
    scd2_transaction_time: last_upd
```

## Known limitations

- **CDC not implemented** (`cdc_start`/`cdc_stop` = `NULL`): the planned
  LogMiner path (DBMS_LOGMNR → `V$LOGMNR_CONTENTS` → `CdcEvent`) mirrors the
  kafka consumer thread; until then use `cursor_column`.
- **Read-only** (`write_batch` = `NULL`): source connector, no sink.
- `read_batch` emits TEXT for all columns; native `NUMBER` is fetched through
  `double` (`%.17g`), so integers/decimals beyond 2^53 lose precision — fetch
  such columns as `TO_CHAR(col)` in a `SELECT` entity if exact text is required.
- `BLOB` is hex-encoded; `XMLTYPE` is best read with `xmlcol.getClobVal()` in a
  `SELECT` entity.
- ABI: built against **Connector ABI v2** (`DFO_CONNECTOR_ABI_VERSION`); the
  loader rejects any other version. (The task brief said ABI "v1"; the live ABI
  in `connector.h` is v2 — using the macro keeps the plugin loadable.)

## Build/verification status

The C source passes `-Wall -Wextra -Wpedantic` (validated against an ODPI-C
header stub). A full compile/link requires ODPI-C installed (`make oracle`).
The gateway already recognises `connector_type: oracle` and resolves
`oracle_connector.so`; once ODPI-C is present the `.so` loads like any other
plugin (ABI v2).
