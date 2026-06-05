# YAML pipelines + GitOps

DataFlow OS pipelines can be authored as YAML files and auto-loaded from a
directory on the gateway host. Combined with `git`, this enables a GitOps
workflow: pipelines live in version control; the gateway picks up changes
on restart.

## Setup

1. Pick a directory for pipelines, e.g. `/etc/dataflow/pipelines/`.
2. Set it in the gateway config:

   ```json
   {
     "port": 8080,
     "data_dir": "./data",
     "pipelines_dir": "/etc/dataflow/pipelines"
   }
   ```

3. Drop `*.yaml` or `*.yml` files into the directory.
4. Restart the gateway. Pipelines load at startup; their progress is logged:

   ```
   [INFO] pipelines_dir: loaded 3 YAML pipeline(s) (0 failed) from /etc/dataflow/pipelines
   ```

YAML pipelines coexist with REST-API pipelines stored in `catalog.db`.
If a YAML file shares an `id` with an existing pipeline, the YAML
version replaces it — files are the source of truth in GitOps mode.

## File format

```yaml
# pipelines/users_etl.yaml
name: users_etl
description: Sync users from Postgres to analytics tables
enabled: true

triggers:
  - type: cron
    cron_expr: "0 */6 * * *"
  - type: webhook
    webhook_token: wh_users_etl_secret

steps:
  - id: extract
    connector_type: postgresql
    connector_config:
      host: db.prod.internal
      database: app
      table: users
    target_table: users_raw

  - id: dedupe
    transform_sql: |
      INSERT INTO users_clean
      SELECT DISTINCT ON (email) *
      FROM users_raw
      ORDER BY email, updated_at DESC
    target_table: users_clean
    depends_on: [extract]

  - id: aggregate
    transform_sql: |
      INSERT INTO users_daily
      SELECT date_trunc('day', created_at) AS day, COUNT(*) AS new_users
      FROM users_clean
      GROUP BY 1
    target_table: users_daily
    depends_on: [dedupe]

webhook_url: ${SLACK_WEBHOOK}
webhook_on:  failure
alert_cooldown: 300
```

The schema mirrors the REST API JSON exactly. See [TRIGGERS.md](TRIGGERS.md)
for the trigger reference.

## MDM pipeline (incremental → normalize → match → SCD2)

The YAML schema is a 1:1 map of the step JSON, so MDM fields need no special
syntax — just set them on the step:

- **Incremental pull**: `connector_config.read_mode: cursor` + `cursor_column`
  (a monotonic column like `updated_at`/`id`/`lsn`). The connector reads only
  rows newer than the last high-watermark instead of the full table.
- **Match**: an ordinary SQL step using the `jaro_winkler(a, b)` /
  `levenshtein(a, b)` scalar functions.
- **SCD2**: set `scd2_business_key` (this is what makes the step an SCD2 step) plus the
  `scd2_*` columns; the step historises `target_table` from its `transform_sql`
  source slice.

```yaml
# pipelines/mdm_customer.yaml
name: mdm_customer
enabled: true

steps:
  # 1. Incremental source — only rows whose updated_at advanced since last run
  - id: ingest
    connector_type: postgresql
    connector_config:
      host: db.prod.internal
      dbname: crm
      table: customers
      read_mode: cursor          # full | cursor | cdc
      cursor_column: updated_at  # high-watermark column
    target_table: raw_customers

  # 2. Normalize with plain SQL (trim/lower are built-in scalars)
  - id: normalize
    transform_sql: |
      SELECT id, trim(lower(name)) AS name, lower(email) AS email, updated_at
      FROM raw_customers
    target_table: clean_customers

  # 3. Fuzzy match candidate duplicates via jaro_winkler
  - id: match
    transform_sql: |
      SELECT a.id AS a_id, b.id AS b_id,
             jaro_winkler(a.name, b.name) AS similarity
      FROM clean_customers a, clean_customers b
      WHERE a.id < b.id AND jaro_winkler(a.name, b.name) >= 0.9
    target_table: customer_matches

  # 4. SCD2 — historise the golden dimension
  - id: historise
    transform_sql: "SELECT * FROM clean_customers"
    target_table: dim_customer
    scd2_business_key: id              # presence of this field = SCD2 step
    scd2_compare_columns: name,email   # empty = compare all non-meta columns
    scd2_transaction_time: updated_at  # ORDER BY for the current-version window
    scd2_effective_from_col: valid_from
    scd2_effective_to_col: valid_to
    scd2_deleted_flag: is_deleted      # source soft-delete marker
```

`connector_config` is written as a nested map; the loader serialises it back to
the JSON string the step schema expects, so `cursor_column` survives intact.

## YAML subset supported

The built-in parser accepts a constrained subset — sufficient for
pipelines but **not** a full YAML 1.2 implementation:

| Feature                       | Supported                            |
|-------------------------------|--------------------------------------|
| Block mappings                | ✓ (`key: value`)                    |
| Block sequences               | ✓ (`- item`)                        |
| Flow sequences                | ✓ (`[a, b, c]`)                     |
| Quoted scalars                | ✓ (`"…"` and `'…'` with `\n` escape)|
| Plain scalars (auto-typed)    | ✓ (`true`/`false`/`null`/`123`/text)|
| Block scalar literal `\|`     | ✓ (preserves newlines)               |
| Block scalar folded `>`       | ✓ (joins with spaces)                |
| Comments `# …`                | ✓                                    |
| Anchors `&` / aliases `*`     | ✗                                    |
| Tags (`!!str`, …)             | ✗                                    |
| Flow mappings `{a: 1}`        | ✗                                    |
| Merge keys `<<:`              | ✗                                    |
| Multi-document streams        | `---` is silently skipped            |

If you need features outside the subset, author the pipeline as JSON and
POST it to `/api/pipelines` — both formats target the same schema.

## Validation API

Before committing a YAML change, validate it against the running gateway:

```sh
curl -X POST http://localhost:8080/api/pipelines/preview-yaml \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: text/yaml" \
  --data-binary @pipelines/users_etl.yaml
```

- 200 → returns the parsed JSON pipeline (NOT saved)
- 400 → `{"error":"yaml parse error","detail":"…","line":N,"col":M}`

## GitOps workflow

A typical setup:

```
                    ┌───────────────┐
                    │ git repo      │
                    │ pipelines/    │
                    └──────┬────────┘
              git push     │
       ┌─────────────────► │
       │                   │ git pull (cron / hook)
       ▼                   ▼
┌──────────────┐    ┌───────────────────────────┐
│ developer    │    │ gateway host              │
│ - edits yaml │    │ /etc/dataflow/pipelines/  │
│ - runs       │    │ + reload (gateway restart)│
│   preview    │    └───────────────────────────┘
└──────────────┘
```

Recommended cron entry on the gateway host:

```cron
# Pull pipeline changes from git every 5 minutes; reload on update
*/5 * * * * cd /etc/dataflow/pipelines && \
  test "$(git pull --quiet origin main 2>&1 | head -c1)" \
       != "" && systemctl restart dataflow-gateway
```

A future iteration may add an in-process git-pull thread + hot-reload
via inotify; for now the operator handles refresh.

## Limitations

- **Hot-reload not yet wired.** Edit a YAML file → restart the gateway to
  pick up changes. (`tests/integration/test_yaml_pipelines.sh` runs the
  full workflow end-to-end with a restart.)
- **Single document per file.** `---` separators are skipped, not used to
  define multiple pipelines per file.
- **No `default_args`-style merging.** Repeat shared step config across
  steps; YAML anchors (`&` / `*`) are not supported.
- **Max file size 1 MiB.** Files larger than this are rejected at load.
- **No `git pull` thread.** Wrap with cron / systemd timer (see above).

## Test coverage

- `tests/unit/test_yaml.c` — 14 cases: scalars, mappings, sequences,
  block scalars (literal + folded), flow arrays, comments, blank-line
  preservation, error path
- `tests/integration/test_yaml_pipelines.sh` — 10 e2e cases including
  preview round-trip, auto-load, webhook fire from auto-loaded pipeline,
  startup log content
