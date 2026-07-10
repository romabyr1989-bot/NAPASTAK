# YAML pipelines

NAPASTAK pipelines can be authored as YAML instead of hand-written JSON.
YAML is purely an authoring format — pipelines are created via the API/UI
and persisted only in `catalog.db`; there is no directory the gateway
watches or auto-loads from.

## File format

```yaml
# users_etl.yaml
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
# mdm_customer.yaml
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

## Validate a YAML doc

Before creating a pipeline, validate it against the running gateway:

```sh
curl -X POST http://localhost:8080/api/pipelines/preview-yaml \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: text/yaml" \
  --data-binary @users_etl.yaml
```

- 200 → returns the parsed JSON pipeline (NOT saved)
- 400 → `{"error":"yaml parse error","detail":"…","line":N,"col":M}`

## Create a pipeline from YAML

`/api/pipelines/from-template` parses the YAML, expands any `{{ var }}`
placeholders against `vars`, and saves the result straight to the catalog —
the pipeline is live (registered with the scheduler) immediately, no restart
needed:

```sh
curl -X POST http://localhost:8080/api/pipelines/from-template \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"template_yaml\": $(python3 -c 'import json,sys;print(json.dumps(open(sys.argv[1]).read()))' users_etl.yaml), \"vars\": {}}"
```

From then on the pipeline is edited like any other — through the UI or
`PUT /api/pipelines/:id` — and lives only in `catalog.db`. There is no
YAML file backing it to fall out of sync with.

## Test coverage

- `tests/unit/test_yaml.c` — 14 cases: scalars, mappings, sequences,
  block scalars (literal + folded), flow arrays, comments, blank-line
  preservation, error path
- `tests/integration/test_yaml_pipelines.sh` — preview-yaml round-trip and
  parse-error reporting
- `tests/integration/test_cdi_pipeline.sh`, `test_agr2party.sh`,
  `test_mdc_pipeline.sh` — exercise real pipeline YAML end-to-end via
  `from-template`
