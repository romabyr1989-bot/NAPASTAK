# SQL templates

Pipeline steps can use parameterized SQL templates — stored inline or as files —
so one query is reused across many steps with different runtime parameters.

## Step fields

| Field          | Description |
|----------------|-------------|
| `sql_template` | Path to a `.sql` file under `sql_templates_dir` (max 64 KB) |
| `sql_params`   | JSON object of `{placeholder: value}` substitutions |
| `transform_sql`| Inline SQL (alternative to `sql_template`) |

`sql_template` (when set) takes precedence over `transform_sql`. The file is read
at **step execution time** — edits apply on the next run without a restart.
`sql_templates_dir` is set in `config.json` (default `./sql`).

The resolved SQL is used by every SQL-driven step kind: local SQL, connector
source, sink, SCD2, match, and Python/Scala input.

## Placeholders

Use `{name}` in the SQL. `{{` is a literal `{`.

### Literal parameters (from `sql_params`)

| Value kind             | `{x}` becomes | Example |
|------------------------|---------------|---------|
| string                 | quoted literal | `{"x":"Bob"}` → `'Bob'` |
| number                 | verbatim       | `{"x":42}` → `42` |
| name ending in `_raw`  | verbatim (identifier) | `{"tbl_raw":"users"}` → `users` |

String values are quote-escaped (internal `'` doubled) — safe against injection.

### Computed parameters (`@`-prefixed)

| Param                  | Resolves to |
|------------------------|-------------|
| `{@now}`               | current timestamp literal `'YYYY-MM-DD HH:MM:SS'` |
| `{@last_run}`          | the pipeline's last run timestamp (epoch if never) |
| `{@step:<id>:<col>}`   | comma-separated quoted list of `col` from step `<id>`'s result |

`@step` enables the "fetch by IDs from a prior step" pattern. The referenced step
must be an earlier **local SQL** step (its result set is cached for the run); an
empty result yields `NULL` (a valid empty `IN` list).

## Examples

### File template + literal params

`sql/merge.sql`:
```sql
MERGE INTO {target_raw} t USING {source_raw} s ON t.{key_raw} = s.{key_raw} ...
```
```yaml
steps:
  - id: merge
    sql_template: merge.sql
    sql_params: '{"target_raw":"mdm_customer","source_raw":"mdm_customer_tmp","key_raw":"id"}'
    connector_type: postgresql
    connector_config: '{"host":"gp.prod","dbname":"mdm","user":"rw","password":"${PW}"}'
```

### Delta load with `@last_run`

`sql/delta.sql`:
```sql
SELECT * FROM {source_raw} WHERE last_upd > {@last_run}
```
```yaml
steps:
  - id: extract_delta
    sql_template: delta.sql
    sql_params: '{"source_raw":"s_contact"}'
    target_table: staging_contacts
```

### IDs from a prior step with `@step`

```yaml
steps:
  - id: find_owners
    transform_sql: "SELECT DISTINCT owner_id FROM staging_agreements"
    target_table: staging_owner_ids
  - id: fetch_details
    sql_template: owners_by_id.sql      # WHERE owner_id IN ({@step:find_owners:owner_id})
    target_table: staging_owner_details
    deps: [0]
```

## Security

- `sql_template` paths cannot contain `..` (no traversal).
- Literal string values are single-quote-escaped.
- `_raw` parameters bypass quoting — use only for **trusted identifiers**, never
  for user-supplied values.

## Implementation / tests

- `lib/sql_template/` — `sql_quote_literal` + `sql_substitute_params` (pure; the
  `@computed` resolver is injected by the gateway).
- `tests/unit/test_sql_substitute.c` — quoting, substitution, escaping, injection.
- `tests/integration/test_sql_templates.sh` — file templates, `@now`/`@step`,
  traversal block, unresolved-param failure, inline regression.
