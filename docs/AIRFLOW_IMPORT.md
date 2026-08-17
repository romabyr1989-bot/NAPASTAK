# Airflow → NAPASTAK DAG Importer

`napastak-import-airflow` is a Python utility that converts existing Airflow DAG
files into NAPASTAK pipeline JSON. The goal is **80% coverage of typical
DAGs** without trying to be 100% semantically equivalent.

It uses Python's stdlib `ast` module — **Airflow itself does NOT need to be
installed**. The importer never executes the DAG file; it just reads the
syntax tree and recognizes the patterns Airflow users actually write.

## Install

```sh
cd sdk/python
pip install -e .
```

The script is registered as `napastak-import-airflow`.

## Quickstart

```sh
# 1. Convert DAGs to JSON (offline)
napastak-import-airflow ~/airflow/dags --output ./dfo_pipelines

# 2. Convert AND apply to a running gateway
napastak-import-airflow ~/airflow/dags --apply \
                   --gateway http://localhost:8080 \
                   --api-key dfo_xxx
```

Output:

```
Reading /home/me/airflow/dags/daily_user_etl.py
Reading /home/me/airflow/dags/hubspot_sync.py
=== Summary ===
  Files scanned:    2
  Converted:        2
  Skipped:          0
  Steps with TODO:  0
  Output dir:       ./dfo_pipelines
```

## What's recognized

### DAG instantiation

Both forms are supported:

```python
# variable form
dag = DAG(dag_id='etl', schedule='@daily', start_date=datetime(2024, 1, 1))

# context-manager form
with DAG(dag_id='etl', schedule_interval='0 2 * * *') as dag:
    ...
```

### Schedule conversion

| Airflow value                  | NAPASTAK cron |
|--------------------------------|------------------|
| `'@daily'`                     | `0 0 * * *`      |
| `'@hourly'`                    | `0 * * * *`      |
| `'@weekly'`                    | `0 0 * * 0`      |
| `'@monthly'`                   | `0 0 1 * *`      |
| `'@yearly'` / `'@annually'`    | `0 0 1 1 *`      |
| `'0 6 * * *'` (literal cron)   | passthrough      |
| `None` / `'@once'`             | `manual`         |
| `timedelta(...)` (non-literal) | `manual` (logged)|

### Operators

| Airflow operator              | NAPASTAK step type   |
|-------------------------------|-------------------------|
| `BashOperator`                | `bash`                  |
| `EmptyOperator` / `DummyOperator` | `bash` (`true`)     |
| `SQLExecuteQueryOperator`     | `sql`                   |
| `PostgresOperator`            | `sql` (connector=postgresql) |
| `MySqlOperator`               | `sql` (connector=mysql) |
| `SnowflakeOperator`           | `sql` (connector=snowflake) |
| `BigQueryOperator` / `BigQueryInsertJobOperator` | `sql` (connector=bigquery) |
| `SimpleHttpOperator` / `HttpOperator` | `http`          |
| `S3CopyObjectOperator` / `S3ToRedshiftOperator` | `connector` (type=s3) |
| `EmailOperator`               | `bash` with TODO note   |
| `PythonOperator`              | `bash` with TODO note (cannot auto-execute Python) |
| anything else                 | `bash` step exiting 1, marked UNSUPPORTED |

`PythonOperator` and unrecognized operators always emit a step that **fails
loudly** (`exit 1`) so you notice them — they're never silently dropped.

### Dependencies

All three Airflow dependency forms are supported:

```python
a >> b >> c                    # chained bit-shift
a << b                          # reverse direction
a.set_downstream(b)            # method form
a.set_upstream(b)              # reverse method form
src >> [a, b, c]               # fanout to list
```

The resulting pipeline JSON expresses dependencies via `depends_on`:

```json
{
  "id": "load_users",
  "depends_on": ["extract_users"]
}
```

### Common knobs

- `retries=N` → `max_retries: N`
- `retry_delay=timedelta(...)` → `retry_delay_sec: 60` (default; we can't
  parse the timedelta literal at this layer)
- Airflow `default_args` propagate to operators by Airflow itself, but the
  importer doesn't try to merge them — set them per-task if you need them.

## Limitations

### What the importer can't do

- **Execute Python code.** `PythonOperator` callables are emitted as bash
  TODO steps that fail loudly. You must rewrite them.
- **Resolve Jinja templating.** `'{{ ds }}'` is preserved as a string. Most
  NAPASTAK steps don't interpret Jinja today.
- **Parse `default_args` / `params`.** The DAG-level dict isn't propagated
  into individual steps. Set values per task.
- **Handle `TaskGroup` / `SubDAG`** — flattened into a single linear list of
  steps; group structure is lost.
- **Recognize custom operators** outside `OPERATOR_MAPPING`. They become
  bash TODO steps. To add support, append to the dict in
  `sdk/python/napastak/airflow_import.py`.
- **Convert `XCom` references.** Cross-task data passing isn't translated;
  TODOs surface where it appears.

### Workflow

A pragmatic flow:

1. Run the importer offline first (without `--apply`).
2. Inspect the output JSON files. Look for `"note"` fields — those are
   places needing human review.
3. Fix the TODOs in the JSON (or the original DAG and re-run the importer).
4. Run `--apply` to push everything to the gateway.
5. Use the UI Pipelines page to verify schedules, run history, and trigger
   manual runs.

## Tests

```sh
cd sdk/python
pip install -e ".[dev]"
pytest tests/test_airflow_import.py -v
```

26 unit tests cover: DAG metadata extraction, both DAG forms, schedule
conversion, all dependency syntax variants, fanout `>>` to list,
`PythonOperator` TODO emission, unsupported-operator handling, and
syntax-error resilience.

## Roadmap

- `--diff` flag to compare imported pipelines against ones already running
- Optional `--strict` mode that fails on any TODO
- HTTP `POST /api/pipelines/preview-airflow` for the UI drag-and-drop importer
- Operator mapping plugin discovery (`/etc/napastak/airflow_operators.toml`)
