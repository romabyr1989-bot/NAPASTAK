# NAPASTAK — Airbyte Connector Runner

`lib/connector/plugins/airbyte/airbyte_runner.c` ships as `airbyte_connector.so`
and lets NAPASTAK run **any** Airbyte source image as a pipeline data source —
PostgreSQL, Stripe, HubSpot, Salesforce, GitHub, Slack, Notion, and ~600 others.

The runner shells out to `docker run` (or `podman run`) for each of the four
Airbyte protocol commands (`spec`, `check`, `discover`, `read`) and adapts the
JSON-on-stdio responses to DFO's connector ABI v1.

## Requirements

- `docker` or `podman` in `PATH`
- The source image already pulled (or available to pull on demand). For
  evaluation use [`airbyte/source-faker`](https://hub.docker.com/r/airbyte/source-faker)
  which needs no credentials.

```sh
docker pull airbyte/source-faker:6.2.6
```

## Pipeline configuration

Connector type is `airbyte`. The `connector_config` object must include:

- `image` — the full image reference, must start with `airbyte/source-` or
  `airbyte/destination-` (whitelist enforced)
- `config` — the source's own config object, matching the schema produced by
  `docker run <image> spec`
- `runtime` (optional) — `docker` | `podman` | `auto` (default `auto`)

Example pipeline payload (POST `/api/pipelines`):

```json
{
  "name": "stripe_to_dfo",
  "cron": "0 */6 * * *",
  "steps": [{
    "connector_type": "airbyte",
    "connector_config": {
      "image": "airbyte/source-stripe:5.5.0",
      "config": {
        "client_secret": "sk_live_xxx",
        "account_id":    "acct_xxx",
        "start_date":    "2024-01-01T00:00:00Z"
      }
    },
    "stream":       "customers",
    "target_table": "stripe_customers"
  }]
}
```

## Pre-curated catalog

[`ui/app.js`](../ui/app.js) ships an `AIRBYTE_CATALOG` map with 30 popular
images at known-good versions as of 2026-05. The pipeline builder UI uses
this catalog to populate the connector picker. To add more sources, append
to that map.

## Type mapping (JSON Schema → ColType)

The runner derives a column type from each property's `type` (and optional
`format`) in the stream's `json_schema`:

| JSON Schema                                     | DFO ColType   |
|-------------------------------------------------|---------------|
| `"integer"`                                     | `COL_INT64`   |
| `"number"`                                      | `COL_DOUBLE`  |
| `"boolean"`                                     | `COL_BOOL`    |
| `"string"` (default)                            | `COL_TEXT`    |
| any with `"format":"date-time"` or `"datetime"` | `COL_INT64`   |
| `"object"` / `"array"`                          | `COL_TEXT` (serialized JSON) |

Nullable types (`["string", "null"]`) are flattened to the non-null variant
with `nullable=true`.

## Security model — read this before enabling in production

Running arbitrary Docker images is a large attack surface. The runner
applies these defenses:

| Defense                              | How                                                             |
|--------------------------------------|-----------------------------------------------------------------|
| Image whitelist                      | only `airbyte/source-*` or `airbyte/destination-*` accepted     |
| Resource limits                      | `--memory=512m --cpus=1`                                        |
| Read-only config mount               | `-v /tmp/airbyte_…:/secrets:ro`                                 |
| Auto-cleanup                         | `--rm` (no leftover containers)                                 |
| No privileges                        | never `--privileged`, no extra mounts, no host networking       |
| Workdir isolation                    | unique `/tmp/airbyte_<pid>_<rand>/` per connector instance      |
| Workdir lifecycle                    | created in `airbyte_create`, removed in `airbyte_destroy`       |
| Whitelist path guard                 | `rm -rf` only runs against paths under `/tmp/airbyte_`          |

What the runner does **not** do today:

- Hash-pinning images (we trust the registry to serve the requested tag)
- Scanning configs for secrets in plain text — that's the operator's job
- Per-pipeline RBAC checks beyond the existing pipeline-edit permission

If you operate a multi-tenant deployment, restrict pipeline creation to
trusted users and consider running the gateway under a non-privileged uid
that has its own Docker daemon socket access policy.

## End-to-end smoke test

`tests/integration/test_airbyte.sh` covers three layers:

1. The `.so` builds and exports `dfo_connector_entry`
2. The gateway accepts an `airbyte` pipeline definition
3. With `airbyte/source-faker:6.2.6` already pulled, the pipeline produces
   rows in the target table

If `docker`/`podman` aren't installed, layer 3 is skipped with a `SKIP`
notice — layers 1–2 still run.

```sh
docker pull airbyte/source-faker:6.2.6
make BUILD=release all
bash tests/integration/test_airbyte.sh
```

## Limitations / known gaps

- **Sync mode is fixed at `full_refresh` / `append`.** The runner emits a
  `ConfiguredAirbyteCatalog` with those fixed modes. Incremental cursor
  state is read from `STATE` messages and persisted in memory but not yet
  written back to disk between pipeline runs.
- **One stream per pipeline step.** If a source emits five streams, you
  need five pipeline steps. This matches DFO's connector ABI which keys
  reads by `entity` (= stream name).
- **No `spec` endpoint exposed yet.** Future work: add
  `GET /api/connectors/airbyte/spec?image=…` so the UI can render a
  dynamic config form from the source's JSON Schema. For now configs are
  authored as plain JSON.
- **TRACE / error messages from the source go to gateway stderr only**,
  not the run history yet.
