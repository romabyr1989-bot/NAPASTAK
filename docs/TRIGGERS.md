# Pipeline Triggers

NAPASTAK pipelines fire from any combination of these triggers. A
single pipeline can have up to 8 triggers — they're OR-ed (any one fires
the pipeline).

| Trigger        | When it fires                                     | Backend          |
|----------------|---------------------------------------------------|------------------|
| `cron`         | At a cron schedule                                | Scheduler thread |
| `webhook`      | HTTP POST/GET hits a secret URL                   | Gateway endpoint |
| `file_arrival` | A file matching a glob appears in a directory     | inotify (Linux)  |
| `manual`       | Only via `POST /api/pipelines/:id/run` or the UI  | —                |

`kafka_message` and `db_change` are reserved for future steps.

## JSON shape

```json
{
  "name": "users_etl",
  "enabled": true,
  "triggers": [
    { "type": "cron",         "cron_expr": "0 */6 * * *" },
    { "type": "webhook",      "webhook_token": "wh_abc123", "webhook_method": "POST" },
    { "type": "file_arrival", "watch_dir": "/var/dfo/incoming", "file_pattern": "users_*.csv" },
    { "type": "manual" }
  ],
  "steps": [...]
}
```

If `triggers[]` is omitted but the legacy `cron` field is set, DFO
synthesises a single `TRIGGER_CRON` entry — pipelines created before this
feature continue to work unchanged.

## Webhook

The webhook URL is `POST /api/triggers/<webhook_token>`. The token IS the
auth — anyone holding it can fire the pipeline, so treat it like a
secret. The endpoint is public (skips JWT/RBAC).

```sh
curl -X POST http://localhost:8080/api/triggers/wh_abc123 \
  -H 'Content-Type: application/json' \
  -d '{"source":"github_action","run":42}'
# → 202 Accepted
# → {"status":"triggered","pipeline_id":"...","pipeline_name":"users_etl"}
```

Status codes:

| Code | When                                                       |
|------|------------------------------------------------------------|
| 202  | Pipeline triggered                                         |
| 400  | Missing token                                              |
| 404  | No pipeline has a trigger with that token (or it's disabled)|
| 405  | Configured `webhook_method` doesn't match request method   |
| 409  | Pipeline is already running                                |

Body content is currently NOT forwarded into the pipeline's environment —
this is a future enhancement. The webhook is a notification, not data.

## File arrival (inotify)

```json
{ "type": "file_arrival",
  "watch_dir": "/var/dfo/incoming",
  "file_pattern": "*.csv" }
```

The watcher listens for `IN_CLOSE_WRITE` and `IN_MOVED_TO` events — that
catches both fresh writes and atomic rename-into-place uploads (typical
of `aws s3 sync`, `rsync`, etc).

`file_pattern` is a glob (`fnmatch(3)`):

| Pattern         | Matches                                |
|-----------------|----------------------------------------|
| `*.csv`         | any `.csv` file                        |
| `users_*.csv`   | `users_2024.csv`, `users_q1.csv`, …    |
| `event_???.json`| `event_001.json`, `event_abc.json`, …  |
| `*`             | any file (default)                     |

**Linux-only.** macOS / *BSD builds log a warning and skip — no firing.
A future iteration could add `kqueue` support; for now, run the gateway
on Linux for file_arrival triggers.

Watches are registered once at startup. Adding a new file_arrival
pipeline requires a gateway restart for the watch to take effect.
**Limit ≤ 64 file watches per process.**

## Manual

Use the `manual` trigger type to indicate a pipeline that should never
auto-fire. Equivalent to setting `enabled: false` plus `triggers: []`,
but more explicit in JSON inspections.

## Example: Slack `/etl` slash-command

1. Create pipeline:

   ```json
   {
     "name": "manual_etl",
     "triggers": [{"type":"webhook","webhook_token":"slack-secret-xyz"}],
     "steps": [...]
   }
   ```

2. Configure Slack slash-command pointing to
   `https://dfo.example.com/api/triggers/slack-secret-xyz`.
3. Typing `/etl` in any channel runs the pipeline.

## Example: GitHub Actions on-merge

```yaml
# .github/workflows/dfo-trigger.yml
on: { push: { branches: [main] } }
jobs:
  trigger:
    runs-on: ubuntu-latest
    steps:
      - run: |
          curl -X POST https://dfo.example.com/api/triggers/${{ secrets.DFO_HOOK }} \
            -d "{\"sha\":\"${{ github.sha }}\"}"
```

## Operational notes

- Webhook tokens are stored in cleartext inside `catalog.db`. Anyone with
  read access to the catalog can fire the pipelines.
- Token rotation: edit the pipeline JSON (PATCH/PUT not yet implemented;
  delete + re-create works) and update the webhook caller.
- File watcher lifecycle is tied to gateway lifetime — no per-pipeline
  start/stop yet.
- The audit log records webhook fires as `pipeline_run_started` events
  via the WebSocket broadcast; persistent audit-table integration is
  pending.
