# NAPASTAK — API Reference

Base URL: `http://localhost:8080`

| Method | Path | Description |
|--------|------|-------------|
| GET    | /health | Status + metrics |
| GET    | /api/tables | List tables |
| GET    | /api/tables/:name/schema | Table schema |
| POST   | /api/tables/query | Execute SQL |
| POST   | /api/ingest/csv?table=X | Upload CSV |
| GET    | /api/pipelines | List pipelines |
| POST   | /api/pipelines | Create pipeline |
| GET    | /api/pipelines/:id | Get pipeline |
| POST   | /api/pipelines/:id/run | Trigger run |
| DELETE | /api/pipelines/:id | Delete pipeline |
| GET    | /api/pipelines/:id/runs | Run history |
| GET    | /api/metrics | Metrics snapshot |
| WS     | /ws | Live event stream |

WebSocket events: `table_updated`, `pipeline_created`, `pipeline_triggered`,
`pipeline_run_started`.
