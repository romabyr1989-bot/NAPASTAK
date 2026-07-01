# NAPASTAK — Architecture

## Process model

Single `dfo_gateway` binary. Subsystems: epoll HTTP server, scheduler thread,
worker thread pool (N=4), WebSocket broadcast. All thread-safe.

## Memory model

Arena-per-request. Each HTTP request allocates `Arena*` on entry, destroys on
response send. Zero malloc/free inside handlers.

## Storage layout

```
data/
  catalog.db           ← SQLite (table schemas, pipelines, run history)
  {table_name}/
    wal.bin            ← append-only TLV [uint32 len][CSV row bytes]
```

## Architecture diagram

```
Browser (index.html / style.css / app.js)
   │  HTTP REST + WebSocket
   ▼
dfo_gateway
   ├── epoll HTTP/1.1 server + WS upgrade
   ├── Router (pattern matching with :params)
   ├── REST API (12 endpoints)
   ├── ThreadPool (4 workers)
   │
   ├── Catalog ──── SQLite
   ├── Scheduler ── cron_next + DAG topo sort
   ├── Metrics ──── lock-free MetricRing (3600 samples)
   │
   ├── Storage ─────── WAL + ColBatch (BATCH_SIZE=8192)
   │
   ├── SQL Parser ──── lexer → recursive descent → AST → logical plan
   │
   ├── QEngine ──────── volcano model operators
   │   SCAN → FILTER → PROJECT → SORT → LIMIT
   │
   └── Connector ABI ── dlopen .so plugins
       ├── csv_connector.so
       └── (pg, http_rest — roadmap)
```

## SQL subset

SELECT DISTINCT, FROM, JOIN (INNER/LEFT/RIGHT/FULL), WHERE, GROUP BY,
HAVING, ORDER BY ASC/DESC, LIMIT, OFFSET, IS NULL, IS NOT NULL, BETWEEN,
IN, LIKE, all arithmetic and comparison operators, scalar functions.

## Dependencies

| Library    | Purpose                |
|------------|------------------------|
| libsqlite3 | Catalog persistence    |
| libpthread | Threading              |
| libm       | Math in observ/eval    |
| libdl      | Plugin (dlopen)        |
