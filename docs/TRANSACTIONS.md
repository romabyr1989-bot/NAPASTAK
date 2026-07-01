# Transaction Support

## Overview

NAPASTAK provides lightweight **optimistic transactions** scoped to a single
connection. Transactions are tracked via a thread-local `TxnId`. There is no
MVCC and no cross-table atomicity.

## SQL Syntax

```sql
BEGIN;

INSERT INTO events (ts, val) VALUES (1700000000, 42);
INSERT INTO events (ts, val) VALUES (1700000001, 99);

COMMIT;
```

To abort:

```sql
ROLLBACK;
```

## How It Works

| Operation  | Mechanism                                                         |
|-----------|-------------------------------------------------------------------|
| `BEGIN`    | Allocates a new `TxnId` in thread-local storage                   |
| `INSERT`   | Appended to the WAL immediately; visible to others after `COMMIT` |
| `UPDATE` / `DELETE` | Buffered in `txn_buffer`; applied atomically on `COMMIT` |
| `COMMIT`   | Flushes `txn_buffer`, marks WAL records as committed              |
| `ROLLBACK` | Discards `txn_buffer`, WAL appends are voided                     |

## Limitations

- **No cross-table atomicity** — a `COMMIT` affecting two tables is not guaranteed
  to be atomic if the process crashes mid-way.
- **No snapshot isolation** — reads inside a transaction may see concurrent commits.
- **No MVCC** — there is no versioning of rows.
- `INSERT` WAL entries become visible only after `COMMIT`, not immediately.

## Best Use Cases

- Batched ingestion into a single table.
- Single-table `UPDATE` / `DELETE` operations that must roll back on error.

## Example — Batch Ingest

```bash
curl -X POST /api/query \
  -H "Authorization: Bearer <token>" \
  -d '{"sql":"BEGIN; INSERT INTO metrics VALUES (1,10); INSERT INTO metrics VALUES (2,20); COMMIT;"}'
```
