# MDM end-to-end: closure & bottlenecks (measure-only)

Status report for the MDM chain `source → normalize → match → SCD2 → REST query`.
This step **measures and documents** — it changes no engine behaviour. Every
bottleneck below is an optimisation of an **existing operator**, not a new
subsystem.

Reproduce:
- E2E correctness: `tests/integration/test_mdm_e2e.sh` (11/11 PASS)
- Load: `make BUILD=release build/release/bin/bench_bench_scd2 && \
  build/release/bin/bench_bench_scd2 build/release/bin/dfo_gateway 19083`

## 1. What works end-to-end

`tests/integration/test_mdm_e2e.sh` drives a 3-stage pipeline twice (with a delta
between runs) and asserts via the REST query API:

- **normalize (SQL)** — `trim`/`lower` applied; `clean_customers` correct. ✓
- **match (SQL)** — the stage executes and materialises `customer_matches`
  (6 candidate pairs). ✓ *runs*, but see gap #1 — similarity is empty.
- **SCD2** — full type-2 versioning across the delta: ✓
  - changed key → old version closed (`valid_to` set) + new open version
  - soft-deleted key → closed, no new version
  - new key → opened; unchanged key → untouched
  - golden state = exactly one open row per business key (4 open, 2 closed, 6 total)
- **REST query** — every assertion reads back through `/api/tables/query`. ✓

The chain is **wired end-to-end and runs without errors**. The only functional
hole is the match *similarity* (gap #1).

The incremental **cursor source** is proven separately at the connector level in
`tests/integration/test_pg_cdc.sh` (two reads, second returns only new rows); see
gap #5 for why it isn't threaded across pipeline *runs* yet.

## 2. Load numbers (Apple/clang, release build)

SCD2 first run, fresh target each size (peak RSS sampled at 5 ms):

| rows    | time   | peak ΔRSS |
|---------|--------|-----------|
| 25 000  | 0.055s | ~11 MB    |
| 50 000  | 0.114s | ~19 MB    |
| 100 000 | 0.295s | ~66 MB    |

SCD2 **delta re-run**, 10k rows all-changed (exercises the classify loop):

| rows   | time   | peak ΔRSS |
|--------|--------|-----------|
| 10 000 | 2.21s  | **~850 MB** |

`op_window` (`ROW_NUMBER() OVER`, one partition, blocking materialisation):

| rows    | time   | peak ΔRSS |
|---------|--------|-----------|
| 25 000  | 0.017s | ~7 MB     |
| 50 000  | 0.037s | ~15 MB    |
| 100 000 | 0.071s | ~34 MB    |

`op_window` peak memory is **linear in partition size** (~0.34 KB/row, doubles as
rows double) — confirming the blocking operator holds the whole partition.

## 3. Bottlenecks (all = optimise existing operators)

### Gap #1 — match scalars were in the wrong evaluator — FIXED
`jaro_winkler` / `levenshtein` were first added to `lib/qengine/qengine.c`
(`eval_expr`), but `/api/tables/query` and pipeline SQL run through a **separate**
evaluator — `eval_func` in `src/gateway/api.c`. So `SELECT jaro_winkler(a,b)`
returned `""` and the match stage was a no-op.
**Fixed (additive):** `api_levenshtein` / `api_jaro_winkler` helpers + two
`strcasecmp` branches added to `api.c`'s `eval_func`. Verified through the gateway
(`jaro_winkler('martha','marhta')=0.9611`) and asserted in `test_mdm_e2e.sh`
(C1 'alice' ~ C4 'alicia' = 0.893). The match stage now emits real similarity.

### Gap #2 — op_window is blocking, O(partition) memory
`apply_windows` (api.c) materialises the full result set plus per-row window-value
arrays before emitting. SCD2's current-version pick (`ROW_NUMBER() OVER (PARTITION
BY business_key ORDER BY transaction_time DESC)`) therefore holds the entire target
partition in memory; peak RSS grows linearly with partition volume (measured above).
**Future opt (existing operator):** stream window evaluation partition-by-partition
instead of materialising the whole input — same operator, bounded memory.

### Gap #3 — SCD2 delta classify is O(source × current) + arena churn
`run_scd2_step` linear-scans the current-version set for **every** source row and
calls `scd2_build_key` (arena alloc) inside the inner loop. A 10k all-changed delta
→ 2.2s and ~850 MB (vs a 100k first run at 0.3s / 66 MB). This dominates re-runs.
**Future opt (existing operator):** hash the current-version keys once (O(source +
current)) and stop re-allocating keys per comparison. No new machinery.

### Gap #4 — SCD2 full-rebuild + temp tables + hard row cap
SCD2 writes through `write_rs_to_table`, which **replaces** the whole target each
run (it can't INSERT — the engine has no INSERT). So every run re-writes the entire
history: O(history) writes regardless of delta size. Also: `__scd2_src_<id>` is left
materialised after the run, and `MAX_RS_ROWS = 100000` caps any result set — SCD2
history cannot exceed 100k rows.
**Future opt (existing path):** partition-wise / streaming rebuild and temp-table
cleanup; raise or stream past the result-set cap. Still the same write path.

### Gap #5 — connector cursor not persisted across pipeline runs
`run_connector_step` drives `DfoReadReq.cursor` as a **row-count offset** and ignores
the high-watermark the connector writes back. Incremental works **within** one read
loop but resets every run, so a pipeline always re-reads from the start.
**Future opt (existing path):** persist the connector's returned cursor in the catalog
between runs. No new subsystem.

### Gap #6 — connector_type ↔ .so name mismatch — FIXED
`run_connector_step` loaded `<connector_type>_connector.so`; `connector_type:
postgresql` (UI/YAML) looked for `postgresql_connector.so`, but the plugin ships as
`pg_connector.so`. **Fixed:** alias `postgresql`/`postgres` → `pg` in
`run_connector_step`. Fixing this exposed two further pre-existing crashes (now
also fixed):
  - **plugin logging crash:** a plugin .so links its own copy of `log.c`, so its
    `g_log` is zero-initialised (`out == NULL`) when dlopen'd; the first `LOG_*`
    inside `pg_create` did `fprintf(NULL)` → SEGV. Fixed defensively in
    `lib/core/log.c` (`log_write` returns early when `l->out == NULL`).
  - **preview-step use-after-free:** the `RUN_FAILED` branch of
    `h_pipeline_preview_step` pointed the zero-copy `resp->body` into an arena and
    then `arena_destroy`'d it before the HTTP layer sent it. Fixed by copying the
    error body to a thread-local buffer first (matches the success path).
Net: a PostgreSQL source step now loads and runs; a failed connection returns a
clean response instead of crashing the gateway. **Kafka** is still unavailable —
`librdkafka` is not installed, so `kafka_connector.so` isn't built.

## 4. Summary

The MDM chain **closes end-to-end** today: normalize → match (real
jaro_winkler similarity) → SCD2 → REST, with correct type-2 versioning across
deltas. Remaining items are all **scaling** improvements to existing operators:
- gap #1 (match scalars) is **FIXED** (wired into `api.c` eval_func); and
- a set of **scaling** improvements to existing operators: stream `op_window` by
  partition (#2), hash the SCD2 classify join (#3), partition-wise SCD2 rebuild +
  cap (#4), persist the connector cursor (#5).

None require a new subsystem or a second write path — they tighten the operators
and wiring that already exist.
