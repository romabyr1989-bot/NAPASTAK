# Master Assembly Pipeline

`pipelines/master_assembly.yaml` builds "golden records" from CDI entities into
master tables, historised with SCD2. Analog of `tsmdm_as_mdm/masters_declararion.py`.

## Logic

For each attribute group (INN, FIO, birth), pick the **latest source row per
`master_id`**, project that group's columns, then merge the groups and historise
the result with an SCD2 step (`scd2_business_key: master_id`, `scd2_hash_col`).

## Why NOT EXISTS instead of ROW_NUMBER() / MAX()

The natural `masters_declararion` SQL —
`ROW_NUMBER() OVER (PARTITION BY master_id ORDER BY existing ASC, last_upd DESC)`
— is **not reliable on the DFO query engine** (verified empirically):

- `ROW_NUMBER() OVER (PARTITION BY …)` numbers multi-row partitions **globally**
  (a single-row partition can get `rn = 3`), so `WHERE rn = 1` silently drops
  master_ids. (SCD2's own current-version lookup tolerates this via an in-memory
  fallback; a plain query does not.)
- `MAX()` is **numeric-only** → `MAX(last_upd)` over ISO-date *text* returns `0`.
- A multi-key window `ORDER BY` compounds the partition bug.

The portable, reliable pattern used here is a correlated **NOT EXISTS** ("there
is no later row for this master_id"):

```sql
SELECT p.master_id, p.src_inn AS inn
FROM cdi_physical_party p
WHERE p.master_id != ''
  AND NOT EXISTS (
    SELECT 1 FROM cdi_physical_party p2
    WHERE p2.master_id = p.master_id AND p2.last_upd > p.last_upd
  )
```

This requires `last_upd` to be an **ISO-8601 string** (`YYYY-MM-DD…`) so the
lexical `>` orders chronologically. The selection is therefore "latest record per
master_id" rather than `masters_declararion`'s "non-empty-first then latest"
(the engine can't express the multi-key priority reliably).

> If two rows share the max `last_upd` (a tie), both are returned — a duplicate
> `master_id`. Ensure `last_upd` is unique-enough, or add a deterministic
> tiebreaker upstream.

## Output tables

| Table           | Description                       |
|-----------------|-----------------------------------|
| `master_person` | golden person records (SCD2)      |
| `master_party`  | golden legal-entity records (SCD2)|

Intermediate per-group tables: `master_person_inn`, `master_person_fio`,
`master_person_birth` (joined by `merge_master_person`).

## DFO table-name note

Table names are `[A-Za-z0-9_]` only (no dots) — the `cdi`/`master` "schemas" are
name prefixes, e.g. `cdi_physical_party`, `master_person`.
