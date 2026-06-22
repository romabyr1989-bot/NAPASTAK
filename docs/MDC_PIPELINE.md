# MDC Pipeline

`pipelines/mdc_pipeline.yaml` — the full Master Data Cleansing cycle:
**load → clean → upload → rescore → match → accept → merge**. Analog of
`tsmdm_as_mdm/minimal.py` `MDC.__call__()`.

## Stages (one YAML step each)

| Step                     | Kind         | Output                 |
|--------------------------|--------------|------------------------|
| `load_clean_person`      | python_file  | `mdc_person_clean`     |
| `load_clean_id_doc`      | python_file  | `mdc_id_doc_clean`     |
| `aggregate_dul_to_person`| python_file  | `mdc_person_with_docs` |
| `generate_match_keys`    | python_file  | `mdc_match_keys`       |
| `match_person`           | match_rules  | `mdc_person_matches`   |
| `accept_matches`         | python_file  | `mdc_master_map`       |
| `apply_master`           | SQL JOIN     | `mdc_person_mastered`  |
| `merge_master_person`    | SCD2         | `master_person`        |

## Clean rules (python_file)

- `clean_person.py` — `normalize_name` (lower+trim+collapse) on FIO,
  `normalize_inn` (digits only) on INN. `"  ИВАНОВ  "` → `"иванов"`, `"12-34"` → `"1234"`.
- `clean_id_doc.py` — `normalize_dul` (alnum, upper); `"4500 123456"` → `"4500123456"`;
  accepted-category filter.

## Match rules (`matching_declararion.py['person']`)

Five rules in `match_rules` (see [MATCH_RULES.md](MATCH_RULES.md)):

| Rule | scan → test | metric | threshold |
|------|-------------|--------|-----------|
| 1.1  | DUL → FIO+BD | word_similarity | 0.80 |
| 1.2  | INN → FIO+BD | word_similarity | 0.80 |
| 2.1  | FIO+BD → DUL | jaro_winkler | 0.66 |
| 2.2  | FIO+BD → INN | word_similarity | 0.50 |
| 3.1  | INN+KPP → NAME (party) | word_similarity | 1.00 |

## DFO execution-model adaptations

- **ACCEPT does not UPDATE via a callback** (a python_file step can't call the
  DFO API — the gateway is blocked running it). It emits a `(source_id,
  master_id)` map; `apply_master` JOINs it onto the persons.
- **DUL aggregation is a python_file** (`aggregate_docs.py`): DFO's
  `GROUP_CONCAT` returns empty, so the join is SQL and the `'~'`-concat is pandas.
- **MERGE uses a correlated `NOT EXISTS`** ("latest per master_id"), not
  `ROW_NUMBER()`/`MAX()` (unreliable on the DFO window/aggregate engine).
- **Empty cells are written as `" "`** and commas are stripped — DFO's CSV
  ingest collapses empty cells and splits on every comma.
- **Flat namespace** → underscore table names (`mdc_person_clean`, …).

## Unit tests

`python3 tools/mdc/test_clean_unit.py` exercises `clean_person`, `clean_id_doc`,
and `generate_keys` without a running gateway.
