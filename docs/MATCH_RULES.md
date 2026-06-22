# Match rules step

Declarative entity resolution via rule chains. Replaces custom Python matching
code (TSMDM `matching_declararion.py`) with a YAML/JSON-configurable pipeline
step. A step becomes a match step when `match_rules` is non-empty.

## Step fields

| Field             | Type        | Required | Description |
|-------------------|-------------|----------|-------------|
| `match_rules`     | JSON string | yes      | Array of rule objects (see below) |
| `transform_sql`   | SQL string  | yes      | Query producing the candidate set |
| `target_table`    | string      | yes      | Receives matched pairs |

The candidate set is materialised once from `transform_sql`; every rule runs
against it in order.

## Rule object

| Field             | Type     | Required | Description |
|-------------------|----------|----------|-------------|
| `rule_name`       | string   | yes      | Label in output and logs |
| `id_column`       | string   | yes      | Primary key column name |
| `scan_columns`    | string[] | yes      | Columns concatenated for **exact-match** grouping |
| `test_columns`    | string[] | yes      | Columns concatenated (space-joined) for similarity |
| `metric_function` | string   | yes      | `word_similarity` · `jaro_winkler` · `levenshtein` |
| `operator`        | string   | yes      | `>=` · `<=` · `>` · `<` |
| `threshold`       | number   | yes      | Metric threshold |
| `normalize`       | string   | no       | `name` · `inn` · `none` (default) — applied to test columns |

A rule:
1. groups candidates by `scan_columns` (exact string match, US-joined);
2. for every unordered pair `(i, j)` within a group computes
   `metric_function(test_i, test_j)`;
3. keeps pairs where `metric  <operator>  threshold`;
4. appends them to the result set.

Rows with an empty scan key are skipped (e.g. a person with no DUL is not
grouped by the DUL rule).

## Output schema

`[id_a TEXT, id_b TEXT, rule_name TEXT, metric_value TEXT]`

All matched pairs across all rules land in `target_table`. Rules do **not**
deduplicate across each other — if pair (A,B) matches rule 1 and rule 2, it
appears twice with different `rule_name` values. `metric_value` is the metric
formatted to 6 decimals.

## Scalar functions available in SQL

These are usable directly in any `transform_sql` (typically to normalise the
candidate set before matching):

| Function                  | Returns | Description |
|---------------------------|---------|-------------|
| `word_similarity(a, b)`   | DOUBLE  | Trigram overlap: `|trigrams(a) ∩ trigrams(b)| / |trigrams(a)|` |
| `jaro_winkler(a, b)`      | DOUBLE  | Jaro-Winkler similarity (prefix-weighted) |
| `levenshtein(a, b)`       | INT     | Edit distance |
| `normalize_name(s)`       | TEXT    | ASCII-lowercase + trim + collapse internal whitespace |
| `normalize_inn(s)`        | TEXT    | Digits only (drops spaces, dashes, letters) |

> `word_similarity(query, document)` is asymmetric and mirrors `pg_trgm`: it
> measures how much of `query` is covered by `document`. If `query` is a
> substring of `document` the score is `1.0`. It is byte-level, so it works on
> UTF-8 (Cyrillic) as long as both sides share the encoding.
>
> `normalize_name` case-folds **ASCII only** — Cyrillic letters pass through
> unchanged (so compare both sides with the same normalisation; case is not an
> issue when both come from the same source column).

## Example pipeline

```yaml
name: mdm_person_match
enabled: true
steps:
  - id: normalize
    transform_sql: |
      SELECT
        party_id,
        normalize_name(last_name)  AS last_name,
        normalize_name(first_name) AS first_name,
        normalize_inn(inn)         AS inn,
        birth_date,
        dul_number
      FROM cdi_persons
    target_table: persons_clean

  - id: match
    transform_sql: "SELECT * FROM persons_clean"
    target_table: person_matches
    deps: [0]
    match_rules: |
      [
        {"rule_name":"1.1 DUL→FIO+BD","id_column":"party_id",
         "scan_columns":["dul_number"],
         "test_columns":["last_name","first_name","birth_date"],
         "metric_function":"word_similarity","operator":">=","threshold":0.80,
         "normalize":"name"},
        {"rule_name":"1.2 INN→FIO+BD","id_column":"party_id",
         "scan_columns":["inn"],
         "test_columns":["last_name","first_name","birth_date"],
         "metric_function":"word_similarity","operator":">=","threshold":0.80,
         "normalize":"name"},
        {"rule_name":"2.1 FIO+BD→DUL","id_column":"party_id",
         "scan_columns":["last_name","birth_date"],
         "test_columns":["dul_number"],
         "metric_function":"jaro_winkler","operator":">=","threshold":0.66}
      ]
```

## Performance

The match step is **O(n²) within each scan group**. For large inputs (>10K rows):

- pre-filter the candidate set in `transform_sql` to a delta window;
- pick high-cardinality `scan_columns` (DUL, INN) so groups stay small — the
  pairwise scan only runs inside same-scan-key groups, not across the whole set.
