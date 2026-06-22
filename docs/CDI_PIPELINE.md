# CDI / HFL Pipeline

Two YAML pipelines that load data from OLTP/Kafka sources into CDI entities,
plus the Python transform scripts they invoke via `python_file` (see
[PYTHON_STEPS.md](PYTHON_STEPS.md)).

## Pipeline A: `hfl_collector` (`pipelines/hfl_collector.yaml`)

Reads OLTP sources (SBL/Siebel via the Oracle connector, …) into CDI buffer
tables, then transforms them into CDI entities.

- Schedule: cron `0 2 * * *` (daily 02:00), or `POST /api/pipelines/hfl_collector/run`.
- Transform scripts: `tools/cdi/transform_person.py`, `transform_document.py`.
- A connector step reads the source table named in **`transform_sql`** (DFO uses
  `transform_sql` as the entity; `connector_config` holds connection params only).

## Pipeline B: `hfl_joker` (`pipelines/hfl_joker.yaml`)

Reads the CDI Kafka topic (JSON messages) into a buffer table, then transforms
each message by the XML mappings.

- Schedule: cron `*/15 * * * *` (every 15 min).
- Transform: `tools/cdi/joker_transform.py`.
- Mappings: `equalized_ruleset.xml` (RDM rules), `cdi2sbl_renamings.xml`
  (attribute renames). Paths come from `DFO_MAPPINGS_FILE` / `DFO_RENAMINGS_FILE`.
  If the files are absent the transform degrades to a 1:1 lower-cased mapping.

### `joker_transform.py` classification

| CCAT | SERVICE_GROUP | party_type |
|------|---------------|------------|
| `P`  | `None` or `5` | `PHYSICAL` |
| `C`  | any           | `LEGAL`    |
| `P`  | `0`           | `LEGAL`    |
| else | —             | `UNKNOWN`  |

Accepts both `{"ows.CLIENT": [{…}]}` and a flat client dict.

## CDI entities in DFO

DFO has a **flat table namespace** (no schemas), so the TSMDM `cdi` schema is the
`cdi_` table-name prefix:

| DFO table              | Entity     | Source      |
|------------------------|------------|-------------|
| `cdi_physical_party`   | person     | SBL, Way4   |
| `cdi_id_doc`           | document   | SBL         |
| `cdi_phone`            | phone      | SBL, Way4   |
| `cdi_address`          | address    | SBL         |
| `cdi_party`            | legal      | SBL         |
| `cdi_merged`           | dedup link | match step  |

## Mapping configuration

XML mappings are read from `DFO_MAPPINGS_FILE` and `DFO_RENAMINGS_FILE` (set them
via the pipeline env / `python_context_dir`). They are optional — missing files
are logged and the transform falls back to identity (lower-cased) mapping.

## Testing without Kafka

`tests/integration/test_cdi_pipeline.sh` seeds `cdi_physical_party` via CSV (no
real Kafka) and validates the YAML + the downstream master assembly.
