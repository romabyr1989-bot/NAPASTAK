# Agreement Connector (agr2party)

Links consents to master clients. Analog of `tsmdm_agr2party/agreement_connector.py`.
The pipeline `pipelines/agr2party.yaml` runs the connector as a `python_file` step.

## Flow

```
consents_agreements ─┐
                     ├─(transform_sql LEFT JOIN owner attrs)→ df → connector.py → agreements_agreement2party
cdi_physical_party  ─┘                                              │
                                                    CDI proxy (HTTP) ┘  → crm_id / mdm_id (UNDEFINED if down)
```

`build_mapping(df)`:
1. maps `agreement_type_cd` (numeric code → name; `3` → `BKI`),
2. looks up the master id per `party_id` via the **external** CDI proxy
   (`MASTER_API_URL`); an unreachable proxy yields `crm_id = 'UNDEFINED'`,
3. emits `verified_by` flags (pipe-separated) and `map__created/lastupd/deleted`.

## Critical DFO execution-model constraints

These shaped the design (all verified empirically against the engine):

- **A python_file step cannot call back into the DFO HTTP API.** The gateway
  thread is blocked running the step, so an in-step `dfo_query`/UPDATE
  dead-locks (times out). Therefore owner attributes come from the
  `transform_sql` JOIN, not a callback; only the external CDI proxy is called.
- **CSV ingest splits on every comma and collapses empty cells** (shifting
  columns). The connector makes every output cell comma-free and non-empty
  (a `" "` sentinel) and `verified_by` is `key=val|key=val`, never JSON.
- **Flat table namespace** → underscore names (`agreements_agreement2party`).
- **`MAX()` over an empty table returns no row**, so the delta is a full scan
  (the connector dedups by `agreement_id` and rewrites the target).

## resync.py / rescore.py — standalone tools

These issue DFO `UPDATE`s, which a python_file step cannot do (the callback
dead-locks). They run as **standalone scripts** (cron / CLI) when the gateway is
**not** busy executing a step:

- `resync.py` — remap updated CRM keys (`crm_key_updates.old_id → new_id`).
- `rescore.py` — recompute BKI aggregate `expired_ts`; persists the processing
  watermark in `DFO_CONTEXT_DIR`.

Both read `DFO_REST_URL` / `DFO_TOKEN` from the environment.

## Tables

| Table                        | Role                                  |
|------------------------------|---------------------------------------|
| `consents_agreements`        | raw consents (source)                 |
| `cdi_physical_party`         | owner attributes (joined in)          |
| `agreements_agreement2party` | the linkage hub (output)              |
| `crm_key_updates`            | CRM key remaps (for resync)           |
