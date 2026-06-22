"""
Client to DataFlow OS — two transports, both returning list[dict]:

  dfo_query(sql, params)        — REST /api/query/named (named params, SELECT-only)
  dfo_pgwire_query(sql, params) — PgWire (psycopg2), for DDL / transactions

DFO stores missing values as the empty string "" rather than SQL NULL, so the
domain SQL below filters with `(col IS NULL OR col = '')` instead of plain
`col IS NULL` — otherwise the deleted/expired filters would never match.
"""
import json
import urllib.request
import urllib.error

import config


def dfo_query(sql: str, params: dict = None) -> list:
    """Execute SQL via DFO REST /api/query/named, return list of row dicts."""
    payload = json.dumps({"sql": sql, "params": params or {}}).encode()
    headers = {"Content-Type": "application/json"}
    if config.DFO_TOKEN:
        headers["Authorization"] = f"Bearer {config.DFO_TOKEN}"
    req = urllib.request.Request(
        f"{config.DFO_REST_URL}/api/query/named", data=payload, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=config.DFO_TIMEOUT_SEC) as r:
            data = json.load(r)
            cols = data.get("columns", [])
            return [dict(zip(cols, row)) for row in data.get("rows", [])]
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"DFO query failed: {e.code} {e.read().decode()}")


def dfo_pgwire_query(sql: str, params: tuple = None) -> list:
    """Execute SQL via the DFO PgWire protocol (psycopg2). Used for DDL."""
    import psycopg2  # imported lazily so the REST path works without psycopg2
    with psycopg2.connect(config.DFO_PGWIRE_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            return []


# Tables the proxy reads from. Created idempotently at startup; the cdi_* tables
# are normally produced by DFO pipelines (steps 1–3) but are created here too so
# the proxy starts cleanly against an empty DFO.
VIEWS_SQL = [
    """
    CREATE TABLE IF NOT EXISTS agreement2party (
        crm_id TEXT, agreement_id TEXT, abs_cd TEXT, owner_id TEXT,
        created_ts TEXT, expired_ts TEXT, scopes_ls TEXT,
        agreement_type_cd TEXT, agreement_file_link TEXT,
        map__deleted TEXT, deleted_ts TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS crm_key_map (
        con_id TEXT, ext_cust_id TEXT, system_num TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS master_person (
        party_id TEXT, mdm_id TEXT, crm_id TEXT,
        last_name TEXT, first_name TEXT, middle_name TEXT,
        birth_date TEXT, inn TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cdi_physical_party (
        hid TEXT, last_name TEXT, first_name TEXT, source_system TEXT, raw_id TEXT
    )
    """,
]


def ensure_tables():
    """Create the proxy's read tables in DFO if absent (idempotent, best-effort).

    Uses PgWire because the REST API is SELECT-only. Failures (e.g. PgWire not
    reachable, or DDL unsupported) are logged, not fatal — the tables may also
    be created by DFO pipelines or CSV ingest."""
    for sql in VIEWS_SQL:
        try:
            dfo_pgwire_query(sql.strip())
        except Exception as e:  # noqa: BLE001 — best-effort startup hook
            print(f"ensure_tables: {e}")
