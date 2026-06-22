#!/usr/bin/env python3
"""
Rescore BKI agreement aggregates (expired window).
Replicates AgreementConnector.rescore_aggregation(method='BKI').

  input : df  — ignored
  output: df  — empty (the rescore is applied via DFO UPDATE)

Persists `on_time` (the processing watermark) in DFO_CONTEXT_DIR so the next run
only rescans the new window.
"""
import os
import json
import datetime
import urllib.request

import pandas as pd

DFO_URL   = os.getenv("DFO_REST_URL", "http://localhost:8080")
DFO_TOKEN = os.getenv("DFO_TOKEN",    "")
HUB_TABLE = os.getenv("HUB_TABLE",    "agreements_agreement2party")
CTX_DIR   = os.getenv("DFO_CONTEXT_DIR", "/tmp")

_on_time_file = os.path.join(CTX_DIR, "rescore_on_time.txt")
try:
    with open(_on_time_file) as f:
        on_time = f.read().strip()
except FileNotFoundError:
    on_time = (datetime.datetime.now() - datetime.timedelta(hours=24)).isoformat()


def dfo_exec(sql: str) -> bool:
    req = urllib.request.Request(
        f"{DFO_URL}/api/tables/query", data=json.dumps({"sql": sql}).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {DFO_TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=60):
            return True
    except Exception as e:  # noqa: BLE001
        print(f"[rescore] error: {e}")
        return False


# Close the BKI window: align expired_ts to the latest per crm_id.
rescore_sql = (
    f"UPDATE {HUB_TABLE} "
    f"SET expired_ts = ("
    f"  SELECT MAX(a2.expired_ts) FROM {HUB_TABLE} a2 "
    f"  WHERE a2.crm_id = {HUB_TABLE}.crm_id AND a2.agreement_type_cd = 'BKI' "
    f"    AND (a2.map__deleted IS NULL OR a2.map__deleted = '')) "
    f"WHERE agreement_type_cd = 'BKI' "
    f"  AND (map__deleted IS NULL OR map__deleted = '') "
    f"  AND last_upd_ts >= '{on_time}'")

if dfo_exec(rescore_sql):
    print(f"[rescore] BKI aggregation done for window >= {on_time}")
    try:
        with open(_on_time_file, "w") as f:
            f.write(datetime.datetime.now().isoformat())
    except OSError as e:
        print(f"[rescore] could not persist on_time: {e}")

if "df" in globals():
    df = pd.DataFrame()  # noqa: F821
