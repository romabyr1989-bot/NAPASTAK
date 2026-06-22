#!/usr/bin/env python3
"""
Resync updated master (CRM) keys onto agreement2party.
Replicates AgreementConnector.resync_masters() / sync_master_ids().

  input : df  — rows (old_id, new_id) from crm_key_updates
  output: df  — empty (the update is applied via DFO UPDATE)

DFO supports UPDATE (exec_stmt), so the remap is a single UPDATE per key.
"""
import os
import json
import datetime
import urllib.request

import pandas as pd

DFO_URL   = os.getenv("DFO_REST_URL", "http://localhost:8080")
DFO_TOKEN = os.getenv("DFO_TOKEN",    "")
HUB_TABLE = os.getenv("HUB_TABLE",    "agreements_agreement2party")


def _lit(s):
    return "'" + str(s).replace("'", "''") + "'"


def dfo_exec(sql: str) -> bool:
    req = urllib.request.Request(
        f"{DFO_URL}/api/tables/query", data=json.dumps({"sql": sql}).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {DFO_TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=30):
            return True
    except Exception as e:  # noqa: BLE001
        print(f"[resync] update error: {e}")
        return False


def run(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        print("[resync] no key updates")
        return pd.DataFrame()
    now = datetime.datetime.now().isoformat()
    updated = 0
    for _, row in frame.iterrows():
        old_id, new_id = str(row.get("old_id", "")), str(row.get("new_id", ""))
        if not old_id or not new_id:
            continue
        sql = (f"UPDATE {HUB_TABLE} SET crm_id = {_lit(new_id)}, map__lastupd = {_lit(now)} "
               f"WHERE crm_id = {_lit(old_id)} AND (map__deleted IS NULL OR map__deleted = '')")
        if dfo_exec(sql):
            updated += 1
    print(f"[resync] updated {updated} master key mappings")
    return pd.DataFrame()


if "df" in globals():
    df = run(df)  # noqa: F821
