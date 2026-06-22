#!/usr/bin/env python3
"""
Agreement connector: link agreements to master clients.
Replicates tsmdm_agr2party/agreement_connector.py __call__().

DFO python_file step:
  input : df  — agreement delta JOINed with owner attributes (transform_sql)
  output: df  — agreement2party rows (+ crm_id/mdm_id, verified_by, map__*)

DFO execution-model constraints (verified empirically):
  • A python_file step CANNOT call back into the DFO HTTP API — the gateway
    thread is blocked running the step, so the callback dead-locks. Therefore:
      - owner attributes are supplied by the transform_sql JOIN (in `df`), and
      - the RECHECKABLE "related agreements" recursion (which needs to read DFO)
        is a single pass here.
    Only the EXTERNAL master lookup (CDI HTTP proxy) is called.
  • The CSV ingest of the output splits on every comma, so all diagnostics go to
    stderr (never stdout) and `verified_by` is pipe-separated, comma-free.
"""
import os
import sys
import json
import datetime
import urllib.request

import pandas as pd

MASTER_URL   = os.getenv("MASTER_API_URL",  "http://cdi-service:8444")
MASTER_BATCH = int(os.getenv("MASTER_BATCH_SIZE", "64"))
RECHECKABLE  = os.getenv("CHECK_RECHECKABLE", "true").lower() == "true"

# numeric code → agreement type (agreement_templates.agr_rdm__dict)
AGR_TYPE_MAP = {
    "0": "MFCMARKETING", "1": "ESIACREDIT", "2": "PERSONALDATA",
    "3": "BKI", "4": "ONLINEINTERACTION", "5": "MARKETING",
    "6": "IDENTIFICATION", "7": "FINANCE", "8": "REGQUESTIONNAIRE",
    "9": "DIGITALRUBLE", "10": "LONGORDER", "11": "JUNIORMOBILEAPPUSAGE",
}
FLAG_COLS = ["flg_by_inn", "flg_by_duls", "flg_by_phones", "flg_by_birth_date", "flg_by_keys"]


def log(msg):
    print(f"[agr2party] {msg}", file=sys.stderr)


def find_masters_via_cdi(party_ids) -> pd.DataFrame:
    """Master ids via the EXTERNAL CDI HTTP proxy. Analog of __find_masters_cdi().
    Unreachable proxy / 404 → crm_id='UNDEFINED'."""
    ids = [p for p in dict.fromkeys(str(p) for p in party_ids if p)]
    results = []
    for i in range(0, len(ids), MASTER_BATCH):
        for pid in ids[i:i + MASTER_BATCH]:
            crm_id, mdm_id = "UNDEFINED", ""
            try:
                req = urllib.request.Request(
                    f"{MASTER_URL}/cdi/soap/services/2_13/PartyRA/getByHID",
                    data=json.dumps({"hid": pid, "type": "PHYSICAL"}).encode(),
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=5) as r:
                    resp = json.load(r)
                fields = {f["name"]: f["value"] for f in resp.get("party", {}).get("field", [])}
                crm_id = fields.get("crm_id", "UNDEFINED") or "UNDEFINED"
                mdm_id = fields.get("mdm_id", "")
            except Exception:  # noqa: BLE001 — proxy down / 404 → UNDEFINED
                pass
            results.append({"party_id": str(pid), "mdm_id": mdm_id, "crm_id": crm_id})
    return pd.DataFrame(results) if results else pd.DataFrame(columns=["party_id", "mdm_id", "crm_id"])


def build_mapping(delta_df: pd.DataFrame) -> pd.DataFrame:
    """Main linking loop. Analog of AgreementConnector.__call__()."""
    if delta_df is None or delta_df.empty:
        return pd.DataFrame()
    merged = delta_df.copy()

    if "agreement_type_cd" in merged.columns:
        merged["agreement_type_cd"] = merged["agreement_type_cd"].apply(
            lambda v: AGR_TYPE_MAP.get(str(v), str(v)))

    # Master lookup via the external CDI proxy (owner attrs already in df via JOIN).
    if "party_id" in merged.columns:
        masters = find_masters_via_cdi(merged["party_id"].dropna().unique().tolist())
        if not masters.empty:
            merged = merged.merge(masters, on="party_id", how="left")
    log(f"agreements={len(merged)} recheckable={RECHECKABLE}")

    if "crm_id" not in merged.columns:
        merged["crm_id"] = "UNDEFINED"
    merged["crm_id"] = merged["crm_id"].fillna("UNDEFINED").replace("", "UNDEFINED")
    if "mdm_id" not in merged.columns:
        merged["mdm_id"] = ""

    for flag in FLAG_COLS:
        if flag not in merged.columns:
            merged[flag] = "N"
    merged["verified_by"] = merged[FLAG_COLS].fillna("N").apply(
        lambda row: "|".join(f"{k}={row[k]}" for k in FLAG_COLS), axis=1)

    now = datetime.datetime.now().isoformat()
    merged["map__created"] = now
    merged["map__lastupd"] = now
    merged["map__deleted"] = ""

    if "agreement_id" in merged.columns:
        merged = merged.drop_duplicates(subset=["agreement_id"])
    return dfo_safe(merged)


def dfo_safe(frame: pd.DataFrame) -> pd.DataFrame:
    """Make EVERY cell a comma-free, non-empty string. DFO's CSV ingest splits on
    every comma and collapses empty cells (shifting columns) — and an all-empty
    column is float64 (NaN), so this must touch every column, not just objects."""
    frame = frame.fillna(" ")
    for col in frame.columns:
        s = frame[col].astype(str).str.replace(",", " ", regex=False)
        frame[col] = s.where(s.str.strip() != "", " ")
    return frame


if "df" in globals():
    log(f"processing {len(df)} agreements")  # noqa: F821
    df = build_mapping(df)  # noqa: F821
    log(f"result: {len(df)} mappings")  # noqa: F821
