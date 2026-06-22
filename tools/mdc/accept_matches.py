#!/usr/bin/env python3
"""
MDC ACCEPT stage: assign a master_id to each matched source_id.
Replicates SourceEntity.accept().

  input : df  — person_matches rows (id_a, id_b, rule_name, metric_value)
  output: df  — (source_id, master_id) mapping, written to a map table

Each connected pair shares one master_id — the lexicographically smaller
source_id is canonical (deterministic). The MERGE step JOINs this map onto
mdc_person_clean. (A python_file step can't UPDATE DFO via a callback — the
gateway is blocked running it — so ACCEPT emits a mapping table instead.)
"""
import sys

import pandas as pd


def log(msg):
    print(f"[accept] {msg}", file=sys.stderr)


def accept(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        log("no matches to accept")
        return pd.DataFrame(columns=["source_id", "master_id"])

    # union-find-lite: each pair collapses to min(id_a, id_b)
    master_of = {}
    for _, row in frame.iterrows():
        a, b = str(row.get("id_a", "")), str(row.get("id_b", ""))
        if a and b:
            m = min(a, b)
            master_of[a] = min(master_of.get(a, m), m)
            master_of[b] = min(master_of.get(b, m), m)

    rows = [{"source_id": sid, "master_id": mid} for sid, mid in master_of.items()]
    log(f"assigned master_id for {len(rows)} records")
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["source_id", "master_id"])


if "df" in globals():
    df = accept(df)  # noqa: F821
