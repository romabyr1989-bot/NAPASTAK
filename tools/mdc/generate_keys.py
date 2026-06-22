#!/usr/bin/env python3
"""
MDC RESCORE stage (part 2): generate match keys per person.
Replicates applicated_templates.keys_generator['person'] — FIO+BD, DUL, INN.

  input : df  — person_with_docs rows
  output: df  — (source_id, system_name, rule_value)
"""
import pandas as pd


def make_key(ordinal: str, key_type: str, value: str) -> str:
    return f"{ordinal}::{key_type}::{value}"


def generate(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if frame is not None and not frame.empty:
        for _, row in frame.iterrows():
            sid = row.get("source_id", "")
            sysn = row.get("system_name", "")
            fn = row.get("cio_first_name", "")
            mn = row.get("cio_middle_name", "")
            bd = row.get("birth_date", "")
            if fn and mn and bd:
                rows.append({"source_id": sid, "system_name": sysn,
                             "rule_value": make_key("01", "FIO+BD", f"{bd}:{fn}:{mn}")})
            for dul in str(row.get("mat_id_doc_set", "")).split("~"):
                if dul:
                    rows.append({"source_id": sid, "system_name": sysn,
                                 "rule_value": make_key("02", "DUL", dul)})
            inn = row.get("cio_inn", "")
            if inn:
                rows.append({"source_id": sid, "system_name": sysn,
                             "rule_value": make_key("03", "INN", inn)})
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["source_id", "system_name", "rule_value"])


if "df" in globals():
    df = generate(df)  # noqa: F821
