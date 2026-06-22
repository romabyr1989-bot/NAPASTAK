#!/usr/bin/env python3
"""
CDI transform: SBL S_DOC_AGREE → id_doc (DFO python_file step).

  input : df  — source rows from transform_sql
  output: df  — id_doc rows
"""
import pandas as pd

COLUMN_MAP = {
    "ROW_ID":   "raw_id",
    "DOC_NUM":  "src_number",
    "DOC_TYPE": "category",
    "ISSUE_DT": "issue_date",
    "EXPIR_DT": "expire_date",
    "BU_ID":    "person_hid",
    "LAST_UPD": "last_upd",
}
SOURCE_SYSTEM = "sbl"


def transform(df: pd.DataFrame) -> pd.DataFrame:
    available = {s: t for s, t in COLUMN_MAP.items() if s in df.columns}
    result = df[list(available.keys())].rename(columns=available)

    result["source_system"] = SOURCE_SYSTEM
    result["hid"] = (result["raw_id"] if "raw_id" in result.columns
                     else pd.Series(range(len(result)))).astype(str)

    # normalise the document number: keep only letters+digits, upper-cased
    if "src_number" in result.columns:
        result["src_number"] = (result["src_number"].fillna("").astype(str)
                                .str.upper().str.replace(r"[^A-ZА-Я0-9]", "", regex=True))
    return result


if "df" in globals():
    df = transform(df)  # noqa: F821
