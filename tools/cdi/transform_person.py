#!/usr/bin/env python3
"""
CDI transform: SBL S_CONTACT → physical_party (DFO python_file step).
Replicates Collector.map_entity_from_source() attribute mappings.

  input : df  — source rows from transform_sql
  output: df  — physical_party rows
"""
import pandas as pd

# SBL S_CONTACT → CDI physical_party (collector attribute_source2target_mappings)
COLUMN_MAP = {
    "ROW_ID":     "raw_id",
    "LAST_NAME":  "src_last_name",
    "FST_NAME":   "src_first_name",
    "MID_NAME":   "src_middle_name",
    "BIRTH_DT":   "birth_date",
    "BU_ID":      "src_inn_org",
    "LAST_UPD":   "last_upd",
    "AMND_STATE": "amnd_state",
}
SOURCE_SYSTEM = "sbl"


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Map source columns to the CDI physical_party schema."""
    # keep only active records when the flag is present
    if "AMND_STATE" in df.columns:
        df = df[df["AMND_STATE"] == "A"].copy()

    available = {s: t for s, t in COLUMN_MAP.items() if s in df.columns}
    result = df[list(available.keys())].rename(columns=available)

    result["source_system"] = SOURCE_SYSTEM
    result["hid"] = (result["raw_id"] if "raw_id" in result.columns
                     else pd.Series(range(len(result)))).astype(str)
    result["party_type"] = "PHYSICAL"

    # normalise name fields (analog of mdc_clean.prettify)
    for col in ("src_last_name", "src_first_name", "src_middle_name"):
        if col in result.columns:
            result[col] = result[col].fillna("").astype(str).str.strip().str.lower()

    return result


if "df" in globals():
    df = transform(df)  # noqa: F821
