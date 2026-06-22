#!/usr/bin/env python3
"""
MDC CLEAN stage: person entity.
Replicates the CleanRule chain for 'person' (normalize_name on FIO,
normalize_inn on INN). Runs as a DFO python_file step.
"""
import re

import pandas as pd  # noqa: F401  (available in the DFO python wrapper)


def normalize_name(s) -> str:
    """prettify(type_='name'): lower + trim + collapse whitespace.
    Coerces non-str input (pandas may read a numeric column as int/float)."""
    if s is None:
        return ""
    s = str(s)
    if s == "nan":
        return ""
    s = re.sub(r"[\n\t\r]", " ", s)
    return re.sub(r" {2,}", " ", s).strip().lower()


def normalize_inn(s) -> str:
    """prettify(type_='inn'): digits only. Coerces numeric input to str first
    (an all-numeric INN column is read by pandas as int64, not str)."""
    if s is None:
        return ""
    s = str(s)
    if s == "nan":
        return ""
    if s.endswith(".0"):   # float-read integer
        s = s[:-2]
    return re.sub(r"[^\d]", "", s)


if "df" in globals() and not df.empty:  # noqa: F821
    for col in ("src_last_name", "src_first_name", "src_middle_name", "src_fullname"):
        if col in df.columns:  # noqa: F821
            df[col.replace("src_", "cio_")] = df[col].apply(normalize_name)  # noqa: F821
    if "src_inn" in df.columns:  # noqa: F821
        df["cio_inn"] = df["src_inn"].apply(normalize_inn)  # noqa: F821
    # master_id is filled later (the MERGE step JOINs the accept map onto it).
    if "master_id" not in df.columns:  # noqa: F821
        df["master_id"] = ""  # noqa: F821
    # DFO CSV ingest collapses empty cells (shifting columns); an all-empty
    # column is float64, so fill EVERY column with a " " sentinel.
    df = df.fillna(" ")  # noqa: F821
    for c in df.columns:  # noqa: F821
        s = df[c].astype(str).str.replace(",", " ", regex=False)  # noqa: F821
        df[c] = s.where(s.str.strip() != "", " ")  # noqa: F821
