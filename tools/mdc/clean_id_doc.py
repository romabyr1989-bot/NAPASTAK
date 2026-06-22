#!/usr/bin/env python3
"""
MDC CLEAN stage: id_doc entity.
Replicates prettify_dul (upper + alnum only) and the accepted-category filter.
Runs as a DFO python_file step.
"""
import re

import pandas as pd  # noqa: F401

ACCEPTED_CATEGORIES = ["01", "03", "21", "22", "36"]


def normalize_dul(number) -> str:
    """prettify_dul(): keep letters/digits only, upper-cased. Coerces non-str
    (pandas may read an all-numeric doc number as int/float)."""
    if number is None:
        return ""
    number = str(number)
    if number == "nan":
        return ""
    if number.endswith(".0"):
        number = number[:-2]
    return re.sub(r"[^A-ZА-Яa-zа-я0-9]", "", number).upper()


if "df" in globals() and not df.empty:  # noqa: F821
    if "src_number" in df.columns:  # noqa: F821
        df["cio_number"] = df["src_number"].apply(normalize_dul)  # noqa: F821
    if "category" in df.columns:  # noqa: F821
        df = df[df["category"].astype(str).isin(ACCEPTED_CATEGORIES) | (df["category"].astype(str) == "")]  # noqa: F821
    # DFO CSV ingest collapses empty cells; fill EVERY column with a " " sentinel.
    df = df.fillna(" ")  # noqa: F821
    for c in df.columns:  # noqa: F821
        s = df[c].astype(str).str.replace(",", " ", regex=False)  # noqa: F821
        df[c] = s.where(s.str.strip() != "", " ")  # noqa: F821
