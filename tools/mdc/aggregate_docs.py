#!/usr/bin/env python3
"""
MDC RESCORE stage (part 1): aggregate id_doc numbers onto each person.

Replaces the SQL `GROUP_CONCAT(cio_number, '~')` — DFO's GROUP_CONCAT returns
empty, so the join is done in SQL (person LEFT JOIN id_doc, one row per doc) and
the per-person `mat_id_doc_set` ('~'-joined DUL numbers) is built here in pandas.

  input : df  — person ⨯ id_doc rows (transform_sql LEFT JOIN, may repeat person)
  output: df  — one row per person with mat_id_doc_set
"""
import pandas as pd

PERSON_COLS = ["source_id", "system_name", "cio_last_name", "cio_first_name",
               "cio_middle_name", "cio_inn", "birth_date", "last_upd", "master_id"]


def aggregate(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    keys = [c for c in PERSON_COLS if c in frame.columns]
    doc_col = "cio_number" if "cio_number" in frame.columns else None

    def join_docs(s):
        vals = []
        for v in s:
            v = str(v)
            if v.endswith(".0"):   # pandas read an int doc number as float
                v = v[:-2]
            if v and v.strip() and v != "nan":
                vals.append(v)
        return "~".join(dict.fromkeys(vals))  # de-dup, order-preserving

    if doc_col:
        agg = (frame.groupby(keys, dropna=False)[doc_col]
                    .apply(join_docs).reset_index().rename(columns={doc_col: "mat_id_doc_set"}))
    else:
        agg = frame[keys].drop_duplicates().copy()
        agg["mat_id_doc_set"] = ""
    # DFO CSV ingest collapses empty cells; fill EVERY column with a " " sentinel.
    agg = agg.fillna(" ")
    for c in agg.columns:
        s = agg[c].astype(str).str.replace(",", " ", regex=False)
        agg[c] = s.where(s.str.strip() != "", " ")
    return agg


if "df" in globals():
    df = aggregate(df)  # noqa: F821
