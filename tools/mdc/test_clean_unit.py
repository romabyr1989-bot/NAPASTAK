#!/usr/bin/env python3
"""Unit tests for the MDC clean / key-gen scripts (no DFO needed)."""
import os
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))


def _run(script: str, frame: pd.DataFrame) -> pd.DataFrame:
    g = {"df": frame, "pd": pd}
    exec(open(os.path.join(HERE, script)).read(), g)  # noqa: S102
    return g["df"]


# clean_person: trim+lower FIO, digits-only INN
r = _run("clean_person.py", pd.DataFrame([{
    "source_id": "S1", "system_name": "CRM",
    "src_last_name": "  ИВАНОВ  ", "src_first_name": "ИВАН",
    "src_middle_name": "ИВАНОВИЧ", "src_fullname": "ИВАНОВ ИВАН ИВАНОВИЧ",
    "src_inn": "12-34 56789012", "birth_date": "1980-01-01", "last_upd": "2024-01-15"}]))
assert r.iloc[0]["cio_last_name"] == "иванов", r.iloc[0]["cio_last_name"]
assert r.iloc[0]["cio_inn"] == "123456789012", r.iloc[0]["cio_inn"]
assert "master_id" in r.columns
print("clean_person: OK")

# clean_id_doc: alnum-only number, category filter
r = _run("clean_id_doc.py", pd.DataFrame([{
    "source_id": "D1", "person_source_id": "S1", "category": "21", "src_number": "4500 123456"}]))
assert r.iloc[0]["cio_number"] == "4500123456", r.iloc[0]["cio_number"]
print("clean_id_doc: OK")

# generate_keys: 3 keys (FIO+BD, DUL, INN) for one full record
r = _run("generate_keys.py", pd.DataFrame([{
    "source_id": "S1", "system_name": "CRM", "cio_last_name": "иванов",
    "cio_first_name": "иван", "cio_middle_name": "иванович", "birth_date": "1980-01-01",
    "cio_inn": "123456789012", "mat_id_doc_set": "4500123456"}]))
assert len(r) == 3, f"expected 3 keys, got {len(r)}: {r['rule_value'].tolist() if not r.empty else []}"
types = {v.split("::")[1] for v in r["rule_value"]}
assert types == {"FIO+BD", "DUL", "INN"}, types
print("generate_keys: OK")

print("\nAll unit tests passed")
