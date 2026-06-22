#!/usr/bin/env python3
"""
Joker CDI transform: Kafka CDI-topic payload → physical_party rows.
Replicates tsmdm_hfl_cdi/naive.py + joker_lekton2cdi.process_message().

Runs as a DFO python_file step:
  input : df  — rows from cdi_joker_incoming_buf (kafka_offset, kafka_key, kafka_value)
  output: df  — physical_party rows

XML mapping files are read from env (set them via python_context_dir / the
pipeline env). If absent, the transform degrades to a 1:1 lower-cased mapping.
"""
import os
import json
import xml.etree.ElementTree as ET

import pandas as pd

MAPPINGS_FILE  = os.getenv("DFO_MAPPINGS_FILE",  "/opt/tsmdm/data/joker/equalized_ruleset.xml")
RENAMINGS_FILE = os.getenv("DFO_RENAMINGS_FILE", "/opt/tsmdm/data/joker/cdi2sbl_renamings.xml")


def load_renamings(filepath: str) -> dict:
    """Parse cdi2sbl_renamings.xml → {entity_name: {src_attr: tgt_attr}}.
    Analog of naive.get_renamings_config()."""
    renamings = {}
    try:
        root = ET.parse(filepath).getroot()
        for entity in root.findall(".//entity"):
            name = entity.get("entity_name", "")
            attrs = {}
            for attr in entity.findall(".//attribute"):
                src = attr.get("name", "")
                for service in attr.findall("variants/service"):
                    if service.get("name") == "SBL":
                        attrs[src] = service.text or src
            renamings[name] = attrs
    except Exception as e:  # noqa: BLE001 — missing/invalid mapping is non-fatal
        print(f"[joker_transform] load_renamings: {e}")
    return renamings


def load_ruleset(filepath: str) -> dict:
    """Parse equalized_ruleset.xml → {entity: {column: {src_val: tgt_val}}}.
    Analog of naive.get_ruleset()."""
    ruleset = {}
    try:
        root = ET.parse(filepath).getroot()
        for rule in root.findall(".//rule"):
            entity = rule.findtext("entity_name", "")
            column = rule.findtext("column_name", "")
            src_val = rule.findtext("source_value", "")
            tgt_val = rule.findtext("target_value", "")
            ruleset.setdefault(entity, {}).setdefault(column, {})[src_val] = tgt_val
    except Exception as e:  # noqa: BLE001
        print(f"[joker_transform] load_ruleset: {e}")
    return ruleset


def classify(client: dict) -> str:
    """CDI client-type classification (joker_lekton2cdi rules)."""
    ccat = client.get("CCAT", "")
    svc_grp = client.get("SERVICE_GROUP")
    if ccat == "P" and (svc_grp is None or svc_grp == "5"):
        return "PHYSICAL"
    if ccat == "C" or (ccat == "P" and svc_grp == "0"):
        return "LEGAL"
    return "UNKNOWN"


def parse_message(raw, renamings: dict, ruleset: dict) -> dict:
    """Transform one CDI-topic JSON message. Analog of process_message()."""
    try:
        msg = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(msg, dict):
        return {}

    # {"ows.CLIENT": [{...}]} or a flat client dict
    if "ows.CLIENT" in msg:
        clients = msg["ows.CLIENT"]
        client = clients[0] if clients else {}
    else:
        client = msg
    if not client:
        return {}

    client_type = classify(client)
    entity_map = renamings.get("physical_party" if client_type == "PHYSICAL" else "party", {})

    row = {}
    for src_col, val in client.items():
        tgt_col = entity_map.get(src_col, src_col.lower())
        rdm = ruleset.get("physical_party", {}).get(src_col, {})
        row[tgt_col] = rdm.get(str(val), val)

    row["party_type"] = client_type
    row["source_system"] = "joker"
    return row


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform a batch of Kafka messages into physical_party rows."""
    renamings = load_renamings(RENAMINGS_FILE)
    ruleset = load_ruleset(MAPPINGS_FILE)

    value_col = "kafka_value" if "kafka_value" in df.columns else df.columns[-1]
    results = [parse_message(v, renamings, ruleset) for v in df[value_col]]
    results = [r for r in results if r]
    if not results:
        return pd.DataFrame()

    out = pd.DataFrame(results)
    for col in out.select_dtypes(include="object").columns:
        out[col] = out[col].fillna("").astype(str).str.strip()
    return out


# DFO python_file entry point — `df` is provided by the engine from transform_sql.
if "df" in globals():
    df = transform(df)  # noqa: F821
