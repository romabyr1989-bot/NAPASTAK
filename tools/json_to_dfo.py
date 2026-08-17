#!/usr/bin/env python3
"""
Load a JSON file (array of objects, or a single object) into a NAPASTAK table.
Replicates applications/json2table.py.

Usage:
  python3 tools/json_to_dfo.py --file inn_samples.json --table inn_samples --token $TOKEN
"""
import argparse
import ast
import csv
import io
import json
import sys
import urllib.request
from pathlib import Path


def json_to_csv(data):
    """List of dicts (or a single dict) → (headers, csv_bytes)."""
    if isinstance(data, dict):
        data = [data]
    if not data:
        return [], b""
    headers = list(dict.fromkeys(
        k for row in data for k in (row.keys() if isinstance(row, dict) else [])))
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers if headers else ["value"])
    for row in data:
        if isinstance(row, dict):
            w.writerow([str(row.get(h, "")) if row.get(h) is not None else "" for h in headers])
        else:
            w.writerow([str(row)])
    return headers, buf.getvalue().encode("utf-8")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--file", required=True, type=Path)
    p.add_argument("--table", required=True, help="Target table (DFO: [A-Za-z0-9_], no dots)")
    p.add_argument("--gateway", default="http://localhost:8080")
    p.add_argument("--token", required=True)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    raw = args.file.read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Fallback for Python-literal dumps (single quotes etc.) — safe literal eval.
        try:
            data = ast.literal_eval(raw)
        except (ValueError, SyntaxError) as e:
            print(f"ERROR: cannot parse {args.file}: {e}", file=sys.stderr)
            sys.exit(1)

    headers, csv_bytes = json_to_csv(data)
    n = len(data) if isinstance(data, list) else 1
    print(f"Parsed {n} records, columns: {headers}")
    if args.dry_run:
        print("DRY RUN: no upload")
        return

    req = urllib.request.Request(
        f"{args.gateway}/api/ingest/csv?table={args.table}", data=csv_bytes,
        headers={"Content-Type": "text/csv", "Authorization": f"Bearer {args.token}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        resp = json.loads(r.read())
    print(f"OK: {resp.get('rows_written', 0)} rows → {args.table}")


if __name__ == "__main__":
    main()
