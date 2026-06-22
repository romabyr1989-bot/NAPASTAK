#!/usr/bin/env python3
"""
Load an Excel (.xlsx) file (or a folder of them) into a DataFlow OS table.
Replicates applications/xlsx2db.py.

Usage:
  python3 tools/xlsx_to_dfo.py --file data.xlsx --table siebel_contacts --token $TOKEN
  python3 tools/xlsx_to_dfo.py --folder ./exports/ --table siebel_contacts \
      --gateway http://localhost:8080 --token $TOKEN --dry-run
"""
import argparse
import csv
import io
import json
import sys
import urllib.request
from pathlib import Path


def read_xlsx(filepath, sheet=None):
    """Read an .xlsx via openpyxl → (headers, rows)."""
    try:
        import openpyxl
    except ImportError:
        print("ERROR: openpyxl required. Install: pip install openpyxl", file=sys.stderr)
        sys.exit(1)
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb[sheet] if sheet else wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        return [], []
    headers = [str(c) if c is not None else f"col_{i}" for i, c in enumerate(rows[0])]
    return headers, [list(r) for r in rows[1:]]


def rows_to_csv(headers, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for row in rows:
        w.writerow([str(v) if v is not None else "" for v in row])
    return buf.getvalue().encode("utf-8")


def upload_to_dfo(csv_bytes, table, gateway, token):
    req = urllib.request.Request(
        f"{gateway}/api/ingest/csv?table={table}", data=csv_bytes,
        headers={"Content-Type": "text/csv", "Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read()).get("rows_written", 0)


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", type=Path, help="Single .xlsx file")
    p.add_argument("--folder", type=Path, help="Folder with .xlsx files")
    p.add_argument("--table", required=True, help="Target table (DFO: [A-Za-z0-9_], no dots)")
    p.add_argument("--gateway", default="http://localhost:8080")
    p.add_argument("--token", required=True)
    p.add_argument("--sheet", default=None, help="Sheet name (default: active)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if args.file:
        files = [args.file]
    elif args.folder:
        files = sorted(args.folder.glob("*.xlsx"))
    else:
        p.error("--file or --folder required")

    total = 0
    for f in files:
        print(f"Processing {f}...")
        headers, rows = read_xlsx(f, args.sheet)
        if not rows:
            print("  Empty file, skipping")
            continue
        if args.dry_run:
            print(f"  DRY RUN: {len(rows)} rows, columns: {headers}")
            continue
        written = upload_to_dfo(rows_to_csv(headers, rows), args.table, args.gateway, args.token)
        print(f"  OK: {written} rows → {args.table}")
        total += written

    if not args.dry_run:
        print(f"\nTotal: {total} rows written")


if __name__ == "__main__":
    main()
