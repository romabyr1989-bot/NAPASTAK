#!/usr/bin/env python3
"""
Parse Greenplum CSV logs (csvlog destination) and load them into DataFlow OS.
Replicates tsmdm_gpdb_logparser/gpdb_logparser.py.

Usage:
  python3 tools/gpdb_log_to_dfo.py --folder /var/log/gpdb/ --table gpdb_query_log --token $TOKEN
  python3 tools/gpdb_log_to_dfo.py --folder /var/log/gpdb/ --table gpdb_query_log \
      --pattern "*.csv" --dry-run
"""
import argparse
import csv
import gzip
import io
import json
import re
import sys
import urllib.request
from pathlib import Path

# Greenplum CSV log columns (gpdb_regex.log_columns)
GPDB_LOG_COLUMNS = [
    "logtime", "loguser", "logdatabase", "logpid", "logthread", "loghost", "logport",
    "logsessiontime", "logtransaction", "logsession", "logcmdcount", "logsegment",
    "logslice", "logdistxact", "loglocalxact", "logsubxact", "logseverity",
    "logstate", "logmessage", "logdetail", "loghint", "logquery", "logquerypos",
    "logcontext", "logdebug", "logcursorpos", "logfunction", "logfile", "logline",
    "logstack",
]
DROP_COLUMNS = {
    "logslice", "logdistxact", "loglocalxact", "logsubxact", "logdetail",
    "loghint", "logcursorpos", "logcontext", "logfile", "logfunction",
    "logline", "logstack",
}
SKIP_QUERIES = {"SELECT 1", "BEGIN", "COMMIT", "DISCARD ALL", "ROLLBACK", ""}


def parse_gpdb_log(filepath, chunksize=10000):
    """Generator yielding chunks (list of dicts) of analytics queries."""
    opener = gzip.open if filepath.suffix == ".gz" else open
    keep = [c for c in GPDB_LOG_COLUMNS if c not in DROP_COLUMNS]
    buf = []
    try:
        with opener(filepath, "rt", encoding="utf-8", errors="replace") as f:
            for row in csv.reader(f):
                if len(row) < len(GPDB_LOG_COLUMNS):
                    row += [""] * (len(GPDB_LOG_COLUMNS) - len(row))
                rec = dict(zip(GPDB_LOG_COLUMNS, row))
                query = rec.get("logquery", "").strip()
                if query in SKIP_QUERIES:
                    continue
                if query and not re.match(r"^\s*(select|with)\b", query, re.I):
                    continue  # analytics-only
                buf.append({k: rec[k] for k in keep if k in rec})
                if len(buf) >= chunksize:
                    yield buf
                    buf = []
        if buf:
            yield buf
    except Exception as e:  # noqa: BLE001
        print(f"ERROR parsing {filepath}: {e}", file=sys.stderr)


def chunk_to_csv(chunk):
    if not chunk:
        return b""
    headers = list(chunk[0].keys())
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    w.writeheader()
    w.writerows(chunk)
    return buf.getvalue().encode("utf-8")


def upload_chunk(csv_bytes, table, gateway, token):
    req = urllib.request.Request(
        f"{gateway}/api/ingest/csv?table={table}", data=csv_bytes,
        headers={"Content-Type": "text/csv", "Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read()).get("rows_written", 0)


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--folder", required=True, type=Path)
    p.add_argument("--table", required=True, help="Target table (DFO: [A-Za-z0-9_], no dots)")
    p.add_argument("--gateway", default="http://localhost:8080")
    p.add_argument("--token", required=True)
    p.add_argument("--chunksize", type=int, default=10000)
    p.add_argument("--pattern", default="gpdb*.csv.gz")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--state-file", type=Path, default=Path(".gpdb_log_parsed.txt"),
                   help="Tracks already-processed log files")
    args = p.parse_args()

    processed = set(args.state_file.read_text().splitlines()) if args.state_file.exists() else set()
    files = sorted(args.folder.glob(args.pattern))
    new_files = [f for f in files if str(f.absolute()) not in processed]
    print(f"Found {len(files)} log files, {len(new_files)} new")

    total = 0
    for f in new_files:
        print(f"Parsing {f.name}...")
        file_rows = 0
        for chunk in parse_gpdb_log(f, args.chunksize):
            if args.dry_run:
                print(f"  DRY RUN: {len(chunk)} rows")
                file_rows += len(chunk)
                continue
            file_rows += upload_chunk(chunk_to_csv(chunk), args.table, args.gateway, args.token)
        if not args.dry_run:
            with open(args.state_file, "a") as sf:
                sf.write(str(f.absolute()) + "\n")
        print(f"  {file_rows} rows processed")
        total += file_rows

    print(f"\nTotal: {total} rows")


if __name__ == "__main__":
    main()
