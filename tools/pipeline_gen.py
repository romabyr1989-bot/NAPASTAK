#!/usr/bin/env python3
"""
Generate NAPASTAK pipelines from a YAML template + TSV config.

Usage:
    pipeline_gen.py --gateway URL --token TOKEN --template FILE --config FILE
                    [--dry-run] [--skip-errors] [--parallel N]

Config TSV format: first row = header (variable names), subsequent rows = values.
Each row produces one pipeline via POST /api/pipelines/from-template.
Lines whose first column starts with '#' are treated as comments and skipped.
"""
import csv
import sys
import json
import argparse
from pathlib import Path
from urllib import request as urlreq, error as urlerr
from concurrent.futures import ThreadPoolExecutor, as_completed


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--gateway",   required=True, help="Gateway base URL, e.g. http://localhost:8080")
    p.add_argument("--token",     required=True, help="JWT bearer token")
    p.add_argument("--template",  required=True, type=Path, help="YAML template file")
    p.add_argument("--config",    required=True, type=Path, help="TSV config file")
    p.add_argument("--dry-run",   action="store_true", help="Print what would be created, do not POST")
    p.add_argument("--skip-errors", action="store_true", help="Continue on HTTP errors")
    p.add_argument("--parallel",  type=int, default=1, metavar="N",
                   help="Number of parallel POST requests (default: 1)")
    return p.parse_args()


def post_template(gateway: str, token: str, template: str, variables: dict) -> dict:
    payload = json.dumps({"template_yaml": template, "vars": variables}).encode()
    req = urlreq.Request(
        f"{gateway}/api/pipelines/from-template",
        data=payload,
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
    )
    with urlreq.urlopen(req, timeout=30) as r:
        return json.load(r)


def main():
    args = parse_args()
    template = args.template.read_text(encoding="utf-8")

    rows = []
    with open(args.config, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            # Skip comment lines (first column starts with #)
            first = next(iter(row.values()), "") or ""
            if first.lstrip().startswith("#"):
                continue
            rows.append(dict(row))

    if args.dry_run:
        print(f"DRY RUN: would create {len(rows)} pipeline(s) from {args.template}")
        for row in rows:
            table = row.get("table_name", row)
            print(f"  -> {table}  vars={json.dumps(row, ensure_ascii=False)}")
        return

    errors = 0

    def create(row):
        try:
            resp = post_template(args.gateway, args.token, template, row)
            return ("OK", resp.get("id", "?"), None)
        except urlerr.HTTPError as e:
            return ("ERR", row.get("table_name", str(row)), e.read().decode())
        except Exception as e:  # noqa: BLE001 — report any failure per-row
            return ("ERR", row.get("table_name", str(row)), str(e))

    with ThreadPoolExecutor(max_workers=max(1, args.parallel)) as pool:
        futures = {pool.submit(create, row): row for row in rows}
        for future in as_completed(futures):
            status, name, err = future.result()
            if status == "OK":
                print(f"OK   {name}")
            else:
                print(f"ERR  {name}: {err}", file=sys.stderr)
                errors += 1
                if not args.skip_errors:
                    pool.shutdown(wait=False, cancel_futures=True)
                    sys.exit(1)

    if errors:
        print(f"\n{errors} error(s). Use --skip-errors to continue past failures.",
              file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
