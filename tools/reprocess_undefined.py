#!/usr/bin/env python3
"""
Batch reprocessing of agreements stuck at crm_id = 'UNDEFINED'.
Replicates applications/loop_agr2party.py.

Logic:
  1. count UNDEFINED agreement2party rows
  2. per batch: trigger the DFO match pipeline (which re-matches them)
  3. agreements unmatched afterwards remain NULL/UNDEFINED

Usage:
  python3 tools/reprocess_undefined.py --token $TOKEN
  python3 tools/reprocess_undefined.py --batch-size 1000 --pipeline-id person_match --dry-run
"""
import argparse
import json
import time
import urllib.request
from datetime import datetime, timezone


def dfo_query(sql, gateway, token):
    req = urllib.request.Request(
        f"{gateway}/api/tables/query", data=json.dumps({"sql": sql}).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.load(r)
    cols = data.get("columns", [])
    return [dict(zip(cols, row)) for row in data.get("rows", [])]


def trigger_pipeline(pipeline_id, gateway, token):
    req = urllib.request.Request(
        f"{gateway}/api/pipelines/{pipeline_id}/run", data=b"{}",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status == 200
    except Exception as e:  # noqa: BLE001
        print(f"  pipeline trigger failed: {e}")
        return False


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--gateway", default="http://localhost:8080")
    p.add_argument("--token", required=True)
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--pipeline-id", default="person_match",
                   help="DFO pipeline id to trigger per batch")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--sleep", type=float, default=2.0)
    args = p.parse_args()
    gw, tok = args.gateway, args.token

    # --dry-run makes NO requests at all (not even the count).
    if args.dry_run:
        print(f"DRY RUN: would count UNDEFINED agreements and trigger "
              f"'{args.pipeline_id}' per batch of {args.batch_size}. No requests made.")
        return

    # DFO stores blanks as "" (not NULL) — match both for map__deleted.
    rows = dfo_query(
        "SELECT count(*) AS cnt FROM agreement2party "
        "WHERE agreement_type_cd = 'BKI' "
        "AND (map__deleted IS NULL OR map__deleted = '') "
        "AND crm_id = 'UNDEFINED'", gw, tok)
    undefined_cnt = int(rows[0]["cnt"]) if rows and rows[0].get("cnt") else 0
    print(f"Reprocessing {undefined_cnt} UNDEFINED agreements in batches of {args.batch_size}")

    processed = 0
    for start in range(0, undefined_cnt, args.batch_size):
        print(f"[{datetime.now(timezone.utc).isoformat()}] batch {start}–{start + args.batch_size}…")
        if not trigger_pipeline(args.pipeline_id, gw, tok):
            print(f"  WARNING: pipeline {args.pipeline_id} trigger failed")
        processed += args.batch_size
        if start + args.batch_size < undefined_cnt:
            time.sleep(args.sleep)

    print(f"\nTriggered {min(processed, undefined_cnt)} agreements for reprocessing")
    print("Unmatched agreements remain NULL — check: "
          "SELECT * FROM agreement2party WHERE crm_id IS NULL OR crm_id = ''")


if __name__ == "__main__":
    main()
