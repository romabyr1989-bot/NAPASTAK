#!/usr/bin/env bash
# Integration test for the SCD2 (slowly-changing-dimension, type 2) pipeline step.
#
# Covers:
#   1. SCD2 step creates the target with valid_from / valid_to columns;
#      first run opens N versions (valid_to empty).
#   2. Second run with one changed record: the old version is closed
#      (valid_to set), a new version is opened; unchanged records are NOT
#      duplicated.
#   3. Third run with is_deleted=1: the version is closed, no new version is
#      opened.
#
# Note: an "open" version is an EMPTY valid_to cell, not SQL NULL — the engine
# stores missing text as "" and "IS NULL" never matches it. /run executes the
# pipeline synchronously, so each run is complete when curl returns.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/dfo_gateway"
PORT=19295
DATA="$(mktemp -d -t dfo_scd2_XXXX)"
SECRET="scd2-test-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW" ]] || { echo "missing $GW"; exit 1; }
python3 -c 'import json' 2>/dev/null || { echo "SKIP: python3 not available"; exit 0; }

cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,
 "jwt_secret":"$SECRET","admin_password":"admin"}
EOF
"$GW" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do
  curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break
  sleep 0.2
done
TOKEN=$(curl -sf -X POST "http://localhost:$PORT/api/auth/token" \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no admin token"; exit 1; }
echo "Gateway ready on :$PORT"
AUTH=(-H "Authorization: Bearer $TOKEN")

# Metrics helper: read a {columns,rows} query result on stdin and emit shell
# assignments — total / open / closed and per-business-key counts.
cat > "$DATA/m.py" <<'PYEOF'
import sys, json
from collections import Counter
d = json.load(sys.stdin)
ci = {c: i for i, c in enumerate(d["columns"])}
rows = d["rows"]
vt, bk = ci["valid_to"], ci["customer_id"]
open_ = sum(1 for r in rows if r[vt] == "")
cnt = Counter(r[bk] for r in rows)
ocnt = Counter(r[bk] for r in rows if r[vt] == "")
print(f"total={len(rows)}")
print(f"open={open_}")
print(f"closed={len(rows)-open_}")
for k in cnt:
    print(f"cnt_{k}={cnt[k]}")
    print(f"open_{k}={ocnt.get(k,0)}")
PYEOF

ingest() {  # ingest CSV ($2) into table ($1), replacing any prior contents
  curl -sf -X DELETE "http://localhost:$PORT/api/tables/$1" "${AUTH[@]}" >/dev/null 2>&1 || true
  curl -sf -X POST "http://localhost:$PORT/api/ingest/csv?table=$1" "${AUTH[@]}" \
    -H 'Content-Type: text/csv' --data-binary "$2" >/dev/null
}
dim() {  # full dim_customer result as JSON
  curl -sf -X POST "http://localhost:$PORT/api/tables/query" "${AUTH[@]}" \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT customer_id, city, valid_from, valid_to FROM dim_customer"}'
}
run() { curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID/run" "${AUTH[@]}" >/dev/null; }

# ── Create the SCD2 pipeline (source = cust_src, target = dim_customer) ───────
PIPELINE=$(cat <<'JSON'
{"name":"scd2_dim","enabled":true,
 "steps":[{"id":"s1","name":"historise",
           "transform_sql":"SELECT * FROM cust_src",
           "target_table":"dim_customer",
           "scd2_business_key":"customer_id",
           "scd2_effective_from_col":"valid_from",
           "scd2_effective_to_col":"valid_to",
           "scd2_transaction_time":"updated_at",
           "scd2_deleted_flag":"is_deleted"}]}
JSON
)
RESP=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d "$PIPELINE")
PID=$(echo "$RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
check "SCD2 pipeline created" "[[ -n '$PID' ]]"

# ── Run 1: initial load — 3 records, all opened ──────────────────────────────
ingest cust_src $'customer_id,name,city,updated_at,is_deleted\nC1,Alice,NYC,100,0\nC2,Bob,LA,100,0\nC3,Carol,SF,100,0'
run
D=$(dim)
check "run1: target has valid_from/valid_to columns" \
  "echo '$D' | grep -q valid_from && echo '$D' | grep -q valid_to"
eval "$(echo "$D" | python3 "$DATA/m.py")"
check "run1: 3 rows total"        "[[ '$total'  == 3 ]]"
check "run1: 3 open (valid_to empty)" "[[ '$open'   == 3 ]]"
check "run1: 0 closed"            "[[ '$closed' == 0 ]]"

# ── Run 2: change C1's city — old closes, new opens, C2/C3 untouched ─────────
ingest cust_src $'customer_id,name,city,updated_at,is_deleted\nC1,Alice,BOSTON,200,0\nC2,Bob,LA,100,0\nC3,Carol,SF,100,0'
run
D=$(dim)
eval "$(echo "$D" | python3 "$DATA/m.py")"
check "run2: 4 rows total (one new version)" "[[ '$total'  == 4 ]]"
check "run2: 3 open"                         "[[ '$open'   == 3 ]]"
check "run2: 1 closed (old C1)"              "[[ '$closed' == 1 ]]"
check "run2: C1 has 2 versions"              "[[ '$cnt_C1' == 2 ]]"
check "run2: C1 has exactly 1 open version"  "[[ '$open_C1' == 1 ]]"
check "run2: C2 not duplicated (1 row)"      "[[ '$cnt_C2' == 1 ]]"
check "run2: C3 not duplicated (1 row)"      "[[ '$cnt_C3' == 1 ]]"

# ── Run 3: C3 deleted — its version closes, nothing reopens; C1/C2 unchanged ─
ingest cust_src $'customer_id,name,city,updated_at,is_deleted\nC1,Alice,BOSTON,200,0\nC2,Bob,LA,100,0\nC3,Carol,SF,300,1'
run
D=$(dim)
eval "$(echo "$D" | python3 "$DATA/m.py")"
check "run3: still 4 rows (no new version)"  "[[ '$total'  == 4 ]]"
check "run3: 2 open"                         "[[ '$open'   == 2 ]]"
check "run3: 2 closed"                       "[[ '$closed' == 2 ]]"
check "run3: C3 has no open version"         "[[ '$open_C3' == 0 ]]"
check "run3: C3 still single row (not reopened)" "[[ '$cnt_C3' == 1 ]]"
check "run3: C1 unchanged (still 2 versions)" "[[ '$cnt_C1' == 2 ]]"
check "run3: C1 still has 1 open version"    "[[ '$open_C1' == 1 ]]"

echo ""
echo "SCD2 test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
