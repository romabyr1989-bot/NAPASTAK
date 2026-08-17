#!/usr/bin/env bash
# Integration test for SCD2 compare_to_change via scd2_hash_col.
#
# Covers:
#   H1: first run materialises N versions AND a 32-hex _dfo_row_hash column.
#   H2: re-running the same source detects no change (hash compare) → 0 new.
#   H3: changing a compare column → exactly 1 new version for that key.
#   H4: changing a column NOT in scd2_compare_columns → 0 new versions
#       (the hash is computed only over compare columns).
#
# An "open" version is an EMPTY valid_to cell (the engine stores missing text as
# "" — see test_scd2.sh). /run is synchronous.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/napastak_gateway"
PORT=19301
DATA="$(mktemp -d -t dfo_scd2h_XXXX)"
SECRET="scd2h-test-$$"
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
for i in {1..30}; do curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break; sleep 0.2; done
TOKEN=$(curl -sf -X POST "http://localhost:$PORT/api/auth/token" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no admin token"; exit 1; }
echo "Gateway ready on :$PORT"
AUTH=(-H "Authorization: Bearer $TOKEN")

# metrics keyed on customer_id / valid_to
cat > "$DATA/m.py" <<'PYEOF'
import sys, json
from collections import Counter
d = json.load(sys.stdin)
ci = {c: i for i, c in enumerate(d["columns"])}
rows = d["rows"]
vt, bk = ci["valid_to"], ci["customer_id"]
open_ = sum(1 for r in rows if r[vt] in ("", None))   # open = SQL NULL (json null) or legacy empty string
cnt  = Counter(r[bk] for r in rows)
ocnt = Counter(r[bk] for r in rows if r[vt] in ("", None))   # open = SQL NULL (json null) or legacy empty string
print(f"total={len(rows)}")
print(f"open={open_}")
print(f"closed={len(rows)-open_}")
for k in cnt:
    print(f"cnt_{k}={cnt[k]}")
    print(f"open_{k}={ocnt.get(k,0)}")
PYEOF

ingest() {
  curl -sf -X DELETE "http://localhost:$PORT/api/tables/$1" "${AUTH[@]}" >/dev/null 2>&1 || true
  curl -sf -X POST "http://localhost:$PORT/api/ingest/csv?table=$1" "${AUTH[@]}" \
    -H 'Content-Type: text/csv' --data-binary "$2" >/dev/null
}
dim() {  # rows with the hash column included
  curl -sf -X POST "http://localhost:$PORT/api/tables/query" "${AUTH[@]}" \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT customer_id, name, city, notes, _dfo_row_hash, valid_from, valid_to FROM dim_cust_hash"}'
}
schema_cols() {  # column names of the target as a JSON array
  curl -sf -X POST "http://localhost:$PORT/api/tables/query" "${AUTH[@]}" \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT * FROM dim_cust_hash"}' \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['columns'])"
}
hash_of() {  # echo the _dfo_row_hash of the open version of business key $1
  dim | python3 -c "
import sys,json
d=json.load(sys.stdin); ci={c:i for i,c in enumerate(d['columns'])}
for r in d['rows']:
    if r[ci['customer_id']]=='$1' and r[ci['valid_to']] in ('', None):
        print(r[ci['_dfo_row_hash']]); break
"
}
run() { curl -sf -X POST "http://localhost:$PORT/api/pipelines/$PID/run?wait=true" "${AUTH[@]}" >/dev/null; }

# SCD2 pipeline WITH scd2_hash_col; compare only name,city (notes excluded).
# scd2_transaction_time=updated_at (monotonic) makes current-version detection
# deterministic — without it, same-second runs share valid_from and the window
# ORDER BY ties arbitrarily (see test_scd2.sh).
PIPELINE=$(cat <<'JSON'
{"name":"scd2_hash","enabled":true,
 "steps":[{"id":"s1","name":"historise",
           "transform_sql":"SELECT * FROM cust_src",
           "target_table":"dim_cust_hash",
           "scd2_business_key":"customer_id",
           "scd2_effective_from_col":"valid_from",
           "scd2_effective_to_col":"valid_to",
           "scd2_transaction_time":"updated_at",
           "scd2_hash_col":"_dfo_row_hash",
           "scd2_compare_columns":"name,city"}]}
JSON
)
PID=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d "$PIPELINE" \
  | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
check "hash pipeline created" "[[ -n '$PID' ]]"

# ── H1: initial load — 3 records, hash column populated ──────────────────────
ingest cust_src $'customer_id,name,city,notes,updated_at\nC1,Alice,NYC,a,100\nC2,Bob,LA,b,100\nC3,Carol,SF,c,100'
run
eval "$(dim | python3 "$DATA/m.py")"
check "H1: 3 rows in dim_cust_hash"               "[[ '$total' == 3 ]]"
check "H1: _dfo_row_hash column present"          "schema_cols | grep -q _dfo_row_hash"
H1=$(hash_of C1)
check "H1: C1 hash is 32-hex non-empty"           "[[ '$H1' =~ ^[0-9a-f]{32}$ ]]"

# ── H2: same source — hash compare detects no change ─────────────────────────
run
eval "$(dim | python3 "$DATA/m.py")"
check "H2: still 3 rows (no new versions)"        "[[ '$total' == 3 ]]"
check "H2: 0 closed"                              "[[ '$closed' == 0 ]]"
check "H2: C1 hash unchanged"                     "[[ \"$(hash_of C1)\" == '$H1' ]]"

# ── H3: change a COMPARE column (city) on C1 → 1 new version ─────────────────
ingest cust_src $'customer_id,name,city,notes,updated_at\nC1,Alice,BOSTON,a,200\nC2,Bob,LA,b,100\nC3,Carol,SF,c,100'
run
eval "$(dim | python3 "$DATA/m.py")"
check "H3: 4 rows (new version of C1)"            "[[ '$total' == 4 ]]"
check "H3: C1 has 2 versions"                     "[[ '$cnt_C1' == 2 ]]"
check "H3: C1 has 1 open version"                 "[[ '$open_C1' == 1 ]]"
check "H3: C2 and C3 not duplicated"              "[[ '$cnt_C2' == 1 && '$cnt_C3' == 1 ]]"
check "H3: C1 open hash differs from H1"          "[[ \"$(hash_of C1)\" != '$H1' ]]"

# ── H4: change a NON-compare column (notes) on C2 → 0 new versions ───────────
ingest cust_src $'customer_id,name,city,notes,updated_at\nC1,Alice,BOSTON,a,200\nC2,Bob,LA,CHANGED,300\nC3,Carol,SF,c,100'
run
eval "$(dim | python3 "$DATA/m.py")"
check "H4: still 4 rows (notes not compared)"     "[[ '$total' == 4 ]]"
check "H4: C2 still single version"               "[[ '$cnt_C2' == 1 ]]"

echo ""
echo "SCD2-hash test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
