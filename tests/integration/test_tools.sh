#!/usr/bin/env bash
# Tests the loader utilities (tools/*.py) + the joker_transform unit logic.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW_BIN="$ROOT/build/release/bin/dfo_gateway"
PORT=19351
DATA="$(mktemp -d -t dfo_tools_XXXX)"
SECRET="tools-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }
skip()  { echo "SKIP: $1 — $2"; SKIP=$((SKIP+1)); }

[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN"; exit 1; }
python3 -c 'import json,pandas' 2>/dev/null || { echo "SKIP: python3/pandas missing"; exit 0; }

cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,"jwt_secret":"$SECRET","admin_password":"admin"}
EOF
"$GW_BIN" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break; sleep 0.2; done
TOKEN=$(curl -sf -X POST "http://localhost:$PORT/api/auth/token" -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no token"; exit 1; }
GW="http://localhost:$PORT"; AUTH=(-H "Authorization: Bearer $TOKEN")
echo "Gateway ready on :$PORT"

qval() { curl -s -X POST "$GW/api/tables/query" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d "{\"sql\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1")}" \
  | python3 -c "import sys,json
try:
  d=json.load(sys.stdin); print(d['rows'][0][0] if d.get('rows') else '')
except: print('')"; }

# ── xlsx_to_dfo ──
if python3 -c 'import openpyxl' 2>/dev/null; then
  python3 -c "
import openpyxl; wb=openpyxl.Workbook(); ws=wb.active
ws.append(['id','name']); ws.append([1,'Alice']); ws.append([2,'Bob'])
wb.save('$DATA/test.xlsx')"
  python3 "$ROOT/tools/xlsx_to_dfo.py" --file "$DATA/test.xlsx" --table xlsx_test --gateway "$GW" --token "$TOKEN" >/dev/null
  check "xlsx_to_dfo: 2 rows loaded" "[[ \"\$(qval 'SELECT count(*) FROM xlsx_test')\" == '2' ]]"
  OUT=$(python3 "$ROOT/tools/xlsx_to_dfo.py" --file "$DATA/test.xlsx" --table xlsx_dry --gateway "$GW" --token "$TOKEN" --dry-run 2>&1)
  check "xlsx_to_dfo --dry-run prints rows, no upload" \
    "[[ '$OUT' == *'DRY RUN'* ]] && [[ -z \"\$(qval 'SELECT count(*) FROM xlsx_dry')\" ]]"
else
  skip "xlsx_to_dfo" "openpyxl not installed"
fi

# ── json_to_dfo (list of dicts) ──
echo '[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]' > "$DATA/list.json"
python3 "$ROOT/tools/json_to_dfo.py" --file "$DATA/list.json" --table json_list --gateway "$GW" --token "$TOKEN" >/dev/null
check "json_to_dfo: list of dicts → 2 rows" "[[ \"\$(qval 'SELECT count(*) FROM json_list')\" == '2' ]]"

# ── json_to_dfo (single dict) ──
echo '{"id":9,"name":"Solo"}' > "$DATA/one.json"
python3 "$ROOT/tools/json_to_dfo.py" --file "$DATA/one.json" --table json_one --gateway "$GW" --token "$TOKEN" >/dev/null
check "json_to_dfo: single dict → 1 row" "[[ \"\$(qval 'SELECT count(*) FROM json_one')\" == '1' ]]"

# ── gpdb_log_to_dfo: parse a CSV log, skip SELECT 1 / BEGIN ──
# 30-column GP csvlog; logquery is column index 21 (0-based). Build 3 rows:
#   row1 = analytics SELECT (kept), row2 = "SELECT 1" (skipped), row3 = BEGIN (skipped)
python3 -c "
import csv
cols=30
def row(q):
    r=['']*cols; r[0]='2024-01-01 00:00:00'; r[1]='u'; r[21]=q; return r
with open('$DATA/gpdb_test.csv','w',newline='') as f:
    w=csv.writer(f)
    w.writerow(row('select count(*) from orders'))
    w.writerow(row('SELECT 1'))
    w.writerow(row('BEGIN'))
"
python3 "$ROOT/tools/gpdb_log_to_dfo.py" --folder "$DATA" --table gpdb_q --gateway "$GW" --token "$TOKEN" \
  --pattern "gpdb_test.csv" --state-file "$DATA/.state" >/dev/null
check "gpdb_log_to_dfo: only the analytics SELECT kept (1 row)" \
  "[[ \"\$(qval 'SELECT count(*) FROM gpdb_q')\" == '1' ]]"
# re-run: state-file should skip the already-processed file
OUT2=$(python3 "$ROOT/tools/gpdb_log_to_dfo.py" --folder "$DATA" --table gpdb_q --gateway "$GW" --token "$TOKEN" \
  --pattern "gpdb_test.csv" --state-file "$DATA/.state" 2>&1)
check "gpdb_log_to_dfo: --state-file skips already-parsed file (0 new)" "[[ '$OUT2' == *'0 new'* ]]"

# ── joker_transform unit test (no DFO) ──
python3 -c "
import sys; sys.path.insert(0,'$ROOT/tools/cdi')
import pandas as pd, joker_transform
d=pd.DataFrame([{'kafka_offset':0,'kafka_value':'{\"ows.CLIENT\":[{\"ROW_ID\":\"R1\",\"LAST_NAME\":\"IVANOV\",\"CCAT\":\"P\"}]}'}])
r=joker_transform.transform(d)
assert 'party_type' in r.columns and r.iloc[0]['party_type']=='PHYSICAL'
l=joker_transform.transform(pd.DataFrame([{'kafka_value':'{\"ows.CLIENT\":[{\"CCAT\":\"C\"}]}'}]))
assert l.iloc[0]['party_type']=='LEGAL'
print('OK')
" 2>/dev/null
check "joker_transform unit: PHYSICAL + LEGAL classification" "[[ \$? -eq 0 ]]"

echo ""
echo "Tools test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
