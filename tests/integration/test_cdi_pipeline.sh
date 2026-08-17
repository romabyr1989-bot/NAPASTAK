#!/usr/bin/env bash
# Tests the CDI/master pipelines: YAML validation + master_assembly e2e
# (no real Kafka/Oracle — cdi_* tables are seeded via CSV).

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW_BIN="$ROOT/build/release/bin/napastak_gateway"
PORT=19350
DATA="$(mktemp -d -t dfo_cdi_XXXX)"
SECRET="cdi-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN"; exit 1; }
python3 -c 'import json' 2>/dev/null || { echo "SKIP: python3 missing"; exit 0; }

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

# ── 1. YAML validation via preview-yaml ──
for f in master_assembly hfl_collector hfl_joker; do
  C=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GW/api/pipelines/preview-yaml" "${AUTH[@]}" \
       -H 'Content-Type: text/yaml' --data-binary @"$ROOT/pipelines/$f.yaml")
  check "$f.yaml validates via preview-yaml (HTTP 200)" "[[ '$C' == '200' ]]"
done
# Pipelines now live only in the catalog — create master_assembly via
# from-template instead of relying on pipelines_dir auto-load.
C=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GW/api/pipelines/from-template" "${AUTH[@]}" \
     -H 'Content-Type: application/json' \
     -d "{\"template_yaml\":$(python3 -c 'import json,sys;print(json.dumps(open(sys.argv[1]).read()))' "$ROOT/pipelines/master_assembly.yaml")}")
check "master_assembly created via from-template (HTTP 201)" "[[ '$C' == '201' ]]"
check "master_assembly present in pipeline list" \
  "curl -sf '$GW/api/pipelines' \"\${AUTH[@]}\" | grep -q master_assembly"

# ── 2. Seed CDI entities (non-empty fields — DFO collapses empty CSV cells) ──
curl -sf -X POST "$GW/api/ingest/csv?table=cdi_physical_party" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'master_id,src_inn,src_last_name,src_first_name,src_middle_name,birth_date,birth_place,last_upd\nM1,123456789012,ivanov,ivan,i,1980-01-01,Moscow,2024-01-15\nM1,111111111111,ivanov,ivan,i,1980-01-01,Moscow,2024-01-10\nM2,987654321098,petrov,petr,p,1990-05-15,SPb,2024-01-15' >/dev/null
curl -sf -X POST "$GW/api/ingest/csv?table=cdi_party" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'master_id,cio_inn,cio_kpp,cio_ogrn,cio_org_name,last_upd\nL1,7700000001,770001001,1027700000001,OOO,2024-01-15' >/dev/null

# ── 3. Run master_assembly ──
PID=$(curl -sf "$GW/api/pipelines" "${AUTH[@]}" | python3 -c "import sys,json;[print(p['id']) for p in json.load(sys.stdin) if p['name']=='master_assembly']" | head -1)
curl -sf -X POST "$GW/api/pipelines/$PID/run" "${AUTH[@]}" >/dev/null

check "master_person: M1 INN from latest row (R1, 123456789012)" \
  "[[ \"\$(qval \"SELECT inn FROM master_person WHERE master_id='M1'\")\" == '123456789012' ]]"
check "master_person: M2 assembled (1 row)" \
  "[[ \"\$(qval \"SELECT count(*) FROM master_person WHERE master_id='M2'\")\" == '1' ]]"
check "master_person: SCD2 valid_from populated for M1" \
  "[[ -n \"\$(qval \"SELECT valid_from FROM master_person WHERE master_id='M1'\")\" ]]"
check "master_party: L1 assembled" \
  "[[ \"\$(qval \"SELECT count(*) FROM master_party WHERE master_id='L1'\")\" == '1' ]]"

echo ""
echo "CDI/master test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
