#!/usr/bin/env bash
# Tests the MDC pipeline: load → clean → aggregate → keys → match → accept →
# apply → merge. No real Kafka/CDI — cdi_* tables seeded via CSV.
# python_file steps are pure transforms (no DFO callbacks), so only the test's
# own curl calls need a token.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW_BIN="$ROOT/build/release/bin/dfo_gateway"
PORT=19372
DATA="$(mktemp -d -t dfo_mdc_XXXX)"
SECRET="mdc-secret-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN"; exit 1; }
python3 -c 'import json,pandas' 2>/dev/null || { echo "SKIP: python3/pandas missing"; exit 0; }

TOKEN=$(python3 -c "
import hmac,hashlib,base64,json,time
b=lambda x: base64.urlsafe_b64encode(x).rstrip(b'=').decode()
h=b(b'{\"alg\":\"HS256\",\"typ\":\"JWT\"}')
p=b(json.dumps({'sub':'admin','role':0,'exp':int(time.time())+3600},separators=(',',':')).encode())
s=b(hmac.new('$SECRET'.encode(),f'{h}.{p}'.encode(),hashlib.sha256).digest())
print(f'{h}.{p}.{s}')")

# Point python_file at the repo's tools/ (the YAML uses the prod /opt path).
sed "s|/opt/tsmdm/scripts|$ROOT/tools|g" "$ROOT/pipelines/mdc_pipeline.yaml" > "$DATA/mdc_pipeline.yaml"
cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,"jwt_secret":"$SECRET","admin_password":"admin"}
EOF
"$GW_BIN" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break; sleep 0.2; done
GW="http://localhost:$PORT"; AUTH=(-H "Authorization: Bearer $TOKEN")
echo "Gateway ready on :$PORT"

qval() { curl -s -X POST "$GW/api/tables/query" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d "{\"sql\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1")}" \
  | python3 -c "import sys,json
try:
  d=json.load(sys.stdin); print(d['rows'][0][0] if d.get('rows') else '')
except: print('')"; }

# YAML validation
C=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GW/api/pipelines/preview-yaml" "${AUTH[@]}" \
     -H 'Content-Type: text/yaml' --data-binary @"$ROOT/pipelines/mdc_pipeline.yaml")
check "mdc_pipeline.yaml validates via preview-yaml" "[[ '$C' == '200' ]]"

# Seed (no commas, no empty fields)
curl -sf -X POST "$GW/api/ingest/csv?table=cdi_physical_party" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'source_id,system_name,src_last_name,src_first_name,src_middle_name,src_fullname,src_inn,birth_date,last_upd\nS1,CRM,IVANOV,IVAN,IVANOVICH,IVANOV IVAN IVANOVICH,123456789012,1980-01-01,2024-01-15\nS2,RMK,Ivanov,Ivan,Ivanovich,Ivanov Ivan Ivanovich,123456789012,1980-01-01,2024-01-10\nS3,CRM,PETROV,PETR,PETROVICH,PETROV PETR PETROVICH,987654321098,1990-05-15,2024-01-15' >/dev/null
curl -sf -X POST "$GW/api/ingest/csv?table=cdi_id_doc" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'source_id,person_source_id,category,src_number\nD1,S1,21,4500 123456\nD2,S2,21,4500 123456' >/dev/null

# Pipelines now live only in the catalog — register it via from-template
# instead of pipelines_dir auto-load.
curl -sf -X POST "$GW/api/pipelines/from-template" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d "{\"template_yaml\":$(python3 -c 'import json,sys;print(json.dumps(open(sys.argv[1]).read()))' "$DATA/mdc_pipeline.yaml")}" >/dev/null
PID=$(curl -sf "$GW/api/pipelines" "${AUTH[@]}" | python3 -c "import sys,json;[print(p['id']) for p in json.load(sys.stdin) if p['name']=='mdc_pipeline']" | head -1)
curl -sf -X POST "$GW/api/pipelines/$PID/run?wait=true" "${AUTH[@]}" >/dev/null

# CLEAN
check "clean: cio_last_name lowercased (S1 -> ivanov)" \
  "[[ \"\$(qval \"SELECT cio_last_name FROM mdc_person_clean WHERE source_id='S1'\")\" == 'ivanov' ]]"
check "clean: cio_inn digits only (S1)" \
  "[[ \"\$(qval \"SELECT cio_inn FROM mdc_person_clean WHERE source_id='S1'\")\" == '123456789012' ]]"
check "clean_id_doc: cio_number normalized (D1 -> 4500123456)" \
  "[[ \"\$(qval \"SELECT cio_number FROM mdc_id_doc_clean WHERE source_id='D1'\")\" == '4500123456' ]]"

# MATCH
check "match: S1 and S2 matched (same DUL/INN)" \
  "[[ \"\$(qval \"SELECT count(*) FROM mdc_person_matches WHERE (id_a='S1' AND id_b='S2') OR (id_a='S2' AND id_b='S1')\")\" -ge 1 ]]"
check "match: S3 not matched with S1" \
  "[[ -z \"\$(qval \"SELECT count(*) FROM mdc_person_matches WHERE (id_a='S3' AND id_b='S1') OR (id_a='S1' AND id_b='S3')\")\" ]]"

# ACCEPT (master map)
check "accept: S1 assigned a master_id" \
  "[[ -n \"\$(qval \"SELECT master_id FROM mdc_master_map WHERE source_id='S1'\")\" ]]"
check "accept: S1 and S2 share one master_id" \
  "[[ \"\$(qval \"SELECT master_id FROM mdc_master_map WHERE source_id='S1'\")\" == \"\$(qval \"SELECT master_id FROM mdc_master_map WHERE source_id='S2'\")\" ]]"

# MERGE
check "merge: master_person has a golden record" \
  "[[ \"\$(qval 'SELECT count(*) FROM master_person')\" -ge 1 ]]"
check "merge: SCD2 valid_from populated" \
  "[[ -n \"\$(qval 'SELECT valid_from FROM master_person LIMIT 1')\" ]]"

echo ""
echo "MDC pipeline test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
