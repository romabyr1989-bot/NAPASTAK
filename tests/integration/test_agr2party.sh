#!/usr/bin/env bash
# Tests the agr2party pipeline (AgreementConnector) + resync UPDATE.
# python_file scripts call back into DFO; they inherit DFO_REST_URL/DFO_TOKEN
# exported into the gateway environment (an admin JWT minted offline from the
# known jwt_secret, so the subprocess can authenticate with role=ADMIN).

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW_BIN="$ROOT/build/release/bin/dfo_gateway"
PORT=19364
DATA="$(mktemp -d -t dfo_agr_XXXX)"
SECRET="agr2party-secret-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN"; exit 1; }
python3 -c 'import json,pandas' 2>/dev/null || { echo "SKIP: python3/pandas missing"; exit 0; }

# Admin JWT (role=0) signed with SECRET, valid 1h.
TOKEN=$(python3 -c "
import hmac,hashlib,base64,json,time
b=lambda x: base64.urlsafe_b64encode(x).rstrip(b'=').decode()
h=b(b'{\"alg\":\"HS256\",\"typ\":\"JWT\"}')
p=b(json.dumps({'sub':'admin','role':0,'exp':int(time.time())+3600},separators=(',',':')).encode())
s=b(hmac.new('$SECRET'.encode(),f'{h}.{p}'.encode(),hashlib.sha256).digest())
print(f'{h}.{p}.{s}')")

# Point python_file at the repo's tools/ (the YAML uses the prod /opt path).
mkdir -p "$DATA/pipes"
sed "s|/opt/tsmdm/scripts|$ROOT/tools|g" "$ROOT/pipelines/agr2party.yaml" > "$DATA/pipes/agr2party.yaml"
cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,"jwt_secret":"$SECRET","admin_password":"admin","pipelines_dir":"$DATA/pipes"}
EOF
# export so python_file subprocesses (connector/resync) inherit them.
# MASTER_API_URL → a dead port so the external CDI lookup fails fast (UNDEFINED).
export DFO_REST_URL="http://localhost:$PORT"
export DFO_TOKEN="$TOKEN"
export MASTER_API_URL="http://127.0.0.1:1"
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

# ── 1. YAML validates ──
C=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GW/api/pipelines/preview-yaml" "${AUTH[@]}" \
     -H 'Content-Type: text/yaml' --data-binary @"$ROOT/pipelines/agr2party.yaml")
check "agr2party.yaml validates via preview-yaml" "[[ '$C' == '200' ]]"

# ── 2. Seed. DFO CSV ingest collapses EMPTY MIDDLE cells (shifting columns),
#       so the empty deleted_ts is the LAST column (trailing empties survive). ──
curl -sf -X POST "$GW/api/ingest/csv?table=consents_agreements" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'agreement_id,abs_cd,owner_id,agreement_type_cd,created_ts,expired_ts,last_upd_ts,scopes_ls,agreement_file_link,deleted_ts\nAGR-001,SiebelCRM,OWN-001,3,2024-01-01,2024-12-31,2024-01-15,legal,http://f1,\nAGR-002,SiebelCRM,OWN-002,3,2024-01-01,2024-12-31,2024-01-15,legal,http://f2,' >/dev/null
curl -sf -X POST "$GW/api/ingest/csv?table=cdi_physical_party" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'owner_id,party_id,last_name,first_name,middle_name,birth_date,inn,source_system\nOWN-001,P-001,ivanov,ivan,ivanovich,1980-01-01,123456789012,sbl\nOWN-002,P-002,petrov,petr,petrovich,1990-05-15,987654321098,sbl' >/dev/null
curl -sf -X POST "$GW/api/ingest/csv?table=crm_key_updates" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'old_id,new_id,last_upd\nOLD-X,NEW-X,2024-01-16' >/dev/null

# ── 3. Run agr2party ──
PID=$(curl -sf "$GW/api/pipelines" "${AUTH[@]}" | python3 -c "import sys,json;[print(p['id']) for p in json.load(sys.stdin) if p['name']=='agr2party']" | head -1)
curl -sf -X POST "$GW/api/pipelines/$PID/run" "${AUTH[@]}" >/dev/null

check "agr2party: agreement2party created (rows > 0)" \
  "[[ \"\$(qval 'SELECT count(*) FROM agreements_agreement2party')\" -gt 0 ]]"
check "agr2party: AGR-001 present with crm_id" \
  "[[ -n \"\$(qval \"SELECT crm_id FROM agreements_agreement2party WHERE agreement_id='AGR-001'\")\" ]]"
check "agr2party: owner enrichment (AGR-001 last_name=ivanov from CDI)" \
  "[[ \"\$(qval \"SELECT last_name FROM agreements_agreement2party WHERE agreement_id='AGR-001'\")\" == 'ivanov' ]]"
check "agr2party: map__created populated" \
  "[[ -n \"\$(qval 'SELECT map__created FROM agreements_agreement2party LIMIT 1')\" ]]"
check "agr2party: BKI type mapped (agreement_type_cd 3 -> BKI)" \
  "[[ \"\$(qval \"SELECT agreement_type_cd FROM agreements_agreement2party WHERE agreement_id='AGR-001'\")\" == 'BKI' ]]"

# ── 4. resync UPDATE (direct invocation, dedicated table — the connector-built
#       agreement2party carries a 27-col schema, so resync runs on its own hub) ──
curl -sf -X POST "$GW/api/ingest/csv?table=agreements_resync_test" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'agreement_id,crm_id,map__lastupd,map__deleted\nR-1,OLD-123,2024-01-01,' >/dev/null
HUB_TABLE=agreements_resync_test python3 -c "
import pandas as pd
df=pd.DataFrame([{'old_id':'OLD-123','new_id':'NEW-456'}])
exec(open('$ROOT/tools/agr2party/resync.py').read())
" 2>/dev/null
check "resync: UPDATE remapped crm_id OLD-123 -> NEW-456" \
  "[[ \"\$(qval \"SELECT crm_id FROM agreements_resync_test WHERE agreement_id='R-1'\")\" == 'NEW-456' ]]"

echo ""
echo "agr2party test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
