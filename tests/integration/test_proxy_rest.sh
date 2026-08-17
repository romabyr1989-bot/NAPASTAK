#!/usr/bin/env bash
# Integration test for tsmdm_proxy REST server (Flask, :8443) against a live DFO.
# Self-contained: starts a DFO gateway, seeds data, launches rest_server alone
# (only needs flask + the /api/query/named endpoint). SKIPs if flask is missing.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW_BIN="$ROOT/build/release/bin/napastak_gateway"
PROXY="$ROOT/services/tsmdm_proxy"
GPORT=19320; RPORT=18450
DATA="$(mktemp -d -t dfo_pxrest_XXXX)"
SECRET="pxrest-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${PROXY_PID:-}" ]] && kill "$PROXY_PID" 2>/dev/null
            [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN"; exit 1; }
python3 -c 'import flask, flask_cors' 2>/dev/null || { echo "SKIP: flask/flask-cors not installed"; exit 0; }

# ── DFO gateway ──
cat > "$DATA/cfg.json" <<EOF
{"port":$GPORT,"data_dir":"$DATA","auth_enabled":true,"jwt_secret":"$SECRET","admin_password":"admin"}
EOF
"$GW_BIN" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do curl -sf "http://localhost:$GPORT/health" >/dev/null 2>&1 && break; sleep 0.2; done
TOKEN=$(curl -sf -X POST "http://localhost:$GPORT/api/auth/token" -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no token"; exit 1; }
GW="http://localhost:$GPORT"; AUTH=(-H "Authorization: Bearer $TOKEN")

# ── seed DFO ──
curl -sf -X POST "$GW/api/ingest/csv?table=master_person" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'party_id,mdm_id,crm_id,last_name,first_name,middle_name,birth_date,inn\nP1,M1,1-ABC,Ivanov,Ivan,Ivanovich,1980-01-01,123' >/dev/null
curl -sf -X POST "$GW/api/ingest/csv?table=crm_key_map" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'con_id,ext_cust_id,system_num\n1-ABC,EXT-001,CRM' >/dev/null
curl -sf -X POST "$GW/api/ingest/csv?table=agreement2party" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'crm_id,agreement_id,abs_cd,owner_id,created_ts,expired_ts,scopes_ls,agreement_type_cd,agreement_file_link,map__deleted,deleted_ts\n1-ABC,AGR-001,BKI,OWN-001,01/01/2024,12/31/2024,legal,BKI,http://file,,' >/dev/null

# ── launch the REST server alone ──
(cd "$PROXY" && exec env DFO_REST_URL="$GW" DFO_TOKEN="$TOKEN" REST_PORT=$RPORT \
    python3 -c "import rest_server; rest_server.run_rest_server()" ) > "$DATA/proxy.log" 2>&1 &
PROXY_PID=$!
UP=0
for i in {1..40}; do curl -sf -o /dev/null "http://localhost:$RPORT/tsmdm/get_crm_id/CRM/EXT-001" 2>/dev/null && { UP=1; break; }; sleep 0.25; done
if [[ "$UP" != 1 ]]; then
  if grep -qiE "ModuleNotFoundError|ImportError" "$DATA/proxy.log"; then
    echo "SKIP: REST server deps missing ($(grep -iE 'No module' "$DATA/proxy.log" | head -1))"
  else
    echo "SKIP: REST server did not start"; sed -n '1,5p' "$DATA/proxy.log"
  fi
  exit 0
fi
P="http://localhost:$RPORT"

# ── Test 1: getPersonInfo ──
R1=$(curl -sf -X POST "$P/tsmdm/getPersonInfo" -H 'Content-Type: application/json' \
  -d '{"personSearchClientData":{"personNames":{"personLastName":"Ivanov","personFirstName":"Ivan"},"personBirthDate":"1980-01-01"}}')
check "REST: getPersonInfo returns a JSON list" "[[ '$R1' == '['* ]]"
check "REST: getPersonInfo finds Ivanov" "[[ '$R1' == *Ivanov* ]]"

# ── Test 2: get_crm_id ──
R2=$(curl -sf "$P/tsmdm/get_crm_id/CRM/EXT-001")
check "REST: get_crm_id returns a list" "[[ '$R2' == '['* ]]"
check "REST: get_crm_id maps EXT-001 → 1-ABC" "[[ '$R2' == *1-ABC* ]]"

# ── Test 3: get_agreements ──
R3=$(curl -sf -X POST "$P/tsmdm/get_agreements" -H 'Content-Type: application/json' -d '{"CRM_ID":"1-ABC"}')
check "REST: get_agreements for 1-ABC returns AGR-001" "[[ '$R3' == *AGR-001* ]]"

# ── Test 4: search_or_create returns existing crm_id ──
R4=$(curl -sf -X POST "$P/tsmdm/search_or_create_client_fl" -H 'Content-Type: application/json' \
  -d '{"RequestId":"req-1","LastName":"Ivanov","Name":"Ivan","BirthDate":"1980-01-01"}')
check "REST: search_or_create returns CrmID 1-ABC" "[[ '$R4' == *1-ABC* ]]"

echo ""
echo "proxy-REST test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
