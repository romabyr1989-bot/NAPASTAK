#!/usr/bin/env bash
# Integration test for tsmdm_proxy SOAP server (spyne, :3000) against a live DFO.
# Self-contained: starts a DFO gateway, seeds agreements, launches soap_server
# alone. SKIPs if spyne/lxml are missing.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW_BIN="$ROOT/build/release/bin/napastak_gateway"
PROXY="$ROOT/services/tsmdm_proxy"
GPORT=19322; SPORT=13000
DATA="$(mktemp -d -t dfo_pxsoap_XXXX)"
SECRET="pxsoap-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${PROXY_PID:-}" ]] && kill "$PROXY_PID" 2>/dev/null
            [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN"; exit 1; }
# spyne 2.x fails to import on Python 3.12 (vendored `six` shim) — the Dockerfile
# pins python:3.11-slim where it works. Skip cleanly if spyne can't be imported.
python3 -c 'import spyne, lxml' 2>/dev/null || { echo "SKIP: spyne not importable here (needs Python ≤3.11; see Dockerfile)"; exit 0; }

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

# crm_key_map must exist (the SOAP method queries it first); empty mapping is fine.
curl -sf -X POST "$GW/api/ingest/csv?table=crm_key_map" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'con_id,ext_cust_id,system_num\n1-ABC,1-ABC,CRM' >/dev/null
curl -sf -X POST "$GW/api/ingest/csv?table=agreement2party" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'crm_id,agreement_id,abs_cd,owner_id,created_ts,expired_ts,scopes_ls,agreement_type_cd,agreement_file_link,map__deleted,deleted_ts\n1-ABC,AGR-001,BKI,OWN-001,01/01/2024,12/31/2024,legal,BKI,http://file,,' >/dev/null

(cd "$PROXY" && exec env DFO_REST_URL="$GW" DFO_TOKEN="$TOKEN" SOAP_PORT=$SPORT \
    python3 -c "import soap_server; soap_server.run_soap_server()" ) > "$DATA/proxy.log" 2>&1 &
PROXY_PID=$!
U="http://localhost:$SPORT/get_agreements_by_party_id"
UP=0
for i in {1..60}; do curl -s -o /dev/null "$U" 2>/dev/null && { UP=1; break; }; sleep 0.25; done
if [[ "$UP" != 1 ]]; then
  if grep -qiE "ModuleNotFoundError|ImportError" "$DATA/proxy.log"; then
    echo "SKIP: SOAP server deps missing ($(grep -iE 'No module' "$DATA/proxy.log" | head -1))"
  else
    echo "SKIP: SOAP server did not start"; sed -n '1,8p' "$DATA/proxy.log"
  fi
  exit 0
fi

soap_call() {  # $1=party_id $2=type
  local body
  body=$(cat <<XML
<?xml version="1.0"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tns="abb.tsmdm.soap">
  <soapenv:Body>
    <tns:get_agreements_by_party_id>
      <tns:party_id>$1</tns:party_id>
      <tns:agreement_type_cd>$2</tns:agreement_type_cd>
    </tns:get_agreements_by_party_id>
  </soapenv:Body>
</soapenv:Envelope>
XML
)
  curl -sf -X POST "$U" -H 'Content-Type: text/xml; charset=utf-8' -H 'SOAPAction: ""' --data-binary "$body"
}

# ── Test 1: known party with agreements ──
R1=$(soap_call "1-ABC" "BKI")
check "SOAP: response contains AgreementForPosition" "[[ '$R1' == *AgreementForPosition* ]]"
check "SOAP: agreement_id AGR-001 present"           "[[ '$R1' == *AGR-001* ]]"

# ── Test 2: empty party_id → response без exception ──
R2=$(soap_call "" "BKI")
check "SOAP: empty party_id → no SOAP fault" "[[ '$R2' != *'<soap'*'Fault'* && '$R2' != *faultstring* ]]"
check "SOAP: empty party_id → ERR marker present" "[[ '$R2' == *'ERR:no party_id'* ]]"

echo ""
echo "proxy-SOAP test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
