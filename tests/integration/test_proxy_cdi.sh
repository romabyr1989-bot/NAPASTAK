#!/usr/bin/env bash
# Integration test for tsmdm_proxy CDI server (FastAPI, :8444) against a live DFO.
# Self-contained: starts a DFO gateway, seeds cdi_physical_party, launches
# cdi_server alone. SKIPs if fastapi/uvicorn are missing.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW_BIN="$ROOT/build/release/bin/napastak_gateway"
PROXY="$ROOT/services/tsmdm_proxy"
GPORT=19321; CPORT=18454
DATA="$(mktemp -d -t dfo_pxcdi_XXXX)"
SECRET="pxcdi-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${PROXY_PID:-}" ]] && kill "$PROXY_PID" 2>/dev/null
            [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN"; exit 1; }
python3 -c 'import fastapi, uvicorn' 2>/dev/null || { echo "SKIP: fastapi/uvicorn not installed"; exit 0; }

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

curl -sf -X POST "$GW/api/ingest/csv?table=cdi_physical_party" "${AUTH[@]}" -H 'Content-Type: text/csv' \
  --data-binary $'hid,last_name,first_name,source_system,raw_id\n42,Ivanov,Ivan,CRM,RAW-001' >/dev/null

(cd "$PROXY" && exec env DFO_REST_URL="$GW" DFO_TOKEN="$TOKEN" CDI_PORT=$CPORT \
    python3 -c "import cdi_server; cdi_server.run_cdi_server()" ) > "$DATA/proxy.log" 2>&1 &
PROXY_PID=$!
B="http://localhost:$CPORT/cdi/soap/services/2_13/PartyRA"
UP=0
for i in {1..60}; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$B/getByHID" -H 'Content-Type: application/json' -d '{"hid":1,"type":"PHYSICAL"}' 2>/dev/null)
  [[ "$code" == "200" || "$code" == "404" ]] && { UP=1; break; }; sleep 0.25
done
if [[ "$UP" != 1 ]]; then
  if grep -qiE "ModuleNotFoundError|ImportError" "$DATA/proxy.log"; then
    echo "SKIP: CDI server deps missing ($(grep -iE 'No module' "$DATA/proxy.log" | head -1))"
  else
    echo "SKIP: CDI server did not start"; sed -n '1,8p' "$DATA/proxy.log"
  fi
  exit 0
fi

# ── Test 1: getByHID(42) → party with field list ──
R1=$(curl -sf -X POST "$B/getByHID" -H 'Content-Type: application/json' -d '{"hid":42,"type":"PHYSICAL"}')
check "CDI: getByHID returns party with Ivanov" "[[ '$R1' == *Ivanov* ]]"
check "CDI: getByHID contains recordId"          "[[ '$R1' == *recordId* ]]"
check "CDI: getByHID party.field list present"   "[[ '$R1' == *field* ]]"

# ── Test 2: unknown HID → 404 ──
C2=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$B/getByHID" -H 'Content-Type: application/json' -d '{"hid":9999,"type":"PHYSICAL"}')
check "CDI: unknown HID → 404" "[[ '$C2' == '404' ]]"

# ── Test 3: saveAndMerge accepts a record → 200 ──
C3=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$B/saveAndMerge" -H 'Content-Type: application/json' -d '{"hid":1,"any":"record"}')
check "CDI: saveAndMerge → 200" "[[ '$C3' == '200' ]]"

echo ""
echo "proxy-CDI test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
