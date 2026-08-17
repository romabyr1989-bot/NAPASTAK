#!/usr/bin/env bash
# End-to-end Avro read: Kafka topic (Confluent Avro) -> DFO table.
#
# Requires a live broker + Schema Registry. Set:
#   KAFKA_BROKERS, SCHEMA_REGISTRY_URL, AVRO_TEST_TOPIC
# Precondition: a producer wrote an Avro message with schema
#   {"type":"record","name":"Person","fields":[
#     {"name":"id","type":"long"},
#     {"name":"name","type":"string"},
#     {"name":"inn","type":["null","string"]}]}
#   data: {id: 42, name: "Ivanov", inn: null}
# Without the env (e.g. CI) the test SKIPs — it never fails for missing infra.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW_BIN="$ROOT/build/release/bin/napastak_gateway"
KAFKA_SO="$ROOT/build/release/lib/kafka_connector.so"
PORT=19394
DATA="$(mktemp -d -t dfo_avro_XXXX)"
SECRET="avro-secret-$$"
PASS=0; FAIL=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN"; exit 1; }
[[ -f "$KAFKA_SO" ]] || { echo "SKIP: kafka_connector.so not built (no librdkafka)"; exit 0; }
if [[ -z "${KAFKA_BROKERS:-}" || -z "${SCHEMA_REGISTRY_URL:-}" || -z "${AVRO_TEST_TOPIC:-}" ]]; then
    echo "SKIP: set KAFKA_BROKERS, SCHEMA_REGISTRY_URL, AVRO_TEST_TOPIC to run"
    exit 0
fi

TOKEN=$(python3 -c "
import hmac,hashlib,base64,json,time
b=lambda x: base64.urlsafe_b64encode(x).rstrip(b'=').decode()
h=b(b'{\"alg\":\"HS256\",\"typ\":\"JWT\"}')
p=b(json.dumps({'sub':'admin','role':0,'exp':int(time.time())+3600},separators=(',',':')).encode())
s=b(hmac.new('$SECRET'.encode(),f'{h}.{p}'.encode(),hashlib.sha256).digest())
print(f'{h}.{p}.{s}')")

cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,"jwt_secret":"$SECRET",
 "admin_password":"admin","plugins_dir":"$ROOT/build/release/lib"}
EOF
"$GW_BIN" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break; sleep 0.2; done
GW="http://localhost:$PORT"; AUTH=(-H "Authorization: Bearer $TOKEN")
echo "Gateway ready on :$PORT"

query_val() { curl -s -X POST "$GW/api/tables/query" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d "{\"sql\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1")}" \
  | python3 -c "import sys,json
try:
  d=json.load(sys.stdin); print(d['rows'][0][0] if d.get('rows') else '')
except: print('')"; }
query_schema() { curl -s -X POST "$GW/api/tables/query" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d "{\"sql\":\"SELECT * FROM $1 LIMIT 1\"}" | python3 -c "import sys,json;print(' '.join(json.load(sys.stdin).get('columns',[])))"; }

CONFIG=$(python3 -c "import json,os;print(json.dumps(json.dumps({
  'brokers':os.environ['KAFKA_BROKERS'],'topic':os.environ['AVRO_TEST_TOPIC'],
  'group_id':'dfo_avro_test','data_format':'avro',
  'schema_registry_url':os.environ['SCHEMA_REGISTRY_URL']})))")
PIPELINE="{\"name\":\"avro_test\",\"enabled\":true,\"steps\":[{\"id\":\"read\",\"connector_type\":\"kafka\",\"connector_config\":$CONFIG,\"target_table\":\"avro_result\"}]}"

PID=$(curl -sf -X POST "$GW/api/pipelines" "${AUTH[@]}" -H 'Content-Type: application/json' -d "$PIPELINE" \
      | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))")
curl -sf -X POST "$GW/api/pipelines/$PID/run" "${AUTH[@]}" >/dev/null
sleep 5

check "avro: column id present"          "query_schema avro_result | grep -q 'id'"
check "avro: column name present"        "query_schema avro_result | grep -q 'name'"
check "avro: name value = Ivanov"        "[[ \"\$(query_val \"SELECT name FROM avro_result WHERE id='42'\")\" == 'Ivanov' ]]"
check "avro: union null (inn empty)"     "[[ -z \"\$(query_val \"SELECT inn FROM avro_result WHERE id='42'\")\" ]]"
check "avro: schema cached (registry hit)" "grep -q 'cached avro schema' '$DATA/gw.log'"

echo ""
echo "kafka avro test: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
