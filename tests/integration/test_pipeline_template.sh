#!/usr/bin/env bash
# Integration tests for YAML template expansion + POST /api/pipelines/from-template
# and the tools/pipeline_gen.py CLI.
#
# Covers:
#   1. from-template: vars: block + external vars{} (external overrides file).
#   2. unresolved {{ var }} → pipeline still saved, LOG_WARN emitted.
#   3. pipeline_gen.py --dry-run: lists rows, performs NO POST.
#   4. pipeline_gen.py (real): creates one pipeline per TSV row.
#
# Uses python3 (no jq dependency). Field values are extracted into plain shell
# variables BEFORE each check to avoid nested-eval quoting pitfalls.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/dfo_gateway"
PORT=19302
DATA="$(mktemp -d -t dfo_tmpl_XXXX)"
SECRET="tmpl-test-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW" ]] || { echo "missing $GW"; exit 1; }
python3 -c 'import json,urllib.request' 2>/dev/null || { echo "SKIP: python3 stdlib missing"; exit 0; }

cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,
 "jwt_secret":"$SECRET","admin_password":"admin"}
EOF
"$GW" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break; sleep 0.2; done
GW_URL="http://localhost:$PORT"
TOKEN=$(curl -sf -X POST "$GW_URL/api/auth/token" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no admin token"; exit 1; }
echo "Gateway ready on :$PORT"
AUTH=(-H "Authorization: Bearer $TOKEN")

# Extract a dotted field from JSON in file $1 (e.g. name, steps.0.target_table).
fieldf() { python3 -c "
import sys,json
d=json.load(open('$1'))
for k in '$2'.split('.'):
    d = d[int(k)] if isinstance(d,list) else d.get(k)
    if d is None: print(''); sys.exit()
print(d)
"; }

# ── Template with a vars: block (schema, pk_column defaults) ──────────────────
cat > "$DATA/tmpl.yaml" <<'YAML'
vars:
  schema: public
  pk_column: id
name: "cdh_{{ table_name }}"
enabled: true
steps:
  - id: s1
    connector_type: postgresql
    connector_config:
      host: "localhost"
      table: "{{ table_name }}"
      schema: "{{ schema }}"
    target_table: "{{ schema }}.{{ table_name }}_raw"
    scd2_business_key: "{{ pk_column }}"
    scd2_effective_from_col: valid_from
    scd2_effective_to_col: valid_to
YAML

# ── Test 1: vars: + external vars{} (external schema overrides file default) ──
python3 -c "import json;print(json.dumps({'template_yaml':open('$DATA/tmpl.yaml').read(),'vars':{'table_name':'orders','schema':'sales'}}))" > "$DATA/body1.json"
curl -sf -X POST "$GW_URL/api/pipelines/from-template" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d @"$DATA/body1.json" > "$DATA/resp1.json"
T1_NAME=$(fieldf "$DATA/resp1.json" name)
T1_TT=$(fieldf "$DATA/resp1.json" steps.0.target_table)
T1_BK=$(fieldf "$DATA/resp1.json" steps.0.scd2_business_key)
T1_CFG=$(fieldf "$DATA/resp1.json" steps.0.connector_config)
T1_ID=$(fieldf "$DATA/resp1.json" id)
check "from-template: name = cdh_orders"                 "[[ '$T1_NAME' == 'cdh_orders' ]]"
check "from-template: schema overridden → sales.orders_raw" "[[ '$T1_TT' == 'sales.orders_raw' ]]"
check "from-template: connector_config carries schema=sales" "[[ '$T1_CFG' == *sales* ]]"
check "from-template: pk_column from vars: default (id)"  "[[ '$T1_BK' == 'id' ]]"
check "from-template: returns saved pipeline with an id"  "[[ -n '$T1_ID' ]]"
check "from-template: pipeline persisted (listed)" \
  "curl -sf '$GW_URL/api/pipelines' \"\${AUTH[@]}\" | grep -q cdh_orders"

# ── Test 2: unresolved {{ var }} → still saved, WARN logged ───────────────────
printf '{"template_yaml":"name: cdh_{{ table_name }}\\nenabled: true\\nsteps: []","vars":{}}' > "$DATA/body2.json"
curl -sf -X POST "$GW_URL/api/pipelines/from-template" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d @"$DATA/body2.json" > "$DATA/resp2.json"
T2_EN=$(fieldf "$DATA/resp2.json" enabled)
T2_NAME=$(fieldf "$DATA/resp2.json" name)
check "from-template: unresolved → pipeline created (enabled)" "[[ '$T2_EN' == 'True' ]]"
check "from-template: unresolved name kept verbatim" "[[ '$T2_NAME' == 'cdh_{{ table_name }}' ]]"
check "from-template: LOG_WARN 'unresolved variable' in log" "grep -q 'unresolved variable' '$DATA/gw.log'"

# ── Test 3: pipeline_gen.py --dry-run (no gateway calls) ──────────────────────
printf 'table_name\tpk_column\norders\tid\nusers\tuid\n' > "$DATA/cfg.tsv"
OUT=$(python3 "$ROOT/tools/pipeline_gen.py" \
  --gateway "$GW_URL" --token "$TOKEN" \
  --template "$DATA/tmpl.yaml" --config "$DATA/cfg.tsv" --dry-run 2>&1)
check "pipeline_gen --dry-run: mentions orders" "[[ '$OUT' == *orders* ]]"
check "pipeline_gen --dry-run: mentions users"  "[[ '$OUT' == *users* ]]"
check "pipeline_gen --dry-run: did NOT create cdh_users" \
  "! curl -sf '$GW_URL/api/pipelines' \"\${AUTH[@]}\" | grep -q cdh_users"

# ── Test 4: pipeline_gen.py real run (parallel) creates both ─────────────────
python3 "$ROOT/tools/pipeline_gen.py" \
  --gateway "$GW_URL" --token "$TOKEN" \
  --template "$DATA/tmpl.yaml" --config "$DATA/cfg.tsv" --parallel 4 >/dev/null 2>&1
LIST=$(curl -sf "$GW_URL/api/pipelines" "${AUTH[@]}")
check "pipeline_gen: cdh_orders created" "echo '$LIST' | grep -q cdh_orders"
check "pipeline_gen: cdh_users created"  "echo '$LIST' | grep -q cdh_users"

echo ""
echo "Template test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
