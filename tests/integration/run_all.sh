#!/usr/bin/env bash
# run_all.sh — Run all integration tests and summarize results
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

TESTS=(
    "$SCRIPT_DIR/test_rbac.sh"
    "$SCRIPT_DIR/test_audit.sh"
    "$SCRIPT_DIR/test_txn.sh"
    "$SCRIPT_DIR/test_replication.sh"
    "$SCRIPT_DIR/test_python_step.sh"
    "$SCRIPT_DIR/test_scala_step.sh"
    "$SCRIPT_DIR/test_scd2.sh"
    "$SCRIPT_DIR/test_scd2_hash.sh"
    "$SCRIPT_DIR/test_gp_connector.sh"
    "$SCRIPT_DIR/test_oracle_connector.sh"
    "$SCRIPT_DIR/test_pipeline_template.sh"
)

TOTAL_PASS=0
TOTAL_FAIL=0
SUITE_PASS=()
SUITE_FAIL=()

for TEST in "${TESTS[@]}"; do
    NAME=$(basename "$TEST")
    echo "========================================"
    echo "Running: $NAME"
    echo "========================================"

    if bash "$TEST"; then
        echo "SUITE PASS: $NAME"
        SUITE_PASS+=("$NAME")
    else
        echo "SUITE FAIL: $NAME"
        SUITE_FAIL+=("$NAME")
    fi
    echo ""
done

echo "========================================"
echo "Summary"
echo "========================================"
echo "Passed suites (${#SUITE_PASS[@]}):"
for s in "${SUITE_PASS[@]}"; do echo "  PASS: $s"; done
echo "Failed suites (${#SUITE_FAIL[@]}):"
for s in "${SUITE_FAIL[@]}"; do echo "  FAIL: $s"; done
echo ""

if [ "${#SUITE_FAIL[@]}" -eq 0 ]; then
    echo "All integration test suites PASSED."
    exit 0
else
    echo "Some integration test suites FAILED."
    exit 1
fi
