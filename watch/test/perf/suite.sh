#!/usr/bin/env bash
# suite.sh — Master test suite runner.
#
# Runs all suites in sequence (fanout → load → buffer), each in its own output
# subdirectory. Each suite manages its own build.
#
# Usage:
#   ./suite.sh [OPTIONS]
#
# Options:
#   --out-dir DIR           Root output directory (default: ./suite-results)
#   --suites "NAME NAME..."  Suites to run in order (default: "fanout load buffer")
#
# All other options are forwarded to every suite unchanged.
# Commonly used:
#   --duration SECS
#   --taskset-watch CPUS  --taskset-subscriber CPUS  --taskset-logger CPUS
#   --mgmt-address ADDR
#
# Example:
#   ./suite.sh --duration 120 --taskset-watch 0-3 --taskset-subscriber 4-11 --taskset-logger 12-13

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

OUT_DIR="./suite-results"
SUITES="fanout load buffer"
PASSTHROUGH=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)  OUT_DIR="$2"; shift 2 ;;
    --suites)   SUITES="$2";  shift 2 ;;
    *)          PASSTHROUGH+=("$1"); shift ;;
  esac
done

RUN_TS="$(date +%Y-%m-%dT%H:%M:%S)"
OUT_DIR="$OUT_DIR/$RUN_TS"
mkdir -p "$OUT_DIR"
read -ra SUITE_LIST <<< "$SUITES"

echo ""
echo "========================================"
echo " Master Suite Runner"
echo " Suites:  ${SUITE_LIST[*]}"
echo " Out dir: $OUT_DIR"
echo "========================================"

FAILED=()

for SUITE in "${SUITE_LIST[@]}"; do
  SUITE_SCRIPT="$SCRIPT_DIR/suite/$SUITE.sh"
  if [[ ! -x "$SUITE_SCRIPT" ]]; then
    echo "ERROR: suite not found or not executable: $SUITE_SCRIPT" >&2
    FAILED+=("$SUITE")
    continue
  fi

  echo ""
  echo "========================================"
  echo " Suite: $SUITE"
  echo "========================================"

  if bash "$SUITE_SCRIPT" \
       --out-dir "$OUT_DIR/$SUITE" \
       "${PASSTHROUGH[@]}"; then
    echo " Suite $SUITE complete. Results: $OUT_DIR/$SUITE"
  else
    echo " Suite $SUITE FAILED (exit $?)." >&2
    FAILED+=("$SUITE")
  fi
done

echo ""
echo "========================================"
echo " Master Suite Runner Complete"
if [[ ${#FAILED[@]} -eq 0 ]]; then
  echo " All suites passed."
else
  echo " Failed suites: ${FAILED[*]}"
fi
echo " Results: $OUT_DIR"
echo "========================================"

[[ ${#FAILED[@]} -eq 0 ]]
