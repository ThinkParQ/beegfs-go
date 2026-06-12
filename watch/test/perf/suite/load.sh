#!/usr/bin/env bash
# suite-load.sh — Injection load saturation suite.
#
# Runs a fixed high subscriber count (64) across a range of injection frequencies,
# from maximum rate down to a slow steady pace. Identifies the saturation point:
# the frequency at which the system stops keeping up and begins dropping events or
# showing degraded throughput.
#
# Expected pattern: avg_eps stays flat (rate-limited by injection speed) until the
# injection rate exceeds dispatch capacity, at which point avg_eps plateaus or drops
# and dropped events appear.
#
# The frequency ladder is intentionally dense around the observed saturation cliff
# (between 10µs and 100µs at 64 subscribers). The 10x jumps of a naive log scale
# would leave the actual threshold completely hidden.
#
# Usage:
#   ./suite-load.sh [OPTIONS]
#
# All options are forwarded to scale-perf-test.sh / run-perf-test.sh.
# Commonly overridden:
#   --duration SECS                        Time per run (default inherited from run-perf-test.sh: 30s)
#   --taskset-watch CPUS                   Pin beegfs-watch (e.g. "0-3")
#   --taskset-subscriber CPUS              Pin subscriber server (e.g. "4-11")
#   --taskset-logger CPUS                  Pin event logger (e.g. "12-13")
#   --subscribers-list "N"                 Override subscriber count (default: 64)
#   --frequency-list "F F F..."            Override frequency ladder
#   --out-dir DIR                          Override output directory (default: ./load-results)
#
# Example:
#   ./suite-load.sh --duration 120 --taskset-watch 0-3 --taskset-subscriber 4-11 --taskset-logger 12-13

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec bash "$SCRIPT_DIR/../scale-perf-test.sh" \
  --subscribers-list "64" \
  --frequency-list "0 10µs 20µs 30µs 50µs 75µs 100µs 250µs 500µs 1ms" \
  --out-dir "./load-results" \
  "$@"
