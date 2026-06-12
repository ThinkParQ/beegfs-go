#!/usr/bin/env bash
# suite-buffer.sh — Ring buffer configuration tuning suite.
#
# Runs a 4×3 matrix of ring buffer sizes against GC frequencies at a fixed high
# subscriber count and max injection rate.
#
# The buffer size range is intentionally dense: previous results showed massive drops
# at 4M slots and no drops at 16M, but the actual minimum safe size is unknown.
# Intermediate sizes (8M, 12M) bracket the threshold.
#
# Buffer sizes tested (number of event slots):
#   4 194 304  ( 4M slots, ~ 36 GiB worst-case memory)  ← default (known to drop at 64 sub max-rate)
#   8 388 608  ( 8M slots, ~ 72 GiB worst-case memory)
#  12 582 912  (12M slots, ~108 GiB worst-case memory)
#  16 777 216  (16M slots, ~144 GiB worst-case memory)  (known clean at 64 sub max-rate)
#
# GC frequencies tested (pushes between GC runs):
#    104 857  ≈  2.5% of 4M  (aggressive GC)
#    419 430  ≈ 10%   of 4M  ← default ratio
#  1 677 721  ≈ 10%   of 16M (lazy GC)
#
# Constraint: gc_frequency must be < buffer_size. A GC frequency larger than the
# buffer size means GC can never run before the buffer is exhausted, guaranteeing drops.
# The 1M buffer is excluded because it is smaller than the largest GC frequency (1.6M).
#
# The on-diagonal entries (matched 10% ratio) represent the recommended configuration.
# Off-diagonal entries reveal the cost of over-aggressive GC (more CPU overhead) or
# under-aggressive GC (slower slot reclamation under backpressure).
#
# Usage:
#   ./suite-buffer.sh [OPTIONS]
#
# All options are forwarded to scale-perf-test.sh / run-perf-test.sh.
# Commonly overridden:
#   --duration SECS                        Time per run (default inherited from run-perf-test.sh: 30s)
#   --taskset-watch CPUS                   Pin beegfs-watch (e.g. "0-3")
#   --taskset-subscriber CPUS              Pin subscriber server (e.g. "4-11")
#   --taskset-logger CPUS                  Pin event logger (e.g. "12-13")
#   --subscribers-list "N"                 Override subscriber count (default: 64)
#   --out-dir DIR                          Override output directory (default: ./buffer-results)
#
# Example:
#   ./suite-buffer.sh --duration 120 --taskset-watch 0-3 --taskset-subscriber 4-11 --taskset-logger 12-13

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec bash "$SCRIPT_DIR/../scale-perf-test.sh" \
  --subscribers-list "64" \
  --frequency-list "0" \
  --buffer-size-list "4194304 8388608 12582912 16777216" \
  --buffer-gc-frequency-list "104857 419430 1677721" \
  --auto-refine \
  --coarse-duration 15 \
  --out-dir "./buffer-results" \
  "$@"
