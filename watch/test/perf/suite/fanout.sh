#!/usr/bin/env bash
# fanout.sh — Subscriber fan-out scaling suite.
#
# Measures aggregate throughput as the number of subscribers doubles from 1 to 128
# at a fixed injection rate (default: 100µs). Isolates the dispatch/fan-out bottleneck:
# how efficiently Watch replicates each event to an increasing number of concurrent receivers.
#
# 10µs is used because:
#   - Max rate saturates the ring buffer before the fan-out path becomes the bottleneck,
#     causing drops at ~16 subscribers regardless of fan-out efficiency.
#   - 100µs (~10k events/sec) is so slow that Watch is idle between events and CPU stays
#     flat at all subscriber counts — the injector is the bottleneck, not fan-out.
#   - 10µs (~100k events/sec) puts real pressure on the dispatch path at higher subscriber
#     counts while leaving headroom to avoid drops through the full 1–128 subscriber range.
# If drops appear at high subscriber counts, increase the frequency (e.g. 25µs or 50µs).
#
# Expected pattern: aggregate avg_eps grows sub-linearly as subscribers increase due to
# the fixed cost of serializing each event to multiple gRPC streams. The inflection point
# identifies where the fan-out path becomes the bottleneck.
#
# Usage:
#   ./suite-fanout.sh [OPTIONS]
#
# All options are forwarded to scale-perf-test.sh / run-perf-test.sh.
# Commonly overridden:
#   --duration SECS                        Time per run (default inherited from run-perf-test.sh: 30s)
#   --taskset-watch CPUS                   Pin beegfs-watch (e.g. "0-3")
#   --taskset-subscriber CPUS              Pin subscriber server (e.g. "4-11")
#   --taskset-logger CPUS                  Pin event logger (e.g. "12-13")
#   --subscribers-list "N N N..."          Override the subscriber counts
#   --out-dir DIR                          Override output directory (default: ./fanout-results)
#
# Example:
#   ./suite-fanout.sh --duration 120 --taskset-watch 0-3 --taskset-subscriber 4-11 --taskset-logger 12-13

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec bash "$SCRIPT_DIR/../scale-perf-test.sh" \
  --subscribers-list "1 2 4 8 16 32 64 128" \
  --frequency-list "25µs" \
  --auto-refine \
  --coarse-duration 15 \
  --out-dir "./fanout-results" \
  "$@"
