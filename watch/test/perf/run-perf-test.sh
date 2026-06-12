#!/usr/bin/env bash
# run-perf-test.sh — Watch subscriber dispatch performance test driver.
#
# Builds the required binaries, then runs a matrix of scenarios against a live
# beegfs-watch instance (started and stopped per scenario).
#
# Event source (choose one):
#   Synthetic (default): test-v2-fileeventlogger injects events via the watch socket.
#   Real mount:          Pass --skip-logger and set --socket-path to the metadata
#                        service's sysFileEventLogTarget (e.g. /run/beegfs/eventlog).
#                        beegfs-watch will create the socket there and the real metadata
#                        service will connect and send events. Use --workload-cmd to
#                        run a command that drives filesystem activity on the mount.
#
# Prerequisites:
#   - A running BeeGFS management service reachable at --mgmt-address
#   - Go toolchain in PATH (for building)
#   - curl in PATH (for pprof collection)
#
# Usage:
#   ./run-perf-test.sh [OPTIONS]
#
# Options:
#   --mgmt-address ADDR     BeeGFS mgmtd address (default: 127.0.0.1:8010)
#   --mgmt-auth-file PATH   Path to conn.auth (default: /etc/beegfs/conn.auth)
#   --mgmt-tls-disable      Disable TLS to mgmtd (flag, no value)
#   --socket-path PATH      Unix socket path (default: /tmp/perf-test-events.sock)
#                           For real events set this to sysFileEventLogTarget from beegfs-meta.conf
#   --skip-logger           Do not start test-v2-fileeventlogger; use real metadata events instead
#   --workload-cmd CMD      Shell command to run as the event workload (only used with --skip-logger)
#                           e.g. --workload-cmd 'find /mnt/beegfs -type f > /dev/null'
#   --duration SECS         Seconds per scenario (default: 30)
#   --ack-frequency DUR     Ack frequency for subscribers (default: 1s)
#   --out-dir DIR           Output directory for results (default: ./perf-results)
#   --skip-build            Skip binary build step (use binaries already in PATH or out-dir)
#   --watch-binary PATH     Path to beegfs-watch binary (built or pre-existing)
#   --log-level N           beegfs-watch log level 0-5 (default: 2 = Warn, lower reduces noise)
#   --frequency DUR         Event injection frequency for test-v2-fileeventlogger (default: 100µs, use 0 for max rate)
#   --workers N             Number of concurrent logger connections (default: 1)
#   --subscribers N         Number of subscriber servers (default: 1)
#   --taskset-watch CPUS    Pin beegfs-watch to these CPUs (e.g. "2-5"); requires taskset
#   --taskset-subscriber CPUS  Pin perf-test-subscriber to these CPUs (e.g. "6-7")
#   --taskset-logger CPUS   Pin test-v2-fileeventlogger to these CPUs (e.g. "0-1")
#   --event-buffer-size N   Ring buffer size in bytes (default: 4194304)
#   --event-buffer-gc-frequency N  Run GC every N pushes (default: 419430)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# ── Defaults ─────────────────────────────────────────────────────────────────
BUILD_ROOT="$REPO_ROOT"  # override with --build-root to build from a different source tree
MGMT_ADDRESS="127.0.0.1:8010"
MGMT_AUTH_FILE="/etc/beegfs/conn.auth"
MGMT_TLS_DISABLE=false
SOCKET_PATH="/tmp/perf-test-events.sock"
DURATION=30
ACK_FREQUENCY="1s"
OUT_DIR="./perf-results"
SKIP_BUILD=false
SKIP_LOGGER=false
WORKLOAD_CMD=""
WATCH_BINARY=""
LOG_LEVEL=2
FREQUENCY="100µs"
WORKERS=1
SUBSCRIBERS=1
TASKSET_WATCH=""
TASKSET_SUBSCRIBER=""
TASKSET_LOGGER=""
EVENT_BUFFER_SIZE=4194304
EVENT_BUFFER_GC_FREQUENCY=419430
PPROF_PORT=6061  # internal; not exposed as a flag

# ── Parse flags ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mgmt-address)      MGMT_ADDRESS="$2";  shift 2 ;;
    --mgmt-auth-file)    MGMT_AUTH_FILE="$2"; shift 2 ;;
    --mgmt-tls-disable)  MGMT_TLS_DISABLE=true; shift ;;
    --socket-path)       SOCKET_PATH="$2";   shift 2 ;;
    --duration)          DURATION="$2";      shift 2 ;;
    --ack-frequency)     ACK_FREQUENCY="$2"; shift 2 ;;
    --out-dir)           OUT_DIR="$2";       shift 2 ;;
    --build-root)        BUILD_ROOT="$2";      shift 2 ;;
    --skip-build)        SKIP_BUILD=true;      shift ;;
    --skip-logger)       SKIP_LOGGER=true;    shift ;;
    --workload-cmd)      WORKLOAD_CMD="$2";   shift 2 ;;
    --watch-binary)      WATCH_BINARY="$2";   shift 2 ;;
    --log-level)         LOG_LEVEL="$2";      shift 2 ;;
    --frequency)             FREQUENCY="$2";         shift 2 ;;
    --workers)               WORKERS="$2";           shift 2 ;;
    --subscribers)           SUBSCRIBERS="$2";       shift 2 ;;
    --taskset-watch)              TASKSET_WATCH="$2";             shift 2 ;;
    --taskset-subscriber)         TASKSET_SUBSCRIBER="$2";        shift 2 ;;
    --taskset-logger)             TASKSET_LOGGER="$2";            shift 2 ;;
    --event-buffer-size)          EVENT_BUFFER_SIZE="$2";         shift 2 ;;
    --event-buffer-gc-frequency)  EVENT_BUFFER_GC_FREQUENCY="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

mkdir -p "$OUT_DIR"
RUN_DIR="$OUT_DIR/$(date +%Y-%m-%dT%H-%M-%S)"
mkdir -p "$RUN_DIR"
RESULTS_CSV="$RUN_DIR/results.csv"

# ── Build ─────────────────────────────────────────────────────────────────────
BIN_DIR="$OUT_DIR/bin"
mkdir -p "$BIN_DIR"
BIN_DIR="$(cd "$BIN_DIR" && pwd)"

if [[ "$SKIP_BUILD" == false ]]; then
  echo "==> Building binaries from $BUILD_ROOT..."
  (cd "$BUILD_ROOT" && go build -o "$BIN_DIR/beegfs-watch"             ./watch/cmd/beegfs-watch/)
  (cd "$BUILD_ROOT" && go build -o "$BIN_DIR/test-v2-fileeventlogger" ./watch/cmd/test-v2-fileeventlogger/)
  (cd "$BUILD_ROOT" && go build -o "$BIN_DIR/perf-test-subscriber"    ./watch/cmd/perf-test-subscriber/)
  echo "    binaries written to $BIN_DIR"
fi

WATCH_BIN="${WATCH_BINARY:-$BIN_DIR/beegfs-watch}"
LOGGER_BIN="$BIN_DIR/test-v2-fileeventlogger"
PERF_BIN="$BIN_DIR/perf-test-subscriber"

BINARIES_TO_CHECK=("$WATCH_BIN" "$PERF_BIN")
[[ "$SKIP_LOGGER" == false ]] && BINARIES_TO_CHECK+=("$LOGGER_BIN")
for b in "${BINARIES_TO_CHECK[@]}"; do
  [[ -x "$b" ]] || { echo "Binary not found or not executable: $b" >&2; exit 1; }
done

# ── Helpers ───────────────────────────────────────────────────────────────────

if [[ -n "$TASKSET_WATCH$TASKSET_SUBSCRIBER$TASKSET_LOGGER" ]]; then
  command -v taskset >/dev/null 2>&1 || { echo "ERROR: taskset not found in PATH (install util-linux)" >&2; exit 1; }
fi

# maybe_taskset CPUSET CMD [ARGS...] — runs CMD under taskset if CPUSET is non-empty.
maybe_taskset() {
  local cpuset="$1"; shift
  if [[ -n "$cpuset" ]]; then
    exec taskset -c "$cpuset" "$@"
  else
    exec "$@"
  fi
}

WATCH_PID=""
LOGGER_PID=""

cleanup() {
  [[ -n "$LOGGER_PID" ]] && kill "$LOGGER_PID" 2>/dev/null && wait "$LOGGER_PID" 2>/dev/null || true
  [[ -n "$WATCH_PID"  ]] && kill "$WATCH_PID"  2>/dev/null && wait "$WATCH_PID"  2>/dev/null || true
  rm -f "$SOCKET_PATH"
}
trap cleanup EXIT

write_config() {
  local cfg_file="$1"
  local n_subs="$2"
  local base_port="$3"

  # Build management section
  local mgmt_tls_line=""
  if [[ "$MGMT_TLS_DISABLE" == true ]]; then
    mgmt_tls_line='tls-disable = true'
  fi
  local auth_disable="false"
  if [[ ! -f "$MGMT_AUTH_FILE" ]]; then
    echo "    WARNING: auth file $MGMT_AUTH_FILE not found; setting auth-disable = true"
    auth_disable="true"
  fi

  {
    echo '[log]'
    echo "type = 'stderr'"
    echo "level = $LOG_LEVEL"
    echo ''
    echo '[management]'
    echo "address = '$MGMT_ADDRESS'"
    echo "auth-file = '$MGMT_AUTH_FILE'"
    echo "auth-disable = $auth_disable"
    [[ -n "$mgmt_tls_line" ]] && echo "$mgmt_tls_line"
    echo ''
    echo '[handler]'
    echo 'max-reconnect-backoff = 5'
    echo 'max-wait-for-response-after-connect = 0'
    echo 'poll-frequency = 1'
    echo ''
    echo '[[metadata]]'
    echo "event-log-target = '$SOCKET_PATH'"
    echo "event-buffer-size = $EVENT_BUFFER_SIZE"
    echo "event-buffer-gc-frequency = $EVENT_BUFFER_GC_FREQUENCY"
    echo ''
    for ((i = 0; i < n_subs; i++)); do
      local port=$((base_port + i))
      echo '[[subscriber]]'
      echo "id = $((i + 1))"
      echo "name = 'perf-sub-$((i + 1))'"
      echo "type = 'grpc'"
      echo "grpc-address = '127.0.0.1:$port'"
      echo 'grpc-tls-disable = true'
      echo ''
    done
  } > "$cfg_file"
}

start_watch() {
  local cfg_file="$1"
  local log_file="$2"

  rm -f "$SOCKET_PATH"

  maybe_taskset "$TASKSET_WATCH" "$WATCH_BIN" \
    --cfg-file "$cfg_file" \
    --developer.perf-profiling-port "$PPROF_PORT" \
    >> "$log_file" 2>&1 &
  WATCH_PID=$!

  # Wait up to 15 seconds for beegfs-watch to create the socket.
  local deadline=$((SECONDS + 15))
  while [[ $SECONDS -lt $deadline ]]; do
    [[ -S "$SOCKET_PATH" ]] && return 0
    sleep 0.2
  done

  echo "ERROR: beegfs-watch did not create socket at $SOCKET_PATH within 15 seconds" >&2
  echo "  Check $log_file for details" >&2
  return 1
}

stop_watch() {
  if [[ -n "$WATCH_PID" ]]; then
    kill "$WATCH_PID" 2>/dev/null || true
    # Give beegfs-watch up to 5 seconds to exit cleanly; SIGKILL if it hangs.
    local i
    for i in {1..50}; do
      kill -0 "$WATCH_PID" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$WATCH_PID" 2>/dev/null; then
      kill -9 "$WATCH_PID" 2>/dev/null || true
    fi
    wait "$WATCH_PID" 2>/dev/null || true
    WATCH_PID=""
  fi
  rm -f "$SOCKET_PATH"
}

stop_logger() {
  if [[ -n "$LOGGER_PID" ]]; then
    kill "$LOGGER_PID" 2>/dev/null || true
    wait "$LOGGER_PID" 2>/dev/null || true
    LOGGER_PID=""
  fi
}

collect_pprof() {
  local out_file="$1"
  local profile_secs="$2"
  curl -sS --max-time $((profile_secs + 10)) \
    "http://localhost:${PPROF_PORT}/debug/pprof/profile?seconds=${profile_secs}" \
    -o "$out_file" 2>/dev/null && echo "    pprof saved to $out_file" \
    || echo "    WARNING: pprof collection failed (is --developer.perf-profiling-port working?)"
}

# ── CSV header ────────────────────────────────────────────────────────────────
echo "scenario,n_subscribers,event_rate,duration_secs,total_events,dropped_events,reconnects,avg_eps,peak_eps" \
  > "$RESULTS_CSV"

# ── Scenario runner ───────────────────────────────────────────────────────────

run_scenario() {
  local label="$1"      # human-readable name for log output
  local n_subs="$2"     # number of subscriber servers
  local freq="$3"       # test-fileeventlogger --frequency value ("0" = max rate)
  local n_workers="$4"  # number of concurrent logger connections
  local base_port=50051

  local pprof_secs=$(( DURATION / 2 ))
  [[ $pprof_secs -lt 5 ]] && pprof_secs=5

  local cfg_file="$RUN_DIR/cfg_${label}.toml"
  local watch_log="$RUN_DIR/watch_${label}.log"
  local perf_log="$RUN_DIR/perf_${label}.log"
  local pprof_out="$RUN_DIR/scenario_${label}_cpu.pprof"

  echo ""
  echo "==> Scenario: $label  (subscribers=$n_subs, workers=$n_workers, freq=$freq, duration=${DURATION}s)"

  write_config "$cfg_file" "$n_subs" "$base_port"

  # Start perf-test-subscriber first so beegfs-watch finds the ports open on first connect.
  echo "    Starting perf-test-subscriber..."
  maybe_taskset "$TASKSET_SUBSCRIBER" "$PERF_BIN" \
    --num-subscribers "$n_subs" \
    --base-port "$base_port" \
    --duration "${DURATION}s" \
    --ack-frequency "$ACK_FREQUENCY" \
    --grpc-disable-tls \
    --report-format csv \
    > "$perf_log" 2>&1 &
  local perf_pid=$!

  # Wait for all subscriber ports to be open before starting beegfs-watch so it
  # connects on the first attempt with no "connection refused" retries.
  # Uses bash /dev/tcp rather than nc so it works without extra tools.
  local deadline=$((SECONDS + 10))
  local port_ready=true
  for ((i = 0; i < n_subs; i++)); do
    local port=$((base_port + i))
    while [[ $SECONDS -lt $deadline ]]; do
      (: >/dev/tcp/127.0.0.1/"$port") 2>/dev/null && break
      if ! kill -0 "$perf_pid" 2>/dev/null; then
        echo "    ERROR: perf-test-subscriber (pid=$perf_pid) exited before port $port opened." >&2
        echo "    Check $perf_log for details." >&2
        port_ready=false
        break 2
      fi
      sleep 0.1
    done
    if [[ $SECONDS -ge $deadline ]]; then
      echo "    WARNING: timed out waiting for subscriber port $port; beegfs-watch may log connection errors." >&2
    fi
  done
  if [[ "$port_ready" == false ]]; then
    kill "$perf_pid" 2>/dev/null || true
    return 1
  fi

  echo "    Starting beegfs-watch..."
  if ! start_watch "$cfg_file" "$watch_log"; then
    echo "    SKIP: beegfs-watch failed to start. Log: $watch_log"
    return 1
  fi
  echo "    beegfs-watch started (pid=$WATCH_PID, socket=$SOCKET_PATH)"

  if [[ "$SKIP_LOGGER" == true ]]; then
    if [[ -n "$WORKLOAD_CMD" ]]; then
      echo "    Running workload: $WORKLOAD_CMD"
      bash -c "$WORKLOAD_CMD" >> "$RUN_DIR/logger_${label}.log" 2>&1 &
      LOGGER_PID=$!
    else
      echo "    No event logger (waiting for real metadata service events on $SOCKET_PATH)"
      LOGGER_PID=""
    fi
  else
    echo "    Starting test-v2-fileeventlogger (freq=$freq, workers=$n_workers)..."
    maybe_taskset "$TASKSET_LOGGER" "$LOGGER_BIN" \
      --socket "$SOCKET_PATH" --frequency "$freq" --workers "$n_workers" \
      >> "$RUN_DIR/logger_${label}.log" 2>&1 &
    LOGGER_PID=$!
  fi

  # Collect pprof in the background starting after a warm-up period.
  sleep 2
  collect_pprof "$pprof_out" "$pprof_secs" &
  local pprof_bg=$!

  # Wait for perf subscriber to finish its run.
  wait "$perf_pid" || true
  wait "$pprof_bg" 2>/dev/null || true
  stop_logger
  stop_watch

  # Parse CSV output from perf subscriber and append to results.
  # perf_log contains CSV lines: port,total_events,dropped_events,reconnects,avg_eps,peak_eps
  # We extract the aggregate line.
  local agg_line
  agg_line=$(grep "^aggregate," "$perf_log" 2>/dev/null | tail -1) || true

  if [[ -z "$agg_line" ]]; then
    echo "    WARNING: no output from perf-test-subscriber. Check $perf_log"
    echo "$label,$n_subs,$freq,$DURATION,ERROR,ERROR,ERROR,ERROR,ERROR" >> "$RESULTS_CSV"
    return
  fi

  # agg_line: aggregate,total,dropped,reconnects,avg_eps,peak_eps
  local total dropped reconnects avg_eps peak_eps
  IFS=',' read -r _ total dropped reconnects avg_eps peak_eps <<< "$agg_line"
  echo "$label,$n_subs,$freq,$DURATION,$total,$dropped,$reconnects,$avg_eps,$peak_eps" >> "$RESULTS_CSV"

  printf "    Results: total=%-12s  dropped=%-8s  reconnects=%-6s  avg_eps=%-10s  peak_eps=%s\n" \
    "$total" "$dropped" "$reconnects" "$avg_eps" "$peak_eps"
}

# ── Run scenarios ─────────────────────────────────────────────────────────────

echo ""
echo "========================================"
echo " Watch Subscriber Performance Test"
echo " Repo:     $REPO_ROOT"
echo " Run dir:  $RUN_DIR"
echo " Duration: ${DURATION}s per scenario"
echo "========================================"

# Build a filename-safe label from the frequency: "0" -> "maxrate", "100µs" -> "100us", etc.
freq_label="${FREQUENCY//µ/u}"
freq_label="${freq_label//[^a-zA-Z0-9_-]/}"
[[ "$FREQUENCY" == "0" ]] && freq_label="maxrate"

run_scenario "${SUBSCRIBERS}sub_${freq_label}" "$SUBSCRIBERS" "$FREQUENCY" "$WORKERS"

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo "========================================"
echo " Summary (from $RESULTS_CSV)"
echo "========================================"
printf "%-20s  %-6s  %-12s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
  "Scenario" "N" "TotalEvents" "Dropped" "Reconnects" "AvgEPS" "PeakEPS" "Duration"
printf "%-20s  %-6s  %-12s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
  "--------" "-" "-----------" "-------" "----------" "------" "-------" "--------"

tail -n +2 "$RESULTS_CSV" | while IFS=',' read -r scenario nsubs freq dur total dropped reconnects avg_eps peak_eps; do
  printf "%-20s  %-6s  %-12s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
    "$scenario" "$nsubs" "$total" "$dropped" "$reconnects" "$avg_eps" "$peak_eps" "${dur}s"
done

echo ""
echo "Full results: $RESULTS_CSV"
echo "pprof files:  $RUN_DIR/scenario_*_cpu.pprof"
echo "  Inspect with: go tool pprof $RUN_DIR/scenario_4sub_maxrate_cpu.pprof"
echo ""
