#!/usr/bin/env bash
# scale-perf-test.sh — Scaling harness for Watch subscriber dispatch performance.
#
# Invokes run-perf-test.sh once per combination of scaling parameters, aggregates
# results, and prints an ASCII bar chart of avg_eps.
#
# Usage:
#   ./scale-perf-test.sh [SCALE OPTIONS] [RUN-PERF-TEST OPTIONS]
#
# Scale options (consumed by this script):
#   --subscribers-list "N N N..."      Subscriber counts to test
#   --max-subscribers N                Test 1,2,4,...,N (doubling)
#   --workers-list "N N N..."          Worker counts to test
#   --max-workers N                    Test 1,2,4,...,N workers (doubling)
#   --frequency-list "F F F..."        Injection frequencies (e.g. "0 100µs 1ms 10ms")
#   --buffer-size-list "N N N..."      Ring buffer sizes to test
#   --buffer-gc-frequency-list "N N N..."  GC frequencies to test
#   --auto-refine                      After the coarse pass, find the first-drop cliff and
#                                      insert a dense linear pass between the last clean value
#                                      and the first dropping value. Applied to the first active
#                                      numeric dimension: subscribers → workers → buffer-size →
#                                      buffer-gc-frequency. Frequency lists are not auto-refined.
#   --refine-steps N                   Intermediate values to add in the refinement pass
#                                      (default: 8)
#   --duration SECS                    Duration for both passes (default: run-perf-test.sh default)
#   --coarse-duration SECS             Duration for the initial discovery pass; overrides --duration
#                                      for the coarse pass only. Shorter is fine since the goal is
#                                      only to detect whether drops occur, not to measure precisely.
#   --refine-duration SECS             Duration for the auto-refine pass; overrides --duration
#                                      for the refine pass only.
#   --out-dir DIR                      Root output directory (default: ./scale-results)
#
# When multiple scale lists are provided, all combinations are tested (cartesian product).
# If no scale options are given, defaults to subscriber scaling: 1 2 4 8 16 32 64.
#
# All other options are forwarded to run-perf-test.sh unchanged.
# Do not pass --subscribers, --workers, --frequency, --event-buffer-size,
# --event-buffer-gc-frequency, or --skip-build directly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_SCRIPT="$SCRIPT_DIR/run-perf-test.sh"

# ── Defaults ─────────────────────────────────────────────────────────────────
SCALE_OUT="./scale-results"
SUBSCRIBERS_LIST=""
MAX_SUBSCRIBERS=0
SUBSCRIBERS_STEP=0
WORKERS_LIST=""
MAX_WORKERS=0
WORKERS_STEP=0
FREQUENCY_LIST=""
BUFFER_SIZE_LIST=""
BUFFER_GC_FREQUENCY_LIST=""
AUTO_REFINE=false
REFINE_STEPS=8
RUN_DURATION=""      # from --duration; base default for both passes
COARSE_DURATION=""   # for the discovery pass only
REFINE_DURATION=""   # for the refine pass only
PASSTHROUGH=()

# ── Parse flags ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --subscribers-list)  SUBSCRIBERS_LIST="$2"; shift 2 ;;
    --max-subscribers)   MAX_SUBSCRIBERS="$2";  shift 2 ;;
    --subscribers-step)  SUBSCRIBERS_STEP="$2"; shift 2 ;;
    --workers-list)      WORKERS_LIST="$2";     shift 2 ;;
    --max-workers)       MAX_WORKERS="$2";      shift 2 ;;
    --workers-step)      WORKERS_STEP="$2";     shift 2 ;;
    --frequency-list)    FREQUENCY_LIST="$2";   shift 2 ;;
    --buffer-size-list)         BUFFER_SIZE_LIST="$2";         shift 2 ;;
    --buffer-gc-frequency-list) BUFFER_GC_FREQUENCY_LIST="$2"; shift 2 ;;
    --auto-refine)       AUTO_REFINE=true;       shift ;;
    --refine-steps)      REFINE_STEPS="$2";      shift 2 ;;
    --duration)          RUN_DURATION="$2";      shift 2 ;;
    --coarse-duration)   COARSE_DURATION="$2";   shift 2 ;;
    --refine-duration)   REFINE_DURATION="$2";   shift 2 ;;
    --out-dir)           SCALE_OUT="$2";         shift 2 ;;
    --subscribers|--workers|--frequency|--event-buffer-size|--event-buffer-gc-frequency|--skip-build)
      echo "ERROR: $1 is managed by scale-perf-test.sh." >&2
      echo "       Use --subscribers-list/--max-subscribers, --workers-list/--max-workers," >&2
      echo "       --frequency-list, --buffer-size-list, or --buffer-gc-frequency-list instead." >&2
      exit 1 ;;
    *)
      PASSTHROUGH+=("$1"); shift ;;
  esac
done

# Resolve pass-level duration defaults: explicit pass override → --duration → (empty = run-perf-test default)
[[ -z "$COARSE_DURATION" ]] && COARSE_DURATION="$RUN_DURATION"
[[ -z "$REFINE_DURATION" ]] && REFINE_DURATION="$RUN_DURATION"

# ── Build scaling lists ───────────────────────────────────────────────────────
# Each dimension is an array. An array containing a single empty string means
# "don't pass this flag; let run-perf-test.sh use its own default."

# build_list MAX [STEP]
# STEP=0 (default): doubles from 1 → 1 2 4 8 … MAX
# STEP>0: linear steps of STEP → STEP 2×STEP 3×STEP … MAX
build_list() {
  local max="$1" step="${2:-0}" n
  if [[ $step -gt 0 ]]; then
    n=$step
    while [[ $n -le $max ]]; do echo "$n"; n=$(( n + step )); done
  else
    n=1
    while [[ $n -le $max ]]; do echo "$n"; n=$(( n * 2 )); done
  fi
}

SUBSCRIBERS_ACTIVE=false
WORKERS_ACTIVE=false
FREQUENCY_ACTIVE=false
BUFFER_SIZE_ACTIVE=false
BUFFER_GC_FREQ_ACTIVE=false

mapfile -t COUNTS     < <(echo "")
mapfile -t WLIST      < <(echo "")
mapfile -t FLIST      < <(echo "")
mapfile -t BSIZELIST  < <(echo "")
mapfile -t GCFREQLIST < <(echo "")

if [[ -n "$SUBSCRIBERS_LIST" ]]; then
  read -ra COUNTS <<< "$SUBSCRIBERS_LIST"; SUBSCRIBERS_ACTIVE=true
elif [[ "$MAX_SUBSCRIBERS" -gt 0 ]]; then
  mapfile -t COUNTS < <(build_list "$MAX_SUBSCRIBERS" "$SUBSCRIBERS_STEP"); SUBSCRIBERS_ACTIVE=true
fi

if [[ -n "$WORKERS_LIST" ]]; then
  read -ra WLIST <<< "$WORKERS_LIST"; WORKERS_ACTIVE=true
elif [[ "$MAX_WORKERS" -gt 0 ]]; then
  mapfile -t WLIST < <(build_list "$MAX_WORKERS" "$WORKERS_STEP"); WORKERS_ACTIVE=true
fi

if [[ -n "$FREQUENCY_LIST" ]]; then
  read -ra FLIST <<< "$FREQUENCY_LIST"; FREQUENCY_ACTIVE=true
fi

if [[ -n "$BUFFER_SIZE_LIST" ]]; then
  read -ra BSIZELIST <<< "$BUFFER_SIZE_LIST"; BUFFER_SIZE_ACTIVE=true
fi

if [[ -n "$BUFFER_GC_FREQUENCY_LIST" ]]; then
  read -ra GCFREQLIST <<< "$BUFFER_GC_FREQUENCY_LIST"; BUFFER_GC_FREQ_ACTIVE=true
fi

# Default when nothing was explicitly set: scale subscribers.
if ! $SUBSCRIBERS_ACTIVE && ! $WORKERS_ACTIVE && ! $FREQUENCY_ACTIVE && ! $BUFFER_SIZE_ACTIVE && ! $BUFFER_GC_FREQ_ACTIVE; then
  COUNTS=(1 2 4 8 16 32 64); SUBSCRIBERS_ACTIVE=true
fi

# ── Output setup ─────────────────────────────────────────────────────────────
mkdir -p "$SCALE_OUT"
SCALE_RESULTS="$SCALE_OUT/scale_results.csv"
echo "label,n_subscribers,workers,frequency,buffer_size,buffer_gc_frequency,total_events,dropped_events,reconnects,avg_eps,peak_eps" \
  > "$SCALE_RESULTS"

_dur_display() {
  local d="$1" label="$2"
  if [[ -n "$d" ]]; then echo " $label: ${d}s"
  else                    echo " $label: (run-perf-test default)"
  fi
}

echo ""
echo "========================================"
echo " Watch Subscriber Scaling Test"
$SUBSCRIBERS_ACTIVE    && echo " Subscriber counts: ${COUNTS[*]}$( [[ $SUBSCRIBERS_STEP -gt 0 ]] && echo " (step=$SUBSCRIBERS_STEP)" || echo " (doubling)" )"
$WORKERS_ACTIVE        && echo " Worker counts:     ${WLIST[*]}$( [[ $WORKERS_STEP -gt 0 ]] && echo " (step=$WORKERS_STEP)" || echo " (doubling)" )"
$FREQUENCY_ACTIVE      && echo " Frequencies:       ${FLIST[*]}"
$BUFFER_SIZE_ACTIVE    && echo " Buffer sizes:      ${BSIZELIST[*]}"
$BUFFER_GC_FREQ_ACTIVE && echo " GC frequencies:    ${GCFREQLIST[*]}"
if $AUTO_REFINE; then
  _dur_display "$COARSE_DURATION" "Coarse duration"
  _dur_display "$REFINE_DURATION" "Refine duration"
  echo " Auto-refine:       enabled ($REFINE_STEPS steps)"
else
  _dur_display "$COARSE_DURATION" "Duration"
fi

# ── Core run logic ────────────────────────────────────────────────────────────
FIRST_RUN=true
PHASE_DURATION=""   # set before each run_all_combinations call

run_one_combination() {
  local N="$1" W="$2" F="$3" B="$4" G="$5"
  local label="" run_args=()

  if [[ -n "$N" ]]; then
    label+="${N}sub"
    run_args+=(--subscribers "$N")
  fi
  if [[ -n "$W" ]]; then
    [[ -n "$label" ]] && label+="_"
    label+="${W}work"
    run_args+=(--workers "$W")
  fi
  if [[ -n "$F" ]]; then
    [[ -n "$label" ]] && label+="_"
    local fsafe="${F//µ/u}"; fsafe="${fsafe//[^a-zA-Z0-9_-]/}"
    [[ "$F" == "0" ]] && fsafe="maxrate"
    label+="$fsafe"
    run_args+=(--frequency "$F")
  fi
  if [[ -n "$B" ]]; then
    [[ -n "$label" ]] && label+="_"
    label+="${B}buf"
    run_args+=(--event-buffer-size "$B")
  fi
  if [[ -n "$G" ]]; then
    [[ -n "$label" ]] && label+="_"
    label+="${G}gc"
    run_args+=(--event-buffer-gc-frequency "$G")
  fi
  [[ -z "$label" ]] && label="default"

  # Inject duration for this pass if set.
  [[ -n "$PHASE_DURATION" ]] && run_args+=(--duration "$PHASE_DURATION")

  echo ""
  echo "----------------------------------------"
  echo " Run: $label"
  echo "----------------------------------------"

  local -a pre_dirs
  mapfile -t pre_dirs < <(find "$SCALE_OUT" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | sort)

  local skip_arg=()
  [[ "$FIRST_RUN" == false ]] && skip_arg=(--skip-build)

  bash "$RUN_SCRIPT" \
    --out-dir "$SCALE_OUT" \
    "${run_args[@]}" \
    "${skip_arg[@]}" \
    "${PASSTHROUGH[@]}"

  FIRST_RUN=false

  local new_dir="" found d pre
  while IFS= read -r d; do
    [[ "$d" == "$SCALE_OUT/bin" ]] && continue
    found=false
    for pre in "${pre_dirs[@]}"; do
      [[ "$d" == "$pre" ]] && found=true && break
    done
    [[ "$found" == false ]] && new_dir="$d" && break
  done < <(find "$SCALE_OUT" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | sort)

  if [[ -z "$new_dir" ]]; then
    echo "WARNING: could not find new run directory for $label" >&2
    echo "$label,${N:--},${W:--},${F:--},${B:--},${G:--},ERROR,ERROR,ERROR,ERROR,ERROR" >> "$SCALE_RESULTS"
    return
  fi

  local results_csv="$new_dir/results.csv"
  if [[ ! -f "$results_csv" ]]; then
    echo "WARNING: results.csv not found in $new_dir" >&2
    echo "$label,${N:--},${W:--},${F:--},${B:--},${G:--},ERROR,ERROR,ERROR,ERROR,ERROR" >> "$SCALE_RESULTS"
    return
  fi

  local agg_line
  agg_line=$(tail -n +2 "$results_csv" | head -1)
  if [[ -z "$agg_line" ]]; then
    echo "WARNING: no data rows in $results_csv" >&2
    echo "$label,${N:--},${W:--},${F:--},${B:--},${G:--},ERROR,ERROR,ERROR,ERROR,ERROR" >> "$SCALE_RESULTS"
    return
  fi

  local _scenario _nsubs _rate _dur total dropped reconnects avg_eps peak_eps
  IFS=',' read -r _scenario _nsubs _rate _dur total dropped reconnects avg_eps peak_eps <<< "$agg_line"
  echo "$label,${N:--},${W:--},${F:--},${B:--},${G:--},$total,$dropped,$reconnects,$avg_eps,$peak_eps" \
    >> "$SCALE_RESULTS"
}

run_all_combinations() {
  for N in "${COUNTS[@]}"; do
    for W in "${WLIST[@]}"; do
      for F in "${FLIST[@]}"; do
        for B in "${BSIZELIST[@]}"; do
          for G in "${GCFREQLIST[@]}"; do
            run_one_combination "$N" "$W" "$F" "$B" "$G"
          done
        done
      done
    done
  done
}

# ── Main (coarse) pass ────────────────────────────────────────────────────────
total_runs=$(( ${#COUNTS[@]} * ${#WLIST[@]} * ${#FLIST[@]} * ${#BSIZELIST[@]} * ${#GCFREQLIST[@]} ))
echo " Total runs: $total_runs"
echo "========================================"

PHASE_DURATION="$COARSE_DURATION"
run_all_combinations

# ── Auto-refine pass ──────────────────────────────────────────────────────────
if [[ "$AUTO_REFINE" == true ]]; then
  echo ""
  echo "========================================"
  echo " Auto-Refine: locating cliff..."
  echo "========================================"

  REFINE_DIM=""
  REFINE_COL=0

  # Priority: subscribers → workers → buffer_size → buffer_gc_frequency
  # (frequency is string-based and cannot be auto-refined)
  if   $SUBSCRIBERS_ACTIVE;     then REFINE_DIM="subscribers";         REFINE_COL=2
  elif $WORKERS_ACTIVE;         then REFINE_DIM="workers";             REFINE_COL=3
  elif $BUFFER_SIZE_ACTIVE;     then REFINE_DIM="buffer_size";         REFINE_COL=5
  elif $BUFFER_GC_FREQ_ACTIVE;  then REFINE_DIM="buffer_gc_frequency"; REFINE_COL=6
  fi

  if [[ -z "$REFINE_DIM" ]]; then
    echo "auto-refine: no numeric scaling dimension active (frequency-only runs cannot be auto-refined)"
  else
    # Find the transition zone: the interval [A, B] where one side has drops and the other is clean.
    # Handles both directions: high-is-worse (subscribers) and low-is-worse (buffer-size).
    cliff=$(awk -F',' -v col="$REFINE_COL" '
      NR == 1             { next }
      $8 == "ERROR"       { next }
      $col == "--"        { next }
      {
        val = $col + 0
        if ($8 + 0 > 0) drop[val]  = 1
        else            clean[val] = 1
      }
      END {
        if (length(drop)  == 0) { print "none_no_drops";  exit }
        if (length(clean) == 0) { print "none_all_drops"; exit }
        min_drop = ""; max_drop = ""; min_clean = ""; max_clean = ""
        for (v in drop)  {
          if (min_drop  == "" || v+0 < min_drop+0)  min_drop  = v
          if (max_drop  == "" || v+0 > max_drop+0)  max_drop  = v
        }
        for (v in clean) {
          if (min_clean == "" || v+0 < min_clean+0) min_clean = v
          if (max_clean == "" || v+0 > max_clean+0) max_clean = v
        }
        if (max_clean+0 < min_drop+0) {
          # clean values are lower (subscribers: more = worse)
          print max_clean " " min_drop
        } else if (max_drop+0 < min_clean+0) {
          # drop values are lower (buffer-size: smaller = worse)
          print max_drop " " min_clean
        } else {
          print "none_interleaved"
        }
      }
    ' "$SCALE_RESULTS")

    case "$cliff" in
      none_no_drops)
        echo "auto-refine: no drops found — all runs clean, no cliff to refine"
        ;;
      none_all_drops)
        echo "auto-refine: all runs dropped events — no clean baseline found, cannot refine"
        ;;
      none_interleaved)
        echo "auto-refine: drops and clean results are interleaved (non-monotonic) — cannot refine"
        ;;
      *)
        read -r cliff_low cliff_high <<< "$cliff"
        range=$(( cliff_high - cliff_low ))
        step=$(( range / (REFINE_STEPS + 1) ))

        if [[ $step -eq 0 ]]; then
          echo "auto-refine: cliff range [$cliff_low, $cliff_high] too narrow for $REFINE_STEPS steps — skipping"
        else
          echo "auto-refine: $REFINE_DIM cliff in [$cliff_low, $cliff_high], inserting $REFINE_STEPS values (step=$step)"
          [[ -n "$REFINE_DURATION" ]] && echo "auto-refine: using duration=${REFINE_DURATION}s for refine pass"

          REFINE_VALS=()
          for i in $(seq 1 "$REFINE_STEPS"); do
            REFINE_VALS+=("$(( cliff_low + i * step ))")
          done

          # Replace the primary dimension with the refinement values only.
          # All other dimension arrays remain unchanged, so the full cartesian
          # product is run with only the new values for the primary dimension.
          case "$REFINE_DIM" in
            subscribers)          COUNTS=("${REFINE_VALS[@]}") ;;
            workers)              WLIST=("${REFINE_VALS[@]}") ;;
            buffer_size)          BSIZELIST=("${REFINE_VALS[@]}") ;;
            buffer_gc_frequency)  GCFREQLIST=("${REFINE_VALS[@]}") ;;
          esac

          total_refine=$(( ${#COUNTS[@]} * ${#WLIST[@]} * ${#FLIST[@]} * ${#BSIZELIST[@]} * ${#GCFREQLIST[@]} ))
          echo "auto-refine: ${#REFINE_VALS[@]} new $REFINE_DIM values → $total_refine additional runs"
          echo ""

          PHASE_DURATION="$REFINE_DURATION"
          run_all_combinations
        fi
        ;;
    esac
  fi
fi

# ── Summary table with ASCII bar chart ───────────────────────────────────────
echo ""
echo "========================================"
echo " Scaling Results"
echo " CSV: $SCALE_RESULTS"
echo "========================================"

max_eps=0
while IFS=',' read -r _l _n _w _f _b _g _tot _drop _reco avg_eps _peak; do
  [[ "$avg_eps" == "ERROR" ]] && continue
  if awk -v a="$avg_eps" -v m="$max_eps" 'BEGIN{exit (a+0 > m+0) ? 0 : 1}'; then
    max_eps="$avg_eps"
  fi
done < <(tail -n +2 "$SCALE_RESULTS")

# Compute label column width dynamically (min 12, max 32).
label_width=12
while IFS=',' read -r label _rest; do
  len="${#label}"
  [[ $len -gt $label_width ]] && label_width=$len
done < <(tail -n +2 "$SCALE_RESULTS")
[[ $label_width -gt 32 ]] && label_width=32

BAR_WIDTH=30
printf "%-${label_width}s  %-10s  %-10s  %s\n" "LABEL" "AVG_EPS" "DROPPED" "BAR"
printf "%-${label_width}s  %-10s  %-10s  %s\n" "-----" "-------" "-------" "---"

while IFS=',' read -r label _n _w _f _b _g _tot dropped _reconnects avg_eps _peak; do
  if [[ "$avg_eps" == "ERROR" ]]; then
    printf "%-${label_width}s  %-10s  %-10s  %s\n" "${label:0:$label_width}" "ERROR" "-" "[run failed]"
    continue
  fi
  bar_len=$(awk -v eps="$avg_eps" -v max_e="$max_eps" -v w="$BAR_WIDTH" \
    'BEGIN { printf "%d", (max_e+0 > 0) ? int(eps/max_e * w + 0.5) : 0 }')
  bar=$(printf '%*s' "$bar_len" '' | tr ' ' '#')
  flag=""
  [[ "$dropped" -gt 0 ]] && flag=" [drops]"
  printf "%-${label_width}s  %-10s  %-10s  %s%s\n" \
    "${label:0:$label_width}" "$avg_eps" "$dropped" "$bar" "$flag"
done < <(tail -n +2 "$SCALE_RESULTS")

echo ""
echo "Full results: $SCALE_RESULTS"
echo "Individual run dirs: $SCALE_OUT/"

# ── Generate chart (requires matplotlib) ─────────────────────────────────────
CHART_PY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/chart.py"
if [[ -f "$CHART_PY" ]] && command -v python3 >/dev/null 2>&1; then
  python3 "$CHART_PY" "$SCALE_RESULTS" || true
else
  echo "chart.py: skipping chart generation (python3 or chart.py not found)"
fi
echo ""
