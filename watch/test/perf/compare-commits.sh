#!/usr/bin/env bash
# compare-commits.sh — Build and benchmark two git commits with the full suite,
# then print a side-by-side performance comparison.
#
# Usage:
#   ./compare-commits.sh REF_A REF_B [OPTIONS]
#
# Arguments:
#   REF_A           First git ref (baseline)
#   REF_B           Second git ref (compared against the baseline)
#
# Options:
#   --threshold N   Minimum avg_eps regression (%) to flag as a regression (default: 5)
#   --out-dir DIR   Root output directory (default: ./compare-results)
#   --suites "..."  Suites to run, forwarded to suite.sh (default: suite.sh default)
#   All other options are forwarded to suite.sh unchanged
#   (--duration, --coarse-duration, --taskset-watch, etc.)
#
# Output layout:
#   <out-dir>/
#     a/<timestamp>/{fanout,load,buffer}/scale_results.csv
#     b/<timestamp>/{fanout,load,buffer}/scale_results.csv
#
# The two commits are run sequentially so system load conditions are comparable.
# Worktrees are cleaned up automatically on exit.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
PERF_RELPATH="watch/test/perf"

# ── Parse args ────────────────────────────────────────────────────────────────
if [[ $# -lt 2 ]]; then
  echo "Usage: $0 REF_A REF_B [OPTIONS]" >&2
  exit 1
fi

REF_A="$1"; shift
REF_B="$1"; shift

OUT_DIR="./compare-results"
THRESHOLD=5
PASSTHROUGH=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threshold) THRESHOLD="$2"; shift 2 ;;
    --out-dir)   OUT_DIR="$2";   shift 2 ;;
    *)           PASSTHROUGH+=("$1"); shift ;;
  esac
done

# Resolve refs to short SHAs for display and directory naming.
SHA_A="$(git -C "$REPO_ROOT" rev-parse --short "$REF_A")"
SHA_B="$(git -C "$REPO_ROOT" rev-parse --short "$REF_B")"

OUT_A="$OUT_DIR/a"
OUT_B="$OUT_DIR/b"
mkdir -p "$OUT_A" "$OUT_B"

# ── Worktree setup ────────────────────────────────────────────────────────────
WT_A="$(mktemp -d /tmp/beegfs-cmp-a.XXXXXX)"
WT_B="$(mktemp -d /tmp/beegfs-cmp-b.XXXXXX)"

cleanup() {
  git -C "$REPO_ROOT" worktree remove --force "$WT_A" 2>/dev/null || rm -rf "$WT_A"
  git -C "$REPO_ROOT" worktree remove --force "$WT_B" 2>/dev/null || rm -rf "$WT_B"
}
trap cleanup EXIT

git -C "$REPO_ROOT" worktree add --detach "$WT_A" "$REF_A"
git -C "$REPO_ROOT" worktree add --detach "$WT_B" "$REF_B"

# ── Run suites ────────────────────────────────────────────────────────────────
# Always run the current suite scripts; --build-root tells run-perf-test.sh which
# source tree to compile from. The worktrees only exist to provide that source.
run_suite() {
  local worktree="$1" out_base="$2" ref="$3" sha="$4"

  echo ""
  echo "========================================"
  echo " Running suite for $ref ($sha)"
  echo " Building from: $worktree"
  echo "========================================"

  bash "$SCRIPT_DIR/suite.sh" \
    --out-dir "$out_base" \
    --build-root "$worktree" \
    "${PASSTHROUGH[@]}"
}

run_suite "$WT_A" "$OUT_A" "$REF_A" "$SHA_A"
run_suite "$WT_B" "$OUT_B" "$REF_B" "$SHA_B"

# ── Locate timestamped run directories ───────────────────────────────────────
find_latest_run() {
  # suite.sh creates a timestamped subdirectory; return the most recent one.
  ls -td "$1"/*/ 2>/dev/null | head -1 | sed 's|/$||'
}

RUN_A="$(find_latest_run "$OUT_A")"
RUN_B="$(find_latest_run "$OUT_B")"

if [[ -z "$RUN_A" || -z "$RUN_B" ]]; then
  echo "ERROR: could not find suite run directories under $OUT_A or $OUT_B" >&2
  exit 1
fi

# ── Comparison ────────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo " Comparison"
echo "   A: $REF_A ($SHA_A) → $RUN_A"
echo "   B: $REF_B ($SHA_B) → $RUN_B"
echo "   Regression threshold: ${THRESHOLD}%"
echo "========================================"

any_regression=false

compare_suite() {
  local suite="$1" csv_a="$2" csv_b="$3"

  echo ""
  echo "--- Suite: $suite ---"

  awk -F',' \
    -v threshold="$THRESHOLD" \
    -v sha_a="$SHA_A" \
    -v sha_b="$SHA_B" \
    -v suite="$suite" \
  '
  function abs(x) { return (x < 0) ? -x : x }

  NR == FNR {
    if (FNR == 1) next
    lbl = $1
    a_eps[lbl]   = $10 + 0
    a_drops[lbl] = $8  + 0
    a_seq[++na]  = lbl
    next
  }
  FNR == 1 { next }
  {
    lbl = $1
    b_eps[lbl]   = $10 + 0
    b_drops[lbl] = $8  + 0
    b_seen[lbl]  = 1
  }

  END {
    # Compute max label width.
    lw = 12
    for (i = 1; i <= na; i++) {
      n = length(a_seq[i]); if (n > lw) lw = n
    }
    if (lw > 36) lw = 36

    fmt = "%-" lw "s  %12s  %12s  %9s  %s\n"
    printf fmt, "LABEL", sha_a " EPS", sha_b " EPS", "CHANGE", "DROPS (A→B)"
    printf fmt, "-----", "---------", "---------", "------", "-----------"

    regressions = 0
    improvements = 0

    for (i = 1; i <= na; i++) {
      lbl = a_seq[i]
      ae  = a_eps[lbl];   be  = b_eps[lbl]
      ad  = a_drops[lbl]; bd  = b_drops[lbl]

      # Compute percentage change in avg_eps.
      if (ae > 0)       pct = (be - ae) / ae * 100.0
      else if (be > 0)  pct = 100.0
      else              pct = 0.0

      # Format pct string.
      sign = (pct >= 0) ? "+" : ""
      pct_str = sprintf("%s%.1f%%", sign, pct)

      # Drop status.
      if      (ad == 0 && bd == 0)  drops_str = "-"
      else if (ad == 0 && bd > 0)   drops_str = "NEW DROPS"
      else if (ad > 0  && bd == 0)  drops_str = "fixed"
      else                           drops_str = sprintf("%d→%d", ad, bd)

      # Flags.
      flag = ""
      if (pct < -threshold || (ad == 0 && bd > 0)) {
        flag = " REGRESSION"
        regressions++
      } else if (pct > threshold || (ad > 0 && bd == 0)) {
        flag = " improvement"
        improvements++
      }

      # Mark B-only labels that A did not have (e.g. different refine range).
      if (!(lbl in b_seen)) {
        be = "missing"; pct_str = "n/a"; drops_str = "n/a"
        flag = " (not in B)"
      }

      printf fmt, substr(lbl, 1, lw), ae, be, pct_str, drops_str flag
    }

    # Print B-only labels (appeared in B but not in A, e.g. different refine range).
    for (lbl in b_seen) {
      found = 0
      for (i = 1; i <= na; i++) { if (a_seq[i] == lbl) { found = 1; break } }
      if (!found) {
        printf fmt, substr(lbl, 1, lw), "missing", b_eps[lbl], "n/a", \
          (b_drops[lbl] > 0 ? sprintf("%d drops", b_drops[lbl]) : "-") " (not in A)"
      }
    }

    print ""
    if (regressions > 0)
      printf "  REGRESSIONS:  %d label(s) dropped >%d%% or gained new drops\n", regressions, threshold
    if (improvements > 0)
      printf "  Improvements: %d label(s) improved >%d%% or lost drops\n", improvements, threshold
    if (regressions == 0 && improvements == 0)
      print  "  No significant changes (threshold=" threshold "%)"
  }
  ' "$csv_a" "$csv_b"
}

found_any=false

while IFS= read -r suite_dir; do
  suite="$(basename "$suite_dir")"
  csv_a="$suite_dir/scale_results.csv"
  csv_b="$RUN_B/$suite/scale_results.csv"

  if [[ ! -f "$csv_a" ]]; then
    echo "  suite $suite: no results in A ($csv_a) — skipping" >&2
    continue
  fi
  if [[ ! -f "$csv_b" ]]; then
    echo "  suite $suite: no results in B ($csv_b) — skipping" >&2
    continue
  fi

  found_any=true
  compare_suite "$suite" "$csv_a" "$csv_b"
done < <(find "$RUN_A" -maxdepth 1 -mindepth 1 -type d | sort)

if [[ "$found_any" == false ]]; then
  echo "No suite results found to compare." >&2
  exit 1
fi

echo ""
echo "========================================"
echo " Full results:"
echo "   A ($SHA_A): $RUN_A"
echo "   B ($SHA_B): $RUN_B"
echo "========================================"
echo ""
