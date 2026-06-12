#!/usr/bin/env python3
"""
chart.py — Generate performance charts from scale_results.csv files.

Produces a bar chart of avg_eps per run label, with peak_eps overlaid as a
line and bars for runs with dropped events highlighted in red.

Usage:
  python3 chart.py RESULTS_CSV [RESULTS_CSV ...]

  Each CSV gets its own <name>.png written alongside it.

Requires: matplotlib  (pip install matplotlib)
"""

import argparse
import csv
import sys
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("csvfiles", nargs="+", type=Path, metavar="CSV")
    p.add_argument("--title", help="Chart title (default: derived from CSV parent directory name)")
    p.add_argument("--width", type=float, default=0,
                   help="Figure width in inches (default: auto-sized to label count)")
    return p.parse_args()


def load_csv(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def safe_float(s, default=0.0) -> float:
    try:
        return float(s)
    except (ValueError, TypeError):
        return default


def make_chart(rows: list[dict], title: str, out_path: Path, width: float = 0) -> bool:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
    except ImportError:
        print(
            "chart.py: matplotlib not available — skipping chart generation.\n"
            "  Install with: pip install matplotlib",
            file=sys.stderr,
        )
        return False

    labels   = [r["label"] for r in rows]
    avg_eps  = [safe_float(r.get("avg_eps"))   for r in rows]
    peak_eps = [safe_float(r.get("peak_eps"))  for r in rows]
    dropped  = [safe_float(r.get("dropped_events")) > 0 for r in rows]

    n = len(labels)
    fig_w = width if width > 0 else max(8.0, n * 1.1 + 2.0)
    fig, ax = plt.subplots(figsize=(fig_w, 5))

    x = list(range(n))
    bar_colors = ["#d62728" if d else "#1f77b4" for d in dropped]

    ax.bar(x, avg_eps, color=bar_colors, alpha=0.85, zorder=2)
    ax.plot(x, peak_eps, "o--", color="#ff7f0e", markersize=5, linewidth=1.2, zorder=3)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=8)
    ax.set_ylabel("Events per second")
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(
        lambda v, _: f"{v/1e6:.2f}M" if v >= 1e6 else
                     f"{v/1e3:.0f}k"  if v >= 1e3 else str(int(v))
    ))
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", alpha=0.3, zorder=0)
    ax.spines[["top", "right"]].set_visible(False)

    ax.legend(handles=[
        mpatches.Patch(color="#1f77b4", alpha=0.85, label="avg_eps"),
        mpatches.Patch(color="#d62728", alpha=0.85, label="avg_eps (dropped events)"),
        plt.Line2D([0], [0], color="#ff7f0e", marker="o", linestyle="--",
                   markersize=5, linewidth=1.2, label="peak_eps"),
    ], fontsize=8, loc="best")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"chart.py: saved {out_path}")
    return True


def main():
    args = parse_args()
    ok = True
    for csv_path in args.csvfiles:
        if not csv_path.exists():
            print(f"chart.py: ERROR: not found: {csv_path}", file=sys.stderr)
            ok = False
            continue
        rows = load_csv(csv_path)
        if not rows:
            print(f"chart.py: WARNING: no data in {csv_path}", file=sys.stderr)
            continue
        title = args.title or f"{csv_path.parent.name} — scale test results"
        make_chart(rows, title, csv_path.with_suffix(".png"), args.width) or (ok := False)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
