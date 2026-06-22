#!/usr/bin/env python3
"""Per-panel verification: run each PromQL/LogQL, classify result."""
import json, os, urllib.request, urllib.parse, sys

PROM = "http://localhost:9090"
LOKI = "http://localhost:3100"
HERE = os.path.dirname(os.path.abspath(__file__))
SUMMARY = json.load(open(os.path.join(HERE, "beegfs-panel-expected.json")))

# Template var substitutions (use 'all' semantics like the dashboard's allValue)
SUBS = {
    "${log_severity:pipe}": "error|warn",  # multi-value pipe formatter
    "$rst_id": ".+",
    "$sync_instance": ".+",
    "$log_severity": "error|warn",
    "$__range": "15m",      # default dashboard range
    "$__rate_interval": "1m",
}


def substitute(expr):
    for k, v in SUBS.items():
        expr = expr.replace(k, v)
    return expr


def prom_query(expr):
    q = urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(f"{PROM}/api/v1/query?{q}") as r:
        return json.load(r)


def loki_query_range(expr):
    import time
    end = int(time.time())
    start = end - 900  # 15m
    q = urllib.parse.urlencode({"query": expr, "start": start * 10**9,
                                "end": end * 10**9, "step": 60})
    with urllib.request.urlopen(f"{LOKI}/loki/api/v1/query_range?{q}") as r:
        return json.load(r)


def loki_query(expr):
    """Instant (for log panels, which we treat as 'logs flowing if any line in last 15m')."""
    import time
    end = int(time.time()) * 10**9
    q = urllib.parse.urlencode({"query": expr, "limit": 5, "time": end})
    try:
        with urllib.request.urlopen(f"{LOKI}/loki/api/v1/query?{q}") as r:
            return json.load(r)
    except Exception as e:
        # Loki rejects log-stream queries via /query for some shapes; fall back
        return loki_query_range(expr)


def count_series(d):
    return len(d.get("data", {}).get("result", []))


rows = []
fail = 0
for panel in SUMMARY:
    pid, title, exp = panel["id"], panel["title"], panel["expected"]
    target_count = 0
    for t in panel["targets"]:
        expr = substitute(t["expr"])
        try:
            if t["datasource"] == "loki":
                # For log streams (no aggregator), use query_range; for metrics use query
                if expr.lstrip().startswith("sum") or "count_over_time" in expr or "rate(" in expr:
                    d = loki_query_range(expr)
                else:
                    d = loki_query(expr)
            else:
                d = prom_query(expr)
        except Exception as e:
            d = {"err": str(e)}
        n = count_series(d) if "data" in d else 0
        target_count = max(target_count, n)
    status = "PASS"
    if exp == "nonempty" and target_count == 0:
        status = "FAIL"; fail += 1
    elif exp == "empty_if_no_failures" and target_count == 0:
        status = "SKIP (no failures generated)"
    elif exp == "warmup_required" and target_count == 0:
        status = "SKIP (warmup)"
    rows.append((pid, title[:50], exp, target_count, status))

print(f"{'id':>3}  {'title':<50}  {'expected':<22}  {'series':>6}  status")
print("-" * 100)
for pid, title, exp, n, s in rows:
    print(f"{pid:>3}  {title:<50}  {exp:<22}  {n:>6}  {s}")
print()
print("FAILURES:", fail)
sys.exit(1 if fail else 0)
