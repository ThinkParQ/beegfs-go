#!/usr/bin/env python3
"""Generate Grafana dashboard JSON for beegfs-remote + beegfs-sync (experimental).

Uses typed helper functions so panel/grid errors surface at generate-time.
Validates gridPos non-overlap before emitting.
"""
import json
import sys
from copy import deepcopy

PROM_UID = "cfh9rfys1kd1cd"
LOKI_UID = "bfhc8n0pzh2wwa"
DASH_UID = "beegfs-remote-sync-exp"

PROM_DS = {"type": "prometheus", "uid": PROM_UID}
LOKI_DS = {"type": "loki",       "uid": LOKI_UID}


def prom_target(refId, expr, *, legend="", instant=False, fmt=None):
    t = {
        "refId": refId,
        "expr": expr,
        "legendFormat": legend,
        "editorMode": "code",
        "range": not instant,
        "instant": instant,
        "datasource": PROM_DS,
    }
    if fmt:
        t["format"] = fmt
    return t


def loki_target(refId, expr, *, legend="", instant=False):
    return {
        "refId": refId,
        "expr": expr,
        "legendFormat": legend,
        "editorMode": "code",
        "datasource": LOKI_DS,
        "queryType": "instant" if instant else "range",
        "range": not instant,
        "instant": instant,
        "maxLines": 1000,
        "step": "",
        "resolution": 1,
    }


def base_panel(pid, title, ptype, grid, *, datasource=None, expected="nonempty",
               description=""):
    return {
        "id": pid,
        "type": ptype,
        "title": title,
        "description": description,
        "datasource": datasource or PROM_DS,
        "gridPos": grid,
        "options": {},
        "fieldConfig": {"defaults": {}, "overrides": []},
        "targets": [],
        "pluginVersion": "12.4.2",
        # custom marker for our verification script; Grafana ignores unknown keys
        "x_expected": expected,
    }


def stat(pid, title, grid, target, *, unit=None, decimals=None,
         thresholds=None, sparkline=False, color_mode="value", expected="nonempty",
         text_mode="auto", reduce_fields="", description=""):
    p = base_panel(pid, title, "stat", grid, expected=expected, description=description)
    p["targets"] = [target]
    p["options"] = {
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": reduce_fields, "values": False},
        "orientation": "auto",
        "textMode": text_mode,
        "colorMode": color_mode,
        "graphMode": "area" if sparkline else "none",
        "justifyMode": "auto",
    }
    defaults = {}
    if unit:
        defaults["unit"] = unit
    if decimals is not None:
        defaults["decimals"] = decimals
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
    if defaults:
        p["fieldConfig"]["defaults"].update(defaults)
    return p


def timeseries(pid, title, grid, targets, *, unit=None, stacked=False,
               legend_placement="bottom", expected="nonempty", datasource=None,
               description=""):
    p = base_panel(pid, title, "timeseries", grid, datasource=datasource,
                   expected=expected, description=description)
    p["targets"] = targets
    custom = {
        "drawStyle": "line",
        "lineInterpolation": "linear",
        "fillOpacity": 20 if stacked else 5,
        "showPoints": "never",
    }
    if stacked:
        custom["stacking"] = {"mode": "normal", "group": "A"}
    p["fieldConfig"]["defaults"] = {"custom": custom}
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    p["options"] = {
        "legend": {"displayMode": "list", "placement": legend_placement, "showLegend": True},
        "tooltip": {"mode": "multi", "sort": "desc"},
    }
    return p


def piechart(pid, title, grid, target, *, expected="nonempty", description=""):
    p = base_panel(pid, title, "piechart", grid, expected=expected, description=description)
    p["targets"] = [target]
    p["options"] = {
        "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
        "pieType": "donut",
        "tooltip": {"mode": "single", "sort": "none"},
        "legend": {"showLegend": True, "placement": "right", "displayMode": "table",
                   "values": ["value", "percent"]},
        "displayLabels": ["name", "percent"],
    }
    return p


def heatmap(pid, title, grid, target, *, expected="nonempty", description=""):
    p = base_panel(pid, title, "heatmap", grid, expected=expected, description=description)
    p["targets"] = [target]
    p["options"] = {
        "calculate": False,
        "yAxis": {"axisPlacement": "left", "reverse": False, "unit": "s"},
        "rowsFrame": {"layout": "auto"},
        "color": {"mode": "scheme", "scheme": "Spectral", "steps": 64, "exponent": 0.5,
                  "reverse": False},
        "cellGap": 1,
        "filterValues": {"le": 1e-9},
        "tooltip": {"show": True, "yHistogram": True},
        "legend": {"show": True},
        "exemplars": {"color": "rgba(255,0,255,0.7)"},
    }
    return p


def table(pid, title, grid, targets, *, transformations=None, expected="nonempty",
          description=""):
    p = base_panel(pid, title, "table", grid, expected=expected, description=description)
    p["targets"] = targets
    p["options"] = {"showHeader": True, "cellHeight": "sm",
                    "footer": {"show": False, "reducer": ["sum"], "countRows": False, "fields": ""}}
    p["transformations"] = transformations or []
    return p


def logs_panel(pid, title, grid, target, *, expected="nonempty", description=""):
    p = base_panel(pid, title, "logs", grid, datasource=LOKI_DS, expected=expected,
                   description=description)
    p["targets"] = [target]
    p["options"] = {
        "showTime": True,
        "showLabels": False,
        "showCommonLabels": True,
        "wrapLogMessage": True,
        "prettifyLogMessage": False,
        "enableLogDetails": True,
        "dedupStrategy": "none",
        "sortOrder": "Descending",
    }
    return p


def row(pid, title, y):
    return {"id": pid, "type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


# -------------- Layout ------------------
# Grid units: rows of 24 cols, panel h in std units.
W_STAT = 4  # 5 stats span 5*4 = 20 — leave 4 empty? Pack tight: 5 stats × 4 = 20, ok.
# Actually plan wants 5 stats in row 1; use widths 5,5,5,5,4 to cover 24.
panels = []

# ---- Row 0 — Service status (operational availability) ----
panels.append(row(999, "Service status (availability + version)", 0))

# Per-service online indicator: count of series with the job label.
# OTel SDK on beegfs-remote/sync does NOT export Go runtime metrics (process_resident_memory_bytes,
# go_goroutines etc come from prometheus self-scrape, not beegfs). To surface those we would have
# to wire OTel's go.opentelemetry.io/contrib/instrumentation/runtime into beegfs code. For now
# we show availability + version + freshness derived from the metric stream we DO receive.
panels.append(stat(
    100, "beegfs-remote online", {"x": 0, "y": 1, "w": 6, "h": 4},
    prom_target("A", 'count(remote_job_requests_total{job="beegfs-remote"}) >= bool 1',
                instant=True, legend="online"),
    thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}],
    color_mode="background", text_mode="value",
))
panels.append(stat(
    101, "beegfs-remote — freshness (s since last sample)", {"x": 6, "y": 1, "w": 6, "h": 4},
    prom_target("A",
                'time() - max(timestamp(remote_job_requests_total{job="beegfs-remote"}))',
                instant=True, legend="age"),
    unit="s", decimals=0, text_mode="value",
    thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 60}, {"color": "red", "value": 120}],
))
panels.append(stat(
    102, "beegfs-sync online", {"x": 12, "y": 1, "w": 6, "h": 4},
    prom_target("A", 'count(sync_work_requests_total{job="beegfs-sync"}) >= bool 1',
                instant=True, legend="online"),
    thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}],
    color_mode="background", text_mode="value",
))
panels.append(stat(
    103, "beegfs-sync — freshness (s since last sample)", {"x": 18, "y": 1, "w": 6, "h": 4},
    prom_target("A",
                'time() - max(timestamp(sync_work_requests_total{job="beegfs-sync"}))',
                instant=True, legend="age"),
    unit="s", decimals=0, text_mode="value",
    thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 60}, {"color": "red", "value": 120}],
))

# ---- Row header 1 ----
panels.append(row(1000, "Triage — what's broken now", 0))

# Panel widths sum to 24
panels.append(stat(
    1, "Failed jobs (last 15m)", {"x": 0, "y": 1, "w": 5, "h": 4},
    prom_target("A", 'sum(increase(remote_job_terminal_total{state="failed", rst_id=~"$rst_id"}[15m]))',
                instant=True, legend="failed"),
    thresholds=[{"color": "green", "value": None},
                {"color": "yellow", "value": 0.5},
                {"color": "red", "value": 1}],
    color_mode="background",
    expected="empty_if_no_failures",
))
panels.append(stat(
    2, "Cancelled jobs (last 15m)", {"x": 5, "y": 1, "w": 5, "h": 4},
    prom_target("A", 'sum(increase(remote_job_terminal_total{state="cancelled", rst_id=~"$rst_id"}[15m]))',
                instant=True, legend="cancelled"),
    thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 1}],
    expected="empty_if_no_failures",
))
panels.append(stat(
    3, "Job throughput (1m)", {"x": 10, "y": 1, "w": 5, "h": 4},
    prom_target("A", 'sum(rate(remote_job_terminal_total{rst_id=~"$rst_id"}[1m]))',
                legend="jobs/s"),
    unit="ops", sparkline=True, decimals=2,
))
panels.append(stat(
    4, "Sync completion Hz (now)", {"x": 15, "y": 1, "w": 5, "h": 4},
    prom_target("A", 'max(scheduler_completed_work_rate_per_second{service_instance_id=~"$sync_instance"})',
                instant=True, legend="Hz"),
    unit="Hz", sparkline=True, decimals=2,
))
panels.append(stat(
    5, "Net arrivals (in − out, 5m)", {"x": 20, "y": 1, "w": 4, "h": 4},
    prom_target("A",
                'clamp_min(sum(increase(remote_job_requests_total[5m])) - sum(increase(remote_job_terminal_total[5m])), 0)',
                instant=True, legend="net"),
    decimals=0,
    expected="warmup_required",
))

# ---- Row header 2 ----
panels.append(row(1001, "Per-RST troubleshooting", 5))

panels.append(timeseries(
    6, "Failures over time per RST", {"x": 0, "y": 6, "w": 12, "h": 8},
    [prom_target("A",
                 'sum by (rst_id, state)(rate(remote_job_terminal_total{state=~"failed|cancelled|unknown", rst_id!="0", rst_id=~"$rst_id"}[5m]))',
                 legend="{{rst_id}} / {{state}}")],
    stacked=True, unit="ops", expected="empty_if_no_failures",
))
_t_a = prom_target("A",
                   'sum by (rst_id, state)(rate(remote_job_terminal_total{rst_id!="0"}[5m]))',
                   instant=True, legend="", fmt="table")
_t_b = prom_target("B",
                   'sum by (rst_id, state)(increase(remote_job_terminal_total{rst_id!="0"}[1h]))',
                   instant=True, legend="", fmt="table")
panels.append(table(
    7, "Per-RST status (5m rate + 1h count)", {"x": 12, "y": 6, "w": 12, "h": 8},
    [_t_a, _t_b],
    transformations=[
        # Merge A and B by (rst_id, state); produces one row per (rst_id, state) with
        # both Value #A (rate) and Value #B (count) columns.
        {"id": "merge", "options": {}},
        {"id": "organize", "options": {
            "excludeByName": {"Time": True, "Time 1": True, "Time 2": True},
            "renameByName": {
                "Value #A": "Rate 5m (jobs/s)",
                "Value #B": "Count last 1h",
                "rst_id":   "RST",
                "state":    "State",
            },
        }},
    ],
))

# ---- Row header 3 ----
panels.append(row(1002, "Job pipeline — volume & completed-job latency", 14))

panels.append(timeseries(
    8, "Job request vs completion rate", {"x": 0, "y": 15, "w": 12, "h": 8},
    [
        prom_target("A", 'sum(rate(remote_job_requests_total[5m]))', legend="requests/s"),
        prom_target("B", 'sum(rate(remote_job_terminal_total[5m]))', legend="terminal/s"),
    ],
    unit="ops",
))
panels.append(piechart(
    9, "Job outcomes (range)", {"x": 12, "y": 15, "w": 12, "h": 8},
    prom_target("A",
                'sum by (state)(increase(remote_job_terminal_total{rst_id=~"$rst_id"}[$__range]))',
                instant=True, legend="{{state}}"),
))
panels.append(heatmap(
    10, "Completed-job duration heatmap", {"x": 0, "y": 23, "w": 12, "h": 8},
    prom_target("A",
                'sum by (le)(rate(remote_job_duration_seconds_bucket{rst_id=~"$rst_id"}[1m]))',
                legend="{{le}}", fmt="heatmap"),
))
panels.append(timeseries(
    11, "Completed-job duration p50 / p95 / p99", {"x": 12, "y": 23, "w": 12, "h": 8},
    [
        prom_target("A", 'histogram_quantile(0.50, sum by (le)(rate(remote_job_duration_seconds_bucket{rst_id=~"$rst_id"}[5m])))', legend="p50"),
        prom_target("B", 'histogram_quantile(0.95, sum by (le)(rate(remote_job_duration_seconds_bucket{rst_id=~"$rst_id"}[5m])))', legend="p95"),
        prom_target("C", 'histogram_quantile(0.99, sum by (le)(rate(remote_job_duration_seconds_bucket{rst_id=~"$rst_id"}[5m])))', legend="p99"),
    ],
    unit="s",
))

# ---- Row header 4 ----
panels.append(row(1003, "Sync worker pipeline", 31))

panels.append(timeseries(
    12, "Sync work lifecycle (per state)", {"x": 0, "y": 32, "w": 8, "h": 8},
    [prom_target("A",
                 'sum by (state)(rate(sync_work_requests_total{service_instance_id=~"$sync_instance"}[5m]))',
                 legend="{{state}}")],
    stacked=True, unit="ops",
))
panels.append(timeseries(
    13, "Sync work by priority", {"x": 8, "y": 32, "w": 8, "h": 8},
    [prom_target("A",
                 'sum by (priority)(rate(sync_work_requests_total{service_instance_id=~"$sync_instance"}[5m]))',
                 legend="prio {{priority}}")],
    stacked=True, unit="ops",
))
panels.append(timeseries(
    14, "Sync scheduler Hz (completion + token release)", {"x": 16, "y": 32, "w": 8, "h": 8},
    [
        prom_target("A",
                    'scheduler_completed_work_rate_per_second{service_instance_id=~"$sync_instance"}',
                    legend="completed {{service_instance_id}}"),
        prom_target("B",
                    'scheduler_tokens_allowed_rate_per_second{service_instance_id=~"$sync_instance"}',
                    legend="tokens {{service_instance_id}}"),
    ],
    unit="Hz",
))

# ---- Row header 5 ----
panels.append(row(1004, "Logs (Loki)", 40))

panels.append(timeseries(
    15, "Log volume by service + severity", {"x": 0, "y": 41, "w": 12, "h": 8},
    [loki_target("A",
                 'sum by (service_name, severity_text)(count_over_time({service_name=~"beegfs-remote|beegfs-sync"} | severity_text=~"${log_severity:pipe}" [1m]))',
                 legend="{{service_name}} {{severity_text}}")],
    stacked=True, unit="lps", datasource=LOKI_DS,
))
panels.append(timeseries(
    16, "Error/warn rate per service", {"x": 12, "y": 41, "w": 12, "h": 8},
    [loki_target("A",
                 'sum by (service_name)(rate({service_name=~"beegfs-remote|beegfs-sync"} | severity_text=~"error|warn" [5m]))',
                 legend="{{service_name}}")],
    unit="cps", datasource=LOKI_DS,
))
panels.append(logs_panel(
    17, "Recent log lines (filtered by severity)", {"x": 0, "y": 49, "w": 24, "h": 12},
    loki_target("A",
                '{service_name=~"beegfs-remote|beegfs-sync"} | severity_text=~"${log_severity:pipe}"'),
))

# ---- Row 6 — RST distribution (placed below logs row 5: row5 ends at y=61) ----
panels.append(row(1005, "RST distribution — which RST is in use", 61))

panels.append(piechart(
    18, "Job terminal-share per RST (range)",
    {"x": 0, "y": 62, "w": 12, "h": 8},
    prom_target("A",
                'sum by (rst_id)(increase(remote_job_terminal_total{rst_id!="0"}[$__range]))',
                instant=True, legend="rst {{rst_id}}"),
))

panels.append(timeseries(
    19, "Job throughput per RST (5m rate)",
    {"x": 12, "y": 62, "w": 12, "h": 8},
    [prom_target("A",
                 'sum by (rst_id)(rate(remote_job_terminal_total{rst_id!="0", rst_id=~"$rst_id"}[5m]))',
                 legend="rst {{rst_id}}")],
    unit="ops",
))


# ---- Shift status-row-1-through-N panels down by 5 (row 0 occupies y=0..4) ----
STATUS_ROW_HEIGHT = 5
# Status row + RST distribution row are not shifted (already placed at right y).
SHIFT_IDS = {999, 100, 101, 102, 103}
for p in panels:
    if p["id"] not in SHIFT_IDS:
        p["gridPos"]["y"] += STATUS_ROW_HEIGHT


# ---- Panel descriptions (shown in Grafana via the (i) info icon) ----
DESCRIPTIONS = {
    100: "Green = beegfs-remote is currently exporting metrics to Prometheus (at least one series visible). Red = no series; service is either down, OTLP push is failing, or all metrics filtered out.",
    101: "Seconds since the most recent sample for `remote_job_requests_total`. Should normally be < OTLP interval (30s). Yellow > 60s, Red > 120s — indicates export pipeline stalled or service died.",
    102: "Same as the remote indicator but for beegfs-sync. Uses `sync_work_requests_total` as the heartbeat series.",
    103: "Freshness of beegfs-sync's metric stream. Spikes if sync stalls or OTLP exporter has trouble shipping.",

    1: "Count of jobs that reached the `failed` terminal state in the last 15m, summed across all RSTs selected by $rst_id. Drill into panel 6 + 7 to identify which RST.",
    2: "Same as panel 1 but for the `cancelled` state. Cancellations are user-initiated; if you didn't cancel and this is non-zero, investigate.",
    3: "Aggregate completion rate across all RSTs (1m rate, jobs/sec). Use this to gauge current cluster activity.",
    4: "Current per-second completion Hz of the busiest sync scheduler. Already a rate from the scheduler — no `rate()` needed.",
    5: "Net job arrivals in last 5m: `requests - terminal`. A growing positive value means jobs are coming in faster than they finish — backlog forming. Negative is clamped to 0. NOT a live 'active jobs' count.",

    6: "Per-RST stacked rate of `failed|cancelled|unknown` terminal states (5m window). Lines stack so total height = total bad-outcome rate. Empty band = all RSTs healthy.",
    7: "Table with one row per (RST, state). `Rate 5m` shows current jobs/sec for that outcome; `Count last 1h` is the absolute number in the last hour. Use to compare absolute volume vs current trend.",

    8: "Two lines: arrival rate of new job requests vs rate of jobs reaching a terminal state. When these diverge (requests > terminal), backlog is building.",
    9: "Share of terminal states (completed/failed/cancelled/offloaded/unknown) over the dashboard's current time range. Useful for at-a-glance outcome quality.",
    10: "Heatmap of completed-job duration distribution over time. Y-axis = duration buckets (seconds), color intensity = number of completions falling in that bucket per second. Wide spread = highly variable durations.",
    11: "p50/p95/p99 of completed-job duration. Histogram populated only at terminal — in-flight long jobs are invisible. p99 reads the highest `le` bucket where data lands, so usefulness depends on bucket boundaries.",

    12: "Stacked per-state rate of sync work transitions (new/queued/processed/completed/rescheduled). Each line is a counter rate; height shows current activity in that lifecycle stage.",
    13: "Stacked sync work rate broken down by priority label (1 highest, 5 lowest). Identifies which priority tier is consuming worker time.",
    14: "Two scheduler-internal gauges: jobs completed/sec (work done) and tokens released/sec (concurrency budget). Tokens > completed means workers idle; completed > tokens means tokens are the bottleneck.",

    15: "Log volume from beegfs-remote + beegfs-sync, stacked by severity. Filter via the `Log severity` template var at top. Spikes in warn/error correlate with bad behaviour in row 2/3.",
    16: "Per-service rate of error+warn log lines (5m window). Use to detect when a service starts emitting warnings even before metrics react.",
    17: "Raw log lines from both services, filtered by `Log severity`. Time-aligned with the panels above — click on a spike in 15/16 to jump here.",

    18: "Slice = share of jobs that reached a terminal state per RST in the dashboard's current time range. Tells you which RST is hottest right now.",
    19: "Per-RST throughput over time (5m rate). Use to spot RST-specific slowdowns or to verify load is going where you expect.",
}

# Apply descriptions
for p in panels:
    pid = p.get("id")
    if pid in DESCRIPTIONS:
        p["description"] = DESCRIPTIONS[pid]


# ---- gridPos validation ----
def validate_grid(panels):
    cells = {}
    for p in panels:
        if p.get("type") == "row":
            continue
        g = p["gridPos"]
        x, y, w, h = g["x"], g["y"], g["w"], g["h"]
        if x + w > 24:
            raise SystemExit(f"panel {p['id']} extends past x=24")
        for dy in range(h):
            for dx in range(w):
                key = (x + dx, y + dy)
                if key in cells:
                    raise SystemExit(f"panel {p['id']} overlaps panel {cells[key]} at {key}")
                cells[key] = p["id"]


validate_grid(panels)

# Strip our marker before emitting (kept only for verify script via export below)
expected_map = {p["id"]: p.pop("x_expected") for p in panels if p.get("type") != "row"}

# Templating
templating = {"list": [
    {
        "name": "rst_id", "label": "RST",
        "datasource": PROM_DS,
        "definition": 'label_values(remote_job_terminal_total{rst_id!="0"}, rst_id)',
        "query": {"qryType": 1,
                  "query": 'label_values(remote_job_terminal_total{rst_id!="0"}, rst_id)',
                  "refId": "PrometheusVariableQueryEditor-VariableQuery"},
        "type": "query",
        "multi": True, "includeAll": True, "allValue": ".+",
        "refresh": 2, "sort": 1, "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
    },
    {
        "name": "sync_instance", "label": "Sync instance",
        "datasource": PROM_DS,
        "definition": "label_values(sync_work_requests_total, service_instance_id)",
        "query": {"qryType": 1,
                  "query": "label_values(sync_work_requests_total, service_instance_id)",
                  "refId": "PrometheusVariableQueryEditor-VariableQuery"},
        "type": "query",
        "multi": True, "includeAll": True, "allValue": ".+",
        "refresh": 2, "sort": 1, "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
    },
    {
        "name": "log_severity", "label": "Log severity",
        "type": "custom",
        "query": "error,warn,info,debug,fatal,trace",
        "options": [
            {"text": "error", "value": "error", "selected": True},
            {"text": "warn",  "value": "warn",  "selected": True},
            {"text": "info",  "value": "info",  "selected": False},
            {"text": "debug", "value": "debug", "selected": False},
            {"text": "fatal", "value": "fatal", "selected": False},
            {"text": "trace", "value": "trace", "selected": False},
        ],
        "multi": True, "includeAll": True, "allValue": "error|warn|info|debug|fatal|trace",
        "current": {"selected": True, "text": ["error", "warn"], "value": ["error", "warn"]},
    },
]}

dashboard = {
    "uid": DASH_UID,
    "title": "BeeGFS Remote & Sync (experimental)",
    "tags": ["experimental", "beegfs", "rst"],
    "schemaVersion": 39,
    "timezone": "browser",
    "refresh": "30s",
    "time": {"from": "now-15m", "to": "now"},
    "templating": templating,
    "panels": panels,
    "editable": True,
    "graphTooltip": 1,
    "links": [],
    "annotations": {"list": [
        {"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"},
         "enable": True, "hide": True, "iconColor": "rgba(0, 211, 255, 1)",
         "name": "Annotations & Alerts", "type": "dashboard"},
    ]},
    "version": 1,
    "weekStart": "",
}

import os
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "beegfs-remote-sync-dashboard.json")
EXP_OUT = os.path.join(HERE, "beegfs-panel-expected.json")

with open(OUT, "w") as f:
    json.dump({"dashboard": dashboard, "folderUid": "beegfs-demo",
               "overwrite": True, "message": "experimental dashboard auto-generated"}, f, indent=2)

with open(EXP_OUT, "w") as f:
    # Pair panel id -> (expected, target expr) for verifier
    summary = []
    for p in panels:
        if p.get("type") == "row":
            continue
        targets = []
        for t in p["targets"]:
            targets.append({"refId": t["refId"],
                            "datasource": t["datasource"]["type"],
                            "expr": t["expr"]})
        summary.append({"id": p["id"], "title": p["title"],
                        "expected": expected_map[p["id"]],
                        "targets": targets})
    json.dump(summary, f, indent=2)

print("wrote", OUT)
print("wrote", EXP_OUT)
print("panel count:", sum(1 for p in panels if p.get("type") != "row"))
