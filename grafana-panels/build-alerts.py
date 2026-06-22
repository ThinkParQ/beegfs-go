#!/usr/bin/env python3
"""Generate Grafana alert rule payloads (one JSON per UID) for the
beegfs-remote-sync demo dashboard.

Each rule has two stages:
  A — Prometheus instant query
  B — __expr__ math threshold ($A > N)
Condition: B. NoData=OK.

Output: /tmp/beegfs-alerts.json (a list, one rule per object).
"""
import json
from copy import deepcopy

PROM_UID = "cfh9rfys1kd1cd"

def query_stage(refId, expr, *, instant=True, from_seconds=600):
    return {
        "refId": refId,
        "queryType": "",
        "relativeTimeRange": {"from": from_seconds, "to": 0},
        "datasourceUid": PROM_UID,
        "model": {
            "editorMode": "code",
            "expr": expr,
            "instant": instant,
            "range": not instant,
            "intervalMs": 60000,
            "maxDataPoints": 43200,
            "refId": refId,
        },
    }


def math_threshold(refId, expression):
    """Math node: evaluates a Grafana math expression like '$A > 3'."""
    return {
        "refId": refId,
        "queryType": "",
        "relativeTimeRange": {"from": 600, "to": 0},
        "datasourceUid": "__expr__",
        "model": {
            "datasource": {"type": "__expr__", "uid": "__expr__"},
            "expression": expression,
            "intervalMs": 1000,
            "maxDataPoints": 43200,
            "refId": refId,
            "type": "math",
        },
    }


def rule(uid, title, *, group, expr, threshold, for_dur, summary,
         severity="warning"):
    return {
        "uid": uid,
        "title": title,
        "ruleGroup": group,
        "condition": "B",
        "data": [
            query_stage("A", expr, instant=True),
            math_threshold("B", threshold),
        ],
        "for": for_dur,
        "noDataState": "OK",
        "execErrState": "Error",
        "annotations": {"summary": summary, "scope": "beegfs-remote-sync experimental"},
        "labels": {"severity": severity, "service": "beegfs-rst"},
        "isPaused": False,
    }


RULES = [
    rule(
        uid="beegfs-job-failed",
        title="BeeGFS — failed jobs detected",
        group="beegfs_remote_sync_demo",
        expr='sum by (rst_id)(increase(remote_job_terminal_total{state="failed", rst_id!="0"}[5m]))',
        threshold='$A > 3',
        for_dur="5m",
        summary="Failed jobs > 3 in 5m for some rst_id",
        severity="critical",
    ),
    rule(
        uid="beegfs-job-backlog",
        title="BeeGFS — job arrivals outpacing completions",
        group="beegfs_remote_sync_demo",
        expr='sum(increase(remote_job_requests_total[15m])) - sum(increase(remote_job_terminal_total[15m]))',
        threshold='$A > 2000',
        for_dur="15m",
        summary="Net job arrivals exceed completions by >2000 in 15m",
        severity="warning",
    ),
    rule(
        uid="beegfs-sync-stuck",
        title="BeeGFS — sync completion stalled with queued work",
        group="beegfs_remote_sync_demo",
        expr=('(sum by (service_instance_id)(sum_over_time(scheduler_completed_work_rate_per_second[15m])) == bool 0)'
              ' * on(service_instance_id) group_left() '
              '(sum by (service_instance_id)(increase(sync_work_requests_total{state="queued"}[15m])) > bool 0)'),
        threshold='$A > 0',
        for_dur="15m",
        summary="Sync worker not completing work but has queued work",
        severity="critical",
    ),
    rule(
        uid="beegfs-remote-down",
        title="BeeGFS — beegfs-remote not reporting",
        group="beegfs_remote_sync_demo",
        expr='absent_over_time(remote_job_requests_total{job="beegfs-remote"}[10m])',
        threshold='$A > 0',
        for_dur="10m",
        summary="No remote_job_requests_total samples from beegfs-remote in 10m",
        severity="critical",
    ),
    rule(
        uid="beegfs-sync-down",
        title="BeeGFS — beegfs-sync not reporting",
        group="beegfs_remote_sync_demo",
        expr='absent_over_time(sync_work_requests_total{job="beegfs-sync"}[10m])',
        threshold='$A > 0',
        for_dur="10m",
        summary="No sync_work_requests_total samples from beegfs-sync in 10m",
        severity="critical",
    ),
]

import os
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "beegfs-alerts.json")
with open(OUT, "w") as f:
    json.dump(RULES, f, indent=2)
print("wrote", OUT, "rules", len(RULES))
