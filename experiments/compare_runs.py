#!/usr/bin/env python3
"""Compare result folders scenario by scenario.

    python experiments/compare_runs.py DIR [DIR ...] [--only a,b]

Each DIR holds <scenario>_runNN.json (and .csv) files written by run_demo.py,
for example by run_impairment_tests.sh or run_robustness_sweep.sh. For every
scenario found, it prints the median across runs of the main metrics, the
lowest per-run final completeness, how many clients went silent (nothing
received in the last 10 s of a run, from the CSV) and, when the scenario's
server log is present, how many clients the server paced as slow readers and
how many of those the load client had not made slow ("paced/wrong").
Public-test folders hold several servers' files with an "ours_" or
"teammate_" prefix; each prefix gets its own row (folder:ours, ...).
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import re
import statistics
from collections import defaultdict

COLUMNS = [  # header, JSON path, format
    ("P50", "p50_ms", "{:.0f}"),
    ("P95", "p95_ms", "{:.0f}"),
    ("P99", "p99_ms", "{:.0f}"),
    ("fresh%", "fresh_pct", "{:.1f}"),
    ("finals%", "fresh_final_pct", "{:.1f}"),
    ("compl%", "final_completeness_pct", "{:.2f}"),
    ("cover%", "normal_update_coverage_pct", "{:.2f}"),
    ("slowP50", "slow_clients.p50_ms", "{:.0f}"),
    ("slowFr%", "slow_clients.fresh_pct", "{:.1f}"),
    ("recP95", "recovery_p95_ms", "{:.1f}"),
]
RUN_FILE = re.compile(r"^(?:(?P<prefix>ours|teammate)_)?(?P<scenario>.+)_run(?P<run>\d\d)\.json$")


def value(summary: dict, path: str):
    for key in path.split("."):
        if not isinstance(summary, dict) or key not in summary:
            return None
        summary = summary[key]
    return summary


def runs_by_scenario(folder: str) -> dict[str, dict[str, list[str]]]:
    """scenario -> row label -> run files (one label per server in the folder)."""
    name = os.path.basename(os.path.normpath(folder))
    found = defaultdict(lambda: defaultdict(list))
    for path in sorted(glob.glob(os.path.join(folder, "*_run[0-9][0-9].json"))):
        match = RUN_FILE.match(os.path.basename(path))
        if match:
            label = f"{name}:{match['prefix']}" if match["prefix"] else name
            found[match["scenario"]][label].append(path)
    return found


def silent_clients(csv_path: str) -> int | None:
    """Clients that received nothing in the last 10 s of the run."""
    if not os.path.exists(csv_path):
        return None
    last = defaultdict(float)
    clients, end = set(), 0.0
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            clients.add(row["client_id"])
            if not row["receive_ts"]:
                continue
            ts = float(row["receive_ts"])
            end = max(end, ts)
            if row["event_type"] in ("CAPTION_UPDATE", "HISTORY"):
                last[row["client_id"]] = max(last[row["client_id"]], ts)
    return sum(1 for c in clients if end - last.get(c, 0.0) > 10)


def slow_ids(csv_path: str) -> set[str]:
    """Clients the load client made slow readers."""
    if not os.path.exists(csv_path):
        return set()
    with open(csv_path, newline="") as f:
        return {row["client_id"] for row in csv.DictReader(f) if row["slow"] == "1"}


def decisions(paths: list[str]) -> str:
    """Clients paced as slow readers in the scenario's server log, and how
    many of them the load client had not made slow (paced/wrong)."""
    log = re.sub(r"_run\d\d\.json$", "", paths[0]) + ".log"
    if not os.path.exists(log):
        return "-"
    text = open(log, encoding="utf-8", errors="replace").read()
    # One server start per run; the runs' logs follow each other in order.
    per_run = text.split("LiveRecall server listening")[1:] or [text]
    paced = wrong = 0
    for log_run, path in zip(per_run, paths):
        ids = set(re.findall(r"client (\S+): slow reader \(stalled", log_run))
        paced += len(ids)
        wrong += len(ids - slow_ids(path[:-5] + ".csv"))
    return f"{paced}/{wrong}"


def row(paths: list[str]) -> list[str]:
    summaries = [json.load(open(p)) for p in paths]
    cells = []
    for _, path, fmt in COLUMNS:
        values = [float(v) for v in (value(s, path) for s in summaries) if v is not None]
        cells.append(fmt.format(statistics.median(values)) if values else "-")
    completeness = [value(s, "final_completeness_pct") for s in summaries]
    cells.append(f"{min(completeness):.2f}" if None not in completeness else "-")
    cells.append(str(sum(value(s, "connection_errors") or 0 for s in summaries)))
    cells.append(str(sum((value(s, "duplicates") or 0) + (value(s, "stale") or 0) for s in summaries)))
    silent = [silent_clients(p[:-5] + ".csv") for p in paths]
    cells.append("/".join("-" if s is None else str(s) for s in silent))
    cells.append(decisions(paths))
    cells.append(str(len(paths)))
    return cells


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("folders", nargs="+")
    p.add_argument("--only", help="comma-separated scenario names")
    args = p.parse_args()
    only = set(args.only.split(",")) if args.only else None

    per_folder = [runs_by_scenario(folder) for folder in args.folders]
    scenarios = sorted({s for runs in per_folder for s in runs})
    labels = [label for runs in per_folder for by_label in runs.values() for label in by_label]
    headers = ["folder"] + [c[0] for c in COLUMNS] + ["minCompl", "errors", "dup+stale",
                                                      "silent", "paced/wrong", "runs"]
    widths = [max(map(len, labels + ["folder"])) + 2] + [max(9, len(h) + 2) for h in headers[1:]]
    for scenario in scenarios:
        if only and scenario not in only:
            continue
        print(f"\n== {scenario}")
        print("".join(h.ljust(widths[0]) if i == 0 else h.rjust(widths[i])
                      for i, h in enumerate(headers)))
        for runs in per_folder:
            for label, paths in runs.get(scenario, {}).items():
                cells = [label] + row(paths)
                print("".join(c.ljust(widths[0]) if i == 0 else c.rjust(widths[i])
                              for i, c in enumerate(cells)))


if __name__ == "__main__":
    main()
